-- cigration is the compound word of citus and migration

CREATE SCHEMA cigration;

DROP TYPE IF EXISTS cigration.old_shard_placement_drop_method CASCADE;
CREATE TYPE cigration.old_shard_placement_drop_method AS ENUM (
   'none', -- do not drop or rename old shards, only record it into cigration.citus_move_shard_placement_remained_old_shard
   'rename', -- move old shards to schema "citus_move_shard_placement_recyclebin"
   'drop' -- drop old shards in source node
);

DROP TYPE IF EXISTS cigration.cigration_shard_transfer_mode CASCADE;
CREATE TYPE cigration.cigration_shard_transfer_mode AS ENUM (
   'force_logical', -- Use logical replication even if the table doesn't have a replica identity. Any concurrent update/delete statements to the table will fail during replication.
   'block_writes'   -- Use COPY (blocking writes) for tables lacking primary key or replica identity.
   -- ,auto: Require replica identity if logical replication is possible, otherwise use legacy behaviour (e.g. for shard repair, PostgreSQL 9.6). This is the default value.
);

DROP TABLE IF EXISTS cigration.citus_move_shard_placement_remained_old_shard CASCADE;
CREATE TABLE cigration.citus_move_shard_placement_remained_old_shard(
    id serial primary key,
    optime timestamptz NOT NULL default now(),
    nodename text NOT NULL,
    nodeport text NOT NULL,
    tablename text NOT NULL,
    drop_method cigration.old_shard_placement_drop_method NOT NULL
);

--update pg_dist_placement on workers which have metadata when executing function cigration.citus_move_shard_placement
--insert the 2pc transaction information into citus_move_shard_placement_transaction when update is failed
--and then manually call function pg_catalog.citus_move_shard_placement_transaction_cleanup() to complete the update operation.
DROP TABLE IF EXISTS cigration.citus_move_shard_placement_transaction CASCADE;
CREATE TABLE cigration.citus_move_shard_placement_transaction(
    id serial primary key,
    optime timestamptz NOT NULL default now(),
    nodename text NOT NULL,
    nodeport integer NOT NULL,
    gid text NOT NULL
);

--use method citus_move_shard_placement to move shard to worker
--when failed then insert the failed record into this table
--and then manually call function cigration.cigration_create_distributed_table_move_shard_cleanup() to complete move operation.
DROP TABLE IF EXISTS cigration.create_distributed_table_move_shard_to_worker CASCADE;
CREATE TABLE cigration.create_distributed_table_move_shard_to_worker(
    id serial primary key,
    optime timestamptz NOT NULL default now(),
    shard_id bigint NOT NULL,
    sourcenodename text NOT NULL,
    sourcenodeport integer NOT NULL,
    targetnodename text NOT NULL,
    targetnodeport integer NOT NULL
);

-- 
-- Custom Types
-- ENUM TYPE
-- init:迁移task初始化完成
-- running:迁移task正在运行
-- completed:迁移task已完成
-- error:迁移task出现错误
-- canceled:迁移task被取消
-- 
DROP TYPE IF EXISTS cigration.migration_status CASCADE;
CREATE TYPE cigration.migration_status AS ENUM('init','running','completed','error','canceled');

-- 
-- 创建事件触发器，在迁移期间阻止删除迁移任务中存在的表
-- 
CREATE OR REPLACE FUNCTION cigration.prevent_drop_table_in_migration_task()
        RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    obj record;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects() WHERE object_type = 'table'
    LOOP
        IF (select count(*) <> 0 from cigration.pg_citus_shard_migration where obj.object_identity = any (all_colocated_logicalrels)) THEN
            RAISE EXCEPTION 'Can not drop table % which is in shard migration task',obj.object_identity;       
        END IF;
    END LOOP;
END
$$;
CREATE EVENT TRIGGER prevent_drop_table_during_migration
ON sql_drop
EXECUTE PROCEDURE cigration.prevent_drop_table_in_migration_task();

-- 
-- 创建事件触发器，在迁移期间阻止对正在迁移的表执行"ALTER TABLE,CREATE INDEX,ALTER INDEX,DROP INDEX"
-- 
CREATE OR REPLACE FUNCTION cigration.prevent_alter_table_in_running_migration_task()
RETURNS event_trigger 
AS $$
BEGIN
    IF (select count(*) <> 0 from cigration.pg_citus_shard_migration where status = 'running') THEN
        RAISE EXCEPTION '"ALTER TABLE,CREATE INDEX,ALTER INDEX,DROP INDEX" SQL is forbidden during shard migration.';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE EVENT TRIGGER prevent_alter_table_during_migration
ON ddl_command_start WHEN TAG IN ('ALTER TABLE','CREATE INDEX','ALTER INDEX','DROP INDEX')
EXECUTE PROCEDURE cigration.prevent_alter_table_in_running_migration_task();


-- 
-- 自定义序列，用于生成jobid
-- 
DROP SEQUENCE IF EXISTS cigration.jobid_seq;
CREATE SEQUENCE IF NOT EXISTS cigration.jobid_seq AS INTEGER INCREMENT BY 1 START WITH 1;

-- 
-- 定义分片迁移的任务表
-- 
DROP TABLE IF EXISTS cigration.pg_citus_shard_migration CASCADE;
CREATE TABLE IF NOT EXISTS cigration.pg_citus_shard_migration(
jobid   integer not null,
taskid    serial not null,
source_nodename text not null,
source_nodeport integer not null,
target_nodename    text not null,
target_nodeport    integer not null,
status cigration.migration_status not null default 'init'::cigration.migration_status,
colocationid    integer not null,
all_colocated_shards_id   bigint[] not null,
all_colocated_shards_size    bigint[] not null,
all_colocated_logicalrels    text[] not null,
total_shard_count   integer not null,
total_shard_size    bigint not null,
create_time timestamp not null,
start_time  timestamp,
end_time    timestamp,
error_message    text
);

COMMENT ON COLUMN cigration.pg_citus_shard_migration.status IS '该task的状态，包含init，running，completed，error，canceled';
COMMENT ON COLUMN cigration.pg_citus_shard_migration.colocationid IS '该组亲和关系的亲和ID';
COMMENT ON COLUMN cigration.pg_citus_shard_migration.all_colocated_shards_id IS '该组亲和关系中，每个分片的shardid';
COMMENT ON COLUMN cigration.pg_citus_shard_migration.all_colocated_shards_size IS '该组亲和关系中，每个分片的大小，顺序与id列一一对应';
COMMENT ON COLUMN cigration.pg_citus_shard_migration.all_colocated_logicalrels IS '该组亲和关系中，每个分片的逻辑表的识别符(shcema名.table名)';
COMMENT ON COLUMN cigration.pg_citus_shard_migration.total_shard_count IS '该组亲和关系中，所有的分片数总和';
COMMENT ON COLUMN cigration.pg_citus_shard_migration.total_shard_size IS '该组亲和关系中，所有的分片的总大小';

create unique index ON cigration.pg_citus_shard_migration (jobid,taskid);

-- 
-- 定义分片迁移的历史任务表
-- 
DROP TABLE IF EXISTS cigration.pg_citus_shard_migration_history CASCADE;
CREATE TABLE IF NOT EXISTS cigration.pg_citus_shard_migration_history(
jobid   integer not null,
taskid    integer not null,
source_nodename text not null,
source_nodeport integer not null,
target_nodename    text not null,
target_nodeport    integer not null,
status cigration.migration_status not null,
colocationid    integer not null,
all_colocated_shards_id   bigint[] not null,
all_colocated_shards_size    bigint[] not null,
all_colocated_logicalrels    text[] not null,
total_shard_count   integer not null,
total_shard_size    bigint not null,
create_time timestamp not null,
start_time  timestamp,
end_time    timestamp,
error_message    text
);

COMMENT ON COLUMN cigration.pg_citus_shard_migration_history.status IS '该task的状态，包含init，running，completed，error，canceled';
COMMENT ON COLUMN cigration.pg_citus_shard_migration_history.colocationid IS '该组亲和关系的亲和ID';
COMMENT ON COLUMN cigration.pg_citus_shard_migration_history.all_colocated_shards_id IS '该组亲和关系中，每个分片的shardid';
COMMENT ON COLUMN cigration.pg_citus_shard_migration_history.all_colocated_shards_size IS '该组亲和关系中，每个分片的大小，顺序与id列一一对应';
COMMENT ON COLUMN cigration.pg_citus_shard_migration_history.all_colocated_logicalrels IS '该组亲和关系中，每个分片的逻辑表的识别符(shcema名.table名)';
COMMENT ON COLUMN cigration.pg_citus_shard_migration_history.total_shard_count IS '该组亲和关系中，所有的分片数总和';
COMMENT ON COLUMN cigration.pg_citus_shard_migration_history.total_shard_size IS '该组亲和关系中，所有的分片的总大小';

create unique index ON cigration.pg_citus_shard_migration_history(jobid,taskid);

-- Record all successfully executed SQL statements.
create table if not exists cigration.pg_citus_shard_migration_sql_log
(
  id bigserial primary key,
  jobid int not null,
  taskid int not null,
  execute_nodename text not null,
  execute_nodeport integer not null,
  functionid text not null,
  sql text,
  execute_time timestamp default now()
);

-- forbidden the citus function create_distributed_table which was called by parameter table_name and distribution_column
CREATE OR REPLACE FUNCTION pg_catalog.create_distributed_table(table_name regclass,
                        distribution_column text,
                        distribution_type citus.distribution_type default 'hash'::citus.distribution_type
                        )
RETURNS void
AS $create_distributed_table$
    BEGIN
    END;
$create_distributed_table$ LANGUAGE plpgsql SET search_path = 'pg_catalog','public';

-- 在指定worker节点集合上创建hash分片表（colocate_with固定为'none'，不需要并发互斥）
CREATE OR REPLACE FUNCTION cigration.cigration_create_distributed_table_with_placement(table_name regclass,
                        distribution_column text,
                        nodenames text[],
                        nodeports integer[]
                        )
RETURNS void
AS $cigration_create_distributed_table_with_placement$
    DECLARE
        source_node_count integer;
        source_nodenames text[];
        source_nodeports integer[];
        target_node_count integer;
        target_nodenames text[];
        target_nodeports integer[];
        shardids bigint[];
        shardids_count integer;
        itmp integer;
        jtmp integer;
        icurrentnode_port integer;
        error_msg text;
        iarraytmp integer :=1;
    BEGIN
        RAISE DEBUG 'BEGIN create distributed table:%(%)', table_name, distribution_column;

    --check current node is cn?
        IF (SELECT CASE WHEN (select count(*) from pg_dist_node)>0 THEN (select groupid from pg_dist_local_group) ELSE -1 END) <> 0 THEN
            RAISE 'Function cigration.cigration_create_distributed_table_with_placement could only be executed on coordinate node';
        END IF;

    --check citus.shard_replication_factor
        IF (select setting from pg_settings where name='citus.shard_replication_factor') <> '1' THEN
            RAISE 'citus.shard_replication_factor must be 1';
        END IF;

    --get source nodes/ports and target nodes/ports
        --check nodenames or nodeports is NULL?
        IF nodenames IS NULL OR nodeports IS NULL THEN
            RAISE  $$nodenames OR nodeports can not be null$$;
        END IF;

        --check nodenames and nodeports length is corract?
        IF array_length(nodenames, 1) < 1 OR array_length(nodenames, 1) <> array_length(nodeports, 1) THEN
            RAISE  'The length of nodenames or nodeports is invalid';
        END IF;

        --check nodenames and nodeports is in pg_dist_node?
        FOR itmp IN 1..array_length(nodenames, 1) LOOP
            SELECT  count(*) 
            INTO    jtmp 
            FROM    pg_dist_node
            WHERE   nodename=nodenames[itmp] AND
                    nodeport=nodeports[itmp] AND
                    shouldhaveshards='t'     AND
                    isactive='t'             AND
                    noderole = 'primary';

            IF jtmp <> 1 THEN
                RAISE  'Specified worker %:% is invalid.',
                        nodenames[itmp], nodeports[itmp];
            END IF;
        END LOOP;

        --Returned the workers which have not been specified
        SELECT  count(*), array_agg(nodename), array_agg(nodeport)
        INTO    STRICT source_node_count, source_nodenames, source_nodeports
        FROM    pg_dist_node
        WHERE   shouldhaveshards='t' AND
                isactive='t'         AND 
                noderole='primary'   AND
                (nodename, nodeport) NOT IN (SELECT * FROM unnest(nodenames, nodeports));
        
        target_nodenames  := nodenames;
        target_nodeports  := nodeports;
        target_node_count := array_length(nodenames, 1);

    --check target node/port is not null?
        IF target_node_count = 0 THEN
            RAISE $$there are no workers that the distributed table can be created.$$;
        END IF;
            
        RAISE DEBUG 'create distributed table:table_name(%) distribution_column(%)',
                        table_name, distribution_column;

        --get the current node port
        SELECT setting INTO icurrentnode_port FROM pg_settings where name='port';

    --create dblink connection
        PERFORM dblink_disconnect(con) 
        FROM (select unnest(a) con from dblink_get_connections() a)b 
        WHERE con = 'citus_create_distributed_table_con';

        PERFORM dblink_connect('citus_create_distributed_table_con',
                                format('host=%s port=%s user=%s dbname=%s',
                                        '127.0.0.1',
                                        icurrentnode_port,
                                        current_user,
                                        current_database()));

        BEGIN
    --set shard_count and shard_replication_factor in dblink connection
            PERFORM * FROM dblink('citus_create_distributed_table_con', 
                                  format($$SET search_path = %s;SET citus.shard_count = %s;SET citus.shard_replication_factor = %s;$$,
                                         (SELECT setting FROM pg_settings where name='search_path'),
                                         (SELECT setting FROM pg_settings where name='citus.shard_count'),
                                         (SELECT setting FROM pg_settings where name='citus.shard_replication_factor'))
                                 ) AS t(result_record text);

    --create distribute table on all workers
            PERFORM * FROM dblink('citus_create_distributed_table_con',
                                  format($$SELECT pg_catalog.create_distributed_table('%s', '%s', '%s', '%s')$$,
                                         table_name, distribution_column, 'hash','none')
                                 ) AS t(result_record text);

            --all workers without metadata
            IF source_node_count = 0 THEN
                RETURN;
            END IF;

    --move shardid from src nodename to des nodename
            FOR itmp IN 1..source_node_count LOOP
                --fetch shardid list with table name/nodename/nodeport
                RAISE DEBUG 'fetch shardid list with %:%:%', source_nodenames[itmp], source_nodeports[itmp],
                            table_name;
                SELECT  count(*), array_agg(shardid) 
                INTO    STRICT shardids_count, shardids 
                FROM    pg_dist_placement
                WHERE   groupid IN
                            (SELECT groupid FROM pg_dist_node WHERE nodename= source_nodenames[itmp] AND
                                                                    nodeport= source_nodeports[itmp])
                AND     shardid IN
                            (SELECT shardid FROM pg_dist_shard WHERE logicalrelid=table_name);

                --no shard in src nodename
                IF shardids_count = 0 THEN
                    CONTINUE;
                END IF;

                RAISE DEBUG 'move shard placement %:%', source_nodenames[itmp], source_nodeports[itmp];
                FOR jtmp IN 1..shardids_count LOOP
                    BEGIN
                        iarraytmp := iarraytmp%target_node_count + 1;
                        EXECUTE format($moveshard$SELECT * FROM dblink('citus_create_distributed_table_con', 
                                                                $$SELECT cigration.citus_move_shard_placement(%s, '%s', %s, '%s', %s, 'drop', 'block_writes')$$
                                                                ) 
                                            AS t(result_record text) $moveshard$,
                                            shardids[jtmp], source_nodenames[itmp], source_nodeports[itmp], 
                                            target_nodenames[iarraytmp], target_nodeports[iarraytmp]);

                    EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                        --insert information when executing move shard failed
                        INSERT INTO cigration.create_distributed_table_move_shard_to_worker
                                (shard_id, sourcenodename, sourcenodeport, targetnodename, targetnodeport)
                        VALUES (shardids[jtmp], source_nodenames[itmp], source_nodeports[itmp], 
                                target_nodenames[iarraytmp], target_nodeports[iarraytmp]);

                        --output warning msg when executing synchronize metadata failed
                        GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                        RAISE WARNING 'failed to move shard[%] from [%:%] to [%:%] in source:%.Please execute function cigration.cigration_create_distributed_table_move_shard_cleanup() manually after shooting the trouble.',
                                       shardids[jtmp], source_nodenames[itmp], source_nodeports[itmp], 
                                       target_nodenames[iarraytmp], target_nodeports[iarraytmp],
                                       error_msg;
                    END;
                END LOOP;
            END LOOP;
        EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
            BEGIN
                PERFORM dblink_disconnect('citus_create_distributed_table_con');

            EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                RAISE WARNING 'failed to call dblink_disconnect:%', error_msg;
            END;

            RAISE;
        END;

    --cleanup
        BEGIN
            PERFORM dblink_disconnect('citus_create_distributed_table_con');

        EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
            GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
            RAISE WARNING 'failed to call dblink_disconnect:%', error_msg;
        END;

    END;
$cigration_create_distributed_table_with_placement$ LANGUAGE plpgsql SET search_path = 'cigration','public';

-- 创建分片表函数，接口和create_distributed_table()完全兼容
-- 为确保colocate_with指向的表名被正确解析，cigration_create_distributed_table()函数内部的search_path需要和会话保持一致
CREATE OR REPLACE FUNCTION cigration.cigration_create_distributed_table(table_name regclass,
                        distribution_column text,
                        distribution_type citus.distribution_type DEFAULT 'hash',
                        colocate_with text DEFAULT 'default'
                        )
RETURNS void
AS $cigration_create_distributed_table$
    DECLARE
    icurrentnode_port integer;
    table_identity text;
    error_msg text;
    BEGIN
        RAISE DEBUG 'BEGIN create distributed table:%(%)', table_name, distribution_column;

    -- check current node is cn?
        IF (SELECT CASE WHEN (select count(*) from pg_dist_node)>0 THEN (select groupid from pg_dist_local_group) ELSE -1 END) <> 0 THEN
            RAISE 'Function cigration.cigration_create_distributed_table could only be executed on coordinate node';
        END IF;

    -- add check for shard migration
        IF distribution_type = 'hash' AND colocate_with <> 'none' THEN
            IF NOT (select pg_try_advisory_xact_lock('cigration.pg_citus_shard_migration'::regclass::int)) THEN
                RAISE EXCEPTION 'Can not call cigration api concurrently.';
            END IF;

            IF (select count(*) <> 0 from cigration.pg_citus_shard_migration) THEN
                RAISE 'create colocated hash distributed table is forbidden during the shard migration.'
                      USING HINT = 'you could check cigration.pg_citus_shard_migration for existing shard migration tasks.';
            END IF;
        END IF;

    -- get the current node port
        SELECT setting INTO icurrentnode_port FROM pg_settings where name='port';

    --在事务块(包括函数，多SQL语句)中调用create_distributed_table()时，如果表名长度大于55字节，会触发分布式死锁。
    --因此通过dblink调用create_distributed_table()，回避这个问题。
    --create dblink connection
        PERFORM dblink_disconnect(con) 
        FROM (select unnest(a) con from dblink_get_connections() a)b 
        WHERE con = 'citus_create_distributed_table_con';

        PERFORM dblink_connect('citus_create_distributed_table_con',
                                format('host=%s port=%s user=%s dbname=%s',
                                        '127.0.0.1',
                                        icurrentnode_port,
                                        current_user,
                                        current_database()));

        BEGIN
    -- set shard_count and shard_replication_factor in dblink connection
            PERFORM * FROM dblink('citus_create_distributed_table_con', 
                                  format($$SET search_path = %s;SET citus.shard_count = %s;SET citus.shard_replication_factor = %s;$$,
                                         (SELECT setting FROM pg_settings where name='search_path'),
                                         (SELECT setting FROM pg_settings where name='citus.shard_count'),
                                         (SELECT setting FROM pg_settings where name='citus.shard_replication_factor'))
                                 ) AS t(result_record text);

    -- create distribute table
            PERFORM * FROM dblink('citus_create_distributed_table_con',
                                  format($$SELECT pg_catalog.create_distributed_table('%s', '%s', '%s', '%s')$$,
                                         table_name, distribution_column, distribution_type, colocate_with)
                                 ) AS t(result_record text);

        EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
            BEGIN
                PERFORM dblink_disconnect('citus_create_distributed_table_con');

            EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                RAISE WARNING 'failed to call dblink_disconnect:%', error_msg;
            END;

            RAISE;
        END;

    --cleanup
        BEGIN
            PERFORM dblink_disconnect('citus_create_distributed_table_con');

        EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
            GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
            RAISE WARNING 'failed to call dblink_disconnect:%', error_msg;
        END;
    END;
$cigration_create_distributed_table$ LANGUAGE plpgsql;

-- move shard to worker
CREATE OR REPLACE FUNCTION cigration.cigration_create_distributed_table_move_shard_cleanup()
RETURNS void
AS $cigration_create_distributed_table_move_shard_cleanup$
    DECLARE
        curs CURSOR FOR SELECT shard_id, sourcenodename, sourcenodeport, targetnodename, targetnodeport
                        FROM cigration.create_distributed_table_move_shard_to_worker;
        ishard_id   bigint;
        isourcenode text;
        isourceport integer;
        itargetnode text;
        itargetport integer;
        error_msg   text;
    BEGIN
        OPEN curs;
        LOOP
            FETCH curs INTO ishard_id, isourcenode, isourceport, itargetnode, itargetport;
            IF FOUND THEN
                BEGIN
                    EXECUTE format($$select citus_move_shard_placement(%s, '%s', %s, '%s', %s, 'drop', 'block_writes')$$, 
                                    ishard_id, isourcenode, isourceport, itargetnode, itargetport);
                    DELETE FROM cigration.create_distributed_table_move_shard_to_worker WHERE CURRENT OF curs;
                EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                    GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                    RAISE WARNING 'failed to move shard[%] from[%:%] to [%:%] in source:%.', ishard_id, isourcenode, isourceport, itargetnode, itargetport, error_msg;
                END;
            ELSE
                EXIT;
            END IF;
        END LOOP;

        CLOSE curs;

        RETURN;
    END;
$cigration_create_distributed_table_move_shard_cleanup$ LANGUAGE plpgsql SET search_path = 'cigration','public';

-- move this shard and it's all colocated shards from source node to target node.
-- drop_method define how to process old shards in the source node, default is 'none' which does not block SELECT.
-- old shards should be drop in the future will be recorded into table cigration.citus_move_shard_placement_remained_old_shard
CREATE OR REPLACE FUNCTION cigration.citus_move_shard_placement(shard_id bigint,
                                              source_node_name text,
                                              source_node_port integer,
                                              target_node_name text,
                                              target_node_port integer,
                                              drop_method cigration.old_shard_placement_drop_method DEFAULT 'none',
                                              shard_transfer_mode cigration.cigration_shard_transfer_mode default 'force_logical',
                                              consistent_check bool default true,
                                              user_lock_timeout integer DEFAULT 70000,
                                              data_sync_timeout integer DEFAULT 3600)
RETURNS void
AS $citus_move_shard_placement$
    DECLARE
        source_node_id integer;
        source_group_id integer;
        target_node_id integer;
        target_group_id integer;
        logical_relid regclass;
        part_method text;
        source_active_shard_id_array  bigint[];
        source_bad_shards_string text;
        target_exist_shard_id_array bigint[];
        target_shard_tables_with_data text;
        logical_relid_array regclass[];
        inh_logical_relid_array regclass[];
        logical_schema_array text[];
        logical_table_array text[];
        colocated_table_count integer;
        inh_colocated_table_count integer;
        noinh_shard_fulltablename_count integer;
        i integer;
        logical_schema text;
        shard_id_array bigint[];
        shard_fulltablename_array text[];
        inh_shard_fulltablename_array text[];
        noinh_shard_fulltablename_array text[];
        tmp_shard_id bigint;
        sub_rel_count_srsubid bigint;
        sub_lag numeric;
        source_wal_lsn pg_lsn;
        error_msg text;
        source_tables_data_count bigint;
        target_tables_data_count bigint;
        metadata_node_count integer;
        metadata_nodename_array text[];
        metadata_nodeport_array integer[];
        j integer;
        icurrentgroupid integer;
        i_source_node_port integer;
        i_target_node_port integer;
        t_start_sync_time timestamptz;
        pub_created boolean := false;
        sub_created boolean := false;
        table_created boolean := false;
        dblink_created boolean := false;
        need_record_source_shard boolean := false;
    BEGIN

    --check current node is cn?
        SELECT CASE WHEN (select count(*) from pg_dist_node)>0 THEN (select groupid from pg_dist_local_group) ELSE -1 END
        INTO   icurrentgroupid;
        IF icurrentgroupid <> 0 THEN
            RAISE 'Function citus_move_shard_placement could only be executed on coordinate node';
        END IF;

    -- check and get node id of target node and target node. Will fail for invalid input.
        IF source_node_name = target_node_name AND source_node_port = target_node_port THEN
            RAISE  'target node can not be same as source node';
        END IF;

        SELECT nodeid, groupid
        INTO   source_node_id,source_group_id
        FROM   pg_dist_node
        WHERE  nodename = source_node_name AND
               nodeport = source_node_port AND
               isactive = 't'              AND
               noderole = 'primary';

        IF source_node_id is NULL OR source_group_id is NULL THEN
            RAISE  'invalid source node %:%', source_node_name, source_node_port;
        END IF;

        SELECT nodeid, groupid
        INTO   target_node_id,target_group_id
        FROM   pg_dist_node
        WHERE  nodename = target_node_name AND
               nodeport = target_node_port AND
               isactive = 't'              AND
               noderole = 'primary';

        IF target_node_id is NULL OR target_group_id is NULL THEN
            RAISE  'invalid target node %:%', target_node_name, target_node_port;
        END IF;

    -- check if the shard is hash shard
        SELECT logicalrelid
        INTO   logical_relid
        FROM   pg_dist_shard
        WHERE  shardid = shard_id;

        IF logical_relid is NULL THEN
            RAISE  'shard % does not exist', shard_id;
        END IF;

        SELECT partmethod
        INTO   part_method
        FROM   pg_dist_partition
        WHERE  logicalrelid = logical_relid;

        IF part_method is NULL OR part_method <> 'h' THEN
            RAISE  '% is not a hash shard', shard_id;
        END IF;

    -- get all colocated tables and there shard id
        SELECT count(logicalrelid), array_agg(logicalrelid) 
        INTO   STRICT  colocated_table_count,logical_relid_array
        FROM pg_dist_partition 
        WHERE colocationid=(select colocationid from pg_dist_partition where logicalrelid=logical_relid);

        SELECT array_agg(nspname), array_agg(relname)
        INTO   STRICT  logical_schema_array, logical_table_array
        FROM pg_class c
        LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.oid = any(logical_relid_array);

        SELECT array_agg(shardid), array_agg(shard_name(logicalrelid, shardid))
        INTO   STRICT  shard_id_array, shard_fulltablename_array
        FROM   (SELECT shardid, logicalrelid
                FROM pg_dist_shard
                WHERE logicalrelid = any(logical_relid_array) AND
                      (shardminvalue,shardmaxvalue)=(select shardminvalue,shardmaxvalue from pg_dist_shard where shardid=shard_id)
                ORDER BY shardid ASC) AS tbl;

    -- check inherit information
        IF shard_transfer_mode = 'force_logical' THEN
            -- inherit table list
            SELECT count(logicalrelid), array_agg(logicalrelid) 
            INTO   STRICT  inh_colocated_table_count,inh_logical_relid_array
            FROM   pg_dist_partition p , pg_class c
            WHERE  colocationid=(select colocationid from pg_dist_partition where logicalrelid=logical_relid) AND
                   p.logicalrelid=c.oid AND c.relhassubclass='t';

            -- all of the inherit table are not partition table?
            SELECT count(logicalrelid)
            INTO   STRICT  inh_colocated_table_count
            FROM   pg_dist_partition p , pg_class c
            WHERE  p.logicalrelid = any(inh_logical_relid_array) AND
                   p.logicalrelid=c.oid AND c.relkind<>'p';

            IF inh_colocated_table_count > 0 THEN
                RAISE  'Inherit table are not supported in function citus_move_shard_placement';
            END IF;

            -- get partition table's full name
            SELECT array_agg(shard_name(logicalrelid, shardid))
            INTO   STRICT  inh_shard_fulltablename_array
            FROM   (SELECT shardid, logicalrelid
                    FROM pg_dist_shard
                    WHERE logicalrelid = any(inh_logical_relid_array) AND
                          (shardminvalue,shardmaxvalue)=(select shardminvalue,shardmaxvalue from pg_dist_shard where shardid=shard_id)
                    ORDER BY shardid ASC) AS tbl;

            -- get colocated tables without partition tables
            SELECT count(table_name), array_agg(table_name)
            INTO   STRICT noinh_shard_fulltablename_count, noinh_shard_fulltablename_array
            FROM
                ((select table_name from unnest(shard_fulltablename_array) table_name)
                EXCEPT
                (select table_name from unnest(inh_shard_fulltablename_array) table_name)) AS tb1;
        END IF;

    -- check if all colocated shards are valid
        SELECT array_agg(shardid)
        INTO   source_active_shard_id_array
        FROM   (SELECT shardid
                FROM   pg_dist_placement
                WHERE  shardid = any(shard_id_array) AND
                       groupid = source_group_id AND
                       shardstate = 1
                ORDER BY shardid ASC) AS tb1;

        IF source_active_shard_id_array is NULL THEN
            RAISE  'shard % in source node do not exist or invalid', shard_id_array;
        ELSIF source_active_shard_id_array <> shard_id_array THEN
            SELECT string_agg(shardid::text,',')
            INTO   STRICT  source_bad_shards_string
            FROM   unnest(shard_id_array) t(shardid)
            WHERE  shardid <> any(source_active_shard_id_array);

            RAISE  'shard % in source node do not exist or invalid', source_bad_shards_string;
        END IF;

        SELECT array_agg(shardid)
        INTO   target_exist_shard_id_array
        FROM   pg_dist_placement
        WHERE  shardid = shard_id AND
               groupid = target_group_id;

        IF target_exist_shard_id_array is not NULL THEN
            RAISE  'shard % already exist in target node', target_exist_shard_id_array;
        END IF;

        RAISE  NOTICE  'BEGIN move shards(%) from %:% to %:%', 
                    array_to_string(shard_id_array,','),
                    source_node_name, source_node_port,
                    target_node_name, target_node_port;

    --fetch worker which has metadata
        SELECT count(*), array_agg(nodename), array_agg(nodeport)
        INTO   STRICT metadata_node_count, metadata_nodename_array, metadata_nodeport_array
        FROM   pg_dist_node
        WHERE  hasmetadata='t' AND isactive='t' AND noderole='primary';

    -- get source and target pg port
        SELECT port 
        INTO i_source_node_port 
        FROM dblink(format('host=%s port=%s user=%s dbname=%s', source_node_name, source_node_port, current_user, current_database()),
                          $$SELECT setting FROM pg_settings WHERE name = 'port'$$) AS tb(port integer);
        SELECT port 
        INTO i_target_node_port 
        FROM dblink(format('host=%s port=%s user=%s dbname=%s', target_node_name, target_node_port, current_user, current_database()),
                          $$SELECT setting FROM pg_settings WHERE name = 'port'$$) AS tb(port integer);

    -- create dblink connection on cn
        PERFORM dblink_disconnect(con) 
        FROM (select unnest(a) con from dblink_get_connections() a)b 
        WHERE con like 'citus_move_shard_placement_%';

        PERFORM dblink_connect('citus_move_shard_placement_source_con',
                                format('host=%s port=%s user=%s dbname=%s',
                                        source_node_name,
                                        i_source_node_port,
                                        current_user,
                                        current_database()));
        dblink_created := true;

        PERFORM dblink_connect('citus_move_shard_placement_target_con',
                                format('host=%s port=%s user=%s dbname=%s',
                                        target_node_name,
                                        i_target_node_port,
                                        current_user,
                                        current_database()));

    --  set lock timeout on cn
        EXECUTE format('SET lock_timeout=%s',user_lock_timeout);

    --  lock tables from executing DDL
        FOR i IN 1..colocated_table_count LOOP
            RAISE  NOTICE  '[%/%] LOCK TABLE %.% IN SHARE UPDATE EXCLUSIVE MODE ...', 
                            i, colocated_table_count, logical_schema_array[i], logical_table_array[i];

            EXECUTE format('LOCK TABLE %I.%I IN SHARE UPDATE EXCLUSIVE MODE', 
                            logical_schema_array[i],
                            logical_table_array[i]);
        END LOOP;

    -- create dblink connection on workers which have metadata, and then lock tables from executing DDL
        IF metadata_node_count >=1 THEN
            FOR j IN 1..metadata_node_count LOOP
                PERFORM dblink_connect(format('citus_move_shard_placement_%s_%s_con', 
                                               metadata_nodename_array[j], 
                                               metadata_nodeport_array[j]),
                                       format('host=%s port=%s user=%s dbname=%s',
                                               metadata_nodename_array[j],
                                               metadata_nodeport_array[j],
                                               current_user,
                                               current_database()));

                --begin transaction on each worker which has metadata
                PERFORM dblink_exec(format('citus_move_shard_placement_%s_%s_con', 
                                            metadata_nodename_array[j], 
                                            metadata_nodeport_array[j]), 
                                    'BEGIN');

                --set lock timeout on workers which have metadata
                PERFORM dblink_exec(format('citus_move_shard_placement_%s_%s_con', 
                                            metadata_nodename_array[j], 
                                            metadata_nodeport_array[j]), 
                                    format('set lock_timeout=%s',user_lock_timeout));
                        
                --lock tables from executing DDL on citus_move_shard_placement_node
                -- FOR i IN 1..colocated_table_count LOOP
                --    RAISE  NOTICE  '[citus_move_shard_placement_%_%_con][%/%] LOCK TABLE %.% IN SHARE UPDATE EXCLUSIVE MODE ...', 
                --                    metadata_nodename_array[j], 
                --                    metadata_nodeport_array[j], 
                --                    i, colocated_table_count, 
                --                    logical_schema_array[i], logical_table_array[i];

                --    PERFORM dblink_exec(format('citus_move_shard_placement_%s_%s_con', 
                --                                metadata_nodename_array[j], 
                --                                metadata_nodeport_array[j]), 
                --                        format('LOCK TABLE %I.%I IN SHARE UPDATE EXCLUSIVE MODE', 
                --                                logical_schema_array[i],
                --                                logical_table_array[i]));
                --END LOOP;
            END LOOP;
        END IF;

        BEGIN
            IF shard_transfer_mode = 'force_logical' THEN
        -- CREATE PUBLICATION in source node
                RAISE  NOTICE  'CREATE PUBLICATION in source node %:%', source_node_name, source_node_port;
                PERFORM dblink_exec('citus_move_shard_placement_source_con', 
                                    'DROP PUBLICATION IF EXISTS citus_move_shard_placement_pub CASCADE');
                PERFORM dblink_exec('citus_move_shard_placement_source_con', 
                                    format('CREATE PUBLICATION citus_move_shard_placement_pub 
                                                FOR TABLE %s',
                                             (select string_agg(table_name,',') from unnest(noinh_shard_fulltablename_array) table_name)));
                pub_created := true;
            END IF;

    -- CREATE SCHEMA IF NOT EXISTS in target node
            FOR logical_schema IN select distinct unnest(logical_schema_array) LOOP
                PERFORM dblink_exec('citus_move_shard_placement_target_con', 
                                format('CREATE SCHEMA IF NOT EXISTS %I',
                                        logical_schema));
            END LOOP;

    -- create shard table in the target node
            RAISE  NOTICE  'create shard table in the target node %:%', target_node_name, target_node_port;
            EXECUTE format($sql$COPY (select '') to PROGRAM $$pg_dump "host=%s port=%s user=%s dbname=%s" -s -t %s | psql "host=%s port=%s user=%s dbname=%s"$$ $sql$,
                            source_node_name,
                            i_source_node_port,
                            current_user,
                            current_database(),
                            (select string_agg(format($a$'%s'$a$,table_name),' -t ') from unnest(shard_fulltablename_array) table_name),
                            target_node_name,
                            i_target_node_port,
                            current_user,
                            current_database());

            SELECT table_name
            INTO   target_shard_tables_with_data
            FROM   dblink('citus_move_shard_placement_target_con',
                            format($$ select string_agg(table_name,',') table_name from ((select tableoid::regclass::text table_name from %s limit 1))a $$,
                                   (select string_agg(table_name,' limit 1) UNION all (select tableoid::regclass::text table_name from ') 
                                           from unnest(shard_fulltablename_array) table_name))
                    ) as a(table_name text);

            IF target_shard_tables_with_data is not NULL THEN
                RAISE  'shard tables(%) with data has exists in target node', target_shard_tables_with_data;
            END IF;

            table_created := true;

            IF shard_transfer_mode = 'force_logical' THEN
        -- CREATE SUBSCRIPTION on target node
                RAISE  NOTICE  'CREATE SUBSCRIPTION on target node %:%', target_node_name, target_node_port;
                PERFORM dblink_exec('citus_move_shard_placement_target_con', 
                                    'DROP SUBSCRIPTION IF EXISTS citus_move_shard_placement_sub CASCADE');
                PERFORM dblink_exec('citus_move_shard_placement_target_con', 
                                    format($$CREATE SUBSCRIPTION citus_move_shard_placement_sub
                                                 CONNECTION 'host=%s port=%s user=%s dbname=%s'
                                                 PUBLICATION citus_move_shard_placement_pub$$,
                                                 source_node_name,
                                                 i_source_node_port,
                                                 current_user,
                                                 current_database()));
                sub_created := true;

        -- wait shard data init sync
                RAISE  NOTICE  'wait for init data sync...';
                SELECT clock_timestamp() INTO t_start_sync_time;

                LOOP
                    SELECT count_srsubid
                    INTO STRICT sub_rel_count_srsubid
                    FROM dblink('citus_move_shard_placement_target_con',
                                        $$SELECT count(srsubid) count_srsubid from pg_subscription, pg_subscription_rel
                                          WHERE pg_subscription.oid=pg_subscription_rel.srsubid            AND
                                                pg_subscription.subname = 'citus_move_shard_placement_sub' AND
                                                (pg_subscription_rel.srsubstate = 's' OR pg_subscription_rel.srsubstate = 'r')$$
                                  ) AS t(count_srsubid int);

                    IF sub_rel_count_srsubid = noinh_shard_fulltablename_count THEN
                        EXIT;
                    ELSE
                        IF EXTRACT(EPOCH from clock_timestamp() - t_start_sync_time) > data_sync_timeout THEN
                            RAISE 'init data sync timeout.';
                        END IF;
                        PERFORM pg_sleep(1);
                    END IF;
                END LOOP;
            END IF;

        --  lock tables from executing SQL
            FOR i IN 1..colocated_table_count LOOP
                IF drop_method = 'none' THEN
                    --  block all sql except for SELECT
                    RAISE  NOTICE  '[%/%] LOCK TABLE %.% IN EXCLUSIVE MODE ...', 
                                i, colocated_table_count, logical_schema_array[i], logical_table_array[i];

                    EXECUTE format('LOCK TABLE %I.%I IN EXCLUSIVE MODE', 
                                logical_schema_array[i],
                                logical_table_array[i]);
                ELSE
                    --  block all sql
                    RAISE  NOTICE  '[%/%] LOCK TABLE %.% ...', 
                                i, colocated_table_count, logical_schema_array[i], logical_table_array[i];

                    EXECUTE format('LOCK TABLE %I.%I', 
                                logical_schema_array[i],
                                logical_table_array[i]);
                END IF;
            END LOOP;

        --lock tables from executing SQL on workers which have metadata
            IF metadata_node_count >=1 THEN
                FOR j IN 1..metadata_node_count LOOP                    
                    --lock tables from executing DDL on citus_move_shard_placement_node
                    FOR i IN 1..colocated_table_count LOOP
                        IF drop_method = 'none' THEN
                            RAISE  NOTICE  '[citus_move_shard_placement_%_%_con][%/%] LOCK TABLE %.% IN EXCLUSIVE MODE ...', 
                                            metadata_nodename_array[j], 
                                            metadata_nodeport_array[j], 
                                            i, colocated_table_count, 
                                            logical_schema_array[i], logical_table_array[i];

                            PERFORM dblink_exec(format('citus_move_shard_placement_%s_%s_con', 
                                                        metadata_nodename_array[j], 
                                                        metadata_nodeport_array[j]), 
                                                format('LOCK TABLE %I.%I IN EXCLUSIVE MODE', 
                                                        logical_schema_array[i],
                                                        logical_table_array[i]));
                        ELSE
                            RAISE  NOTICE  '[citus_move_shard_placement_%_%_con][%/%] LOCK TABLE %.% ...', 
                                            metadata_nodename_array[j], 
                                            metadata_nodeport_array[j], 
                                            i, colocated_table_count, 
                                            logical_schema_array[i], logical_table_array[i];

                            PERFORM dblink_exec(format('citus_move_shard_placement_%s_%s_con', 
                                                        metadata_nodename_array[j], 
                                                        metadata_nodeport_array[j]), 
                                                format('LOCK TABLE %I.%I', 
                                                        logical_schema_array[i],
                                                        logical_table_array[i]));
                        END IF;
                    END LOOP;
                END LOOP;
            END IF;

            IF shard_transfer_mode = 'force_logical' THEN
        -- wait shard data sync
                RAISE  NOTICE  'wait for data sync...';
                SELECT clock_timestamp() INTO t_start_sync_time;

                SELECT sourcewallsn
                INTO STRICT source_wal_lsn
                FROM dblink('citus_move_shard_placement_source_con', 
                                    $$select pg_current_wal_lsn()$$
                              ) AS t(sourcewallsn pg_lsn);

                LOOP
                    SELECT lag
                    INTO STRICT sub_lag
                    FROM dblink('citus_move_shard_placement_target_con', 
                                        format($$select pg_wal_lsn_diff('%s',latest_end_lsn)
                                                 FROM pg_stat_subscription
                                                 WHERE subname = 'citus_move_shard_placement_sub' AND latest_end_lsn is not NULL$$,
                                                 source_wal_lsn::text)
                                  ) AS t(lag numeric);

                    IF sub_lag <= 0 THEN
                        EXIT;
                    ELSE
                        IF EXTRACT(EPOCH from clock_timestamp() - t_start_sync_time) > data_sync_timeout THEN
                            RAISE 'data sync timeout.';
                        END IF;
                        PERFORM pg_sleep(1);
                    END IF;
                END LOOP;
            ELSIF shard_transfer_mode = 'block_writes' THEN
            --  COPY DATA FROM SOURCENODE TO TARGETNODE
                FOR i IN 1..colocated_table_count LOOP
                    RAISE  NOTICE  '[%/%]copy table[%] data from source[%:%] to target[%:%]...',
                                    i,colocated_table_count,shard_fulltablename_array[i],
                                    source_node_name,i_source_node_port,target_node_name,i_target_node_port;
                    EXECUTE format($sql$COPY (select '') to PROGRAM $$psql "host=%s port=%s user=%s dbname=%s" -Atc 'copy %s to stdout' | psql "host=%s port=%s user=%s dbname=%s" -Atc 'copy %s from stdout'$$ $sql$,
                                    source_node_name,
                                    i_source_node_port,
                                    current_user,
                                    current_database(),
                                    shard_fulltablename_array[i],
                                    target_node_name,
                                    i_target_node_port,
                                    current_user,
                                    current_database(),
                                    shard_fulltablename_array[i]);

                END LOOP;
            END IF;

    --check target table is consistently with the source table
            IF consistent_check THEN
                RAISE  NOTICE  'check target table is consistently with the source table...';
                FOR i IN 1..colocated_table_count LOOP
                    --target table count
                    SELECT c1
                    INTO target_tables_data_count
                    FROM dblink('citus_move_shard_placement_target_con', 
                                format($$select count(*) FROM %s$$,
                                                 shard_fulltablename_array[i])
                               ) AS t(c1 bigint);

                    --source table count
                    SELECT c1
                    INTO source_tables_data_count
                    FROM dblink('citus_move_shard_placement_source_con', 
                                format($$select count(*) FROM %s$$,
                                                 shard_fulltablename_array[i])
                               ) AS t(c1 bigint);

                    IF source_tables_data_count <> target_tables_data_count THEN
                        RAISE 'shard table[%] data sync failed:[%] records is expected bus was [%]',
                               shard_fulltablename_array[i], source_tables_data_count, target_tables_data_count;
                    END IF;
                END LOOP;
            END IF;

    -- UPDATE pg_dist_placement
            RAISE  NOTICE  'UPDATE pg_dist_placement';
            UPDATE pg_dist_placement
            SET groupid=target_group_id 
            WHERE shardid=any(shard_id_array) and groupid=source_group_id;

    --UPDATE pg_dist_placement on workers which have metadata
            IF metadata_node_count >=1 THEN
                FOR j IN 1..metadata_node_count LOOP    
                    RAISE  NOTICE  '[citus_move_shard_placement_%_%_con] UPDATE pg_dist_placement ...', 
                                    metadata_nodename_array[j], 
                                    metadata_nodeport_array[j];

                    PERFORM dblink_exec(format('citus_move_shard_placement_%s_%s_con', 
                                                metadata_nodename_array[j], 
                                                metadata_nodeport_array[j]), 
                                        format('UPDATE pg_dist_placement SET groupid=%s WHERE shardid=any(array[%s]) and groupid=%s',
                                                target_group_id, array_to_string(shard_id_array,','), source_group_id));
                END LOOP;
            END IF;


    --prepare transaction on workers which have metadata
            IF metadata_node_count >=1 THEN
                FOR j IN 1..metadata_node_count LOOP    
                    RAISE  NOTICE  '[citus_move_shard_placement_%_%_con] PREPARE TRANSACTION ...', 
                                    metadata_nodename_array[j], 
                                    metadata_nodeport_array[j];

                    PERFORM dblink_exec(format('citus_move_shard_placement_%s_%s_con', 
                                                metadata_nodename_array[j], 
                                                metadata_nodeport_array[j]), 
                                        format($$PREPARE TRANSACTION 'citus_move_shard_placement_%s_%s_tran'$$,
                                                metadata_nodename_array[j], 
                                                metadata_nodeport_array[j]));
                END LOOP;
            END IF;

    --UPDATE pg_dist_placement on workers which have metadata
            IF metadata_node_count >=1 THEN
                FOR j IN 1..metadata_node_count LOOP    
                    RAISE  NOTICE  '[citus_move_shard_placement_%_%_con] COMMIT PREPARED TRANSACTION ...', 
                                    metadata_nodename_array[j], metadata_nodeport_array[j];
                    BEGIN
                        PERFORM dblink_exec(format('citus_move_shard_placement_%s_%s_con', 
                                                    metadata_nodename_array[j], 
                                                    metadata_nodeport_array[j]), 
                                            format($$COMMIT PREPARED 'citus_move_shard_placement_%s_%s_tran'$$,
                                                    metadata_nodename_array[j], 
                                                    metadata_nodeport_array[j]));
                    EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                        --insert transaction information when executing COMMIT PREPARED TRANSACTION is failed
                        INSERT INTO cigration.citus_move_shard_placement_transaction(nodename, nodeport, gid)
                        VALUES (metadata_nodename_array[j], metadata_nodeport_array[j],
                                format('citus_move_shard_placement_%s_%s_tran',
                                        metadata_nodename_array[j], 
                                        metadata_nodeport_array[j]));

                        --output warning msg when executing COMMIT PREPARED TRANSACTION is failed.
                        --and then executing COMMIT PREPARED TRANSACTION on the next worker which has metadata
                        GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                        RAISE WARNING 'failed to execute update pg_dist_placement on workers[%:%] in source:%.Please execute function cigration.citus_move_shard_placement_transaction_cleanup() manually after shooting the trouble.',
                                       metadata_nodename_array[j], 
                                       metadata_nodeport_array[j], 
                                       error_msg;
                    END;
                END LOOP;
            END IF;

    -- drop old shard
            IF drop_method = 'drop' THEN
                RAISE  NOTICE  'DROP old shard tables in source node';
            ELSIF drop_method = 'rename' THEN
                RAISE  NOTICE  'Move old shard tables in source node to shcema "citus_move_shard_placement_recyclebin"';

                -- CREATE SCHEMA IF NOT EXISTS citus_move_shard_placement_recyclebin
                BEGIN
                    PERFORM dblink_exec('citus_move_shard_placement_source_con', 
                         'CREATE SCHEMA IF NOT EXISTS citus_move_shard_placement_recyclebin');
                EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                    GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                    RAISE WARNING 'failed to CREATE SCHEMA  citus_move_shard_placement_recyclebin in source:%', error_msg;
                END;
            END IF;

            FOR i IN 1..colocated_table_count LOOP
                IF drop_method = 'drop' THEN
                    BEGIN
                        PERFORM dblink_exec('citus_move_shard_placement_source_con', 
                                            format($$DROP TABLE %s$$,
                                                     shard_fulltablename_array[i]));
                    EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                        need_record_source_shard := true;
                        GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                        RAISE WARNING 'failed to DROP TABLE % in source:%', shard_fulltablename_array[i], error_msg;
                    END;
                ELSIF drop_method = 'rename' THEN
                    BEGIN
                        PERFORM dblink_exec('citus_move_shard_placement_source_con', 
                                            format($$ALTER TABLE %s SET SCHEMA citus_move_shard_placement_recyclebin$$,
                                                     shard_fulltablename_array[i]));
                    EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                        need_record_source_shard := true;
                        GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                        RAISE WARNING 'failed to RENAME TABLE % in source:%', shard_fulltablename_array[i], error_msg;
                    END;
                ELSE
                    need_record_source_shard := true;
                END IF;
                
                IF need_record_source_shard THEN
                    BEGIN
                        INSERT INTO citus.citus_move_shard_placement_remained_old_shard(nodename, nodeport, tablename, drop_method) 
                                SELECT source_node_name, source_node_port, shard_fulltablename_array[i], drop_method;
                    EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                        GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                        RAISE WARNING 'failed to record shard % into citus.citus_move_shard_placement_remained_old_shard:%', 
                                    shard_fulltablename_array[i], error_msg;
                    END;
                END IF;
            END LOOP;

    -- error cleanup
        EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
            IF sub_created THEN
                BEGIN
                PERFORM dblink_exec('citus_move_shard_placement_target_con', 
                                    'ALTER SUBSCRIPTION citus_move_shard_placement_sub DISABLE');
                PERFORM dblink_exec('citus_move_shard_placement_target_con', 
                                    'DROP SUBSCRIPTION IF EXISTS citus_move_shard_placement_sub CASCADE');
                EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                    GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                    RAISE WARNING 'failed to DROP SUBSCRIPTION citus_move_shard_placement_sub in target node:%', error_msg;
                END;
            END IF;

            IF pub_created THEN
                BEGIN
                PERFORM dblink_exec('citus_move_shard_placement_source_con', 
                                    'DROP PUBLICATION IF EXISTS citus_move_shard_placement_pub CASCADE');
                EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                    GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                    RAISE WARNING 'failed to DROP PUBLICATION citus_move_shard_placement_pub in source node:%', error_msg;
                END;
            END IF;

            IF table_created THEN
                FOR i IN 1..colocated_table_count LOOP
                    BEGIN
                    PERFORM dblink_exec('citus_move_shard_placement_target_con', 
                                     format('DROP TABLE %s',
                                             shard_fulltablename_array[i]));
                    EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                        GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                        RAISE WARNING 'failed to DROP TABLE % in target node:%', shard_fulltablename_array[i], error_msg;
                    END;
                END LOOP;
            END IF;

            IF dblink_created THEN
            BEGIN
                PERFORM dblink_disconnect(con) 
                FROM (select unnest(a) con from dblink_get_connections() a)b 
                WHERE con like 'citus_move_shard_placement_%';

                EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                    GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                    RAISE WARNING 'failed to call dblink_disconnect:%', error_msg;
                END;
            END IF;

            RAISE;
        END;

    -- cleanup
        IF sub_created THEN
            BEGIN
                RAISE  NOTICE  'DROP SUBSCRIPTION and PUBLICATION';
                PERFORM dblink_exec('citus_move_shard_placement_target_con', 
                                    'ALTER SUBSCRIPTION citus_move_shard_placement_sub DISABLE');
                PERFORM dblink_exec('citus_move_shard_placement_target_con', 
                                    'DROP SUBSCRIPTION IF EXISTS citus_move_shard_placement_sub CASCADE');
            EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                RAISE WARNING 'failed to DROP SUBSCRIPTION citus_move_shard_placement_sub:%', error_msg;
            END;
        END IF;

        IF pub_created THEN
            BEGIN
                PERFORM dblink_exec('citus_move_shard_placement_source_con', 
                                    'DROP PUBLICATION IF EXISTS citus_move_shard_placement_pub CASCADE');
            EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                RAISE WARNING 'failed to DROP PUBLICATION citus_move_shard_placement_pub:%', error_msg;
            END;
        END IF;

        BEGIN
        PERFORM dblink_disconnect(con) 
        FROM (select unnest(a) con from dblink_get_connections() a)b 
        WHERE con like 'citus_move_shard_placement_%';

        EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
            GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
            RAISE WARNING 'failed to call dblink_disconnect:%', error_msg;
        END;

        RAISE  NOTICE  'END';
    END;
$citus_move_shard_placement$ LANGUAGE plpgsql SET search_path = 'cigration','public';

-- drop old shards in source node
CREATE OR REPLACE FUNCTION cigration.citus_move_shard_placement_cleanup()
RETURNS void
AS $$
    DECLARE
        cleanup_count integer :=0;
    BEGIN
        SELECT count(*)
        INTO   cleanup_count
        FROM
        (
            (
                select pg_dist_shard.logicalrelid::text || '_' || pg_dist_shard_placement.shardid tn,
                       pg_dist_shard_placement.nodename nn,
                       pg_dist_shard_placement.nodeport np
                from pg_dist_shard_placement,pg_dist_shard
                where pg_dist_shard_placement.shardid=pg_dist_shard.shardid
                order by pg_dist_shard.logicalrelid, pg_dist_shard_placement.shardid
            )
            except
            (
                (
                    select pg_dist_shard.logicalrelid::text || '_' || pg_dist_shard_placement.shardid tn,
                           pg_dist_shard_placement.nodename nn,
                           pg_dist_shard_placement.nodeport np
                    from pg_dist_shard_placement,pg_dist_shard
                    where pg_dist_shard_placement.shardid=pg_dist_shard.shardid
                    order by pg_dist_shard.logicalrelid, pg_dist_shard_placement.shardid
                )
                except
                (
                    select tablename, nodename, nodeport::int from citus.citus_move_shard_placement_remained_old_shard
                )
            )
        ) A;

        IF cleanup_count > 0 THEN
            RAISE 'The metadata of actual table is contained in table citus_move_shard_placement_remained_old_shard. Please check citus metadata again!';
        END IF;

        delete from citus.citus_move_shard_placement_remained_old_shard where id in
            (select id 
             from (select id,dblink_exec(format('host=%s port=%s user=%s dbname=%s', nodename, nodeport, current_user, current_database()),
                                            'DROP TABLE IF EXISTS ' || tablename) drop_result 
                   from citus.citus_move_shard_placement_remained_old_shard)a 
             where drop_result='DROP TABLE');

        PERFORM run_command_on_workers('DROP SCHEMA IF EXISTS citus_move_shard_placement_recyclebin CASCADE');
    END;
$$ LANGUAGE plpgsql SET search_path = 'cigration','public';


CREATE OR REPLACE FUNCTION cigration.cigration_print_log(func_name text,log_message text,log_level text default 'notice')
RETURNS void
-- 
-- 函数名：cigration.cigration_print_log
-- 函数功能：打印日志
-- 参数：func_name text
--       log_message text
--       log_level text
-- 返回值：无
-- 
AS $cigration_print_log$
BEGIN  
    if (log_level = 'notice') then
        raise notice '% % : %',clock_timestamp(),func_name,log_message;
    elsif (log_level = 'debug') then
        raise debug '% % : %',clock_timestamp(),func_name,log_message;
    else
        raise exception 'only support log message in notice or debug level.';
    end if;
END;
$cigration_print_log$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION cigration.cigration_get_shard_size(shardid_list bigint[])
RETURNS TABLE(shard_nodename text,shard_nodeport int,shard_id bigint,shard_state int,shardidfullrelname text,shard_size bigint)
-- 
-- 函数名：cigration.cigration_get_shard_size
-- 函数功能：在CN上通过dblink获取指定分片的大小
-- 参数：shardid bigint[]
-- 返回值：shard_size
-- 
AS $$
DECLARE    
    shard bigint;
    fullrelname text;
    shard_size bigint;  
    shard_info_record record;
BEGIN
    -- 执行节点必须是CN节点
    IF (SELECT CASE WHEN (select count(*) from pg_dist_node)>0 THEN (select groupid from pg_dist_local_group) ELSE -1 END) <> 0 THEN
        RAISE EXCEPTION 'function cigration_get_shard_size could only be executed on coordinate node.';
    END IF;
    
    -- 入参的非空判断
    if (shardid_list is null) then
        RAISE EXCEPTION 'shardid_list should not be null.';
    end if;
    
    -- 集群节点是否都健康
    if (select count(*) <> 0 from pg_dist_node where isactive = 'f' or noderole <> 'primary') then
        RAISE EXCEPTION 'the citus cluster may have some inactive or secondary node.';
    end if;
    
    -- 是否有shardid不在集群内（可能是手动输入有误的）
    foreach shard in array shardid_list
    loop
        if (select count(*) = 0 from pg_dist_shard where shardid = shard) then
            RAISE EXCEPTION 'shardid : % is not in the metedata.',shard;
        end if;
    end loop;
    
    for shard_info_record in select logicalrelid,a.shardid,b.nodename,b.nodeport,b.shardstate from pg_dist_shard a,pg_dist_shard_placement b where a.shardid = b.shardid and a.shardid in (select unnest(shardid_list)) order by logicalrelid,a.shardid
    loop
        fullrelname := shard_name(shard_info_record.logicalrelid, shard_info_record.shardid);
        
        PERFORM dblink_connect(format('%s','get_shard_size_con_' || shard_info_record.shardid),format('host=%s port=%s user=%s dbname=%s',shard_info_record.nodename,shard_info_record.nodeport,current_user,current_database()));
        EXECUTE FORMAT($tmp$select * from dblink(%L,$sql$select pg_relation_size(%L)$sql$) as (tbsize bigint)$tmp$,'get_shard_size_con_' || shard_info_record.shardid,fullrelname) into shard_size;
        PERFORM dblink_disconnect(con) FROM (select unnest(a) con from dblink_get_connections() a)b WHERE con = 'get_shard_size_con_' || shard_info_record.shardid;
        
        RETURN QUERY select shard_info_record.nodename,shard_info_record.nodeport,shard_info_record.shardid,shard_info_record.shardstate,fullrelname,shard_size;
    end loop;

EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
    -- 关闭连接
    PERFORM dblink_disconnect(con) FROM (select unnest(a) con from dblink_get_connections() a)b WHERE con ~ '^get_shard_size_con';
    raise;
END;
$$ LANGUAGE plpgsql;
 

CREATE OR REPLACE FUNCTION cigration.cigration_create_drain_node_job(input_source_nodes text[])
RETURNS TABLE(jobid integer,taskid integer,all_colocated_shards_id bigint[],source_nodename text,source_nodeport integer,target_nodename text,target_nodeport integer,total_shard_count int,total_shard_size bigint)
-- 
-- 函数名：cigration.cigration_create_drain_node_job
-- 函数功能：创建一个缩容场景的JOB
-- 参数：input_source_nodes text[]，形式类似：array['192.168.1.1:5432','192.168.1.2:5432']
-- 返回值：返回生成的迁移任务记录
-- 
AS $$
DECLARE
    -- 连接信息
    source_node_list text[];
    target_node_list text[];
    
    var_jobid int := 0;
BEGIN
    --执行节点必须是CN节点
    IF (SELECT CASE WHEN (select count(*) from pg_dist_node)>0 THEN (select groupid from pg_dist_local_group) ELSE -1 END) <> 0 THEN
        RAISE EXCEPTION 'function cigration_create_drain_node_job could only be executed on coordinate node.';
    END IF;
    
    --检查是否有未归档的JOB
    if (select count(*) <> 0 from cigration.pg_citus_shard_migration) then
        RAISE EXCEPTION 'can not create a new job when there are some uncleanuped jobs.';
    end if;
    
    --检查集群健康状态
    if (SELECT count(*) <> 0 FROM pg_dist_node WHERE noderole = 'primary' AND isactive = 'f') then
        RAISE EXCEPTION 'there are some invalid primary node in the cluster.';
    end if;

    -- 输入参数检查
    if (array_length(input_source_nodes,1) = 0) then
        RAISE EXCEPTION 'input_source_nodes is empty';
    end if;
    
    -- 判断输入源节点是否在集群中，不在就报错
    if (select count(*) <> array_length(input_source_nodes,1) from pg_dist_node where noderole = 'primary' and concat(nodename, ':', nodeport) = any(input_source_nodes)) then
        RAISE EXCEPTION 'some nodes in the parameter input_source_nodes are not in this citus cluster or not primary note.';
    end if;

    -- 如果本次缩容把所有的存储分片表的WK都去掉了，也有问题，报错退出
    if (select count(*) = 0 from pg_dist_node where noderole = 'primary' and shouldhaveshards = true and concat(nodename, ':', nodeport) <> all(input_source_nodes)) then
        RAISE EXCEPTION 'there are no normal workers left for shards migration after delete %.',input_source_nodes;
    end if;
    
    select array_agg(concat(nodename, ':', nodeport))
    into strict target_node_list
    from pg_dist_node
    where noderole = 'primary' and shouldhaveshards = true and concat(nodename, ':', nodeport) <> all(input_source_nodes);
    
    select array_agg(concat(nodename, ':', nodeport)) into source_node_list from pg_dist_node where noderole = 'primary';
    
    perform cigration.cigration_print_log('cigration_create_drain_node_job','call function cigration_create_general_shard_migration_job to generate a migration job.');
    select cigration.cigration_create_general_shard_migration_job(source_node_list,target_node_list) into var_jobid;
    
    RETURN QUERY select tb.jobid,tb.taskid,tb.all_colocated_shards_id,tb.source_nodename,tb.source_nodeport,tb.target_nodename,tb.target_nodeport,tb.total_shard_count,tb.total_shard_size
                 from cigration.pg_citus_shard_migration as tb where tb.jobid = var_jobid;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION cigration.cigration_create_rebalance_job()
RETURNS TABLE(jobid integer,taskid integer,all_colocated_shards_id bigint[],source_nodename text,source_nodeport integer,target_nodename text,target_nodeport integer,total_shard_count int,total_shard_size bigint)
-- 
-- 函数名：cigration.cigration_create_rebalance_job
-- 函数功能：创建一个扩容/再平衡场景的JOB
-- 参数：无
-- 返回值：返回生成的迁移任务记录
-- 
AS $$
DECLARE
    -- 连接信息
    source_node_list text[];
    target_node_list text[];

    var_jobid int := 0;
BEGIN
    --执行节点必须是CN节点
    IF (SELECT CASE WHEN (select count(*) from pg_dist_node)>0 THEN (select groupid from pg_dist_local_group) ELSE -1 END) <> 0 THEN
        RAISE EXCEPTION 'function cigration_create_rebalance_job could only be executed on coordinate node.';
    END IF;
    
    --检查是否有未归档的JOB
    if (select count(*) <> 0 from cigration.pg_citus_shard_migration) then
        RAISE EXCEPTION 'can not create a new job when there are some uncleanuped jobs.';
    end if;
    
    --检查集群健康状态
    if (SELECT count(*) <> 0 FROM pg_dist_node WHERE noderole = 'primary' AND isactive = 'f') then
        RAISE EXCEPTION 'there are some invalid primary node in the cluster.';
    end if;
    
    -- 计算分片迁移的策略
    select array_agg(concat(nodename, ':', nodeport)) into source_node_list from pg_dist_node where noderole = 'primary';
    
    select array_agg(concat(nodename, ':', nodeport)) into strict target_node_list from pg_dist_node where noderole = 'primary' and shouldhaveshards = true;
    
    perform cigration.cigration_print_log('cigration_create_rebalance_job','call function cigration_create_general_shard_migration_job to generate a migration job.');
    select cigration.cigration_create_general_shard_migration_job(source_node_list,target_node_list) into var_jobid;
    
    RETURN QUERY select tb.jobid,tb.taskid,tb.all_colocated_shards_id,tb.source_nodename,tb.source_nodeport,tb.target_nodename,tb.target_nodeport,tb.total_shard_count,tb.total_shard_size
                 from cigration.pg_citus_shard_migration as tb where tb.jobid = var_jobid;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION cigration.cigration_create_move_node_job(input_source_nodename text, input_source_nodeport integer, input_target_nodename text, input_target_nodeport integer)
RETURNS TABLE(jobid integer,taskid integer,all_colocated_shards_id bigint[],source_nodename text,source_nodeport integer,target_nodename text,target_nodeport integer,total_shard_count int,total_shard_size bigint)
-- 
-- 函数名：cigration.cigration_create_move_node_job
-- 函数功能：创建一个迁移场景的JOB
-- 参数：input_source_nodename text，形式类似：'192.168.1.1'
--       input_source_nodeport integer,
--       input_target_nodename text，形式类似：'192.168.1.2'
--       input_target_nodeport integer
-- 返回值：返回生成的迁移任务记录
-- 
AS $$
DECLARE
    -- 连接信息
    source_node_list text[];
    target_node_list text[];

    var_shouldhaveshards boolean := null;
    var_jobid int := 0;
BEGIN
    -- 执行节点必须是CN节点
    IF (SELECT CASE WHEN (select count(*) from pg_dist_node)>0 THEN (select groupid from pg_dist_local_group) ELSE -1 END) <> 0 THEN
        RAISE EXCEPTION 'function cigration_create_move_node_job could only be executed on coordinate node.';
    END IF;
    
    --检查是否有未归档的JOB
    if (select count(*) <> 0 from cigration.pg_citus_shard_migration) then
        RAISE EXCEPTION 'can not create a new job when there are some uncleanuped jobs.';
    end if;
    
    --检查集群健康状态
    if (SELECT count(*) <> 0 FROM pg_dist_node WHERE noderole = 'primary' AND isactive = 'f') then
        RAISE EXCEPTION 'there are some invalid primary node in the cluster.';
    end if;

    -- 判断输入源节点是否在集群中，不在就报错
    if (select count(*) <> 1 from pg_dist_node where noderole = 'primary' and nodename = input_source_nodename and nodeport = input_source_nodeport) then
        RAISE EXCEPTION 'the source node is not in this citus cluster or not primary note.';
    end if;

    select shouldhaveshards into var_shouldhaveshards from pg_dist_node where noderole = 'primary' and nodename = input_target_nodename and nodeport = input_target_nodeport;
    
    -- 判断输入目的节点是否在集群中，不在就报错
    if var_shouldhaveshards is null then
        RAISE EXCEPTION 'the target node is not in this citus cluster or not primary note.';
    -- 如果目标节点的shouldhaveshards是false的话，报错退出
    elsif var_shouldhaveshards = false then
        RAISE EXCEPTION 'The shouldhaveshards property of the target nodes can not be false.';
    end if;

    perform cigration.cigration_print_log('cigration_create_move_node_job','call function cigration_create_general_shard_migration_job to generate a migration job.');
    select cigration.cigration_create_general_shard_migration_job(array[concat(input_source_nodename, ':', input_source_nodeport)], array[concat(input_target_nodename, ':', input_target_nodeport)]) into var_jobid;
    
    RETURN QUERY select tb.jobid,tb.taskid,tb.all_colocated_shards_id,tb.source_nodename,tb.source_nodeport,tb.target_nodename,tb.target_nodeport,tb.total_shard_count,tb.total_shard_size
                 from cigration.pg_citus_shard_migration as tb where tb.jobid = var_jobid;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION cigration.cigration_create_general_shard_migration_job(source_nodes text[],target_nodes text[])
RETURNS int
-- 
-- 函数名：cigration.cigration_create_general_shard_migration_job
-- 函数功能：实际创建迁移JOB，计算分片迁移的策略，得出源上的哪些分片需要迁移到哪些目标上
-- 参数：source_nodes text[]，形式类似：array['192.168.1.1:5432','xx.xx.xx.xx:5432']
--       target_nodes text[]，形式类似：array['192.168.1.2:5432']
-- 返回值：int job的id值
-- 
AS $cigration_create_general_shard_migration_job$
DECLARE
    logical_tb record;
    parent_tbnames regclass[];
    tbname_list_without_parenttb regclass[];
    representative_rel regclass;
    shard_count integer;
    mix_unlogged_colocationid_count integer;
    
    upper_limit integer;
    lower_limit integer;
    target_upper_limit integer;
    worker_count integer;
    current_shard_count integer;
    
    source_node_element text;
    source_nodename text;
    source_nodeport integer;
    shard_to_move bigint[];
    tmp_shardid bigint;
    
    -- task相关的变量
    var_total_shard_count int;
    var_all_colocated_shards_id bigint[];
    parent_shardid_list bigint[];
    parent_logicalrel_list text[];
    var_total_shard_size bigint;
    var_all_colocated_shards_size bigint[];
    var_all_colocated_logicalrels text[];
    target_node_element text;
    target_nodename text;
    target_nodeport integer;
    target_has_been_chosen text[];
    found_target_node boolean;
    
    --var_source_nodeport integer;
    --var_target_nodeport integer;
    var_jobid integer;
    
    create_schema_info text;
    create_schema_execute_info record;
BEGIN
    -- 执行节点必须是CN节点
    IF (SELECT CASE WHEN (select count(*) from pg_dist_node)>0 THEN (select groupid from pg_dist_local_group) ELSE -1 END) <> 0 THEN
        RAISE EXCEPTION 'function cigration_create_move_node_job could only be executed on coordinate node.';
    END IF;

    IF NOT (select pg_try_advisory_xact_lock('cigration.pg_citus_shard_migration'::regclass::int)) THEN
        RAISE EXCEPTION 'Can not call cigration api concurrently.';
    END IF;

    --检查是否有涉及输入节点的未归档的JOB
    if (select count(*) <> 0 from cigration.pg_citus_shard_migration m
        where array[concat(m.source_nodename,':',m.source_nodeport), concat(m.target_nodename,':',m.target_nodeport)] && 
              (source_nodes || target_nodes)
       ) then
        RAISE EXCEPTION 'can not create a new job when there are some uncleanuped jobs in source_nodes:% or target_nodes:%.',source_nodes,target_nodes;
    end if;
    
    --检查集群健康状态
    if (SELECT count(*) <> 0 FROM pg_dist_node WHERE noderole = 'primary' AND isactive = 'f') then
        RAISE EXCEPTION 'there are some invalid primary node in the cluster.';
    end if;

    -- 输入参数检查
    if (array_length(source_nodes,1) = 0) then
        RAISE EXCEPTION 'source_nodes is empty';
    end if;

    if (array_length(target_nodes,1) = 0) then
        RAISE EXCEPTION 'target_nodes is empty';
    end if;

    -- 判断输入源节点是否在集群中，不在就报错
    if (select count(*) <> array_length(source_nodes,1) from pg_dist_node where noderole = 'primary' and concat(nodename, ':', nodeport) = any(source_nodes)) then
        RAISE EXCEPTION 'some nodes in the parameter source_nodes are not in this citus cluster or not primary note.';
    end if;

    -- 判断输入目的节点是否在集群中，不在就报错
    if (select count(*) <> array_length(target_nodes,1) from pg_dist_node where noderole = 'primary' and concat(nodename, ':', nodeport) = any(target_nodes)) then
        RAISE EXCEPTION 'some nodes in the parameter target_nodes are not in this citus cluster or not primary note.';
    end if;
    
    -- 判断输入目的节点的shouldhaveshards属性是否都为true，不满足就报错
    if (select count(*) <> array_length(target_nodes,1) from pg_dist_node where shouldhaveshards = true and concat(nodename, ':', nodeport) = any(target_nodes)) then
        RAISE EXCEPTION 'some nodes in the parameter target_nodes are not in this citus cluster or not primary note.';
    end if;
    
    -- 检查是否存在和普通分片表亲和的unlogged分片表
    with t as
    (
        select logicalrelid, count(*) shard_count
        from (select logicalrelid, min(shardid) min_shardid from pg_dist_shard group by logicalrelid) s 
             join pg_dist_placement p on(s.min_shardid = p.shardid)
        group by logicalrelid
    ),t2 as(
    select d.colocationid, count(distinct c.relpersistence) persistence_count
    from pg_dist_partition d 
         join pg_class c on(d.logicalrelid = c.oid)
         join t on( d.logicalrelid = t.logicalrelid)
    where partmethod = 'h' and t.shard_count = 1
    group by d.colocationid
    having count(distinct c.relpersistence) > 1
    )
    select count(*) into mix_unlogged_colocationid_count from t2;
    
    if (mix_unlogged_colocationid_count > 0) then
        RAISE EXCEPTION 'there are unlogged distributed table which colocate with normal distributed table in % colocation groups.', mix_unlogged_colocationid_count;
    end if;

    -- 生成每次任务的ID
    select nextval('cigration.jobid_seq') into strict var_jobid;
    
    foreach source_node_element in array source_nodes
    -- 按迁移源节点轮询，依次算出每个源上有哪些分片需要迁移走
    loop	
        -- debug info
        raise debug 'source_node_element:%',source_node_element;
        source_nodename := split_part(source_node_element, ':', 1);
        source_nodeport := split_part(source_node_element, ':', 2)::int;

        for logical_tb in
            with t as
            (
                select logicalrelid, count(*) shard_count
                from (select logicalrelid, min(shardid) min_shardid from pg_dist_shard group by logicalrelid) s 
                     join pg_dist_placement p on(s.min_shardid = p.shardid)
                group by logicalrelid
            )
            select array_agg(d.logicalrelid) as tb_list, d.colocationid
            from pg_dist_partition d 
                 join pg_class c on(d.logicalrelid = c.oid)
                 join t on( d.logicalrelid = t.logicalrelid)
            where partmethod = 'h' and c.relpersistence = 'p' and t.shard_count = 1
            group by d.colocationid
        loop
            -- 把分区主表筛选出来，如果有的话
            -- 分区主表不先筛选出来，后面选择亲和的分片时，每个分区都会和分区主表亲和一次，会冗余产生冲突
            select array_agg(inhparent)
            into parent_tbnames
            from pg_inherits where inhparent in (select unnest(logical_tb.tb_list));
            
            if (parent_tbnames is null) then
                -- 选一个分片作为这组亲和表的代表
                select logical_tb.tb_list[1] into strict representative_rel;
            else
                -- 去掉分区主表之后的所有表
                select array_agg(tmp)
                into strict tbname_list_without_parenttb 
                from unnest(logical_tb.tb_list) tmp
                where tmp not in (select unnest(parent_tbnames));
                
                -- 选一个分片作为这组亲和表的代表
                select tbname_list_without_parenttb[1] into strict representative_rel;
            end if;
            
            -- 获取该组亲和表的分片数           
            select count(*)
            into strict shard_count
            from pg_dist_shard_placement
            where concat(nodename, ':', nodeport) = any(source_nodes || target_nodes)
                  and shardid in (select shardid from pg_dist_shard where logicalrelid = representative_rel);
            
            -- debug info
            raise debug 'tb:% has % shard in node %',representative_rel,shard_count,source_nodes || target_nodes;
        
            -- 算出均衡值的上下限
            -- 如果这个源不在目标节点中，说明该节点是需要缩容的掉的，这种情况下这个节点上的那么平衡值的上限就是0
            worker_count := array_length(target_nodes,1);
            if (array_position(target_nodes,source_node_element) is null) then
                upper_limit := 0;
            else
                upper_limit := (shard_count + worker_count - 1) / worker_count;
            end if;
            -- 选择目标节点的时候，用于判断该目标节点是否已经均衡的上限值
            target_upper_limit := (shard_count + worker_count - 1) / worker_count;
            -- lower_limit := shard_count / worker_count;
            -- debug info
            raise debug 'upper_limit is % and target_upper_limit is %',upper_limit,target_upper_limit;
            
            -- 满足这个条件的即为均衡状态：分片数/worker集群数 <= worker上的分片数 <= (分片数+worker集群数-1)/worker集群数
            -- 选出所有的需要从源迁走的分片
            with ordered_shardid as
            (select a.shardid
			 from pg_dist_shard_placement a,pg_dist_shard b
			 where a.shardid = b.shardid and b.logicalrelid = representative_rel and a.nodename = source_nodename and a.nodeport = source_nodeport
			 order by a.shardid offset upper_limit)
            select array_agg(shardid) into shard_to_move from ordered_shardid;
            
            if (shard_to_move is not null) then
                foreach tmp_shardid in array shard_to_move
                loop
                    -- 找出与tmp_shardid_list亲和的所有分片
                    if ( parent_tbnames is not null ) then
                        SELECT count(*),array_agg(shardid),array_agg((select identity from pg_identify_object('pg_class'::regclass,logicalrelid,0)))
                        into strict var_total_shard_count,var_all_colocated_shards_id,var_all_colocated_logicalrels
                        FROM pg_dist_shard 
                        WHERE shardid in (select shardid from pg_dist_shard where logicalrelid in (select unnest(tbname_list_without_parenttb)::regclass)) and (shardminvalue,shardmaxvalue)=(select shardminvalue,shardmaxvalue from pg_dist_shard where shardid = tmp_shardid ORDER BY shardid ASC);
                    else
                        SELECT count(*),array_agg(shardid),array_agg((select identity from pg_identify_object('pg_class'::regclass,logicalrelid,0)))
                        into strict var_total_shard_count,var_all_colocated_shards_id,var_all_colocated_logicalrels
                        FROM pg_dist_shard 
                        WHERE shardid in (select shardid from pg_dist_shard where logicalrelid in (select unnest(logical_tb.tb_list)::regclass)) and (shardminvalue,shardmaxvalue)=(select shardminvalue,shardmaxvalue from pg_dist_shard where shardid = tmp_shardid ORDER BY shardid ASC);
                    end if;
                    
                    -- 找出这些分片的大小，单位B
                    select sum(result.shard_size),array_agg(result.shard_size)
                    into strict var_total_shard_size,var_all_colocated_shards_size
                    from cigration.cigration_get_shard_size(var_all_colocated_shards_id) as result;
                    
                    -- 将分区主表加到var_all_colocated_shards_id列中
                    if ( parent_tbnames is not null ) then
                        select array_agg(a.shardid),array_agg((select identity from pg_identify_object('pg_class'::regclass,a.logicalrelid,0)))
                        into strict parent_shardid_list,parent_logicalrel_list
                        from pg_dist_shard a,pg_dist_shard_placement b 
                        where a.shardid = b.shardid
						      and logicalrelid in (select unnest(parent_tbnames)::regclass)
						      and b.nodename = source_nodename and b.nodeport = source_nodeport
							  and (shardminvalue,shardmaxvalue)=(select shardminvalue,shardmaxvalue from pg_dist_shard where shardid = tmp_shardid ORDER BY shardid ASC);

                        var_total_shard_count := var_total_shard_count + array_length(parent_shardid_list,1);
                        var_all_colocated_shards_id := array_cat(parent_shardid_list,var_all_colocated_shards_id);
                        var_all_colocated_logicalrels := array_cat(parent_logicalrel_list,var_all_colocated_logicalrels);

                        select array_cat(array_agg(0::bigint),var_all_colocated_shards_size)
                        into strict var_all_colocated_shards_size
                        from generate_series(1,array_length(parent_shardid_list,1))id;
                    end if;

                    found_target_node := false;
                    foreach target_node_element in array target_nodes
                    loop
                        target_nodename := split_part(target_node_element, ':', 1);
                        target_nodeport := split_part(target_node_element, ':', 2)::int;

                        -- 计算每个task的目标节点
                        -- 先计算当前这个目标节点上已经有多少个分片了
                        select count(*)
                        into current_shard_count
                        from pg_dist_shard_placement
                        where nodename = target_nodename and nodeport = target_nodeport
						      and shardid in (select shardid from pg_dist_shard where logicalrelid = representative_rel);
                        
                        if (array_length(array_positions(target_has_been_chosen,logical_tb.colocationid || ':' || target_node_element),1) is not null) then
                            -- 如果当前这个目标节点已经被选择过，那么节点上分片数必须加上被选择过的次数（动态平衡）
                            current_shard_count := current_shard_count + array_length(array_positions(target_has_been_chosen,logical_tb.colocationid || ':' || target_node_element),1);
                            
                            raise debug 'current_shard_count : % ',current_shard_count;
                        end if;
                        
                        if (current_shard_count < target_upper_limit) then
                            found_target_node := true;
                            -- 每次选择一个目标节点就记录一次
                            select array_append(target_has_been_chosen,logical_tb.colocationid || ':' || target_node_element) into target_has_been_chosen;
                            -- debug info
                            raise debug '% has been chosen to be the target node',target_node_element;
                            raise debug 'target_has_been_chosen : % ',target_has_been_chosen;
                            
                            -- 将该task信息写入job表中，需要判断入参中提供的端口号是空还是具体值
                            insert into cigration.pg_citus_shard_migration (jobid,source_nodename,source_nodeport,target_nodename,target_nodeport,
                                                                            colocationid,all_colocated_shards_id,all_colocated_shards_size,all_colocated_logicalrels,
                                                                            total_shard_count,total_shard_size,create_time)
                            values (var_jobid,source_nodename,source_nodeport,target_nodename,target_nodeport,
                                    logical_tb.colocationid,var_all_colocated_shards_id,var_all_colocated_shards_size,var_all_colocated_logicalrels,
                                    var_total_shard_count,var_total_shard_size,now());

                            exit;
                        end if;
                    end loop;
                    
                    if (not found_target_node) then
                        raise exception 'There is no suitable target node for shard migration after go through all targets.';
                    end if;
                                       
                end loop;
            else
                -- 这组亲和表在这个WK上没有需要迁移的分片
                continue;
            end if;
        end loop;
    end loop;
    
    -- CREATE BACKUP SCHEMA ON ALL WORKER NODES
    create_schema_info := format('CREATE SCHEMA IF NOT EXISTS cigration_recyclebin_%s', var_jobid);
    
    for create_schema_execute_info in SELECT nodename, success, result FROM run_command_on_workers(create_schema_info)
    loop
        if create_schema_execute_info.success = 'f' then
            RAISE EXCEPTION '%', format('create schema failed on worker[%s],details:%s', 
                    create_schema_execute_info.nodename, create_schema_execute_info.result);
        end if;
    end loop;
    
    RAISE DEBUG 'cigration_create_general_shard_migration_job : % ',create_schema_info;
    
    return var_jobid;
END;
$cigration_create_general_shard_migration_job$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION cigration.cigration_start_shard_migration_task(jobid_input int,
                                                             taskid_input int, 
                                                             longtime_tx_threshold interval default '30 min',
                                                             with_replica_identity_check boolean default false)
RETURNS text
-- 
-- 函数名：cigration.cigration_start_shard_migration_task
-- 函数功能：启动单个task
-- 参数：jobid_input integer,
--       taskid_input integer,
--       longtime_tx_threshold, 迁移任务时允许的最大长事务执行时间
--       with_replica_identity_check 任务启动时是否检查replica_identity
-- 返回值：返回任务的启动结果，init_succeed：启动成功，如果有异常，直接抛出exception
-- 
AS $cigration_start_shard_migration_task$
DECLARE
    var_source_nodename text;
    var_source_nodeport int;
    var_target_nodename text;
    var_target_nodeport int;
    error_msg text;
    -- 分片迁移函数的返回值
    shardmove_func_result boolean;
    -- dblink参数
    dblink_update_state text := 'migration_update_state_tb';
    default_port int;
BEGIN
    -- 执行节点必须是CN节点
    IF (SELECT CASE WHEN (select count(*) from pg_dist_node)>0 THEN (select groupid from pg_dist_local_group) ELSE -1 END) <> 0 THEN
        RAISE EXCEPTION 'function cigration_start_shard_migration_task could only be executed on coordinate node.';
    END IF;

    SELECT setting INTO default_port FROM pg_settings where name='port';
    
    -- 异常判断，如果这个task不存在或者状态不是init或cancel，报错退出
    if (select case count(*) when 0 then true else false end from cigration.pg_citus_shard_migration where taskid = taskid_input and jobid = jobid_input and status in ('init','canceled')) then
        RAISE EXCEPTION 'jobid:[%] task:[%] does not exists or the status is not "init".',jobid_input,taskid_input;
    end if;
    
    -- 调用分片迁移函数，根据函数返回值判断最终的返回值
    select cigration.cigration_move_shard_placement(jobid_input,taskid_input,longtime_tx_threshold,with_replica_identity_check) into strict shardmove_func_result;
    
    if (shardmove_func_result) then
        update cigration.pg_citus_shard_migration set status = 'running',start_time = now() where taskid = taskid_input and jobid = jobid_input;
        return 'init_succeed';
    else
        raise exception 'task % in job % start failed.check errors in the task_info table:cigration.pg_citus_shard_migration.',taskid_input,jobid_input;
    end if;
    
EXCEPTION 
    WHEN QUERY_CANCELED THEN
        GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;

        PERFORM dblink_connect(dblink_update_state,format('host=%s port=%s dbname=%s user=%s','127.0.0.1',default_port,current_database(),current_user));
        PERFORM dblink_exec(dblink_update_state,format('update cigration.pg_citus_shard_migration set status = ''canceled'' where taskid = %s and jobid = %s',taskid_input,jobid_input));
        PERFORM dblink_disconnect(dblink_update_state);
        raise;
    
    WHEN OTHERS THEN
        -- GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
        GET STACKED DIAGNOSTICS error_msg = PG_EXCEPTION_CONTEXT;
        -- debug 信息
        raise debug 'func:cigration_start_shard_migration_task encountered some errors.';
        raise debug 'error_msg:%',error_msg;
        raise;
END;
$cigration_start_shard_migration_task$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION cigration.cigration_complete_shard_migration_task(jobid_input integer,
                                                                taskid_input integer,
                                                                init_sync_timeout integer default 0,
                                                                data_sync_timeout integer default 15,
                                                                delay_threshold bigint default 104857600,
                                                                lock_timeout integer default 5,
                                                                check_only boolean default false)
RETURNS text
-- 
-- 函数名：cigration.cigration_complete_shard_migration_task
-- 函数功能：检查指定任务的迁移进度，如果已经迁移完毕，就结束任务
-- 参数： jobid_input integer
--        taskid_input integer
--        init_sync_timeout integer (单位s)
--        data_sync_timeout integer (单位s)
--        delay_threshold bigint (单位B)
--        lock_timeout integer (单位s)
--        check_only boolean 
-- 返回值：complete：元数据切换完成(迁移任务完成)
--         wait_init_data_sync：等待数据初始同步，重新执行本函数
--         replication_broken: 增量数据同步阶段复制断开，修复错误后重新执行本函数
--         wait_data_sync：等待增量数据同步，重新执行本函数
--         lock_timeout: 锁超时退出，重新执行本函数
--         data_sync: 数据已同步，但是未更新元数据（仅检查迁移状态时返回）
-- 
AS $complete_shard_migration_task$
DECLARE
    -- task信息的变量
    shardid_list bigint[];
    source_node_name text;
    target_node_name text;
    i_source_node_port integer;
    i_target_node_port integer;
    logical_relid_list regclass[];
    task_status cigration.migration_status;
    
    -- 用于判断逻辑复制延迟的变量
    t_start_sync_time timestamptz;
    t_start_getlock_time timestamptz;
    sub_rel_count_srsubid bigint;
    source_wal_lsn pg_lsn;
    sub_lag numeric;
    sleep_time integer := 0;
    dblink_source_name text := 'migration_get_source_wal_lsn_con';
    dblink_target_name text := 'migration_get_target_wal_lsn_con';
    dblink_exwk_name text;
    
    -- 循环变量
    logical_relid text;
    shard bigint;
    exwk record;
    
    --异常处理变量
    error_msg text;
    
    -- insert into cigration.pg_citus_shard_migration_sql_log
    execute_sql text;

BEGIN
    -- 执行节点必须是CN节点
    IF (SELECT CASE WHEN (select count(*) from pg_dist_node)>0 THEN (select groupid from pg_dist_local_group) ELSE -1 END) <> 0 THEN
        RAISE EXCEPTION 'function cigration_complete_shard_migration_task could only be executed on coordinate node.';
    END IF;
    
    -- 如果该task不是running状态，直接退出
    select status
    into strict task_status
    from cigration.pg_citus_shard_migration where taskid = taskid_input and jobid = jobid_input;
    
    if (task_status <> 'running' and check_only is false) then
        raise exception 'only running status is supported,but now is %.',task_status;
    elsif (task_status <> 'running' and check_only is true) then
        return 'task_is_not_running';
    end if;
    
    -- 获取task相关的信息
    select all_colocated_shards_id,source_nodename,source_nodeport,target_nodename,target_nodeport
    into strict shardid_list,source_node_name,i_source_node_port,target_node_name,i_target_node_port
    from cigration.pg_citus_shard_migration
    where taskid = taskid_input and jobid = jobid_input;

    -- 获取该task的逻辑表名列表和分片的完整表名
    foreach shard in array shardid_list
    loop
        select array_append(logical_relid_list, logicalrelid)
        into logical_relid_list
        from pg_dist_shard 
        where shardid = shard and 
              logicalrelid not in (select inhparent from pg_inherits);
    end loop;
    
    -- debug info 
    raise debug 'logical_relid_list:%',logical_relid_list;
    
    -- 创建到源和目标节点的dblink
    PERFORM dblink_connect(dblink_source_name,format('host=%s port=%s user=%s dbname=%s',source_node_name,i_source_node_port,current_user,current_database()));
    
    PERFORM dblink_connect(dblink_target_name,format('host=%s port=%s user=%s dbname=%s',target_node_name,i_target_node_port,current_user,current_database()));
    
    SELECT clock_timestamp() INTO t_start_sync_time;
    
    loop
        -- 判断初始同步是否完成
        SELECT count_srsubid
        INTO STRICT sub_rel_count_srsubid
        FROM dblink(dblink_target_name,
                            $$SELECT count(srsubid) count_srsubid from pg_subscription, pg_subscription_rel
                              WHERE pg_subscription.oid = pg_subscription_rel.srsubid AND
                                    pg_subscription.subname = 'citus_move_shard_placement_sub' AND
                                    (pg_subscription_rel.srsubstate = 's' OR pg_subscription_rel.srsubstate = 'r')$$
                      ) AS t(count_srsubid int);

        IF sub_rel_count_srsubid = array_length(logical_relid_list,1) THEN --初始同步已完成      
            LOOP
                -- 进一步判断增量同步是否完成
                SELECT sourcewallsn
                INTO STRICT source_wal_lsn
                FROM dblink(dblink_source_name, $$select pg_current_wal_lsn()$$) AS t(sourcewallsn pg_lsn);

                SELECT lag
                INTO sub_lag
                FROM dblink(dblink_source_name, 
                                format($$select pg_wal_lsn_diff('%s',replay_lsn)
                                         FROM pg_stat_replication
                                         WHERE application_name = 'citus_move_shard_placement_sub' AND replay_lsn is not NULL$$,
                                         source_wal_lsn::text)
                          ) AS t(lag numeric);

                IF sub_lag is NULL THEN -- 复制断开
                    -- 清理掉dblink后直接退出
                    PERFORM dblink_disconnect(con)
                    FROM (select unnest(a) con from dblink_get_connections() a)b 
                    WHERE con in (dblink_source_name,dblink_target_name);

                    RETURN 'replication_broken';
                END IF;

                -- debug info
                raise debug 'WAL delay in incremental synchronization stage is % B,delay_threshold is % B.',sub_lag,delay_threshold;

                IF sub_lag <= delay_threshold THEN -- 延迟小于指定的阈值，尝试获取锁阻止更新，完成迁移
                    perform cigration.cigration_print_log('cigration_complete_shard_migration_task','init data sync has completed.');
                    SELECT clock_timestamp() INTO t_start_getlock_time;
                    
                    -- 锁住正在迁移的分片表
                    if (check_only is false) then
                        -- 先建立到各个扩展WK的dblink
                        for exwk in select nodename,nodeport from pg_dist_node where noderole = 'primary' and hasmetadata = 't' order by nodename,nodeport
                        loop
                            dblink_exwk_name := concat('exwk_con_' ,exwk.nodename, '_', exwk.nodeport);
                            perform dblink_connect(dblink_exwk_name,format('host=%s port=%s dbname=%s user=%s',exwk.nodename,exwk.nodeport,current_database(),current_user));
                            perform dblink_exec(dblink_exwk_name,'begin');
                            perform dblink_exec(dblink_exwk_name,format('SET lock_timeout= ''%s s''',lock_timeout));
                        end loop;
                    
                        EXECUTE format('SET lock_timeout= ''%s s''',lock_timeout);
                        foreach logical_relid in array logical_relid_list
                        loop
                            EXECUTE format('LOCK TABLE %s IN EXCLUSIVE MODE', logical_relid);
                            for exwk in select nodename,nodeport from pg_dist_node where noderole = 'primary' and hasmetadata = 't' order by nodename
                            loop
                                dblink_exwk_name := concat('exwk_con_' ,exwk.nodename, '_', exwk.nodeport);
                                perform dblink_exec(dblink_exwk_name,format('LOCK TABLE %s IN EXCLUSIVE MODE', logical_relid));
                            end loop;
                        end loop;
                    end if;
                    
                    -- 停更新之后，等待数据同步
                    SELECT sourcewallsn
                    INTO STRICT source_wal_lsn
                    FROM dblink(dblink_source_name, $$select pg_current_wal_lsn()$$) AS t(sourcewallsn pg_lsn);
                    loop
                        SELECT lag
                        INTO sub_lag
                        FROM dblink(dblink_source_name, 
                                        format($$select pg_wal_lsn_diff('%s',replay_lsn)
                                                 FROM pg_stat_replication
                                                 WHERE application_name = 'citus_move_shard_placement_sub' AND replay_lsn is not NULL$$,
                                                 source_wal_lsn::text)
                                  ) AS t(lag numeric);

                        IF sub_lag is NULL THEN -- 复制断开
                            -- 清理掉dblink后直接退出
                            PERFORM dblink_disconnect(con)
                            FROM (select unnest(a) con from dblink_get_connections() a)b 
                            WHERE con in (dblink_source_name,dblink_target_name);

                            RETURN 'replication_broken';
                        END IF;

                        raise debug 'WAL delay in incremental synchronization stage is % B, source_wal_lsn is %.', sub_lag, source_wal_lsn;

                        IF sub_lag <= 0 THEN
                            perform cigration.cigration_print_log('cigration_complete_shard_migration_task','ready to update metedata.');
                            if (check_only is false) then
                                -- 更新元数据
                                perform cigration.cigration_update_metadata_after_migration(jobid_input,taskid_input);
                                
                                -- 清理掉发布和订阅
                                perform cigration.cigration_print_log('cigration_complete_shard_migration_task','cleanup subscription and publication.');
                                BEGIN
                                    PERFORM dblink_exec(dblink_target_name,'ALTER SUBSCRIPTION citus_move_shard_placement_sub DISABLE');
                                    PERFORM dblink_exec(dblink_target_name, 'DROP SUBSCRIPTION IF EXISTS citus_move_shard_placement_sub CASCADE');
                                    execute_sql := format('insert into cigration.pg_citus_shard_migration_sql_log (jobid, taskid, execute_nodename, execute_nodeport, functionid, sql) values(%L, %L, %L, %L, %L, %L)', 
                                            jobid_input, taskid_input, target_node_name, i_target_node_port,
                                            'cigration.cigration_complete_shard_migration_task',
                                            'DROP SUBSCRIPTION IF EXISTS citus_move_shard_placement_sub CASCADE');
                                    execute execute_sql;
                                EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                                    GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                                    RAISE EXCEPTION 'failed to DROP SUBSCRIPTION citus_move_shard_placement_sub in target node:%', error_msg;
                                END;
                            
                                BEGIN
                                    PERFORM dblink_exec(dblink_source_name,'DROP PUBLICATION IF EXISTS citus_move_shard_placement_pub CASCADE');
                                    execute_sql := format('insert into cigration.pg_citus_shard_migration_sql_log (jobid, taskid, execute_nodename, execute_nodeport, functionid, sql) values(%L, %L, %L, %L, %L, %L)', 
                                            jobid_input, taskid_input, source_node_name, i_source_node_port,
                                            'cigration.cigration_complete_shard_migration_task',
                                            'DROP PUBLICATION IF EXISTS citus_move_shard_placement_pub CASCADE');
                                    execute execute_sql;
                                EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                                    GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                                    RAISE EXCEPTION 'failed to DROP PUBLICATION citus_move_shard_placement_pub in source node:%', error_msg;
                                END;
            
                                -- 清理掉全部的dblink后直接退出
                                PERFORM dblink_disconnect(con)
                                FROM (select unnest(a) con from dblink_get_connections() a)b 
                                WHERE con like 'migration%';
                                
                                -- 更新JOB表
                                perform cigration.cigration_print_log('cigration_complete_shard_migration_task','update task status.');
                                update cigration.pg_citus_shard_migration set status = 'completed',end_time = clock_timestamp() where taskid = taskid_input and jobid = jobid_input;
                                
                                return 'complete';
                            else
                                -- 清理掉全部的dblink后直接退出
                                PERFORM dblink_disconnect(con)
                                FROM (select unnest(a) con from dblink_get_connections() a)b 
                                WHERE con like 'migration%';
                            
                                -- 只检查同步状态，不更新元数据
                                return 'data_sync';
                            end if;
                        ELSE
                            if sleep_time < data_sync_timeout then -- 最多等data_sync_timeout
                                sleep_time := sleep_time + 1;
                                perform pg_sleep(1);
                            else
                                -- 清理掉dblink后直接退出
                                PERFORM dblink_disconnect(con)
                                FROM (select unnest(a) con from dblink_get_connections() a)b 
                                WHERE con in (dblink_source_name,dblink_target_name);

                                return 'wait_data_sync';
                            end if;
                        END IF;
                    end loop;
                ELSE -- 增量同步的延迟大于阈值，退出
                    -- 清理掉dblink后直接退出
                    PERFORM dblink_disconnect(con) 
                    FROM (select unnest(a) con from dblink_get_connections() a)b 
                    WHERE con in (dblink_source_name,dblink_target_name);
                    
                    return 'wait_data_sync';
                END IF;
            END LOOP;
        ELSE
            if (init_sync_timeout = 0) then -- 初始同步未完成，如果同步超时阈值为0，直接退出
                -- 清理掉dblink后直接退出
                PERFORM dblink_disconnect(con) 
                FROM (select unnest(a) con from dblink_get_connections() a)b 
                WHERE con in (dblink_source_name,dblink_target_name);
                
                return 'wait_init_data_sync';
            elsif (init_sync_timeout = -1) then -- 无限等待
                perform pg_sleep(1);
            else -- 超时的话就退出
                IF EXTRACT(EPOCH from clock_timestamp() - t_start_sync_time) > init_sync_timeout THEN
                    -- 清理掉dblink后直接退出
                    PERFORM dblink_disconnect(con) 
                    FROM (select unnest(a) con from dblink_get_connections() a)b 
                    WHERE con in (dblink_source_name,dblink_target_name);

                    return 'wait_init_data_sync';
                ELSE
                    perform pg_sleep(1);
                END IF;
            end if;
        END IF;
    end loop;
    
EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
    PERFORM dblink_disconnect(con) 
    FROM (select unnest(a) con from dblink_get_connections() a)b 
    WHERE con like 'migration%' or con like 'exwk_con_%';
    
    GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
    if error_msg = 'canceling statement due to lock timeout' then
        return 'lock_timeout';
    end if;
    
    raise;
END;
$complete_shard_migration_task$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION cigration.cigration_monitor_shard_migration_task(jobid_input integer,
                                                               taskid_input integer)
RETURNS text
-- 
-- 函数名：cigration.cigration_monitor_shard_migration_task: 
-- 函数功能：只检查指定任务的迁移进度，不更新元数据
-- 参数: jobid_input integer
--       taskid_input integer
-- 返回值: wait_init_data_sync：等待数据初始同步，重新执行本函数
--         wait_data_sync：等待增量数据同步，重新执行本函数
--         data_sync:数据已同步，但是未更新元数据（仅检查迁移状态时返回）
--         task_is_not_running:监控了一个非running状态的task
-- 
AS $cigration_monitor_shard_migration_task$
DECLARE
    result text;
BEGIN
    select cigration.cigration_complete_shard_migration_task(jobid_input,taskid_input,check_only=>true) into result;
    RETURN result;
END;
$cigration_monitor_shard_migration_task$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION cigration.cigration_update_metadata_after_migration(jobid_input integer,taskid_input integer)
RETURNS void
-- 
-- 函数名：cigration.cigration_update_metadata_after_migration
-- 函数功能：更新CN和扩展WK上的元数据
-- 参数：jobid_input integer
--       taskid_input integer
-- 返回值：无
-- 
AS $cigration_update_metadata_after_migration$
DECLARE
    -- task信息变量
    shardid_list bigint[];
    source_node_name text;
    target_node_name text;
    i_source_node_port integer;
    i_target_node_port integer;
    
    -- 扩展WK的信息
    node_info record;
    
    -- dblink连接信息
    dblink_name text;
BEGIN
    --执行节点必须是CN节点
    IF (SELECT CASE WHEN (select count(*) from pg_dist_node)>0 THEN (select groupid from pg_dist_local_group) ELSE -1 END) <> 0 THEN
        RAISE EXCEPTION 'function cigration_update_metadata_after_migration could only be executed on coordinate node.';
    END IF;
    
    -- 获取task相关的信息
    select all_colocated_shards_id,source_nodename,source_nodeport,target_nodename,target_nodeport
    into strict shardid_list,source_node_name,i_source_node_port,target_node_name,i_target_node_port
    from cigration.pg_citus_shard_migration
    where taskid = taskid_input and jobid = jobid_input;

    -- 检查源节点和目的节点是否合法 
    if (select count(*) <> 1 from pg_dist_node where noderole = 'primary' and nodename = source_node_name and nodeport = i_source_node_port) then
        RAISE EXCEPTION 'the source node %:% is not in this citus cluster or not primary note.', source_node_name, i_source_node_port;
    end if;

    if (select count(*) <> 1 from pg_dist_node where noderole = 'primary' and nodename = target_node_name and nodeport = i_target_node_port and shouldhaveshards = true) then
        RAISE EXCEPTION 'the target node %:% is not in this citus cluster or not primary note or shouldhaveshards is not true.', target_node_name, i_target_node_port;
    end if;

    -- 更新CN上的元数据 
    UPDATE pg_dist_shard_placement set nodename = target_node_name, nodeport = i_target_node_port where shardid = any(shardid_list) and nodename = source_node_name and nodeport = i_source_node_port;
    -- perform cigration.cigration_print_log('cigration_update_metadata_after_migration','metadata has been updated in the coordinator node.');
    
    -- 同步元数据的更新到扩展WK上，采用2pc的方式更新
    for node_info in select nodename,nodeport from pg_dist_node where noderole = 'primary' and hasmetadata = 't' order by nodename,nodeport
    loop
        dblink_name := concat('exwk_con_' ,node_info.nodename, '_', node_info.nodeport);
        perform dblink_exec(dblink_name,format('UPDATE pg_dist_shard_placement set nodename = ''%s'', nodeport = %s where shardid = any(array[%s]) and nodename = ''%s'' and nodeport = ''%s''',target_node_name,i_target_node_port,array_to_string(shardid_list,','),source_node_name,i_source_node_port));
    end loop;
    
    -- prepare
    for node_info in select nodename,nodeport from pg_dist_node where noderole = 'primary' and hasmetadata = 't' order by nodename,nodeport
    loop
        dblink_name := concat('exwk_con_' ,node_info.nodename, '_', node_info.nodeport);
        perform dblink_exec(dblink_name,format($$PREPARE TRANSACTION 'citus_move_shard_placement_%s_%s_tran'$$,jobid_input,taskid_input));
        perform cigration.cigration_print_log('cigration_update_metadata_after_migration',format('prepare transaction citus_move_shard_placement_%s_%s_tran in node %s:%s.',jobid_input,taskid_input,node_info.nodename,node_info.nodeport));
    end loop;
    
    -- commit prepare
    for node_info in select nodename,nodeport from pg_dist_node where noderole = 'primary' and hasmetadata = 't' order by nodename,nodeport
    loop
        dblink_name := concat('exwk_con_' ,node_info.nodename, '_', node_info.nodeport);
        perform dblink_exec(dblink_name,format($$COMMIT PREPARED 'citus_move_shard_placement_%s_%s_tran'$$,jobid_input,taskid_input));
        perform cigration.cigration_print_log('cigration_update_metadata_after_migration',format('commit prepare transaction citus_move_shard_placement_%s_%s_tran in node %s:%s.',jobid_input,taskid_input,node_info.nodename,node_info.nodeport));
    end loop;
    
    -- 清理掉dblink后直接退出
    PERFORM dblink_disconnect(con) 
    FROM (select unnest(a) con from dblink_get_connections() a)b 
    WHERE con like 'exwk_con_%';
    
EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
    PERFORM dblink_disconnect(con) 
    FROM (select unnest(a) con from dblink_get_connections() a)b 
    WHERE con like 'exwk_con_%';
    
    raise;
END;
$cigration_update_metadata_after_migration$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION cigration.cigration_rollback_metadata(jobid_input integer,taskid_input integer)
RETURNS text
-- 
-- 函数名：cigration.cigration_rollback_metadata
-- 函数功能：更新过的元数据需要回滚的时候调用
-- 参数：jobid_input integer
--       taskid_input integer
-- 返回值：text 回滚元数据的sql
-- 
AS $cigration_rollback_metadata$
DECLARE
    -- task信息变量
    shardid_list bigint[];
    source_node_name text;
    target_node_name text;
    i_source_node_port integer;
    i_target_node_port integer;
BEGIN
    -- 执行节点必须是CN节点
    IF (SELECT CASE WHEN (select count(*) from pg_dist_node)>0 THEN (select groupid from pg_dist_local_group) ELSE -1 END) <> 0 THEN
        RAISE EXCEPTION 'function cigration_rollback_metadata could only be executed on coordinate node.';
    END IF;
    
    -- 获取task相关的信息
    select all_colocated_shards_id,source_nodename,source_nodeport,target_nodename,target_nodeport
    into strict shardid_list,source_node_name,i_source_node_port,target_node_name,i_target_node_port
    from cigration.pg_citus_shard_migration
    where taskid = taskid_input and jobid = jobid_input;
    
    -- 判断任务状态，非completed状态的任务就报错退出
    if (select count(*) = 0 from cigration.pg_citus_shard_migration where status = 'completed' and taskid = taskid_input and jobid = jobid_input) then
        RAISE EXCEPTION 'task [%] in job [%] is not completed.',taskid_input,jobid_input;
    end if;

    -- 更新CN上的元数据
    return format('UPDATE pg_dist_shard_placement set nodename = ''%s'', nodeport = %s  where shardid = any(array[%s]) and nodename = ''%s'' and nodeport = %s',source_node_name,i_source_node_port,array_to_string(shardid_list,','),target_node_name,i_target_node_port);
END;
$cigration_rollback_metadata$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION cigration.cigration_drop_old_shard(jobid_input int,taskid_input int)
RETURNS boolean
-- 
-- 函数名：cigration.cigration_drop_old_shard
-- 函数功能：清理掉迁移完成之后的旧分片副本
-- 参数：taskid_input int
--       jobid_input int
-- 返回值: true/false
-- 
AS $cigration_drop_old_shard$
DECLARE
    -- task信息变量
    shardid_list bigint[];
    var_shardid bigint;
    source_node_name text;
    target_node_name text;
    i_source_node_port integer;
    i_target_node_port integer;
    logical_relid_list text[];
    logical_relid text;
    current_nodename text;
    shard bigint;
    
    -- insert into cigration.pg_citus_shard_migration_sql_log
    execute_sql text;
    
    -- dblink信息
    dblink_source_name text := 'drop_old_shard';
BEGIN
    -- 执行节点必须是CN节点
    IF (SELECT CASE WHEN (select count(*) from pg_dist_node)>0 THEN (select groupid from pg_dist_local_group) ELSE -1 END) <> 0 THEN
        RAISE EXCEPTION 'function cigration_drop_old_shard could only be executed on coordinate node.';
    END IF;
    
    -- 判断任务状态，非completed状态的任务就报错退出
    if (select count(*) = 0 from cigration.pg_citus_shard_migration where status = 'completed' and taskid = taskid_input and jobid = jobid_input) then
        RAISE EXCEPTION 'task [%] in job [%] is not completed.',taskid_input,jobid_input;
    end if;

    -- 获取task相关的信息
    select all_colocated_shards_id,source_nodename,source_nodeport,target_nodename,target_nodeport
    into strict shardid_list,source_node_name,i_source_node_port,target_node_name,i_target_node_port
    from cigration.pg_citus_shard_migration
    where taskid = taskid_input and jobid = jobid_input;
    
    -- 从元数据中取出最新的分片位置，如果分片位置和任务记录一样的话不清理，报错
    if (select count(*) <> 0 from pg_dist_shard_placement where nodename = source_node_name and nodeport = i_source_node_port and shardid in (select unnest(shardid_list))) then
        raise exception 'shardid:% is still active in this citus cluster,can not be deleted.',shardid_list;
    end if;
    
    -- 获取该task的逻辑表名列表
    foreach shard in array shardid_list
    loop
        select array_append(logical_relid_list, shard_name(logicalrelid, shardid))
        into strict logical_relid_list
        from pg_dist_shard 
        where shardid = shard; 
    end loop;

    -- 创建到源端的dblink
    perform dblink_connect(dblink_source_name,format('host=%s port=%s user=%s dbname=%s',source_node_name,i_source_node_port,current_user,current_database()));
    
    -- 移动旧的分片到备份schema下
    foreach logical_relid in array logical_relid_list
    loop
        PERFORM dblink_exec(dblink_source_name,format('ALTER TABLE IF EXISTS %s SET SCHEMA cigration_recyclebin_%s', logical_relid, jobid_input));
        
        execute_sql := format('insert into cigration.pg_citus_shard_migration_sql_log (jobid, taskid, execute_nodename, execute_nodeport, functionid, sql) values(%L, %L, %L, %L, %L, %L)', 
                jobid_input, taskid_input, source_node_name, i_source_node_port,
                'cigration.cigration_drop_old_shard',
                format('ALTER TABLE IF EXISTS %s SET SCHEMA cigration_recyclebin_%s', logical_relid, jobid_input));
        execute execute_sql;
        -- debug信息
        RAISE DEBUG 'ALTER SHARD [%] SET SCHEMA cigration_recyclebin_%.', logical_relid, jobid_input;
    end loop;
    
    -- 清理掉dblink后直接退出
    PERFORM dblink_disconnect(con)
    FROM (select unnest(a) con from dblink_get_connections() a)b 
    WHERE con = dblink_source_name;
    
    return true;
    
EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
    PERFORM dblink_disconnect(con)
    FROM (select unnest(a) con from dblink_get_connections() a)b 
    WHERE con = dblink_source_name;

    raise;
END;
$cigration_drop_old_shard$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION cigration.cigration_cancel_shard_migration_task(jobid_input int,taskid_input int)
RETURNS text
-- 
-- 函数名：cigration.cancel_shard_migration_task
-- 函数功能：取消指定task
-- 参数：jobid_input int
--       taskid_input int
-- 返回值：cancel_succeed：取消成功
-- 
AS $cigration_cancel_shard_migration_task$
DECLARE
    -- task信息变量
    shardid_list bigint[];
    var_shardid bigint;
    source_node_name text;
    target_node_name text;
    i_source_node_port integer;
    i_target_node_port integer;
    logical_relid_list text[];
    logical_relid text;
    shard bigint;
    has_sub int;
    var_subslotname text;
    has_subslot boolean;
    
    -- dblink变量
    dblink_source_con_name text;
    dblink_target_con_name text;
    
    -- 异常处理变量
    error_msg text;
    
    -- insert into cigration.pg_citus_shard_migration_sql_log
    execute_sql text;

BEGIN
    -- 执行节点必须是CN节点
    IF (SELECT CASE WHEN (select count(*) from pg_dist_node)>0 THEN (select groupid from pg_dist_local_group) ELSE -1 END) <> 0 THEN
        RAISE EXCEPTION 'function cigration_cancel_shard_migration_task could only be executed on coordinate node.';
    END IF;
    
    -- 判断任务状态
    if (select count(*) = 0 from cigration.pg_citus_shard_migration where taskid = taskid_input and jobid = jobid_input and status = 'running') then
        raise exception 'only the task in running status can be canceled.';
    end if;
    
    -- 取出任务信息
    select all_colocated_shards_id,source_nodename,source_nodeport,target_nodename,target_nodeport
    into strict shardid_list,source_node_name,i_source_node_port,target_node_name,i_target_node_port
    from cigration.pg_citus_shard_migration
    where taskid = taskid_input and jobid = jobid_input;

    -- 获取该task的逻辑表名列表
    foreach shard in array shardid_list
    loop
        select array_append(logical_relid_list, shard_name(logicalrelid, shardid))
        into strict logical_relid_list
        from pg_dist_shard 
        where shardid = shard; 
    end loop;
    
    -- 删除目标端的订阅和源端的发布
    dblink_source_con_name := 'migration_source_con_' || shardid_list[1];
    dblink_target_con_name := 'migration_target_con_' || shardid_list[1];
    
    BEGIN
        PERFORM dblink_connect(dblink_source_con_name,format('host=%s port=%s user=%s dbname=%s',source_node_name,i_source_node_port,current_user,current_database()));
        PERFORM dblink_connect(dblink_target_con_name,format('host=%s port=%s user=%s dbname=%s',target_node_name,i_target_node_port,current_user,current_database()));

        SELECT sub
        INTO STRICT has_sub
        FROM dblink(dblink_target_con_name, $dblink$select count(*) from pg_subscription where subname = 'citus_move_shard_placement_sub'$dblink$) AS t(sub int);
        
        if (has_sub <> 0) then
            PERFORM dblink_exec(dblink_target_con_name,'ALTER SUBSCRIPTION citus_move_shard_placement_sub DISABLE');

            -- 目的端复制操不存在时，先将slot_name置为NONE，否则DROP SUBSCRIPTION会失败
            SELECT subslotname
            INTO var_subslotname
            FROM dblink(dblink_target_con_name,'select subslotname from pg_subscription where subname = ''citus_move_shard_placement_sub'' ') a(subslotname text);

            SELECT count > 0
            INTO has_subslot
            FROM dblink(dblink_source_con_name,
                             format('select count(*) from pg_get_replication_slots() where slot_name=''%s'' ', var_subslotname)) a(count int);

            IF NOT has_subslot THEN
                PERFORM dblink_exec(dblink_target_con_name,'ALTER SUBSCRIPTION citus_move_shard_placement_sub SET (slot_name = NONE)');
            END IF;

            PERFORM dblink_exec(dblink_target_con_name,'DROP SUBSCRIPTION IF EXISTS citus_move_shard_placement_sub CASCADE');
            execute_sql := format('insert into cigration.pg_citus_shard_migration_sql_log (jobid, taskid, execute_nodename, execute_nodeport, functionid, sql) values (%L, %L, %L, %L, %L, %L)',
                    jobid_input, taskid_input, target_node_name, i_target_node_port,
                    'cigration.cigration_cancel_shard_migration_task',
                    'DROP SUBSCRIPTION IF EXISTS citus_move_shard_placement_sub CASCADE');
            execute execute_sql;
        end if;
    EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
        GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
        RAISE EXCEPTION 'failed to DROP SUBSCRIPTION citus_move_shard_placement_sub in target node:%', error_msg;
    END;

    BEGIN
        PERFORM dblink_exec(dblink_source_con_name, 'DROP PUBLICATION IF EXISTS citus_move_shard_placement_pub CASCADE');
        execute_sql := format('insert into cigration.pg_citus_shard_migration_sql_log (jobid, taskid, execute_nodename, execute_nodeport, functionid, sql) values (%L, %L, %L, %L, %L, %L)',
                jobid_input, taskid_input, source_node_name, i_source_node_port,
                'cigration.cigration_cancel_shard_migration_task',
                'DROP PUBLICATION IF EXISTS citus_move_shard_placement_pub CASCADE');
        execute execute_sql;
    EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
        GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
        RAISE EXCEPTION 'failed to DROP PUBLICATION citus_move_shard_placement_pub in source node:%', error_msg;
    END;

    -- 从元数据中取出最新的分片位置，如果分片位置和任务记录一样的话不清理，报错
    if (select count(*) <> 0 from pg_dist_shard_placement where nodename = target_node_name and nodeport = i_target_node_port and shardid in (select unnest(shardid_list))) then
        raise exception 'shardid:% is still active in this citus cluster,can not be deleted.',shardid_list;
    end if;

    -- 目标端的定义:ALTER TABLE SET SCHEMA
    foreach logical_relid in array logical_relid_list
    loop
        BEGIN
            PERFORM dblink_exec(dblink_target_con_name,format('ALTER TABLE IF EXISTS %s SET SCHEMA cigration_recyclebin_%s',
                    logical_relid, jobid_input));
            execute_sql := format('insert into cigration.pg_citus_shard_migration_sql_log (jobid, taskid, execute_nodename, execute_nodeport, functionid, sql) values (%L, %L, %L, %L, %L, %L)',
                    jobid_input, taskid_input, target_node_name, i_target_node_port, 
                    'cigration.cigration_cancel_shard_migration_task',
                    format('ALTER TABLE IF EXISTS %s SET SCHEMA cigration_recyclebin_%s', logical_relid, jobid_input));
            execute execute_sql;
            RAISE DEBUG 'ALTER TABLE [%] SET SCHEMA cigration_recyclebin_%.', logical_relid, jobid_input;
        EXCEPTION WHEN DUPLICATE_TABLE THEN
            --如果已经存在，则直接drop
            PERFORM dblink_exec(dblink_target_con_name,format('DROP TABLE IF EXISTS %s', logical_relid));
            
            execute_sql := format('insert into cigration.pg_citus_shard_migration_sql_log (jobid, taskid, execute_nodename, execute_nodeport, functionid, sql) values(%L, %L, %L, %L, %L, %L)', 
                    jobid_input, taskid_input, target_node_name, i_target_node_port, 
                    'cigration.cigration_cancel_shard_migration_task',
                    format('DROP TABLE IF EXISTS %s', logical_relid));
            execute execute_sql;
            -- debug信息
            RAISE DEBUG 'DROP SHARD [%].', logical_relid;
            CONTINUE;
        END;
    end loop;
    
    -- 更新JOB表
    update cigration.pg_citus_shard_migration set status = 'canceled',start_time = null where jobid = jobid_input and taskid = taskid_input;
    
    -- 清理掉dblink后退出函数
    PERFORM dblink_disconnect(con)
    FROM (select unnest(a) con from dblink_get_connections() a)b 
    WHERE con in (dblink_source_con_name,dblink_target_con_name);
    
    return 'cancel_succeed';
    
EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
    -- 清理dblink
    PERFORM dblink_disconnect(con)
    FROM (select unnest(a) con from dblink_get_connections() a)b 
    WHERE con in (dblink_source_con_name,dblink_target_con_name);
    
    raise;
END;
$cigration_cancel_shard_migration_task$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION cigration.cigration_cancel_shard_migration_job(jobid_input int default null,taskid_input int default null)
RETURNS text
-- 
-- 函数名：cigration.cigration_cancel_shard_migration_job
-- 函数功能：job级别的取消函数，将非running状态的task从迁移任务表中删除，将任务信息归档到历史表
-- 参数：jobid_input int 
-- 返回值：boolean。取消成功返回true
-- 
AS $cigration_cancel_shard_migration_job$
BEGIN
    -- 执行节点必须是CN节点
    IF (SELECT CASE WHEN (select count(*) from pg_dist_node)>0 THEN (select groupid from pg_dist_local_group) ELSE -1 END) <> 0 THEN
        RAISE EXCEPTION 'function cigration_cancel_shard_migration_job could only be executed on coordinate node.';
    END IF;

    IF NOT (select pg_try_advisory_xact_lock_shared('cigration.pg_citus_shard_migration'::regclass::int)) THEN
        RAISE EXCEPTION 'Can not call cigration api concurrently.';
    END IF;

    --判断jobid_input是否为空
    IF (jobid_input is null) THEN
        IF (SELECT COUNT(distinct jobid) FROM cigration.pg_citus_shard_migration) > 1 THEN
            RAISE EXCEPTION 'There is more than one job, the jobid must be specified explicitly.';
        ELSIF (SELECT COUNT(distinct jobid) FROM cigration.pg_citus_shard_migration) = 1 THEN
            SELECT distinct jobid INTO STRICT jobid_input FROM cigration.pg_citus_shard_migration;
        ELSE
            RAISE WARNING 'there are no migration job';
            RETURN true;
        END IF;
    END IF;

    IF NOT (select pg_try_advisory_xact_lock('cigration.pg_citus_shard_migration'::regclass::int, jobid_input)) THEN
        RAISE EXCEPTION 'Can not get lock for job %.', jobid_input
              USING HINT = 'If the shard migration job is runing, please send CTRL-C or call pg_terminate_backend() to stop it first.';
    END IF;

    -- 判断任务状态
    if (select count(*) <> 0 from cigration.pg_citus_shard_migration where jobid = jobid_input and status = 'error') then
        raise exception 'some tasks are in error status in job [%].please cleanup the error task first by call: select cigration.cigration_cleanup_error_env().',jobid_input;
    end if;

    if (taskid_input is null) then        
        perform cigration.cigration_cancel_shard_migration_task(jobid,taskid) from cigration.pg_citus_shard_migration where jobid = jobid_input and status = 'running' order by taskid;
        
        perform cigration.cigration_cleanup_shard_migration_task(jobid,taskid) from cigration.pg_citus_shard_migration where jobid = jobid_input;
    else
        if (select count(*) <> 0 from cigration.pg_citus_shard_migration where jobid = jobid_input and taskid = taskid_input and status = 'running') then
            perform cigration.cigration_cancel_shard_migration_task(jobid_input,taskid_input);
        end if;
        
        perform cigration.cigration_cleanup_shard_migration_task(jobid_input,taskid_input);
    end if;
    
    return true;
END;
$cigration_cancel_shard_migration_job$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION cigration.cigration_cleanup_shard_migration_task(jobid_input integer,taskid_input integer)
RETURNS void
-- 
-- 函数名：cigration.cigration_cleanup_shard_migration_task()
-- 函数功能：历史任务的归档函数
-- 参数：jobid_input int 
-- 返回值：void
-- 
AS $cigration_cleanup_shard_migration_task$
BEGIN
    -- 执行节点必须是CN节点
    IF (SELECT CASE WHEN (select count(*) from pg_dist_node)>0 THEN (select groupid from pg_dist_local_group) ELSE -1 END) <> 0 THEN
        RAISE EXCEPTION 'function cigration_cleanup_shard_migration_task could only be executed on coordinate node.';
    END IF;
    
    if (select status = 'completed' from cigration.pg_citus_shard_migration where jobid = jobid_input and taskid = taskid_input) then
        -- 已经完成的迁移任务，需要先把旧的分片副本删除
        perform cigration.cigration_drop_old_shard(jobid,taskid) from cigration.pg_citus_shard_migration where jobid = jobid_input and taskid = taskid_input;
            
        insert into cigration.pg_citus_shard_migration_history select * from cigration.pg_citus_shard_migration where jobid = jobid_input and taskid = taskid_input;
        -- update cigration.pg_citus_shard_migration_history set status = 'canceled' where jobid = jobid_input and taskid = taskid_input;
        delete from cigration.pg_citus_shard_migration where jobid = jobid_input and taskid = taskid_input;
    elsif (select status = 'canceled' or status = 'init' from cigration.pg_citus_shard_migration where jobid = jobid_input and taskid = taskid_input) then
        -- 处于初始状态或者被取消的任务，直接归档元数据即可
        insert into cigration.pg_citus_shard_migration_history select * from cigration.pg_citus_shard_migration where jobid = jobid_input and taskid = taskid_input;
        -- update cigration.pg_citus_shard_migration_history set status = 'canceled' where jobid = jobid_input and taskid = taskid_input;
        delete from cigration.pg_citus_shard_migration where jobid = jobid_input and taskid = taskid_input;
    else
        raise exception 'only the completed,canceled or init task can be archived.'
              USING HINT = 'if the task status is error,call function cigration_cleanup_error_env first.';
    end if;
END;
$cigration_cleanup_shard_migration_task$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION cigration.cigration_cleanup_error_env()
RETURNS void
-- 
-- 函数名：cigration.cigration_cleanup_error_env
-- 函数功能：清理分片迁移失败后的残留环境
-- 参数：无
-- 返回值：无
-- 
AS $cigration_cleanup_error_env$
DECLARE
    -- task信息变量
    shardid_list bigint[];
    var_shardid bigint;
    source_node_name text;
    target_node_name text;
    logical_relid_list text[];
    logical_relid text;
    task_info record;
    shard bigint;
    logical_slot_name text;
    
    -- dblink变量
    dblink_source_con_name text := 'migration_to_clearup_source';
    dblink_target_con_name text := 'migration_to_clearup_target';
    
    -- 异常处理变量
    error_msg text;
    
    -- 判断是否有订阅和发布
    has_sub int;
    has_pub int;
    
    -- insert into cigration.pg_citus_shard_migration_sql_log
    execute_sql text;
BEGIN
    -- 执行节点必须是CN节点
    IF (SELECT CASE WHEN (select count(*) from pg_dist_node)>0 THEN (select groupid from pg_dist_local_group) ELSE -1 END) <> 0 THEN
        RAISE EXCEPTION 'function cigration_cleanup_error_env could only be executed on coordinate node.';
    END IF;
    
    for task_info in select jobid,taskid,all_colocated_shards_id,source_nodename,source_nodeport,target_nodename,target_nodeport from cigration.pg_citus_shard_migration where status in ('error','init','canceled')
    loop
        select null into logical_relid_list;
        -- 获取该task的逻辑表名列表
        foreach shard in array task_info.all_colocated_shards_id
        loop
            select array_append(logical_relid_list, shard_name(logicalrelid, shardid))
            into strict logical_relid_list
            from pg_dist_shard 
            where shardid = shard; 
        end loop;
          
        -- 创建到源和目标节点的dblink
        BEGIN
            PERFORM dblink_connect(dblink_source_con_name,format('host=%s port=%s user=%s dbname=%s',task_info.source_nodename,task_info.source_nodeport,current_user,current_database()));
            
            PERFORM dblink_connect(dblink_target_con_name,format('host=%s port=%s user=%s dbname=%s',task_info.target_nodename,task_info.target_nodeport,current_user,current_database()));
        EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
            GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
            RAISE EXCEPTION 'failed to connect source or target node:%', error_msg;
        END;
        
        -- 如果目标节点上有正在执行的任务，保留逻辑逻辑订阅不删除
        if (select count(*) = 0 from cigration.pg_citus_shard_migration where status = 'running' and target_nodename = task_info.target_nodename) then
            -- 清理掉可能残留的逻辑复制
            BEGIN
                SELECT sub
                INTO STRICT has_sub
                FROM dblink(dblink_target_con_name, $dblink$select count(*) from pg_subscription where subname = 'citus_move_shard_placement_sub'$dblink$) AS t(sub int);
                
                if (has_sub <> 0) then
                    PERFORM dblink_exec(dblink_target_con_name,'ALTER SUBSCRIPTION citus_move_shard_placement_sub DISABLE');
                    PERFORM dblink_exec(dblink_target_con_name,'DROP SUBSCRIPTION IF EXISTS citus_move_shard_placement_sub CASCADE');
                    
                    execute_sql := format('insert into cigration.pg_citus_shard_migration_sql_log (jobid, taskid, execute_nodename, execute_nodeport,functionid, sql) values(%L, %L, %L, %L, %L, %L)', 
                            task_info.jobid, task_info.taskid, task_info.target_nodename, task_info.target_nodeport, 
                            'cigration.cigration_cleanup_error_env',
                            'DROP SUBSCRIPTION IF EXISTS citus_move_shard_placement_sub CASCADE');
                    execute execute_sql;
                end if;
            EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                RAISE EXCEPTION 'failed to DROP SUBSCRIPTION citus_move_shard_placement_sub in target node:%', error_msg;
            END;
        end if;
        
        -- 如果源节点上有正在执行的任务，保留逻辑发布不删除
        if (select count(*) = 0 from cigration.pg_citus_shard_migration where status = 'running' and source_nodename = task_info.source_nodename) then
            BEGIN
                SELECT pub
                INTO STRICT has_pub
                FROM dblink(dblink_source_con_name, $dblink$select count(*) from pg_publication where pubname = 'citus_move_shard_placement_pub'$dblink$) AS t(pub int);

                if (has_pub <> 0) then
                    PERFORM dblink_exec(dblink_source_con_name,'DROP PUBLICATION IF EXISTS citus_move_shard_placement_pub CASCADE');
                    
                    execute_sql := format('insert into cigration.pg_citus_shard_migration_sql_log (jobid, taskid, execute_nodename, execute_nodeport, functionid, sql) values(%L, %L, %L, %L, %L, %L)', 
                            task_info.jobid, task_info.taskid, task_info.source_nodename, task_info.source_nodeport, 
                            'cigration.cigration_cleanup_error_env',
                            'DROP PUBLICATION IF EXISTS citus_move_shard_placement_pub CASCADE');
                    execute execute_sql;
                end if;
            EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                RAISE EXCEPTION 'failed to DROP PUBLICATION citus_move_shard_placement_pub in source node:%', error_msg;
            END;
        end if;

        -- 从元数据中取出最新的分片位置，如果分片位置和任务记录一样的话不清理，报错
        if (select count(*) <> 0 from pg_dist_shard_placement where nodename = task_info.target_nodename and nodeport = task_info.target_nodeport
                and shardid in (select unnest(task_info.all_colocated_shards_id))) then
            raise exception 'shardid:% is still active in this citus cluster,can not be deleted.',shardid_list;
        end if;

        -- 目标端的定义移动到:cigration_recyclebin_jobid下面
        foreach logical_relid in array logical_relid_list
        loop
            BEGIN
                -- 正常情况下，分区表的主表删除之后，就不需要再删除分区子表了，加了if exists可以回避表不存在的问题
                PERFORM dblink_exec(dblink_target_con_name,format('ALTER TABLE IF EXISTS %s SET SCHEMA cigration_recyclebin_%s', 
                        logical_relid, task_info.jobid));

                execute_sql := format('insert into cigration.pg_citus_shard_migration_sql_log (jobid, taskid, execute_nodename, execute_nodeport, functionid, sql) values(%L, %L, %L, %L, %L, %L)', 
                        task_info.jobid, task_info.taskid, task_info.target_nodename, task_info.target_nodeport, 
                        'cigration.cigration_cleanup_error_env',
                        format('ALTER TABLE IF EXISTS %s SET SCHEMA cigration_recyclebin_%s', logical_relid, task_info.jobid));
                execute execute_sql;
                raise debug 'dblink_target_con_name:%,  logical_relid:%',dblink_target_con_name,logical_relid;
            EXCEPTION WHEN DUPLICATE_TABLE THEN
                --如果已经存在，则直接drop
                PERFORM dblink_exec(dblink_target_con_name,format('DROP TABLE IF EXISTS %s', logical_relid));
                
                execute_sql := format('insert into cigration.pg_citus_shard_migration_sql_log (jobid, taskid, execute_nodename, execute_nodeport, functionid, sql) values(%L, %L, %L, %L, %L, %L)', 
                        task_info.jobid, task_info.taskid, task_info.target_nodename, task_info.target_nodeport,
                        'cigration.cigration_cleanup_error_env',
                        format('DROP TABLE IF EXISTS %s', logical_relid));
                execute execute_sql;
                -- debug信息
                RAISE DEBUG 'DROP SHARD [%].', logical_relid;
                CONTINUE;
            END;
        end loop;
        
        -- 删除失效的逻辑复制槽(同源不可并行的前提下，只会有一个逻辑复制槽)
        perform-- 
        from dblink(dblink_source_con_name,format('select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_type = ''logical'' and plugin = ''pgoutput'' and active = ''f'' and slot_name = ''%s''','slot_' || replace(task_info.target_nodename,'.','_'))) as tmp(func_result text);
        
        -- 清理dblink
        PERFORM dblink_disconnect(con)
        FROM (select unnest(a) con from dblink_get_connections() a)b 
        WHERE con in(dblink_target_con_name,dblink_source_con_name);
        
        -- 将该任务恢复成init状态，同时清空error_message字段
        update cigration.pg_citus_shard_migration set status = 'init',error_message = null where jobid = task_info.jobid and taskid = task_info.taskid;
    end loop;
    
EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
    -- 清理dblink
    PERFORM dblink_disconnect(con)
    FROM (select unnest(a) con from dblink_get_connections() a)b 
    WHERE con in (dblink_source_con_name,dblink_target_con_name);
    
    raise;
END;
$cigration_cleanup_error_env$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION cigration.cigration_check_before_migration(jobid_input integer,
                                                         taskid_input integer,
                                                         longtime_tx_threshold interval,
                                                         with_replica_identity_check boolean)
RETURNS text
-- 
-- 函数名：cigration.check_before_migration
-- 函数功能：开始分片迁移之前的检查函数
-- 参数：jobid_input integer,
--       taskid_input integer,
--       longtime_tx_threshold interval, 迁移任务时允许的最大长事务执行时间
--       with_replica_identity_check boolean 任务启动时是否检查replica_identity
-- 返回值：true/false 
--  
AS $cigration_check_before_migration$
DECLARE
    -- 任务信息
    shard_id_array bigint[];
    shard_id bigint;
    source_node_name text;
    target_node_name text;

    fullname text[];
    tmp_record record;
    shard_id_array_without_partition bigint[];
    -- 是否有唯一索引的检查
    non_primary_unique_count int :=0;
    has_unique int;
    result text := '';
BEGIN
    --执行节点必须是CN节点
    IF (SELECT CASE WHEN (select count(*) from pg_dist_node)>0 THEN (select groupid from pg_dist_local_group) ELSE -1 END) <> 0 THEN
        RAISE EXCEPTION 'function cigration_check_before_migration could only be executed on coordinate node.';
    END IF;
    
    -- 根据taskid取出任务信息，开启迁移任务
    select source_nodename,target_nodename,all_colocated_shards_id
    into strict source_node_name,target_node_name,shard_id_array
    from cigration.pg_citus_shard_migration 
    where taskid = taskid_input and jobid = jobid_input;
    
    -- 检查长事务，存在超过阈值的事务就报错退出
    if (select count(*) > 0 from pg_stat_activity where backend_type = 'client backend' and now() - xact_start >= longtime_tx_threshold and query !~ 'cigration.cigration_run_shard_migration_job' and query !~ 'cigration.cigration_complete_shard_migration_task') then
        result := 'there are some transactions executed over ' || longtime_tx_threshold || '.';
    end if;
    
    -- 检查该分片上有没有主键和唯一约束，需要排除分区主表
    select array_agg(shardid)
    into strict shard_id_array_without_partition
    from pg_dist_shard 
    where shardid = any(shard_id_array) and 
          logicalrelid not in (select inhparent from pg_inherits);
    
    if (with_replica_identity_check is true) then
        foreach shard_id in array shard_id_array_without_partition
        loop
            if not (select relhasindex from pg_class where oid = (select logicalrelid from pg_dist_shard where shardid = shard_id)) then
                result := result || 'it does not has a replica identity or it does not has a primary key.';
                exit;
            else
                if not (select indisprimary from pg_index where indrelid = (select logicalrelid from pg_dist_shard where shardid = shard_id)) then
                    select count(tmp.result)
                    into has_unique
                    from (select (indisunique and indisreplident) as result
                          from pg_index 
                          where indisunique is true and indrelid = (select logicalrelid from pg_dist_shard where shardid = shard_id)
                          ) as tmp
                    where tmp.result is true;

                    if (has_unique is null or has_unique = 0) then
                        result := result || 'it does not has a replica identity or it does not has a primary key.';
                        exit;
                    end if;
                end if;
            end if;
        end loop;
    end if;
    
    -- 与该shardid亲和的分片有无正在迁移的任务
    for tmp_record in select all_colocated_shards_id from cigration.pg_citus_shard_migration where status = 'running'
    loop
        if (shard_id = any(tmp_record.all_colocated_shards_id)) then
            result := result || 'this shard has already been in a running migration task.';
        end if;
    end loop;
    
    -- 同源的迁移任务不可并行
    for tmp_record in select source_nodename,target_nodename from cigration.pg_citus_shard_migration where status = 'running'
    loop
        if (source_node_name = tmp_record.source_nodename) then
            result := result || 'there is already a migration task with the same source_node in the running status.';
        elsif (target_node_name = tmp_record.target_nodename) then
            result := result || 'there is already a migration task with the same target_node in the running status.';
        end if;
    end loop;
    
    return result;
END;
$cigration_check_before_migration$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION cigration.cigration_move_shard_placement(jobid_input integer,
                                                             taskid_input integer,
                                                             longtime_tx_threshold interval,
                                                             with_replica_identity_check boolean)
RETURNS boolean
--  
-- 函数名：cigration.cigration_move_shard_placement
-- 函数功能：分片迁移函数，用于同步DDL，创建初始逻辑复制同步数据
-- 参数：jobid_input integer,
--       taskid_input integer
-- 返回值：true/false
--
AS $cigration_move_shard_placement$
DECLARE
    source_group_id integer;
    target_group_id integer;
    source_node_name text;
    source_node_port integer;
    target_node_name text;
    target_node_port integer;
    i_source_node_port integer;
    i_target_node_port integer;
    target_shard_tables_with_data text;
    colocated_table_count integer;
    i integer;
    logical_schema text;
    shard_id_array bigint[];
    shard_fulltablename_array text[];
    shard_fulltablename_array_without_partition text[];
    error_msg text;
    pub_created boolean := false;
    sub_created boolean := false;
    table_created boolean := false;
    default_port int;
    logical_slot_name text;

    -- dblink参数
    dblink_created boolean := false;
    dblink_source_con_name text;
    dblink_target_con_name text;
    dblink_update_state text := 'migration_update_state_tb_' || jobid_input || '_' || taskid_input;
    -- 迁移前的检查
    check_result text := '';
    
    -- insert into cigration.pg_citus_shard_migration_sql_log
    execute_sql text;
BEGIN
    -- 根据taskid取出任务信息，开启迁移任务
    select source_nodename,source_nodeport,target_nodename,target_nodeport,all_colocated_shards_id
    into strict source_node_name,source_node_port,target_node_name,target_node_port,shard_id_array
    from cigration.pg_citus_shard_migration 
    where taskid = taskid_input and jobid = jobid_input;
    
    select setting into strict default_port from pg_settings where name = 'port';

    -- 迁移前的检查
    select cigration.cigration_check_before_migration(jobid_input,taskid_input,longtime_tx_threshold,with_replica_identity_check) into strict check_result;
    if (check_result <> '') then
        -- debug info
        raise debug 'check_result:%',check_result;
        
        -- 这里使用dblink的原因是为了防止更新被回滚掉
        perform dblink_connect(dblink_update_state,format('host=127.1 dbname=%s user=%s port=%s',current_database(),current_user,default_port));
        perform dblink_exec(dblink_update_state,format('update cigration.pg_citus_shard_migration set status = ''error'', error_message = ''%s'' where taskid = %s and jobid = %s',check_result,taskid_input,jobid_input));
        perform dblink_disconnect(dblink_update_state);
        return false;
    end if;

    SELECT groupid
    INTO   source_group_id
    FROM   pg_dist_node
    WHERE  nodename = source_node_name;

    SELECT groupid
    INTO   target_group_id
    FROM   pg_dist_node
    WHERE  nodename = target_node_name;

    -- 获取亲和表的数量和逻辑表名
    SELECT total_shard_count
    INTO   STRICT  colocated_table_count
    FROM   cigration.pg_citus_shard_migration
    WHERE  taskid = taskid_input and jobid = jobid_input;
    
    select array_agg(shard_name(logicalrelid,shardid))
    into STRICT shard_fulltablename_array
    from pg_dist_shard
    where shardid = any(shard_id_array);
    
    -- 如果有分区主表，在创建逻辑发布的时候需要把分区主表从列表里删除
    select array_agg(shard_name(logicalrelid,shardid))
    into strict shard_fulltablename_array_without_partition
    from pg_dist_shard 
    where shardid = any(shard_id_array) and 
          logicalrelid not in (select inhparent from pg_inherits);
          
    -- debug info
    raise debug 'shard_fulltablename_array_without_partition:%',shard_fulltablename_array_without_partition;

    -- 获取WK上的实际端口值(适用于CN和worker之间有中间件，CN元数据中的worker端口不是实际端口场景，比如pgbouncer，haparoxy)
    SELECT port 
    INTO i_source_node_port 
    FROM dblink(format('host=%s port=%s user=%s dbname=%s', source_node_name, source_node_port, current_user, current_database()),
                      $$SELECT setting FROM pg_settings WHERE name = 'port'$$) AS tb(port integer);
    SELECT port 
    INTO i_target_node_port 
    FROM dblink(format('host=%s port=%s user=%s dbname=%s', target_node_name, target_node_port, current_user, current_database()),
                      $$SELECT setting FROM pg_settings WHERE name = 'port'$$) AS tb(port integer);

    RAISE  NOTICE  'BEGIN move shards(%) from %:% to %:%', 
                array_to_string(shard_id_array,','),
                source_node_name, i_source_node_port,
                target_node_name, i_target_node_port;

    -- debug信息
    raise debug 'shard_fulltablename_array:%',shard_fulltablename_array;
    
    -- 考虑并发的情况，需要将dblink的名字作出区分
    dblink_source_con_name := 'migration_source_con_' || shard_id_array[1];
    PERFORM dblink_connect(dblink_source_con_name,
                            format('host=%s port=%s user=%s dbname=%s',
                                    source_node_name,
                                    i_source_node_port,
                                    current_user,
                                    current_database()));
    dblink_created := true;

    dblink_target_con_name := 'migration_target_con_' || shard_id_array[1];
    PERFORM dblink_connect(dblink_target_con_name,
                            format('host=%s port=%s user=%s dbname=%s',
                                    target_node_name,
                                    i_target_node_port,
                                    current_user,
                                    current_database()));
    
    BEGIN
        -- CREATE PUBLICATION in source node
        RAISE  NOTICE  'CREATE PUBLICATION in source node %:%', source_node_name, i_source_node_port;
        -- PERFORM dblink_exec(dblink_source_con_name, 'DROP PUBLICATION IF EXISTS citus_move_shard_placement_pub CASCADE');
        PERFORM dblink_exec(dblink_source_con_name, 
                            format('CREATE PUBLICATION citus_move_shard_placement_pub 
                                        FOR TABLE %s',
                                     (select string_agg(table_name,',') from unnest(shard_fulltablename_array_without_partition) table_name)));

        execute_sql := format('insert into cigration.pg_citus_shard_migration_sql_log (jobid, taskid, execute_nodename, execute_nodeport, functionid, sql) values(%L, %L, %L, %L, %L, %L)', 
                jobid_input, taskid_input, source_node_name, i_source_node_port,
                'cigration.cigration_move_shard_placement',
                format('CREATE PUBLICATION citus_move_shard_placement_pub FOR TABLE %s',
                        (select string_agg(table_name,',') from unnest(shard_fulltablename_array_without_partition) table_name)));
        execute execute_sql;
        pub_created := true;

        -- CREATE SCHEMA IF NOT EXISTS in target node
        FOR logical_schema IN SELECT nspname
                              FROM   pg_class c
                                     LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                              WHERE c.oid in (SELECT distinct logicalrelid 
                                              FROM pg_dist_shard
                                              WHERE shardid = any(shard_id_array))
        LOOP
            PERFORM dblink_exec(dblink_target_con_name, format('CREATE SCHEMA IF NOT EXISTS %I',logical_schema));
        END LOOP;
        
        -- create shard table in the target node
        RAISE  NOTICE  'create shard table in the target node %:%', target_node_name, i_target_node_port;
        EXECUTE format($sql$COPY (select '') to PROGRAM $$pg_dump "host=%s port=%s user=%s dbname=%s" -s -t %s | psql "host=%s port=%s user=%s dbname=%s"$$ $sql$,
                        source_node_name,
                        i_source_node_port,
                        current_user,
                        current_database(),
                        (select string_agg(format($a$'%s'$a$,table_name),' -t ') from unnest(shard_fulltablename_array) table_name),
                        target_node_name,
                        i_target_node_port,
                        current_user,
                        current_database());

        -- 确认目标端的ddl同步成功（同名表存在且数据为空）
        SELECT table_name
        INTO   target_shard_tables_with_data
        FROM   dblink(dblink_target_con_name,
                        format($$ select string_agg(table_name,',') table_name from ((select tableoid::regclass::text table_name from %s limit 1))a $$,
                               (select string_agg(table_name,' limit 1) UNION all (select tableoid::regclass::text table_name from ') 
                                       from unnest(shard_fulltablename_array) table_name))
                ) as a(table_name text);
            
        IF target_shard_tables_with_data is not NULL THEN
            RAISE  'shard tables(%) with data has exists in target node', target_shard_tables_with_data;
        END IF;

        table_created := true;
        
        -- logical slots name
        select 'slot_' || replace(target_node_name,'.','_') into logical_slot_name;
        -- conneect source node to create a logical slot for subscription first
        RAISE  NOTICE  'CREATE LOGICAL SLOT on source node %:%', source_node_name, i_source_node_port;
        PERFORM-- 
        from dblink(dblink_source_con_name, 
                    format('select pg_create_logical_replication_slot(''%s'',''pgoutput'')',logical_slot_name)) as tmp(func_return text);
        -- CREATE SUBSCRIPTION on target node
        RAISE  NOTICE  'CREATE SUBSCRIPTION on target node %:%', target_node_name, i_target_node_port;
        --PERFORM dblink_exec(dblink_target_con_name,'DROP SUBSCRIPTION IF EXISTS citus_move_shard_placement_sub CASCADE');
        PERFORM dblink_exec(dblink_target_con_name, 
                            format($$CREATE SUBSCRIPTION citus_move_shard_placement_sub
                                         CONNECTION 'host=%s port=%s user=%s dbname=%s'
                                         PUBLICATION citus_move_shard_placement_pub with (create_slot = false,slot_name = '%s')$$,
                                         source_node_name,
                                         i_source_node_port,
                                         current_user,
                                         current_database(),
                                         logical_slot_name));
        
        execute_sql := format('insert into cigration.pg_citus_shard_migration_sql_log (jobid, taskid, execute_nodename, execute_nodeport, functionid, sql) values(%L, %L, %L, %L, %L, %L)', 
                jobid_input, taskid_input, target_node_name, i_target_node_port,
                'cigration.cigration_move_shard_placement',
                format($$CREATE SUBSCRIPTION citus_move_shard_placement_sub
                                         CONNECTION 'host=%s port=%s user=%s dbname=%s'
                                         PUBLICATION citus_move_shard_placement_pub with (create_slot = false,slot_name = '%s')$$,
                                         source_node_name,
                                         i_source_node_port,
                                         current_user,
                                         current_database(),
                                         logical_slot_name));
        execute execute_sql;
        sub_created := true;
        
        -- 返回之前断开dblink
        PERFORM dblink_disconnect(con) 
        FROM (select unnest(a) con from dblink_get_connections() a)b 
        WHERE con in(dblink_source_con_name,dblink_target_con_name);
        
        RETURN true;
    END;
    
    -- error cleanup
    EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
        GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
        -- 将错误消息中单引号统一换成大写字母X，防止update的时候格式有问题
        select replace(error_msg,'''',$para$X$para$) into error_msg;
             
        PERFORM dblink_connect(dblink_update_state,format('host=%s port=%s dbname=%s user=%s','127.0.0.1',default_port,current_database(),current_user));
        PERFORM dblink_exec(dblink_update_state,format('update cigration.pg_citus_shard_migration set status = ''error'',error_message = ''%s'' where taskid = %s and jobid = %s ',error_msg,taskid_input,jobid_input));
        PERFORM dblink_disconnect(dblink_update_state);
        
        IF sub_created THEN
            BEGIN
                PERFORM dblink_exec(dblink_target_con_name, 
                                    'ALTER SUBSCRIPTION citus_move_shard_placement_sub DISABLE');
                PERFORM dblink_exec(dblink_target_con_name, 
                                    'DROP SUBSCRIPTION IF EXISTS citus_move_shard_placement_sub CASCADE');
        
                execute_sql := format('insert into cigration.pg_citus_shard_migration_sql_log (jobid, taskid, execute_nodename, execute_nodeport, functionid, sql) values(%L, %L, %L, %L, %L, %L)', 
                        jobid_input, taskid_input, target_node_name, i_target_node_port,
                        'cigration.cigration_move_shard_placement',
                        'DROP SUBSCRIPTION IF EXISTS citus_move_shard_placement_sub CASCADE');
                execute execute_sql;
            EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                RAISE WARNING 'failed to DROP SUBSCRIPTION citus_move_shard_placement_sub in target node:%', error_msg;
            END;
        END IF;

        IF pub_created THEN
            BEGIN
                PERFORM dblink_exec(dblink_source_con_name, 
                                'DROP PUBLICATION IF EXISTS citus_move_shard_placement_pub CASCADE');
        
                execute_sql := format('insert into cigration.pg_citus_shard_migration_sql_log (jobid, taskid, execute_nodename, execute_nodeport, functionid, sql) values(%L, %L, %L, %L, %L, %L)', 
                        jobid_input, taskid_input, source_node_name, i_source_node_port,
                        'cigration.cigration_move_shard_placement',
                        'DROP PUBLICATION IF EXISTS citus_move_shard_placement_pub CASCADE');
                execute execute_sql;
            EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                RAISE WARNING 'failed to DROP PUBLICATION citus_move_shard_placement_pub in source node:%', error_msg;
            END;
        END IF;

        -- 从元数据中取出最新的分片位置，如果分片位置和任务记录一样的话不清理，报错
        if (select count(*) <> 0 from pg_dist_shard_placement where nodename = target_node_name and nodeport = target_node_port and shardid in (select unnest(shard_id_array))) then
            raise exception 'shardid:% is still active in this citus cluster,can not be deleted.',shard_id_array;
        end if;

        IF table_created THEN
            FOR i IN 1..colocated_table_count LOOP
                BEGIN
                    -- 异常处理中，此处的drop没有必要移动到备份schema下面，因此，直接进行drop
                    PERFORM dblink_exec(dblink_target_con_name, 
                                 format('DROP TABLE IF EXISTS %s',
                                         shard_fulltablename_array[i]));
        
                    execute_sql := format('insert into cigration.pg_citus_shard_migration_sql_log (jobid, taskid, execute_nodename, execute_nodeport, functionid, sql) values(%L, %L, %L, %L, %L, %L)', 
                            jobid_input, taskid_input, target_node_name, i_target_node_port,
                            'cigration.cigration_move_shard_placement',
                            format('DROP TABLE IF EXISTS %s', shard_fulltablename_array[i]));
                    execute execute_sql;
                EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                    GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                    RAISE WARNING 'failed to DROP TABLE % in target node:%', shard_fulltablename_array[i], error_msg;
                END;
            END LOOP;
        END IF;
        
        IF dblink_created THEN
            BEGIN
                PERFORM dblink_disconnect(con) 
                FROM (select unnest(a) con from dblink_get_connections() a)b 
                WHERE con in(dblink_source_con_name,dblink_target_con_name,dblink_update_state);
            
            EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                RAISE WARNING 'failed to call dblink_disconnect:%', error_msg;
            END;
        END IF;
        
        RAISE;
END;
$cigration_move_shard_placement$ 
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION cigration.cigration_cleanup_recyclebin(jobid_input integer default null)
RETURNS void
-- 
-- 函数名：cigration.cigration_cleanup_recyclebin
-- 函数功能：清空各个worker上残留的回收站
-- 参数：jobid_input integer
-- 返回值：void
--  
AS $cigration_cleanup_recyclebin$
DECLARE
    recordinfo record;
    execute_sql text;
BEGIN
    --执行节点必须是CN节点
    IF (SELECT CASE WHEN (select count(*) from pg_dist_node)>0 THEN (select groupid from pg_dist_local_group) ELSE -1 END) <> 0 THEN
        RAISE EXCEPTION 'function cigration_cleanup_recyclebin could only be executed on coordinate node.';
    END IF;

    if (jobid_input is null) then
        IF NOT (select pg_try_advisory_xact_lock('cigration.pg_citus_shard_migration'::regclass::int)) THEN
            RAISE EXCEPTION 'Can not call cigration api concurrently.';
        END IF;

        IF (select count(*) from cigration.pg_citus_shard_migration where status='running') > 0 THEN
            RAISE EXCEPTION 'Can not cleanup recyclebin while there are running migration jobs.';
        END IF;
        for recordinfo in select distinct split_part(schema_name,'_',3)::int jobid
                          from cigration.cigration_get_recyclebin_metadata()
                          where split_part(schema_name,'_',3)::int not in (SELECT DISTINCT(jobid) from cigration.pg_citus_shard_migration)
                          ORDER BY split_part(schema_name,'_',3)::int loop
            
            execute_sql := format('select run_command_on_workers(''DROP SCHEMA IF EXISTS cigration_recyclebin_%s CASCADE'')', recordinfo.jobid);
            EXECUTE execute_sql;
        end loop;
    else
        IF NOT (select pg_try_advisory_xact_lock_shared('cigration.pg_citus_shard_migration'::regclass::int)) THEN
            RAISE EXCEPTION 'Can not call cigration api concurrently.';
        END IF;

        IF NOT (select pg_try_advisory_xact_lock('cigration.pg_citus_shard_migration'::regclass::int, jobid_input)) THEN
            RAISE EXCEPTION 'Can not get lock for job %.', jobid_input;
        END IF;

        IF (select count(*) from cigration.pg_citus_shard_migration where jobid=jobid_input) > 0 THEN
            RAISE EXCEPTION 'The input param jobid_input is invalid because of the migration job depends it.'
                USING HINT = 'If you want to cancle the shard migration job, call function cigration_cancel_shard_migration_job() first';
        END IF;
        execute_sql := format('select run_command_on_workers(''DROP SCHEMA IF EXISTS cigration_recyclebin_%s CASCADE'')', jobid_input);
        EXECUTE execute_sql;
    end if;
END;
$cigration_cleanup_recyclebin$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION cigration.cigration_get_recyclebin_metadata()
RETURNS TABLE(nodename text, nodeport int, schema_name text)
-- 
-- 函数名：cigration.cigration_get_recyclebin_metadata
-- 函数功能：在CN上通过dblink获取各个worker上归档旧分片的schema
-- 参数：无
-- 返回值：
--      nodename    ：垃圾站的node名称信息
--      nodeport    ：垃圾站的node端口信息
--      schema_name ：垃圾站的schema信息
-- 
AS $$
BEGIN
    --执行节点必须是CN节点
    IF (SELECT CASE WHEN (select count(*) from pg_dist_node)>0 THEN (select groupid from pg_dist_local_group) ELSE -1 END) <> 0 THEN
        RAISE EXCEPTION 'function cigration_cleanup_recyclebin could only be executed on coordinate node.';
    END IF;

    RETURN QUERY select a.nodename, a.nodeport,b.* from
                 pg_dist_node a,
                 dblink(format('host=%s port=%s user=%s dbname=%s',a.nodename,a.nodeport,current_user, current_database()),
                    'SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname ~ ''^cigration_recyclebin_'' ORDER BY 1') b(schemaname text)
                 order by a.nodename, a.nodeport;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cigration.cigration_generate_parallel_schedule(jobid_input int default NULL, init_sync_timeout int default 7200)
RETURNS TABLE(execute_sql text)
-- 
-- 函数名：cigration.cigration_generate_parallel_schedule
-- 函数功能：对指定的jobid生成可并行调度的SQL
-- 参数：
--      jobid_input       : jobid信息
--      init_sync_timeout : 最大同步等待时间
-- 返回值：
--      execute_sql       : 可并行调度的SQL
-- 
AS $cigration_generate_parallel_schedule$
DECLARE
    execute_sql text;
    current_jobid int := 0;
BEGIN
    --执行节点必须是CN节点
    IF (SELECT CASE WHEN (select count(*) from pg_dist_node)>0 THEN (select groupid from pg_dist_local_group) ELSE -1 END) <> 0 THEN
        RAISE EXCEPTION 'function cigration_cleanup_recyclebin could only be executed on coordinate node.';
    END IF;

    IF NOT (select pg_try_advisory_xact_lock('cigration.pg_citus_shard_migration'::regclass::int)) THEN
        RAISE EXCEPTION 'Can not call cigration api concurrently.';
    END IF;

    --判断jobid_input是否为空
    IF (jobid_input is null) THEN
        IF (SELECT COUNT(distinct jobid) FROM cigration.pg_citus_shard_migration) > 1 THEN
            RAISE EXCEPTION 'Citus shard migration infomation is corupt, please check cigration.pg_citus_shard_migration again.';
        ELSIF (SELECT COUNT(distinct jobid) FROM cigration.pg_citus_shard_migration) = 1 THEN
            SELECT distinct jobid INTO STRICT current_jobid FROM cigration.pg_citus_shard_migration;
        END IF;
    ELSE
        current_jobid := jobid_input;
    END IF;

    --去除同源或同目标
    RETURN QUERY    select format('select cigration_run_shard_migration_job(%s, ''%s'', %s);', current_jobid, taskids, init_sync_timeout)
                    from
                    (
                        WITH citus_shard_migration_all AS 
                        (
                            select array_agg(taskid) as taskids, source_nodename, target_nodename from cigration.pg_citus_shard_migration where jobid=current_jobid group by source_nodename, target_nodename order by taskids
                        ), citus_shard_migration_middle as
                        (
                            select * from (select ROW_NUMBER() over(partition by source_nodename order by target_nodename) RowNum , citus_shard_migration_all.*  from citus_shard_migration_all) as t1 where RowNum = 1
                        )
                        select * from (select ROW_NUMBER() over(partition by target_nodename order by source_nodename) RowNum1 , citus_shard_migration_middle.*  from citus_shard_migration_middle) as t1 where RowNum1 = 1
                    ) as t2;
END;
$cigration_generate_parallel_schedule$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cigration.cigration_run_shard_migration_job(jobid_input int default NULL,
                                                                       taskids int[] default NULL,
                                                                       init_sync_timeout int default 7200,
                                                                       longtime_tx_threshold interval default '30 min',
                                                                       with_replica_identity_check boolean default false)
RETURNS boolean
-- 
-- 函数名：cigration.cigration_run_shard_migration_job
-- 函数功能：对指定的jobid及taskid执行迁移任务
-- 参数：
--      jobid_input                 : jobid信息
--      taskids                     : 待迁移的task列表
--      init_sync_timeout           : 最大同步等待时间
--      longtime_tx_threshold       : 迁移任务时允许的最大长事务执行时间
--      with_replica_identity_check : 任务启动时是否检查replica_identity
-- 返回值：无
-- 
AS $cigration_run_shard_migration_job$
DECLARE
    target_taskids integer[];
    recordinfo record;
    icurrentnode_port integer;
    dblink_created boolean := false;
    task_total_cnt integer := 0;
    task_total_size bigint := 0;
    completed_task_cnt integer := 0;
    completed_task_size bigint := 0;
    canceled_task_cnt integer := 0;
    canceled_task_size bigint := 0;
    pg_size_pretty_processed text;
    pg_size_pretty_total text;
    exec_result_msg text;
    error_msg text;
BEGIN
    --执行节点必须是CN节点
    IF (SELECT CASE WHEN (select count(*) from pg_dist_node)>0 THEN (select groupid from pg_dist_local_group) ELSE -1 END) <> 0 THEN
        RAISE EXCEPTION 'function cigration_run_shard_migration_job could only be executed on coordinate node.';
    END IF;

    IF NOT (select pg_try_advisory_xact_lock_shared('cigration.pg_citus_shard_migration'::regclass::int)) THEN
        RAISE EXCEPTION 'Can not call cigration api concurrently.';
    END IF;

    --判断jobid_input是否为空
    IF (jobid_input is null) THEN
        IF (SELECT COUNT(distinct jobid) FROM cigration.pg_citus_shard_migration) > 1 THEN
            RAISE EXCEPTION 'There is more than one job, the jobid must be specified explicitly.';
        ELSIF (SELECT COUNT(distinct jobid) FROM cigration.pg_citus_shard_migration) = 1 THEN
            SELECT distinct jobid INTO STRICT jobid_input FROM cigration.pg_citus_shard_migration;
        ELSE
            RAISE WARNING 'there are no migration job';
            RETURN true;
        END IF;
    END IF;
    
    IF NOT (select pg_try_advisory_xact_lock('cigration.pg_citus_shard_migration'::regclass::int, jobid_input)) THEN
        RAISE EXCEPTION 'job % have been started.', jobid_input;
    END IF;

    IF (SELECT COUNT(*) FROM cigration.pg_citus_shard_migration WHERE jobid=jobid_input AND status in('running','error')) > 1 THEN
        RAISE EXCEPTION 'Can not start job %, because it contains task with running or error status.', jobid_input;
    END IF;

    IF (taskids is null) THEN
        select array_agg(taskid) INTO STRICT target_taskids FROM cigration.pg_citus_shard_migration where jobid=jobid_input;
    ELSE
        target_taskids := taskids;
    END IF;

    --get the current node port
    SELECT setting INTO icurrentnode_port FROM pg_settings where name='port';

    --get the task total count
    SELECT count(*) INTO task_total_cnt from cigration.pg_citus_shard_migration where jobid=jobid_input and taskid in (select unnest(target_taskids));
    
    IF task_total_cnt = 0 THEN
        RAISE WARNING 'there are no migration tasks';
        RETURN true;
    END IF;

    --get the task total size
    SELECT sum(total_shard_size) INTO task_total_size from cigration.pg_citus_shard_migration where jobid=jobid_input and taskid in (select unnest(target_taskids));
    SELECT pg_size_pretty(task_total_size) INTO pg_size_pretty_total;

    for recordinfo in SELECT taskid, source_nodename, source_nodeport, target_nodename, target_nodeport, status, error_message, total_shard_size from cigration.pg_citus_shard_migration where jobid=jobid_input and taskid in (select unnest(target_taskids)) ORDER BY taskid loop
        --check task status
        IF recordinfo.status = 'error' THEN
            raise 'Task[%] of job[%] is in error status because of ''%''. Please calling ''SELECT cigration.cigration_cleanup_error_env()'' to cleanup!', recordinfo.taskid, jobid_input, recordinfo.error_message;
        ELSIF recordinfo.status = 'canceled' THEN
            canceled_task_cnt := canceled_task_cnt + 1;
            canceled_task_size := canceled_task_size + recordinfo.total_shard_size;
            SELECT pg_size_pretty(canceled_task_size + completed_task_size) INTO pg_size_pretty_processed;
            raise notice '% [%/%] migration task % skipped. (processed/total/percent: %/%/% %%)', 
                    clock_timestamp(), canceled_task_cnt+completed_task_cnt, task_total_cnt, recordinfo.taskid, 
                    pg_size_pretty_processed, pg_size_pretty_total, ((canceled_task_size + completed_task_size)*100)/task_total_size;
            CONTINUE;
        END IF;

        --create dblink connection
        PERFORM dblink_disconnect(con) 
        FROM (select unnest(a) con from dblink_get_connections() a)b 
        WHERE con = 'cigration_run_shard_migration_job';

        PERFORM dblink_connect('cigration_run_shard_migration_job',
                                format('host=%s port=%s user=%s dbname=%s',
                                        '127.0.0.1',
                                        icurrentnode_port,
                                        current_user,
                                        current_database()));
        dblink_created := true;

        --shard migration
        BEGIN
            -- 设置log_min_messages为notice，记录迁移详细信息到错误日志文件
            PERFORM dblink_exec('cigration_run_shard_migration_job','set log_min_messages to notice');

            IF recordinfo.status = 'init' THEN
                --start migration
                EXECUTE format($$SELECT * FROM dblink('cigration_run_shard_migration_job', 
                                                      $sql$SELECT cigration.cigration_start_shard_migration_task(%s, %s, longtime_tx_threshold=>'%s', with_replica_identity_check=>'%s')$sql$
                                                      ) AS t(result_record text)$$,
                                jobid_input, recordinfo.taskid, longtime_tx_threshold, with_replica_identity_check);

            END IF;

            IF recordinfo.status in ('init', 'running') THEN
                --complete migration
                SELECT exec_result into STRICT exec_result_msg
                    from dblink('cigration_run_shard_migration_job', 
                                       format('SELECT cigration.cigration_complete_shard_migration_task(%s, %s, init_sync_timeout=>%s)',
                                       jobid_input, recordinfo.taskid, init_sync_timeout)) t(exec_result text);

                -- checking the execute result of cigration_complete_shard_migration_task
                IF exec_result_msg != 'complete' THEN
                    raise warning 'Method cigration.cigration_complete_shard_migration_task(%, %, init_sync_timeout=>%) finished with ''%'' when migrating from %:% to %:%, please calling ''cigration.cigration_run_shard_migration_job'' again!', jobid_input, recordinfo.taskid, init_sync_timeout, exec_result_msg, recordinfo.source_nodename, recordinfo.source_nodeport, recordinfo.target_nodename, recordinfo.target_nodeport;
                    return false;
                END IF;
            END IF;

            --cleanup migration
            EXECUTE format($$SELECT * FROM dblink('cigration_run_shard_migration_job', 
                                                  'SELECT cigration.cigration_cleanup_shard_migration_task(%s, %s)'
                                                  ) AS t(result_record text)$$,
                            jobid_input, recordinfo.taskid);

            completed_task_cnt := completed_task_cnt + 1;
            completed_task_size := completed_task_size + recordinfo.total_shard_size;
            SELECT pg_size_pretty(canceled_task_size + completed_task_size) INTO pg_size_pretty_processed;
--            raise notice '% [%/%] migration task % completed. (processed/total/percent: %/%/% %%)', 
--                    clock_timestamp(), canceled_task_cnt+completed_task_cnt, task_total_cnt, recordinfo.taskid, 
--                    pg_size_pretty_processed, pg_size_pretty_total, ((canceled_task_size + completed_task_size)*100)/task_total_size;

            raise notice '% [%/%] migration task % completed. (processed/total/percent: %/%/% %%)', 
                    clock_timestamp(), canceled_task_cnt+completed_task_cnt, task_total_cnt, recordinfo.taskid, 
                    pg_size_pretty_processed, pg_size_pretty_total,
                    CASE task_total_size WHEN 0 THEN 100 ELSE ((canceled_task_size + completed_task_size)*100)/task_total_size END;


        EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
            IF dblink_created THEN
                BEGIN
                    PERFORM dblink_disconnect('cigration_run_shard_migration_job');
                EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
                    GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
                    RAISE WARNING 'failed to call dblink_disconnect:%', error_msg;
                END;
            END IF;

            RAISE;
        END;

        --cleanup
        --每次循环都重新获取dblink
        BEGIN
            PERFORM dblink_disconnect('cigration_run_shard_migration_job');
        EXCEPTION WHEN QUERY_CANCELED or OTHERS THEN
            GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
            RAISE WARNING 'failed to call dblink_disconnect:%', error_msg;
        END;
    end loop;
    --all task migration complete
    return true;
END;
$cigration_run_shard_migration_job$ LANGUAGE plpgsql;

-- 
-- Judge table structure
-- If column numbers,names,types and primary key in two tables are same,
-- then we consider two tables' structure are same
-- RETURN true : different 
-- RETURN false: same 
--
CREATE or replace FUNCTION cigration.judge_table_structure(source_table regclass,target_table regclass)
RETURNS boolean
AS $$
    DECLARE
      source_attr_names text[];
      target_attr_names text[];
      source_attr_types text[];
      target_attr_types text[];
      source_pk_cols text[];
      target_pk_cols text[];
    BEGIN
      -- judge if source table is completely same as target table
      SELECT array_agg(attname)
      INTO   STRICT source_attr_names
      FROM   pg_attribute
      WHERE  attrelid = source_table AND attnum > 0 AND NOT attisdropped;
        
      SELECT array_agg(format_type(a.atttypid,a.atttypmod))
      INTO   STRICT source_attr_types
      FROM   pg_class as c,pg_attribute as a
      WHERE  a.attrelid = source_table AND a.attrelid = c.oid AND a.attnum>0 AND NOT attisdropped; 
      --WHERE  c.relname = source_table::text AND a.attrelid = c.oid AND a.attnum>0 AND NOT attisdropped;
      
      SELECT array_agg(attname)
      INTO   STRICT target_attr_names
      FROM   pg_attribute
      WHERE  attrelid = target_table AND attnum > 0 AND NOT attisdropped;
        
      SELECT array_agg(format_type(a.atttypid,a.atttypmod))
      INTO   STRICT target_attr_types
      FROM   pg_class as c,pg_attribute as a
      WHERE  a.attrelid = target_table AND a.attrelid = c.oid AND a.attnum>0 AND NOT attisdropped;
      --WHERE  c.relname = target_table::text AND a.attrelid = c.oid AND a.attnum>0 AND NOT attisdropped;
      
      SELECT array_agg(pg_attribute.attname)
      INTO STRICT source_pk_cols
      FROM pg_constraint
      INNER JOIN pg_class ON pg_constraint.conrelid = pg_class.oid
      INNER JOIN pg_attribute ON pg_attribute.attrelid = pg_class.oid and pg_attribute.attnum  between pg_constraint.conkey[1] and pg_constraint.conkey[array_length(pg_constraint.conkey, 1)]
      WHERE pg_attribute.attrelid = source_table AND pg_constraint.contype = 'p';
            
      SELECT array_agg(pg_attribute.attname)
      INTO STRICT target_pk_cols
      FROM pg_constraint
      INNER JOIN pg_class ON pg_constraint.conrelid = pg_class.oid
      INNER JOIN pg_attribute ON pg_attribute.attrelid = pg_class.oid and pg_attribute.attnum  between pg_constraint.conkey[1] and pg_constraint.conkey[array_length(pg_constraint.conkey, 1)]
      WHERE pg_attribute.attrelid = target_table AND pg_constraint.contype = 'p';

      if ((source_pk_cols is NULL) or (target_pk_cols is NULL) or (source_attr_types != target_attr_types) or (source_pk_cols != target_pk_cols) or (source_attr_names != target_attr_names)) then
          RETURN true;
      else
          RETURN false;
      end if;
    END;
$$ LANGUAGE plpgsql;


-- 
-- Creates a reference table exactly like the specified target table along with
-- a trigger to redirect any INSERTed,UPDATEDed,DELETEd or TRUNCATEd rows from 
-- the target table to reference table.Returns the name of the proxy table that 
-- was created.
--
CREATE or replace FUNCTION cigration.create_sync_trigger_for_table(source_table regclass,target_table regclass)
RETURNS text
AS $create_sync_trigger_for_table$
    DECLARE
        judge_result boolean;
        target_table_name text;
        target_schema_name text;
        source_table_name text;
        source_schema_name text;
        attr_names text[];
        pk_colname text[];
        attr_list text;
        param_list text;
        insert_list text;
        new_list text;
        old_list text;
        update_list text;
        delete_list text;
        set_list text;
        where_list_delete text;
        where_list_update text;
          insert_command text;
         update_command text;
        delete_command text;
    BEGIN
      -- if parameter is write as 'sc1.tb1',then change '.' to '_' to name func_tmpl
      SELECT cigration.judge_table_structure(source_table,target_table) INTO judge_result;
      if ( judge_result ) then
          RAISE EXCEPTION 'There is no primary key in source table or source table structure is not same as the target table.';
          --RETURN NULL;
      end if;
      
      -- get schema name and table name from system table
      -- we do not recommend use type conversion like 'regclass::text'
      -- because if table's schema has already been in search_path,then pg will ignore schema automatically
      -- e.g. search_path is "test","$user",public, then 'test.tb1'::regclass::text will be 'tb1' instead of 'test.tb1'
      SELECT schemaname,relname 
      INTO source_schema_name,source_table_name
      FROM pg_stat_all_tables where relid = source_table;
      
      SELECT schemaname,relname 
      INTO target_schema_name,target_table_name
      FROM pg_stat_all_tables where relid = target_table;
      
      DECLARE
          -- templates to create dynamic functions, tables, and triggers
          func_name text := 'cigration.sync_trigger_' || source_table::oid || '_to_' || target_table::oid;
          
          func_tmpl CONSTANT text := $$CREATE or replace FUNCTION %s()
                                     RETURNS trigger
                                     AS $sync_trigger$
                                     BEGIN
                                        if (TG_OP = 'INSERT') then
                                              EXECUTE %L USING %s;
                                         elsif (TG_OP = 'UPDATE') then
                                              EXECUTE %L USING %s;
                                         elsif (TG_OP = 'DELETE') then
                                              EXECUTE %L USING %s;
                                         elsif (TG_OP = 'TRUNCATE') then
                                              truncate %s;
                                         end if;
                                        RETURN NULL;
                                     END;
                                     $sync_trigger$ LANGUAGE plpgsql;$$;
          -- INSERT UPDATE DELETE trigger
          trigger_tmpl CONSTANT text := $$CREATE TRIGGER sync_trigger
                                        AFTER INSERT or UPDATE or DELETE ON %s FOR EACH ROW
                                        EXECUTE PROCEDURE %s()$$;
          -- TRUNCATE trigger
          trigger_truncate_tmpl CONSTANT text := $$CREATE TRIGGER sync_trigger_truncate
                                                     AFTER TRUNCATE ON %s FOR EACH STATEMENT
                                                 EXECUTE PROCEDURE %s()$$;
        BEGIN
            -- get list of all attributes in table, we'll need
            SELECT array_agg(attname)
            INTO   STRICT attr_names
            FROM   pg_attribute
            WHERE  attrelid = source_table AND attnum > 0 AND NOT attisdropped;

            -- build fully specified column list and USING clause from attr. names for insert cmd
            SELECT string_agg(quote_ident(attr_name), ','),
                   string_agg(format('NEW.%I', attr_name), ',')
            INTO   STRICT attr_list,
                          insert_list
            FROM   unnest(attr_names) AS attr_name;

            -- get primary key of source table
            SELECT array_agg(pg_attribute.attname)
            INTO STRICT pk_colname
            FROM pg_constraint
            INNER JOIN pg_class ON pg_constraint.conrelid = pg_class.oid
            INNER JOIN pg_attribute ON pg_attribute.attrelid = pg_class.oid and pg_attribute.attnum  between pg_constraint.conkey[1] and pg_constraint.conkey[array_length(pg_constraint.conkey, 1)]
            WHERE pg_attribute.attrelid = source_table AND pg_constraint.contype = 'p';
                    
            -- build where list for delete
            SELECT string_agg(format('%I = %s', pk_colname[i],'$' || i),' and ')
            INTO   STRICT where_list_delete
            FROM   generate_series(1, array_length(pk_colname, 1)) AS i;

            -- build ($1, $2, $3)-style VALUE list to bind parameters
            SELECT string_agg('$' || param_num, ',')
            INTO   STRICT param_list
            FROM   generate_series(1, array_length(attr_names, 1)) AS param_num;
            
            -- build set list for update
            SELECT string_agg(format('%I = %s', attr_names[i],'$' || i),',')
            INTO   STRICT set_list
            FROM   generate_series(1, array_length(attr_names, 1)) AS i;

            -- build where list for update
            SELECT string_agg(format('%I = %s', pk_colname[i],'$' || array_length(attr_names, 1)+i),' and ')
            INTO   STRICT where_list_update
            FROM   generate_series(1, array_length(pk_colname, 1)) AS i;
            
            -- build using list for update
            SELECT string_agg(format('NEW.%I', attr_name), ',')
            INTO   new_list
            FROM   unnest(attr_names) AS attr_name;

            SELECT string_agg(format('OLD.%I', pk_col), ',')
            INTO   old_list
            FROM   unnest(pk_colname) AS pk_col;

            SELECT new_list || ',' || old_list
            INTO   update_list;

            -- build using list for delete
            SELECT string_agg(format('OLD.%I', pk_col), ',')
            INTO   delete_list
            FROM   unnest(pk_colname) AS pk_col;

            -- use the above lists to generate appropriate command
            insert_command = format('INSERT INTO %s (%s) VALUES (%s)', target_schema_name || '.' || target_table_name, attr_list, param_list);
            update_command = format('UPDATE %s SET %s WHERE %s', target_schema_name || '.' || target_table_name, set_list, where_list_update);
            delete_command = format('DELETE FROM %s WHERE %s', target_schema_name || '.' || target_table_name, where_list_delete);

            -- use the command to make one-off trigger targeting specified table
            EXECUTE format(func_tmpl,func_name,insert_command,insert_list,update_command,update_list,delete_command,delete_list,target_schema_name || '.' || target_table_name);

            -- install the trigger on source table
            EXECUTE format(trigger_tmpl, source_schema_name || '.' || source_table_name,func_name);
            EXECUTE format(trigger_truncate_tmpl, source_schema_name || '.' || source_table_name,func_name);

            RETURN NULL;
          END;
    END;
$create_sync_trigger_for_table$ LANGUAGE plpgsql;


-- 
-- Specify a set of partition column's value of the citus distribution table,
-- return shardtable's name and the worker node and the port.
-- partition_values' type is text, char or varchar.
-- 
-- Sample:
--  SELECT * FROM pg_get_dist_shard_placement('testtb',ARRAY['1','2','3'])
--
CREATE OR REPLACE FUNCTION cigration.pg_get_dist_shard_placement(
        distributed_table regclass,  -- citus distributed table name
        partition_values text[]      -- partition column value array
    ) RETURNS TABLE
        (
            partitionvalue text,     -- Partition column value
            shardtable text,         -- shard tablename
            nodename text,           -- nodename of shardtable
            nodeport int             -- nodeport number of shardtable
        ) AS $$
DECLARE
    check_distributed_table bigint :=0;
BEGIN

    SELECT
    COUNT( 1 ) INTO
        check_distributed_table
    FROM
        pg_dist_partition
    WHERE
        logicalrelid = distributed_table
        AND partmethod = 'h'
        AND partkey ~ ':vartype (1043|1042|25) '; -- varchar/char/text

    ASSERT check_distributed_table = 1  , 
        'only support distributed table with hash partition, and partition column datatype must be varchar,char,text,smallint,int or bigint';
    
    
    RETURN QUERY SELECT
            a.partitionvalue,
            logicalrelid || '_' || b.shardid AS shardtable,
            c.nodename,
            c.nodeport::int
        FROM
            UNNEST(partition_values) AS a(partitionvalue)
        LEFT JOIN pg_dist_shard b ON
            (
                logicalrelid = distributed_table
                AND shardminvalue::int <= hashtext(a.partitionvalue)
                AND hashtext(a.partitionvalue) <= shardmaxvalue::int
            )
        LEFT JOIN pg_dist_shard_placement c ON
            (
                b.shardid = c.shardid
                AND shardstate = 1
            );
END;
$$ LANGUAGE plpgsql;

-- 
-- Specify a set of partition column's value of the citus distribution table,
-- return shardtable's name and the worker node and the port.
-- partition_values' type is smallint.
-- 
-- Sample:
--  SELECT * FROM pg_get_dist_shard_placement('testtb',ARRAY[1,2,3])
--
CREATE OR REPLACE FUNCTION cigration.pg_get_dist_shard_placement(
        distributed_table regclass,  -- citus distributed table name
        partition_values smallint[]  -- partition column value array
    ) RETURNS TABLE
        (
            partitionvalue smallint, -- Partition column value
            shardtable text,         -- shard tablename
            nodename text,           -- nodename of shardtable
            nodeport int             -- nodeport number of shardtable
        ) AS $$
DECLARE
    check_distributed_table bigint :=0;
BEGIN

    SELECT
    COUNT( 1 ) INTO
        check_distributed_table
    FROM
        pg_dist_partition
    WHERE
        logicalrelid = distributed_table
        AND partmethod = 'h'
        AND partkey ~ ':vartype 21 '; -- smallint

    ASSERT check_distributed_table = 1  , 
        'only support distributed table with hash partition, and partition column datatype must be varchar,char,text,smallint,int or bigint';
    
    
    RETURN QUERY SELECT
            a.partitionvalue,
            logicalrelid || '_' || b.shardid AS shardtable,
            c.nodename,
            c.nodeport::int
        FROM
            UNNEST(partition_values) AS a(partitionvalue)
        LEFT JOIN pg_dist_shard b ON
            (
                logicalrelid = distributed_table
                AND shardminvalue::int <= hashint2(a.partitionvalue)
                AND hashint2(a.partitionvalue) <= shardmaxvalue::int
            )
        LEFT JOIN pg_dist_shard_placement c ON
            (
                b.shardid = c.shardid
                AND shardstate = 1
            );
END;
$$ LANGUAGE plpgsql;

-- 
-- Specify a set of partition column's value of the citus distribution table,
-- return shardtable's name and the worker node and the port.
-- partition_values' type is int.
-- 
-- Sample:
--  SELECT * FROM pg_get_dist_shard_placement('testtb',ARRAY[1,2,3])
--
CREATE OR REPLACE FUNCTION cigration.pg_get_dist_shard_placement(
        distributed_table regclass,  -- citus distributed table name
        partition_values int[]       -- partition column value array
    ) RETURNS TABLE
        (
            partitionvalue int,      -- Partition column value
            shardtable text,         -- shard tablename
            nodename text,           -- nodename of shardtable
            nodeport int             -- nodeport number of shardtable
        ) AS $$
DECLARE
    check_distributed_table bigint :=0;
BEGIN

    SELECT
    COUNT( 1 ) INTO
        check_distributed_table
    FROM
        pg_dist_partition
    WHERE
        logicalrelid = distributed_table
        AND partmethod = 'h'
        AND partkey ~ ':vartype 23 '; -- int

    ASSERT check_distributed_table = 1  , 
        'only support distributed table with hash partition, and partition column datatype must be varchar,char,text,smallint,int or bigint';
    
    
    RETURN QUERY SELECT
            a.partitionvalue,
            logicalrelid || '_' || b.shardid AS shardtable,
            c.nodename,
            c.nodeport::int
        FROM
            UNNEST(partition_values) AS a(partitionvalue)
        LEFT JOIN pg_dist_shard b ON
            (
                logicalrelid = distributed_table
                AND shardminvalue::int <= hashint4(a.partitionvalue)
                AND hashint4(a.partitionvalue) <= shardmaxvalue::int
            )
        LEFT JOIN pg_dist_shard_placement c ON
            (
                b.shardid = c.shardid
                AND shardstate = 1
            );
END;
$$ LANGUAGE plpgsql;

-- 
-- Specify a set of partition column's value of the citus distribution table,
-- return shardtable's name and the worker node and the port.
-- partition_values' type is bigint.
-- 
-- Sample:
--  SELECT * FROM pg_get_dist_shard_placement('testtb',ARRAY[1,2,3])
--
CREATE OR REPLACE FUNCTION cigration.pg_get_dist_shard_placement(
        distributed_table regclass,  -- citus distributed table name
        partition_values bigint[]    -- partition column value array
    ) RETURNS TABLE
        (
            partitionvalue bigint,   -- Partition column value
            shardtable text,         -- shard tablename
            nodename text,           -- nodename of shardtable
            nodeport int             -- nodeport number of shardtable
        ) AS $$
DECLARE
    check_distributed_table bigint :=0;
BEGIN

    SELECT
    COUNT( 1 ) INTO
        check_distributed_table
    FROM
        pg_dist_partition
    WHERE
        logicalrelid = distributed_table
        AND partmethod = 'h'
        AND partkey ~ ':vartype 20 '; -- bigint

    ASSERT check_distributed_table = 1  , 
        'only support distributed table with hash partition, and partition column datatype must be varchar,char,text,smallint,int or bigint';
    
    
    RETURN QUERY SELECT
            a.partitionvalue,
            logicalrelid || '_' || b.shardid AS shardtable,
            c.nodename,
            c.nodeport::int
        FROM
            UNNEST(partition_values) AS a(partitionvalue)
        LEFT JOIN pg_dist_shard b ON
            (
                logicalrelid = distributed_table
                AND shardminvalue::int <= hashint8(a.partitionvalue)
                AND hashint8(a.partitionvalue) <= shardmaxvalue::int
            )
        LEFT JOIN pg_dist_shard_placement c ON
            (
                b.shardid = c.shardid
                AND shardstate = 1
            );
END;
$$ LANGUAGE plpgsql;

-- 
-- overload func "pg_get_dist_shard_placement"(partition column type is text).
--
CREATE OR REPLACE FUNCTION cigration.pg_get_dist_shard_placement(
        distributed_table regclass,  -- citus distributed table name
        partition_value text         -- partition column value
    ) RETURNS TABLE
        (
            partitionvalue text,     -- Partition column value
            shardtable text,         -- shard tablename
            nodename text,           -- nodename of shardtable
            nodeport int             -- nodeport number of shardtable
        ) AS $$
BEGIN
    RETURN QUERY SELECT
    *
FROM
    cigration.pg_get_dist_shard_placement(
        distributed_table,
        ARRAY[partition_value]
    );
END;
$$ LANGUAGE plpgsql;

-- 
-- overload func "pg_get_dist_shard_placement"(partition column type is smallint).
--
CREATE OR REPLACE FUNCTION cigration.pg_get_dist_shard_placement(
        distributed_table regclass,  -- citus distributed table name
        partition_value smallint     -- partition column value
    ) RETURNS TABLE
        (
            partitionvalue smallint, -- Partition column value
            shardtable text,         -- shard tablename
            nodename text,           -- nodename of shardtable
            nodeport int             -- nodeport number of shardtable
        ) AS $$
BEGIN
    RETURN QUERY SELECT
    *
FROM
    cigration.pg_get_dist_shard_placement(
        distributed_table,
        ARRAY[partition_value]
    );
END;
$$ LANGUAGE plpgsql;

-- 
-- overload func "pg_get_dist_shard_placement"(partition column type is int).
--
CREATE OR REPLACE FUNCTION cigration.pg_get_dist_shard_placement(
        distributed_table regclass,  -- citus distributed table name
        partition_value int          -- partition column value
    ) RETURNS TABLE
        (
            partitionvalue int,      -- Partition column value
            shardtable text,         -- shard tablename
            nodename text,           -- nodename of shardtable
            nodeport int             -- nodeport number of shardtable
        ) AS $$
BEGIN
    RETURN QUERY SELECT
    *
FROM
    cigration.pg_get_dist_shard_placement(
        distributed_table,
        ARRAY[partition_value]
    );
END;
$$ LANGUAGE plpgsql;

-- 
-- overload func "pg_get_dist_shard_placement"(partition column type is bigint).
--
CREATE OR REPLACE FUNCTION cigration.pg_get_dist_shard_placement(
        distributed_table regclass,  -- citus distributed table name
        partition_value bigint       -- partition column value
    ) RETURNS TABLE
        (
            partitionvalue bigint,   -- Partition column value
            shardtable text,         -- shard tablename
            nodename text,           -- nodename of shardtable
            nodeport int             -- nodeport number of shardtable
        ) AS $$
BEGIN
    RETURN QUERY SELECT
    *
FROM
    cigration.pg_get_dist_shard_placement(
        distributed_table,
        ARRAY[partition_value]
    );
END;
$$ LANGUAGE plpgsql;

-- 
--  auto cleanup shards which should be deleted
--
CREATE OR REPLACE FUNCTION cigration.cleanup_deleted_shard_placements () 
    RETURNS TABLE (nodename TEXT,     -- nodename of shardtable
        nodeport INT,                 -- nodeport number of shardtable
        shardid BIGINT,             -- shardid
        RESULT TEXT                 -- execute status, either the command's status string or ERROR
)
AS $$
DECLARE
    SQL TEXT := $sql$ DO $sqlbody$
    DECLARE
        table_name TEXT;
BEGIN
    SELECT
        string_agg(OID::regclass::text,
            '_[error]_') INTO table_name
    FROM
        pg_class
    WHERE
        OID = '%s'::regclass
        AND relkind = 'r'
        AND relnamespace NOT IN (
            SELECT
                OID
            FROM
                pg_namespace
            WHERE
                nspname ~ '^(pg_toast|pg_temp_[0-9]+|pg_toast_temp_[0-9]+|pg_catalog|information_schema)$');
    EXECUTE format('DROP TABLE %%I CASCADE',
        table_name);
END;
$sqlbody$ $sql$;
BEGIN
    RETURN QUERY
    SELECT
        p.nodename,
        p.nodeport::INT,
        p.shardid,
        dblink_exec('host=' || p.nodename || ' port=' || p.nodeport || ' dbname=' || current_database(),
            format(SQL,
                shard_name (logicalrelid,
                    p.shardid)),
            FALSE)
        RESULT
    FROM
        pg_dist_shard_placement p
    LEFT JOIN pg_dist_shard s ON (s.shardid = p.shardid)
WHERE
    shardstate = 4;
    RETURN;
END;
$$
LANGUAGE plpgsql;

--  
-- repare inactive_shard 
--
CREATE OR REPLACE FUNCTION cigration.pg_repair_inactive_shard ()
    RETURNS void
AS $$
DECLARE
    inactive_shard_shardid BIGINT := 0;
    inactive_shard_nodename TEXT := '';
    inactive_shard_nodeport INT := 0;
    has_good_shard BIGINT := NULL;
    good_shard_nodename TEXT := '';
    good_shard_nodeport INT := 0;
    tmp RECORD;
BEGIN
    FOR tmp IN
    SELECT
        p.shardid,
        p.nodename,
        p.nodeport,
        s.logicalrelid
    FROM
        pg_dist_shard_placement p LEFT JOIN pg_dist_shard s ON (s.shardid = p.shardid)
    WHERE
        p.shardstate = 3 LOOP
            inactive_shard_shardid = tmp.shardid;
            inactive_shard_nodename = tmp.nodename;
            inactive_shard_nodeport = tmp.nodeport;
            SELECT
                count(shardid),
                nodename,
                nodeport INTO has_good_shard,
                good_shard_nodename,
                good_shard_nodeport
            FROM
                pg_dist_shard_placement
            WHERE
                shardid = inactive_shard_shardid
                AND shardstate = 1
            GROUP BY
                nodename,
                nodeport
            LIMIT 1;
            if has_good_shard is NULL then
                RAISE EXCEPTION '%_% has no good shard to repair inactive shard!',tmp.logicalrelid,tmp.shardid;
            else
                PERFORM
                    master_copy_shard_placement (inactive_shard_shardid,
                        good_shard_nodename,
                        good_shard_nodeport,
                        inactive_shard_nodename,
                        inactive_shard_nodeport);
            end if;
        END LOOP;
END;
$$
LANGUAGE plpgsql;