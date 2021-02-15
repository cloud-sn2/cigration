/** 
测试环境信息
CN：127.0.0.1
    WORKER1：worker_1_host/57637
    WORKER2：worker_2_host/57638
    WORKER3：worker_3_host/57639

worker_1_host、worker_2_host、worker_3_host：都是127.0.0.1
*/

set datestyle = ISO, YMD;
set search_path = cigration_regress_test, cigration, public;
set citus.shard_count = 8;
set client_min_messages to warning;

drop schema if exists cigration_regress_test cascade;
create schema cigration_regress_test;

--
-- 1. 建表
--

-- 创建1组亲和的分片表

create table dist1(c1 int primary key, c2 text);
create table dist2(c1 int primary key, c2 text);
create table dist3(c1 int primary key, c2 text);

select create_distributed_table('dist1','c1', colocate_with=>'none');
select create_distributed_table('dist2','c1', colocate_with=>'dist1');
select create_distributed_table('dist3','c1', colocate_with=>'dist1');

-- 插入数据
insert into dist1 select generate_series(1,100000), 'aaa';
insert into dist2 select generate_series(1,10000), 'bbb';
insert into dist3 select generate_series(1,100000), 'ccc';

-- 查看所有分片的初始分布
select nodename,
       nodeport,
       logicalrelid,
       shardminvalue,
       shardmaxvalue
from pg_dist_shard_placement p
     join pg_dist_shard s on(p.shardid = s.shardid)
where logicalrelid::text ~ 'dist'
order by nodename,nodeport,logicalrelid,shardminvalue;

--
-- 2. 分片迁移
--

-- 创建迁移worker1的所有分片到worker3的job并获取第一个迁移任务的jobid和taskid
select jobid, taskid from cigration_create_move_node_job(:'worker_1_host', :worker_1_port, :'worker_3_host', :worker_3_port) limit 1 \gset

select source_nodename,source_nodeport,target_nodename,target_nodeport,status,total_shard_count
from pg_citus_shard_migration where jobid=:jobid and taskid=:taskid;

-- 迁移任务的初始状态为init
select cigration_monitor_shard_migration_task(:jobid, :taskid);

-- 启动迁移任务
select cigration_start_shard_migration_task(:jobid, :taskid);
select cigration_monitor_shard_migration_task(:jobid, :taskid);

-- 取消迁移任务
select cigration_cancel_shard_migration_task(:jobid, :taskid);
select cigration_monitor_shard_migration_task(:jobid, :taskid);
select status from pg_citus_shard_migration where jobid=:jobid and taskid=:taskid;

-- 检查分片位置未变更，且SQL执行正常
select nodename,
       nodeport,
       logicalrelid,
       shardminvalue,
       shardmaxvalue
from pg_dist_shard_placement p
     join pg_dist_shard s on(p.shardid = s.shardid)
where logicalrelid::text ~ 'dist'
order by nodename,nodeport,logicalrelid,shardminvalue;

select count(*) from dist1 a left join dist2 b on(a.c1=b.c1) left join dist3 c on (a.c1=c.c1);

-- 重新启动迁移任务
select cigration_start_shard_migration_task(:jobid, :taskid);
select pg_sleep(10);
select cigration_monitor_shard_migration_task(:jobid, :taskid);

-- 人为构造主键冲突触发逻辑复制错误
SELECT
    source_nodename,
    source_nodeport,
    target_nodename,
    target_nodeport,
    logicalrelid::text table_name,
    shard_name(logicalrelid,s.shardid)
FROM pg_citus_shard_migration m
     JOIN pg_dist_shard s ON(all_colocated_shards_id[1] = s.shardid)
WHERE jobid=:jobid and taskid=:taskid \gset


SELECT dblink_exec(format('host=%s port=%s user=%s dbname=%s',
                  :'target_nodename', :'target_nodeport', CURRENT_USER, current_database()), 
                   format($$insert into %s values(-1,'zzz')$$, 
                          :'shard_name')
                   );

SELECT dblink_exec(format('host=%s port=%s user=%s dbname=%s',
                  :'source_nodename', :'source_nodeport', CURRENT_USER, current_database()), 
                   format($$insert into %s values(-1,'yyy')$$, 
                          :'shard_name')
                   );


select cigration_complete_shard_migration_task(:jobid, :taskid);
select cigration_monitor_shard_migration_task(:jobid, :taskid);

-- 取消迁移任务
select cigration_cancel_shard_migration_task(:jobid, :taskid);
select cigration_monitor_shard_migration_task(:jobid, :taskid);
select status from pg_citus_shard_migration where jobid=:jobid and taskid=:taskid;

-- 检查分片位置未变更，且SQL执行正常
select nodename,
       nodeport,
       logicalrelid,
       shardminvalue,
       shardmaxvalue
from pg_dist_shard_placement p
     join pg_dist_shard s on(p.shardid = s.shardid)
where logicalrelid::text ~ 'dist'
order by nodename,nodeport,logicalrelid,shardminvalue;

-- 删除人为构造主键冲突时插入的数据
SELECT dblink_exec(format('host=%s port=%s user=%s dbname=%s',
                  :'source_nodename', :'source_nodeport', CURRENT_USER, current_database()), 
                   format($$delete from %s where c1 = -1$$, 
                          :'shard_name')
                   );

select count(*) from dist1 a left join dist2 b on(a.c1=b.c1) left join dist3 c on (a.c1=c.c1);

-- 重置被取消的迁移任务
select cigration_cleanup_error_env();
select cigration_monitor_shard_migration_task(:jobid, :taskid);
select status from pg_citus_shard_migration where jobid=:jobid and taskid=:taskid;

-- 重新启动并完成迁移任务
select cigration_start_shard_migration_task(:jobid, :taskid);
select cigration_complete_shard_migration_task(:jobid, :taskid, init_sync_timeout=>10, data_sync_timeout=>20);

-- 检查分片位置已变更，且SQL执行正常
select nodename,
       nodeport,
       logicalrelid,
       shardminvalue,
       shardmaxvalue
from pg_dist_shard_placement p
     join pg_dist_shard s on(p.shardid = s.shardid)
where logicalrelid::text ~ 'dist'
order by nodename,nodeport,logicalrelid,shardminvalue;

-- 归档迁移任务
select cigration_cleanup_shard_migration_task(:jobid, :taskid);

select source_nodename,source_nodeport,target_nodename,target_nodeport,status,total_shard_count
from pg_citus_shard_migration where jobid=:jobid and taskid=:taskid;

select source_nodename,source_nodeport,target_nodename,target_nodeport,status,total_shard_count
from pg_citus_shard_migration_history where jobid=:jobid and taskid=:taskid;

-- 清除pg_citus_shard_migration中的所有未启动的任务
select count(*) from pg_citus_shard_migration;
select cigration_cleanup_shard_migration_task(jobid, taskid) from pg_citus_shard_migration;
select count(*) from pg_citus_shard_migration;

--
-- 3. 旧分片清理
--

select nodename,nodeport,count(*)
from cigration_get_recyclebin_metadata()
group by nodename,nodeport
order by nodename,nodeport;

select cigration_cleanup_recyclebin();

select nodename,nodeport,count(*)
from cigration_get_recyclebin_metadata()
group by nodename,nodeport
order by nodename,nodeport;

