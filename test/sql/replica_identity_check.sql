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
-- 1. 无主键分片表的迁移(with_replica_identity_check=true)
--

-- 创建无主键的分片表

create table nopk_tb1(c1 int, c2 text);
select create_distributed_table('nopk_tb1','c1', colocate_with=>'none');

-- 插入数据
insert into nopk_tb1 select generate_series(1,10000), 'aaa';

-- 查看所有分片的初始分布
select nodename,
       nodeport,
	   logicalrelid,
	   shardminvalue,
	   shardmaxvalue
from pg_dist_shard_placement p
     join pg_dist_shard s on(p.shardid = s.shardid)
order by nodename,nodeport,logicalrelid,shardminvalue;

-- 创建并执行迁移任务
select jobid, taskid from cigration_create_move_node_job(:'worker_1_host', :worker_1_port, :'worker_3_host', :worker_3_port) limit 1 \gset

select cigration_run_shard_migration_job(:jobid, with_replica_identity_check=>true);

-- 检查迁移任务状态，期待迁移异常
select source_nodename,source_nodeport,target_nodename,target_nodeport,status,total_shard_count,error_message
from pg_citus_shard_migration where jobid=:jobid and taskid=:taskid;

--
-- 2. 无主键分片表的迁移(with_replica_identity_check=false)
--

-- 清理错误环境
select cigration_cleanup_error_env();

-- 使用默认with_replica_identity_check参数（true）继续执行迁移任务
select cigration_run_shard_migration_job(:jobid);

select source_nodename,source_nodeport,target_nodename,target_nodeport,status,total_shard_count,error_message
from pg_citus_shard_migration;

-- 查看迁移后所有分片的分布
select nodename,
       nodeport,
	   logicalrelid,
	   shardminvalue,
	   shardmaxvalue
from pg_dist_shard_placement p
     join pg_dist_shard s on(p.shardid = s.shardid)
order by nodename,nodeport,logicalrelid,shardminvalue;

-- 检查分片迁移后SQL执行正常
select count(*) from nopk_tb1;

-- DROP表
drop table nopk_tb1;

--
-- 3. 无主键有replica_identity分片表的迁移(with_replica_identity_check=true)
--

-- 创建无主键有replica_identity的分片表
create table replica_identity_tb1(c1 int not null, c2 text);
create unique index idx_replica_identity_tb1_c1 on replica_identity_tb1(c1);
alter table replica_identity_tb1 REPLICA IDENTITY USING INDEX idx_replica_identity_tb1_c1 ;

select create_distributed_table('replica_identity_tb1','c1', colocate_with=>'none');

-- 插入数据
insert into replica_identity_tb1 select generate_series(1,10000), 'aaa';

-- 查看所有分片的初始分布
select nodename,
       nodeport,
	   logicalrelid,
	   shardminvalue,
	   shardmaxvalue
from pg_dist_shard_placement p
     join pg_dist_shard s on(p.shardid = s.shardid)
order by nodename,nodeport,logicalrelid,shardminvalue;

-- 创建并执行迁移任务
select jobid, taskid from cigration_create_move_node_job(:'worker_1_host', :worker_1_port, :'worker_3_host', :worker_3_port) limit 1 \gset

select cigration_run_shard_migration_job(:jobid, with_replica_identity_check=>true);

select source_nodename,source_nodeport,target_nodename,target_nodeport,status,total_shard_count,error_message
from pg_citus_shard_migration;

-- 查看迁移后所有分片的分布
select nodename,
       nodeport,
	   logicalrelid,
	   shardminvalue,
	   shardmaxvalue
from pg_dist_shard_placement p
     join pg_dist_shard s on(p.shardid = s.shardid)
order by nodename,nodeport,logicalrelid,shardminvalue;

-- 检查分片迁移后SQL执行正常
select count(*) from replica_identity_tb1;

-- DROP表
drop table replica_identity_tb1;


-- 4. 清空垃圾站
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


--
-- 5. 测试环境清理
--
select cigration_cleanup_recyclebin();
-- 确认pg_citus_shard_migration中的记录为空
select count(*) from pg_citus_shard_migration;

