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

-- 创建2组亲和的分片表

create table dist1(c1 int primary key, c2 text);
create table dist2(c1 int primary key, c2 text);
create table dist3(c1 int primary key, c2 text);
create table dist4(c1 int primary key, c2 text);

select create_distributed_table('dist1','c1', colocate_with=>'none');
select create_distributed_table('dist2','c1', colocate_with=>'dist1');
select create_distributed_table('dist3','c1', colocate_with=>'dist1');
select create_distributed_table('dist4','c1', colocate_with=>'none');

-- 插入数据
insert into dist1 select generate_series(1,100000), 'aaa';
insert into dist2 select generate_series(1,10000), 'bbb';
insert into dist4 select generate_series(1,100000), 'ddd';

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

-- 迁移worker1的所有分片到worker3（worker迁移）
select jobid from cigration_create_worker_migration_job(:'worker_1_host', :'worker_3_host') limit 1 \gset

select cigration_batch_run_migration_tasks(:jobid);

select nodename,
       nodeport,
	   logicalrelid,
	   shardminvalue,
	   shardmaxvalue
from pg_dist_shard_placement p
     join pg_dist_shard s on(p.shardid = s.shardid)
where logicalrelid::text ~ 'dist'
order by nodename,nodeport,logicalrelid,shardminvalue;

-- 检查分片迁移后SQL执行正常
select count(*) from dist1 a left join dist2 b on(a.c1=b.c1) left join dist3 c on (a.c1=c.c1);
select count(*) from dist4;

-- 分片再平衡(扩容)
select jobid from cigration_create_rebalance_job() limit 1 \gset

select cigration_batch_run_migration_tasks(:jobid);

select nodename,
       nodeport,
	   logicalrelid,
	   shardminvalue,
	   shardmaxvalue
from pg_dist_shard_placement p
     join pg_dist_shard s on(p.shardid = s.shardid)
where logicalrelid::text ~ 'dist'
order by nodename,nodeport,logicalrelid,shardminvalue;

-- 检查分片迁移后SQL执行正常
select count(*) from dist1 a left join dist2 b on(a.c1=b.c1) left join dist3 c on (a.c1=c.c1);
select count(*) from dist4;

-- 从worker2和worker3迁移走所有分片（缩容）
select jobid from cigration_create_del_node_job(array[:'worker_2_host',:'worker_3_host']) limit 1 \gset

select cigration_batch_run_migration_tasks(:jobid);

select nodename,
       nodeport,
	   logicalrelid,
	   shardminvalue,
	   shardmaxvalue
from pg_dist_shard_placement p
     join pg_dist_shard s on(p.shardid = s.shardid)
where logicalrelid::text ~ 'dist'
order by nodename,nodeport,logicalrelid,shardminvalue;

-- 检查分片迁移后SQL执行正常
select count(*) from dist1 a left join dist2 b on(a.c1=b.c1) left join dist3 c on (a.c1=c.c1);
select count(*) from dist4;

-- 分片再平衡(扩容)
select jobid from cigration_create_rebalance_job() limit 1 \gset

select cigration_batch_run_migration_tasks(:jobid);

select nodename,
       nodeport,
	   logicalrelid,
	   shardminvalue,
	   shardmaxvalue
from pg_dist_shard_placement p
     join pg_dist_shard s on(p.shardid = s.shardid)
where logicalrelid::text ~ 'dist'
order by nodename,nodeport,logicalrelid,shardminvalue;

-- 检查分片迁移后SQL执行正常
select count(*) from dist1 a left join dist2 b on(a.c1=b.c1) left join dist3 c on (a.c1=c.c1);
select count(*) from dist4;


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


--
-- 4. 测试环境清理
--

-- 确认pg_citus_shard_migration中的记录为空
select count(*) from pg_citus_shard_migration;

truncate pg_citus_shard_migration_history;
truncate pg_citus_shard_migration_sql_log;
