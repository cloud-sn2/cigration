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

-- 创建1组亲和的分区分片表(Citus目前不支持多级分区的分片表)

-- dist1是范围分区表
CREATE TABLE dist1(c1 int, c2 text, c3 int, c4 int, primary key(c1,c3))
    PARTITION BY RANGE (c3);

CREATE TABLE dist1_1 PARTITION OF dist1
    FOR VALUES FROM (1) TO (50001);
CREATE TABLE dist1_2 PARTITION OF dist1
    FOR VALUES FROM (50001) TO (100001);

-- dist2是列表分区表
CREATE TABLE dist2(c1 int, c2 text, c3 int, c4 int, primary key(c1,c3))
    PARTITION BY LIST (c3);

CREATE TABLE dist2_1 PARTITION OF dist2
    FOR VALUES IN (0,1,2,3,4);

CREATE TABLE dist2_2 PARTITION OF dist2
    FOR VALUES IN (5,6,7,8,9);


-- dist3是非分区表
create table dist3(c1 int primary key, c2 text, c3 int, c4 int);

-- 特殊表名(63字节)的范围分区表
CREATE TABLE "1234567890123456789012345678901234567890_Maxdist4_0123456789012"(c1 int, c2 text, c3 int, c4 int)
    PARTITION BY RANGE (c3);

CREATE TABLE "1234567890123456789012345678901234567890_Maxdist4_01234567890_1" PARTITION OF "1234567890123456789012345678901234567890_Maxdist4_0123456789012"
    FOR VALUES FROM (1) TO (50001);
CREATE TABLE "1234567890123456789012345678901234567890_Maxdist4_01234567890_2" PARTITION OF "1234567890123456789012345678901234567890_Maxdist4_0123456789012"
    FOR VALUES FROM (50001) TO (100001);


select create_distributed_table('dist1','c1', colocate_with=>'default');
select create_distributed_table('dist2','c1', colocate_with=>'default');
select create_distributed_table('dist3','c1', colocate_with=>'default');
select create_distributed_table('"1234567890123456789012345678901234567890_Maxdist4_0123456789012"','c1', colocate_with=>'default');

-- 插入数据
insert into dist1 select id, 'aaa', id, id from generate_series(1,100000) id;
insert into dist2 select id, 'bbb', id%10, id from generate_series(1,10000) id;
insert into dist3 select id, 'ccc', id, id from generate_series(1,100000) id;
insert into "1234567890123456789012345678901234567890_Maxdist4_0123456789012" select id, 'ccc', id, id from generate_series(1,100000) id;

-- 查看所有分片的初始分布
select nodename,
       nodeport,
	   logicalrelid,
	   shardminvalue,
	   shardmaxvalue
from pg_dist_shard_placement p
     join pg_dist_shard s on(p.shardid = s.shardid)
order by nodename,nodeport,logicalrelid,shardminvalue;

--
-- 2. 分片迁移
--

-- 迁移worker1的所有分片到worker3（worker迁移）
select jobid from cigration_create_worker_migration_job(:'worker_1_host', :worker_1_port, :'worker_3_host', :worker_3_port) limit 1 \gset

select source_nodename,source_nodeport,target_nodename,target_nodeport,status,total_shard_count
from pg_citus_shard_migration;

select cigration_run_shard_migration_job(:jobid);

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

-- 分片再平衡(扩容)
select jobid from cigration_create_rebalance_job() limit 1 \gset

select cigration_run_shard_migration_job(:jobid);

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
select count(*) from dist1 a 
  left join dist2 b on(a.c1=b.c1) 
  left join dist3 c on (a.c1=c.c1) 
  left join "1234567890123456789012345678901234567890_Maxdist4_0123456789012" d on (a.c1=d.c1);

-- 从worker2和worker3迁移走所有分片（缩容）
select jobid from cigration_create_worker_empty_job(array[concat(:'worker_2_host', ':', :worker_2_port), concat(:'worker_3_host', ':', :worker_3_port)]) limit 1 \gset

select cigration_run_shard_migration_job(:jobid);

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
select count(*) from dist1 a 
  left join dist2 b on(a.c1=b.c1) 
  left join dist3 c on (a.c1=c.c1) 
  left join "1234567890123456789012345678901234567890_Maxdist4_0123456789012" d on (a.c1=d.c1);

-- 分片再平衡(扩容)
select jobid from cigration_create_rebalance_job() limit 1 \gset

select cigration_run_shard_migration_job(:jobid);

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
select count(*) from dist1 a 
  left join dist2 b on(a.c1=b.c1) 
  left join dist3 c on (a.c1=c.c1) 
  left join "1234567890123456789012345678901234567890_Maxdist4_0123456789012" d on (a.c1=d.c1);


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
