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
-- 1. 创建分片表(create_distributed_table())
--

-- 创建表
create table dist1(c1 int primary key, c2 text);

-- 直接调用2参create_distributed_table()抛出异常，引导用户使用cigration_create_distributed_table()。
select create_distributed_table('dist1','c1');

-- 直接调用3参create_distributed_table()抛出异常，引导用户使用cigration_create_distributed_table()。
select create_distributed_table('dist1','c1', 'hash');

--
-- 2. 创建分片表(cigration_create_distributed_table())
--

-- 2参调用cigration_create_distributed_table()
select cigration_create_distributed_table('dist1','c1');

with t as(
select min(colocationid) min_colocationid from pg_dist_partition where logicalrelid::text ~ 'dist'
)
select logicalrelid, partmethod, partkey,colocationid - min_colocationid + 1 as relative_colocationid
from pg_dist_partition,t where logicalrelid::text ~ 'dist' order by colocationid,logicalrelid::text;

select count(*) from dist1;

-- 2参 + distribution_type 调用cigration_create_distributed_table()
create table dist2(c1 int primary key, c2 text);
select cigration_create_distributed_table('dist2', 'c1', distribution_type=>'hash');

create table dist3(c1 int, c2 text);
select cigration_create_distributed_table('dist3', 'c1', 'append');

with t as(
select min(colocationid) min_colocationid from pg_dist_partition where logicalrelid::text ~ 'dist'
)
select logicalrelid, partmethod, partkey,colocationid - min_colocationid + 1 as relative_colocationid
from pg_dist_partition,t where logicalrelid::text ~ 'dist' order by colocationid,logicalrelid::text;

select count(*) from dist2;
select count(*) from dist3;

-- 2参 + colocate_with 调用cigration_create_distributed_table()

create table dist4(c1 int primary key, c2 text);
select cigration_create_distributed_table('dist4','c1', colocate_with=>'none');

create table dist5(c1 int primary key, c2 text);
select cigration_create_distributed_table('dist5','c1', colocate_with=>'dist1');

create table dist6(c1 int primary key, c2 text);
select cigration_create_distributed_table('dist6','c1', colocate_with=>'default');

with t as(
select min(colocationid) min_colocationid from pg_dist_partition where logicalrelid::text ~ 'dist'
)
select logicalrelid, partmethod, partkey,colocationid - min_colocationid + 1 as relative_colocationid
from pg_dist_partition,t where logicalrelid::text ~ 'dist' order by colocationid,logicalrelid::text;

select count(*) from dist1 a left join dist5 b on(a.c1=b.c1) left join dist6 c on (a.c1=c.c1);

--
-- 3. 存在迁移任务时，不允许创建colocate_with非none的hash分片表，防止破坏分片表的亲和依赖
--

-- 创建分片迁移任务
select jobid from cigration_create_worker_migration_job(:'worker_1_host', :'worker_3_host') limit 1 \gset

-- 创建colocate_with非none的分片表，期待异常
create table dist7(c1 int primary key, c2 text);
select cigration_create_distributed_table('dist7','c1', colocate_with=>'default');

select cigration_create_distributed_table('dist7','c1', colocate_with=>'dist1');


-- 创建colocate_with为none的分片表，期待成功
select cigration_create_distributed_table('dist7','c1', colocate_with=>'none');

-- 创建append的分片表，期待成功
create table dist8(c1 int, c2 text);
select cigration_create_distributed_table('dist8','c1', distribution_type=>'append');

drop table dist7, dist8;


--
-- 4. 在指定worker上创建分片(单分片，单worker)
--
set citus.shard_count = 1;
create table dist7(c1 int primary key, c2 text);
select cigration_create_distributed_table('dist7','c1', ARRAY[:'worker_2_host'], ARRAY[:worker_2_port]);

-- 查看分片的分布
select nodename,
       nodeport,
	   logicalrelid,
	   shardminvalue,
	   shardmaxvalue
from pg_dist_shard_placement p
     join pg_dist_shard s on(p.shardid = s.shardid)
where logicalrelid::text ~ 'dist7'
order by nodename,nodeport,logicalrelid,shardminvalue;

with t as(
select min(colocationid) min_colocationid from pg_dist_partition where logicalrelid::text ~ 'dist'
)
select logicalrelid, partmethod, partkey,colocationid - min_colocationid + 1 as relative_colocationid
from pg_dist_partition,t where logicalrelid::text ~ 'dist' order by colocationid,logicalrelid::text;

select count(*) from dist7;

--
-- 5. 在指定worker上创建分片(多分片,多worker)
--
set citus.shard_count = 8;
create table dist8(c1 int primary key, c2 text);
select cigration_create_distributed_table('dist8','c1', ARRAY[:'worker_2_host',:'worker_3_host'], ARRAY[:worker_2_port,:worker_3_port]);

-- 查看分片的分布
select nodename,
       nodeport,
	   logicalrelid,
	   shardminvalue,
	   shardmaxvalue
from pg_dist_shard_placement p
     join pg_dist_shard s on(p.shardid = s.shardid)
where logicalrelid::text ~ 'dist8'
order by nodename,nodeport,logicalrelid,shardminvalue;

with t as(
select min(colocationid) min_colocationid from pg_dist_partition where logicalrelid::text ~ 'dist'
)
select logicalrelid, partmethod, partkey,colocationid - min_colocationid + 1 as relative_colocationid
from pg_dist_partition,t where logicalrelid::text ~ 'dist' order by colocationid,logicalrelid::text;

select count(*) from dist8;


--
-- 6. 指定worker创建分片表时，不允许创建副本数大于1的分片表（分片迁移不支持多副本）
--
create table dist9(c1 int primary key, c2 text);

set citus.shard_replication_factor =2;
select cigration_create_distributed_table('dist7','c1', ARRAY[:'worker_2_host'], ARRAY[:worker_2_port]);


--
-- 7. 清理测试环境
--

truncate pg_citus_shard_migration;
