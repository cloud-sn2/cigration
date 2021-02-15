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

-- 插入数据
insert into dist1 select generate_series(1,100000), 'aaa';

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
-- 2. 创建迁移任务（init）
--

-- 创建迁移worker1的所有分片到worker3的job并获取第一个迁移任务的jobid和taskid
select jobid, taskid from cigration_create_move_node_job(:'worker_1_host', :worker_1_port, :'worker_3_host', :worker_3_port) limit 1 \gset

select source_nodename,source_nodeport,target_nodename,target_nodeport,status,total_shard_count
from pg_citus_shard_migration where jobid=:jobid and taskid=:taskid;


--
-- 3. 变更表定义（init）
--

-- 可以变更还未开始迁移的表
alter table dist1 add c3 text;
create index dist1_idx on dist1(c2);
alter index dist1_idx rename to dist1_idx2;
drop index dist1_idx2;
create index dist1_idx on dist1(c2);

-- 禁止删除迁移任务中存在的表，期待异常
drop table dist1;

-- 禁止级联删除迁移任务中存在的表，期待异常
drop schema if exists cigration_regress_test cascade;

-- 允许变更不相关的表
create table tb1(id int);
alter table tb1 add c2 text;
create index tb1_idx on tb1(c2);
alter index tb1_idx rename to tb1_idx2;
drop index tb1_idx2;
drop table tb1;

--
-- 4. 变更表定义（running）
--
select cigration_start_shard_migration_task(:jobid, :taskid);

-- 禁止变更已开始迁移的表，期待异常
alter table dist1 add c4 text;
create index dist1_idx_err on dist1(c2);
alter index dist1_idx rename to dist1_idx2;
drop index dist1_idx;

-- 禁止删除迁移任务中存在的表，期待异常
drop table dist1;

-- 允许创建和删除不相关的表(暂不支持alter排除不相关的表)
create table tb1(id int);
alter table tb1 add c2 text;
create index tb1_idx on tb1(c2);
alter index tb1_idx rename to tb1_idx2;
drop index tb1_idx2;
drop table tb1;

--
-- 5. 完成迁移（cleanuped）
--
select cigration_complete_shard_migration_task(:jobid, :taskid, init_sync_timeout=>10, data_sync_timeout=>20);
select cigration_cleanup_shard_migration_task(:jobid, :taskid);

-- 检查迁移任务开始前的表定义变更生效
select c3 from dist1 limit 1;

--
-- 6. 变更表定义（cleanuped）
--

-- 允许变更已完成迁移并归档的表
alter table dist1 add c4 text;
create index dist1_idx_ok on dist1(c2);
alter index dist1_idx rename to dist1_idx2;
drop index dist1_idx2;

-- 禁止删除迁移任务中存在的表，期待异常
drop table dist1;

-- 归档所有剩余迁移任务
select cigration_cleanup_shard_migration_task(jobid, taskid) from pg_citus_shard_migration;
select count(*) from pg_citus_shard_migration;

-- 允许删除已完成迁移并归档的表
drop table dist1;


--
-- 7. 旧分片清理
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

