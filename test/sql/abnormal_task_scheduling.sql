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
-- 2. 创建迁移任务（init）
--

-- 创建迁移worker1的所有分片到worker3的job并获取第一个迁移任务的jobid和taskid
select jobid, taskid from cigration_create_worker_migration_job(:'worker_1_host', :worker_1_port, :'worker_3_host', :worker_3_port) limit 1 \gset

select source_nodename,source_nodeport,target_nodename,target_nodeport,status,total_shard_count
from pg_citus_shard_migration where jobid=:jobid and taskid=:taskid;

-- 迁移任务的初始状态为init
select status from pg_citus_shard_migration where jobid=:jobid and taskid=:taskid;
select cigration_monitor_shard_migration_task(:jobid, :taskid);

-- 禁止创建新的迁移作业
select jobid, taskid from cigration_create_worker_empty_job(array[concat(:'worker_2_host', ':', :worker_2_port), concat(:'worker_3_host', ':', :worker_3_port)]);
select jobid, taskid from cigration_create_rebalance_job();
select jobid, taskid from cigration_create_worker_migration_job(:'worker_1_host', :worker_1_port, :'worker_3_host', :worker_3_port);

-- 禁止取消和完成init迁移任务
select cigration_cancel_shard_migration_task(:jobid, :taskid);
select cigration_complete_shard_migration_task(:jobid, :taskid);

-- 禁止对非completed迁移任务删除旧分片
select cigration_drop_old_shard(:jobid, :taskid);


--
-- 3. 启动迁移任务（running）
--
select cigration_start_shard_migration_task(:jobid, :taskid);
select status from pg_citus_shard_migration where jobid=:jobid and taskid=:taskid;

-- 禁止启动running迁移任务
select cigration_start_shard_migration_task(:jobid, :taskid);

-- 禁止对非completed迁移任务删除旧分片
select cigration_drop_old_shard(:jobid, :taskid);

-- 禁止清理running迁移任务
select cigration_cleanup_shard_migration_task(:jobid, :taskid);


--
-- 4. 取消迁移任务（canceled）
--

select cigration_cancel_shard_migration_task(:jobid, :taskid);
select status from pg_citus_shard_migration where jobid=:jobid and taskid=:taskid;
select cigration_monitor_shard_migration_task(:jobid, :taskid);

-- 禁止取消完成canceled迁移任务
select cigration_cancel_shard_migration_task(:jobid, :taskid);
select cigration_complete_shard_migration_task(:jobid, :taskid);


--
-- 5. 构造运行出错的迁移任务（init->error）
--

-- 注入故障
select target_nodename from pg_citus_shard_migration where jobid=:jobid and taskid=:taskid \gset
update pg_citus_shard_migration set target_nodename = 'faked_host' where jobid=:jobid and taskid=:taskid;

-- 启动迁移任务（error）
select cigration_start_shard_migration_task(:jobid, :taskid);
select status from pg_citus_shard_migration where jobid=:jobid and taskid=:taskid;
select cigration_monitor_shard_migration_task(:jobid, :taskid);

-- 禁止启动取消完成error迁移任务
select cigration_start_shard_migration_task(:jobid, :taskid);
select cigration_cancel_shard_migration_task(:jobid, :taskid);
select cigration_complete_shard_migration_task(:jobid, :taskid);

-- 禁止对非completed迁移任务删除旧分片
select cigration_drop_old_shard(:jobid, :taskid);

-- 禁止清理error迁移任务
select cigration_cleanup_shard_migration_task(:jobid, :taskid);

-- 解除故障
update pg_citus_shard_migration set target_nodename = :'target_nodename' where jobid=:jobid and taskid=:taskid;

-- 清理环境并恢复到init状态
select cigration_cleanup_error_env();
select status from pg_citus_shard_migration where jobid=:jobid and taskid=:taskid;


--
-- 6. 构造运行出错的迁移任务（running->running）
--

-- 启动迁移任务（running）
select cigration_start_shard_migration_task(:jobid, :taskid);
select status from pg_citus_shard_migration where jobid=:jobid and taskid=:taskid;

-- 注入故障
select target_nodename from pg_citus_shard_migration where jobid=:jobid and taskid=:taskid \gset
update pg_citus_shard_migration set target_nodename = 'faked_host' where jobid=:jobid and taskid=:taskid;

-- 完成迁移任务（running）
-- running状态下出错仍然是running
select cigration_complete_shard_migration_task(:jobid, :taskid);
select status from pg_citus_shard_migration where jobid=:jobid and taskid=:taskid;
select cigration_monitor_shard_migration_task(:jobid, :taskid);

-- 解除故障
update pg_citus_shard_migration set target_nodename = :'target_nodename' where jobid=:jobid and taskid=:taskid;


--
-- 7. 完成迁移任务（completed）
--

-- 检查迁移完成前，SQL执行正常
select count(*) from dist1 a left join dist2 b on(a.c1=b.c1) left join dist3 c on (a.c1=c.c1);

-- 完成迁移任务（completed）
select cigration_complete_shard_migration_task(:jobid, :taskid, init_sync_timeout=>10, data_sync_timeout=>20);
select status from pg_citus_shard_migration where jobid=:jobid and taskid=:taskid;
select cigration_monitor_shard_migration_task(:jobid, :taskid);

-- 检查迁移完成后，SQL执行正常
select count(*) from dist1 a left join dist2 b on(a.c1=b.c1) left join dist3 c on (a.c1=c.c1);

-- 禁止取消，启动，完成completed迁移任务
select cigration_cancel_shard_migration_task(:jobid, :taskid);
select cigration_complete_shard_migration_task(:jobid, :taskid);
select cigration_start_shard_migration_task(:jobid, :taskid);

--
-- 8. 归档迁移任务（history）
--
select cigration_cleanup_shard_migration_task(:jobid, :taskid);
select status from pg_citus_shard_migration where jobid=:jobid and taskid=:taskid;
select status from pg_citus_shard_migration_history where jobid=:jobid and taskid=:taskid;

-- 禁止对归档迁移任务执行调度操作
select cigration_cancel_shard_migration_task(:jobid, :taskid);
select cigration_complete_shard_migration_task(:jobid, :taskid);
select cigration_start_shard_migration_task(:jobid, :taskid);
select cigration_drop_old_shard(:jobid, :taskid);
select cigration_cleanup_shard_migration_task(:jobid, :taskid);


--
-- 9. 归档剩余迁移任务（history）
--

-- 设置一个任务为canceled状态，确认canceled任务可以被归档
select cigration_start_shard_migration_task(jobid, taskid)
from pg_citus_shard_migration order by taskid limit 1;

select cigration_cancel_shard_migration_task(jobid, taskid)
from pg_citus_shard_migration order by taskid limit 1;

-- 归档所有剩余迁移任务
select status from pg_citus_shard_migration order by taskid;

select cigration_cleanup_shard_migration_task(jobid, taskid) from pg_citus_shard_migration;

-- 归档所有剩余迁移任务到历史任务表
select count(*) from pg_citus_shard_migration;
select count(*) from pg_citus_shard_migration_history;

--
-- 10. 旧分片清理
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
-- 11. 测试环境清理
--

truncate pg_citus_shard_migration_history;
truncate pg_citus_shard_migration_sql_log;
