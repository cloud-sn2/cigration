#citus分片迁移测试说明
---
# 1. 测试环境准备
分片测试前需要将如下内容追加到`/etc/hosts`文件中， 测试完成后再从文件中删除以下内容。

	127.0.0.1       worker_1_host
	127.0.0.1       worker_2_host
	127.0.0.1       worker_3_host

# 2. 分片迁移测试执行

在 `auto_testcase_enhance` 目录下执行

	make -C . check-shard_migration

在其它目录下执行（-C 后写实际目录名）

	make -C /tmp/auto_testcase_enhance check-shard_migration

# 3. 测试结果说明
查看结果输出diff文件`regression.diffs`，若文件中仅有时间戳的差异或分片id显示顺序不同，则表示测试通过。

可通过以下命令查看是否有时间戳以外的其他的不同，一般输出为空，否则通过diff文件查看具体差异。

	cat regression.diffs | grep '^[-|+]' | grep -v '^---' | grep -v '^+++' | grep -v '^[-|+]NOTICE'
