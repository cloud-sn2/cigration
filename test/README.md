#Citus分片迁移测试说明
---
# 1. 环境准备

将postgrsql的安装目录加入到PATH环境变量，比如

```
export PATH=/usr/pgsql-12/bin:$PATH
```

# 2. 执行回归测试

	cd test
	make check

# 3. 执行指定回归测试

通过`EXTRA_TESTS`代入要测试的case名称

```
make check_base EXTRA_TESTS=single_task_execute
make check_base EXTRA_TESTS='single_task_execute replica_identity_check'
```

注意：执行指定测试case时，可能由于序列值的差异，导致diff结果误判，需要人工判断。

