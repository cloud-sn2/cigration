MODULE = cigration
PGFILEDESC = "cigration - Citus shards migration tool"

EXTENSION = cigration
DATA = $(wildcard *--*.sql)

TESTS  = $(wildcard sql/*.sql)
REGRESS = $(patsubst sql/%.sql,%,$(TESTS))

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
