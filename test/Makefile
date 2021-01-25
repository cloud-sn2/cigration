# Makefile for tests of the Citus extension

print-%  : ; @echo $* = $($*)

#citus_top_builddir = ../../..

#include Makefile.global
cur_dir:=$(dir $(realpath $(firstword $(MAKEFILE_LIST))))

citus_abs_srcdir:=${cur_dir}
citus_abs_top_srcdir:=${cur_dir}
postgres_abs_srcdir:=/var/lib/pgsql/rpm10/BUILD/postgresql-10.2
postgres_abs_builddir:=/var/lib/pgsql/rpm10/BUILD/postgresql-10.2

PG_CONFIG:=/usr/pgsql-12/bin/pg_config
PGXS:=$(shell $(PG_CONFIG) --pgxs)

# Citus is built using PostgreSQL's pgxs
USE_PGXS=1
include $(PGXS)

# ensure MAJORVERSION is defined (missing in older versions)
ifndef MAJORVERSION
MAJORVERSION := $(basename $(VERSION))
endif

##
## Citus regression support
##
MULTI_INSTALLDIR=$(CURDIR)/tmp_check/install
pg_regress_multi_check = $(PERL) $(citus_abs_srcdir)/pg_regress_multi.pl --pgxsdir="$(pgxsdir)" --bindir="$(bindir)" --libdir="$(libdir)" --majorversion="$(MAJORVERSION)" --postgres-builddir="$(postgres_abs_builddir)"  --postgres-srcdir="$(postgres_abs_srcdir)"
MULTI_REGRESS_OPTS = --inputdir=$(citus_abs_srcdir) $(pg_regress_locale_flags)

# XXX: Can't actually do useful testruns against install - $libdir
# etc will point to the directory configured during postgres'
# build. We could copy the installed tree around, but that's quite
# likely to be mixed with other binaries and such...
cleandir-main:
###	echo rm -rf '$(CURDIR)'/tmp_check/install
###
tempinstall-main: cleandir-main
####	mkdir -p $(MULTI_INSTALLDIR)
###	$(MAKE) DESTDIR=$(MULTI_INSTALLDIR) -C $(citus_top_builddir) install > tmp_check/install.log 2>&1

# Test input and expected files.  These are created by pg_regress itself, so we
# don't have a rule to create them.  We do need rules to clean them however.
input_files := $(patsubst $(citus_abs_srcdir)/input/%.source,sql/%.sql, $(wildcard $(citus_abs_srcdir)/input/*.source))
output_files := $(patsubst $(citus_abs_srcdir)/output/%.source,expected/%.out, $(wildcard $(citus_abs_srcdir)/output/*.source))

# have make check actually run all tests, but keep check-full as an
# intermediate, for muscle memory backward compatibility.
check-shard_migration: all tempinstall-main
	$(pg_regress_multi_check) --load-extension=citus \
	-- $(MULTI_REGRESS_OPTS) --schedule=$(citus_abs_srcdir)/shard_migration $(EXTRA_TESTS)

clean distclean maintainer-clean:
	rm -f $(output_files) $(input_files)
	rm -rf tmp_check/

