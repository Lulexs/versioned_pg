MODULES = versioned_int
EXTENSION = versioned_int
DATA = versioned_int--0.1.0.sql

PG_CONFIG = /usr/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)