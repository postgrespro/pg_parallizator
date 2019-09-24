# contrib/pg_parallizator/Makefile

MODULE_big = pg_parallizator
OBJS = pg_parallizator.o $(WIN32RES)
PGFILEDESC = "pg_parallizator - parallel index construction"

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/pg_parallizator
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
