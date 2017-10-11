top_srcdir = ./

SUBDIRS = src

all:
	@for d in $(SUBDIRS) ; do $(MAKE) -C $$d $@ ; done

clean-spec:

distclean-spec:

include $(top_srcdir)/make.common
