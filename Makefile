top_srcdir = ./

SUBDIRS = src docs

all:
	@for d in $(SUBDIRS) ; do $(MAKE) -C $$d $@ ; done

doc:
	@for d in $(SUBDIRS) ; do $(MAKE) -C $$d $@ ; done

docker-build: docker/Dockerfile
	docker build -t rixed/ramen:try --squash docker/

install-spec:
	@true

check-spec:
	@for d in $(SUBDIRS) ; do $(MAKE) -C $$d $@ ; done

clean-spec:
	@true # Avoids "nothing to be done for ..." message

distclean-spec:
	@true

include $(top_srcdir)/make.common
