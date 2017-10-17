top_srcdir = ./

SUBDIRS = src docs

all:
	@for d in $(SUBDIRS) ; do $(MAKE) -C $$d $@ ; done

docker-build: docker/Dockerfile
	docker build -t rixed/ramen:try --squash docker/

clean-spec:

distclean-spec:

include $(top_srcdir)/make.common
