top_srcdir = ./

SUBDIRS = src docs

.PHONY: doc docker-dev docker-demo docker-latest docker-build docker-push

all:
	@for d in $(SUBDIRS) ; do $(MAKE) -C $$d $@ ; done

doc:
	@for d in $(SUBDIRS) ; do $(MAKE) -C $$d $@ ; done

docker-dev: docker/Dockerfile-dev
	docker build -t rixed/ramen:dev --squash -f $< docker/

docker-demo: docker/Dockerfile-demo
	docker build -t rixed/ramen:demo --squash -f $< docker/

docker-latest: docker/Dockerfile-latest
	docker build -t rixed/ramen:latest --squash -f $< docker/

docker-build: docker-dev docker-demo docker-latest

docker-push:
	docker push rixed/ramen:dev
	docker push rixed/ramen:demo
	docker push rixed/ramen:latest

install-spec:
	@true

check-spec:
	@for d in $(SUBDIRS) ; do $(MAKE) -C $$d $@ ; done

clean-spec:
	@true # Avoids "nothing to be done for ..." message

distclean-spec:
	@true

include $(top_srcdir)/make.common
