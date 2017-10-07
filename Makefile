top_srcdir = ./

all: tuto.html
	$(MAKE) -C src

include $(top_srcdir)/make.common
