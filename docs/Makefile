top_srcdir = ../

all: tutorial.html

clean-spec:

distclean-spec:
	$(RM) tutorial.html

install-spec: tutorial.html
	scp tutorial.html pim:public_html/ramen_tutorial.html
	
include $(top_srcdir)/make.common
