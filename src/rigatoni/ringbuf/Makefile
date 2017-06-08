top_srcdir = ../..

NAME = ringbuf

LIB_SOURCES = \
  ringbuf.c ringbuf.h

SOURCES = $(LIB_SOURCES)

LIB_NAME = libringbuf

all: $(LIB_NAME).a

libringbuf.a: ringbuf.o
	$(AR) rs $@ $^

clean-spec:

distclean-spec:
	$(RM) $(LIB_NAME).a

include $(top_srcdir)/make.common

# Dependencies

include .depend
