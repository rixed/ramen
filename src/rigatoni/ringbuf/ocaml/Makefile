top_srcdir = ../../..
CPPFLAGS += -I ../ringbuf
OCAMLFLAGS += -cclib -L../ringbuf -cclib -lringbuf -I ../ringbuf
OCAMLOPTFLAGS += -cclib -L../ringbuf -cclib -lringbuf -I ../ringbuf

NAME = rigatoni

LIB_SOURCES = \
	Ringbuf.ml

TESTABLE_SOURCES = \
	$(LIB_SOURCES) \

SOURCES = \
	$(TESTABLE_SOURCES)

PACKAGES =
LIB_NAME = rigatoni

INSTALLED = \
	META $(LIB_NAME).cmxa $(LIB_NAME).a \
	$(LIB_SOURCES:.ml=.cmx) $(LIB_SOURCES:.ml=.cmi)

all: .depend $(LIB_NAME).cmxa

$(LIB_NAME).cmxa: $(LIB_SOURCES:.ml=.cmx) wrap_ringbuf.o
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -a -package "$(PACKAGES)" -cclib wrap_ringbuf.o $^ -o $@

$(LIB_NAME).cma: $(LIB_SOURCES:.ml=.cmo)
	$(OCAMLC) $(OCAMLFLAGS) -a -linkpkg -package "$(PACKAGES)" -custom -cclib wrap_ringbuf.o $^ -o $@

$(LIB_NAME).top: wrap_ringbuf.o

clean-spec:

distclean-spec:
	$(RM) $(LIB_NAME).cmxa

include $(top_srcdir)/make.common

# Dependencies

include .depend
