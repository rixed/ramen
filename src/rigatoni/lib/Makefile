top_srcdir = ../..

NAME = rigatoni

LIB_SOURCES = \
	Lang.ml

TESTABLE_SOURCES = \
	$(LIB_SOURCES) \

SOURCES = \
	$(TESTABLE_SOURCES)

PACKAGES = batteries stdint parsercombinator
LIB_NAME = rigatoni

INSTALLED = \
	META $(LIB_NAME).cmxa $(LIB_NAME).cma \
	$(LIB_SOURCES:.ml=.cmx) $(LIB_SOURCES:.ml=.cmi)

all: .depend $(LIB_NAME).cmxa $(LIB_NAME).cma

$(LIB_NAME).cmxa: $(LIB_SOURCES:.ml=.cmx)
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -a -package "$(PACKAGES)" $^ -o $@

$(LIB_NAME).cma: $(LIB_SOURCES:.ml=.cmo)
	$(OCAMLC) $(OCAMLFLAGS) -a -linkpkg -package "$(PACKAGES)" -custom $^ -o $@

clean-spec:

distclean-spec:
	$(RM) $(LIB_NAME).cmxa $(LIB_NAME).cma

include $(top_srcdir)/make.common

# Dependencies

include .depend
