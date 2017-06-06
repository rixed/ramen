top_srcdir = ../..
OCAMLPATH = ..

SOURCES = \
	TestGen.ml test_simplest.ml AlertDb.ml run_test.ml

PACKAGES = ramen configurations batteries

all: .depend run_test

run_test: TestGen.cmx test_simplest.cmx AlertDb.ml run_test.cmx
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg -package "$(PACKAGES)" $^ -o $@

clean-spec:

distclean-spec:
	$(RM) run_test

include $(top_srcdir)/make.common

# Dependencies

include .depend
