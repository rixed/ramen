top_srcdir = ..

SOURCES = \
	run_test.ml

PACKAGES = ramen batteries

all: .depend run_test

run_test: run_test.cmx
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg -package "$(PACKAGES)" $^ -o $@

clean-spec:

distclean-spec:
	$(RM) run_test

include $(top_srcdir)/make.common

# Dependencies

include .depend
