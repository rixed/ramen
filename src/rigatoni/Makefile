top_srcdir = ..

TESTABLE_SOURCES = \
	RamenLogger.ml Lang.ml HttpSrv.ml \
	rigatoni.ml

SOURCES = $(TESTABLE_SOURCES)

PACKAGES = lwt.ppx batteries cmdliner stdint parsercombinator cohttp.lwt

INSTALLED = \
  rigatoni

SUBDIRS = ringbuf

all: rigatoni

lib/rigatoni.cmxa:
	$(MAKE) -C lib rigatoni.cmxa

rigatoni: RamenLogger.cmx Lang.cmx HttpSrv.cmx rigatoni.cmx
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg -package "$(PACKAGES)" $^ -o $@

clean-spec:

distclean-spec:
	$(RM) rigatoni

include $(top_srcdir)/make.common

# Dependencies

include .depend
