top_srcdir = ..

LINKED_WITH_TESTS = \
	Log.ml

TESTABLE_SOURCES = \
	HttpSrv.ml

SOURCES = \
	Log.ml Lang.ml RamenConf.ml \
	$(TESTABLE_SOURCES) \
	rigatoni.ml

PACKAGES = ppp lwt.ppx batteries cmdliner stdint parsercombinator cohttp.lwt

INSTALLED = \
  rigatoni

SUBDIRS = ringbuf

all: rigatoni

lib/rigatoni.cmxa:
	$(MAKE) -C lib rigatoni.cmxa

rigatoni: $(SOURCES:.ml=.cmx)
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg -package "$(PACKAGES)" $^ -o $@

clean-spec:

distclean-spec:
	$(RM) rigatoni

include $(top_srcdir)/make.common

# Dependencies

include .depend
