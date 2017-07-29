top_srcdir = ..

TESTABLE_SOURCES = \
	RamenSharedTypes.ml EthAddr.ml Ipv4.ml Ipv6.ml Lang.ml Compiler.ml

# For the actual command line building all_tests.opt:
LINKED_FOR_TESTS = \
	Log.ml Helpers.ml RamenSharedTypes.ml RingBufLib.ml \
	RamenParsing.ml \
	EthAddr.ml Ipv4.ml Ipv6.ml Lang.ml CodeGen_OCaml.ml \
	RingBuf.ml RamenConf.ml Compiler.ml

RIGATONI_SOURCES = \
	Consts.ml Log.ml Helpers.ml RamenSharedTypes.ml \
	RamenParsing.ml EthAddr.ml Ipv4.ml Ipv6.ml Lang.ml \
	RingBufLib.ml RingBuf.ml \
	CodeGen_OCaml.ml \
	RamenConf.ml RamenExport.ml RamenProcesses.ml \
	Compiler.ml HttpSrv.ml ApiCmd.ml RingBufCmd.ml \
	rigatoni.ml

CODEGENLIB_SOURCES = \
	Consts.ml Log.ml Helpers.ml \
	RamenParsing.ml EthAddr.ml Ipv4.ml Ipv6.ml \
	RamenSharedTypes.ml RingBufLib.ml RingBuf.ml \
	Globs.ml CodeGenLib_IO.ml CodeGenLib.ml

LIBRINGBUF_SOURCES = \
	ringbuf/ringbuf.c ringbuf/ringbuf.h

CONFIGURATOR_SOURCES = \
	Consts.ml Log.ml Helpers.ml RamenSharedTypes.ml Conf_of_sqlite.ml Configurator.ml

SOURCES = \
	$(RIGATONI_SOURCES) wrap_ringbuf.c $(CODEGENLIB_SOURCES) \
	$(LIBRINGBUF_SOURCES) $(LIBRINGBUF_OCAML_SOURCES) \
	www/FakeStdint.ml www/node_edit.ml \
	$(CONFIGURATOR_SOURCES)

JS_PACKAGES = ocaml-vdom js_of_ocaml-lwt js_of_ocaml-ppx lwt.ppx
PACKAGES = \
	ppp lwt.ppx batteries cmdliner stdint parsercombinator \
	cohttp-lwt-unix num inotify.lwt sqlite3 $(JS_PACKAGES) \
	binocle unix

INSTALLED = \
  rigatoni ramen_configurator \
	codegen.cmxa $(CODEGENLIB_SOURCES:.ml=.cmi) $(CODEGENLIB_SOURCES:.ml=.cmx) \
	ringbuf/libringbuf.a

all: $(INSTALLED) ringbuf_test.opt www/node_edit.js

doc: HELP.html

ringbuf/libringbuf.a: ringbuf/ringbuf.o
	$(AR) rs $@ $^

# We have to force -cclib -lstdint_stubs right after -cclib wrap_ringbuf.o
# otherwise -package stdint would put it before and gcc would not include the
# symbols we need as we are the only users.
MOREFLAGS = -package "$(PACKAGES)" -I ringbuf -cclib wrap_ringbuf.o -cclib -lstdint_stubs -cclib -Lringbuf -cclib -lringbuf

rigatoni: $(RIGATONI_SOURCES:.ml=.cmx) ringbuf/libringbuf.a wrap_ringbuf.o
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) $(filter %.cmx, $^) -o $@

codegen.cmxa: $(CODEGENLIB_SOURCES:.ml=.cmx) ringbuf/libringbuf.a wrap_ringbuf.o
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -a       $(MOREFLAGS) $(filter %.cmx, $^) -o $@

codegen.cma: $(CODEGENLIB_SOURCES:.ml=.cmo) ringbuf/libringbuf.a wrap_ringbuf.o
	$(OCAMLC)   $(OCAMLFLAGS)    -a       $(MOREFLAGS) $(filter %.cmo, $^) -o $@

ringbuf_test.opt: Log.cmx Helpers.cmx RingBufLib.cmx RingBuf.cmx ringbuf_test.cmx ringbuf/libringbuf.a wrap_ringbuf.o
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) $(filter %.cmx, $^) -o $@

ramen_configurator: $(CONFIGURATOR_SOURCES:.ml=.cmx)
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) $(filter %.cmx, $^) -o $@

all_tests.opt: ringbuf/libringbuf.a wrap_ringbuf.o $(LINKED_FOR_TESTS:.ml=.cmx) all_tests.ml
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) -package qcheck $(filter %.cmx, $^) $(filter %.ml, $^) -o $@

# Web thingies

JS_EXTRA = www/graph_layout.js

# Ignore PPP (but then have to use another name for the cmi won't be the same)
# and fake Stdint:
RamenSharedTypes_JS.cmo: RamenSharedTypes.ml www/FakeStdint.cmo
	$(OCAMLC) -I www -open FakeStdint $(OCAMLFLAGS) -o $@ -c $<

www/node_edit.byte: RamenSharedTypes_JS.cmo www/node_edit.cmo
	$(OCAMLC) $(OCAMLFLAGS) -package "$(JS_PACKAGES)" -no-check-prims -linkpkg -o $@ $^

clean-spec:
	$(RM) ringbuf/*.a ringbuf/libringbuf.a
	$(RM) www/node_edit.byte www/*.cmi www/*.cmo

distclean-spec:
	$(RM) rigatoni codegen.cmxa www/node_edit.js

check-spec: ringbuf_test.opt
	@./ringbuf_test.opt || echo "FAILURE"

include $(top_srcdir)/make.common

# Dependencies

include .depend
