top_srcdir = ..

TESTABLE_SOURCES = \
	RamenSharedTypes.ml EthAddr.ml Ipv4.ml Ipv6.ml Lang.ml Compiler.ml \
	Helpers.ml RamenBloomFilter.ml

# For the actual command line building all_tests.opt:
LINKED_FOR_TESTS = \
	RamenLog.ml Helpers.ml RamenSharedTypes.ml RingBufLib.ml \
	RamenParsing.ml \
	EthAddr.ml Ipv4.ml Ipv6.ml Lang.ml \
	RingBuf.ml RamenConf.ml CodeGen_OCaml.ml Compiler.ml \
	RamenBloomFilter.ml

CANNELLONI_SOURCES = \
	Consts.ml RamenLog.ml Helpers.ml RamenSharedTypes.ml \
	RamenParsing.ml EthAddr.ml Ipv4.ml Ipv6.ml Lang.ml \
	RingBufLib.ml RingBuf.ml \
	RamenConf.ml RamenExport.ml RamenProcesses.ml \
	CodeGen_OCaml.ml Compiler.ml HttpSrv.ml ApiCmd.ml RingBufCmd.ml \
	cannelloni.ml

CODEGENLIB_SOURCES = \
	Consts.ml RamenLog.ml Helpers.ml Globs.ml \
	RamenParsing.ml EthAddr.ml Ipv4.ml Ipv6.ml \
	RamenSharedTypes.ml RingBufLib.ml RingBuf.ml \
	RamenBloomFilter.ml \
	CodeGenLib_IO.ml CodeGenLib_State.ml CodeGenLib.ml

LIBRINGBUF_SOURCES = \
	ringbuf/ringbuf.c ringbuf/ringbuf.h

CONFIGURATOR_SOURCES = \
	Consts.ml RamenLog.ml Helpers.ml RamenSharedTypes.ml \
	SqliteHelpers.ml Conf_of_sqlite.ml \
	ramen_configurator.ml

ALERTER_SOURCES = \
	Consts.ml RamenLog.ml Helpers.ml SqliteHelpers.ml \
	ramen_alerter.ml

SOURCES = \
	$(CANNELLONI_SOURCES) wrap_ringbuf.c $(CODEGENLIB_SOURCES) \
	$(LIBRINGBUF_SOURCES) $(LIBRINGBUF_OCAML_SOURCES) \
	$(CONFIGURATOR_SOURCES) $(ALERTER_SOURCES) \
	ringbuf_test.ml

PACKAGES = \
	ppp lwt.ppx batteries cmdliner stdint parsercombinator \
	cohttp-lwt-unix num inotify.lwt sqlite3 \
	binocle unix cryptohash owl

INSTALLED = \
  cannelloni ramen_configurator ramen_alerter \
	codegen.cmxa $(CODEGENLIB_SOURCES:.ml=.cmi) $(CODEGENLIB_SOURCES:.ml=.cmx) \
	ringbuf/libringbuf.a

all: $(INSTALLED) ringbuf_test.opt

doc: HELP.html

ringbuf/libringbuf.a: ringbuf/ringbuf.o
	$(AR) rs $@ $^

# We have to force -cclib -lstdint_stubs right after -cclib wrap_ringbuf.o
# otherwise -package stdint would put it before and gcc would not include the
# symbols we need as we are the only users.
MOREFLAGS = -package "$(PACKAGES)" -I ringbuf -cclib wrap_ringbuf.o -cclib -lstdint_stubs -cclib -Lringbuf -cclib -lringbuf

cannelloni: $(CANNELLONI_SOURCES:.ml=.cmx) ringbuf/libringbuf.a wrap_ringbuf.o
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) $(filter %.cmx, $^) -o $@

codegen.cmxa: $(CODEGENLIB_SOURCES:.ml=.cmx) ringbuf/libringbuf.a wrap_ringbuf.o
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -a       $(MOREFLAGS) $(filter %.cmx, $^) -o $@

codegen.cma: $(CODEGENLIB_SOURCES:.ml=.cmo) ringbuf/libringbuf.a wrap_ringbuf.o
	$(OCAMLC)   $(OCAMLFLAGS)    -a       $(MOREFLAGS) $(filter %.cmo, $^) -o $@

ringbuf_test.opt: RamenLog.cmx Helpers.cmx RamenSharedTypes.cmx RingBufLib.cmx RingBuf.cmx ringbuf_test.cmx ringbuf/libringbuf.a wrap_ringbuf.o
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) $(filter %.cmx, $^) -o $@

ramen_configurator: $(CONFIGURATOR_SOURCES:.ml=.cmx)
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) $(filter %.cmx, $^) -o $@

ramen_alerter: $(ALERTER_SOURCES:.ml=.cmx)
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) $(filter %.cmx, $^) -o $@

all_tests.opt: ringbuf/libringbuf.a wrap_ringbuf.o $(LINKED_FOR_TESTS:.ml=.cmx) all_tests.ml
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) -package qcheck $(filter %.cmx, $^) $(filter %.ml, $^) -o $@

# Web thingies

clean-spec:
	$(RM) ringbuf/*.a ringbuf/libringbuf.a

distclean-spec:
	$(RM) cannelloni codegen.cmxa ringbuf/ringbuf.o

check-spec: ringbuf_test.opt

long-check: cannelloni
	@$(MAKE) check
	@./ringbuf_test.opt || echo "FAILURE"
	./tests/tops
	./tests/basic_aggr
	./tests/count_lines
	./tests/sliding_window
	./tests/fun_with_funcs
	./tests/lag
	./tests/word_split

include $(top_srcdir)/make.common

# Dependencies

include .depend
