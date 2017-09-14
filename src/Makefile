top_srcdir = .

TESTABLE_SOURCES = \
	EthAddr.ml Ipv4.ml Ipv6.ml Lang.ml Compiler.ml \
	Helpers.ml RamenBloomFilter.ml CodeGen_OCaml.ml

# For the actual command line building all_tests.opt:
LINKED_FOR_TESTS = \
	RamenLog.ml Helpers.ml RamenSharedTypes.ml RingBufLib.ml \
	RamenParsing.ml \
	EthAddr.ml Ipv4.ml Ipv6.ml Lang.ml \
	RingBuf.ml RamenConf.ml CodeGen_OCaml.ml Compiler.ml \
	RamenBloomFilter.ml

RAMEN_SOURCES = \
	Consts.ml RamenLog.ml Helpers.ml RamenBitmask.ml RamenSharedTypes.ml \
	RamenParsing.ml EthAddr.ml Ipv4.ml Ipv6.ml Lang.ml \
	RingBufLib.ml RingBuf.ml \
	RamenConf.ml RamenExport.ml RamenProcesses.ml \
	CodeGen_OCaml.ml Compiler.ml HttpSrv.ml ApiCmd.ml RingBufCmd.ml \
	ramen.ml

CODEGENLIB_SOURCES = \
	Consts.ml RamenLog.ml Helpers.ml Globs.ml \
	RamenParsing.ml EthAddr.ml Ipv4.ml Ipv6.ml \
	RamenSharedTypes.ml RingBufLib.ml RingBuf.ml \
	RamenBloomFilter.ml \
	CodeGenLib_IO.ml CodeGenLib_State.ml CodeGenLib.ml

LIBRINGBUF_SOURCES = \
	ringbuf/ringbuf.h ringbuf/ringbuf.c

LIBCOLCOMP_SOURCES = \
	colcomp/growblock.h colcomp/growblock.c \
	colcomp/colcomp.h colcomp/colcomp.c

CONFIGURATOR_SOURCES = \
	Consts.ml RamenLog.ml Helpers.ml RamenSharedTypes.ml \
	SqliteHelpers.ml Conf_of_sqlite.ml \
	ramen_configurator.ml

ALERTER_SOURCES = \
	Consts.ml RamenLog.ml Helpers.ml SqliteHelpers.ml \
	ramen_alerter.ml

SOURCES = \
	$(RAMEN_SOURCES) wrap_ringbuf.c $(CODEGENLIB_SOURCES) \
	$(LIBRINGBUF_SOURCES) $(LIBRINGBUF_OCAML_SOURCES) \
	$(LIBCOLCOMP_SOURCES) \
	$(CONFIGURATOR_SOURCES) $(ALERTER_SOURCES) \
	ringbuf_test.ml colcomp/colcomp_test.c

PACKAGES = \
	ppp lwt.ppx batteries cmdliner stdint parsercombinator \
	cohttp-lwt-unix num inotify.lwt sqlite3 \
	binocle unix cryptohash owl

INSTALLED = \
  ramen ramen_configurator ramen_alerter \
	codegen.cmxa $(CODEGENLIB_SOURCES:.ml=.cmi) $(CODEGENLIB_SOURCES:.ml=.cmx) \
	ringbuf/libringbuf.a colcomp/libcolcomp.a

all: $(INSTALLED) ringbuf_test.opt

doc: HELP.html

ringbuf/libringbuf.a: ringbuf/ringbuf.o
	$(AR) rs $@ $^

colcomp/libcolcomp.a: colcomp/growblock.o colcomp/colcomp.o
	$(AR) rs $@ $^

# We have to force -cclib -lstdint_stubs right after -cclib wrap_ringbuf.o
# otherwise -package stdint would put it before and gcc would not include the
# symbols we need as we are the only users.
MOREFLAGS = -package "$(PACKAGES)" -I ringbuf -I colcomp -cclib wrap_ringbuf.o -cclib -lstdint_stubs -cclib -Lringbuf -cclib -lringbuf -cclib -Lcolcomp -cclib -lcolcomp

ramen: $(RAMEN_SOURCES:.ml=.cmx) ringbuf/libringbuf.a wrap_ringbuf.o colcomp/libcolcomp.a
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) $(filter %.cmx, $^) -o $@

codegen.cmxa: $(CODEGENLIB_SOURCES:.ml=.cmx) ringbuf/libringbuf.a wrap_ringbuf.o colcomp/libcolcomp.a
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -a       $(MOREFLAGS) $(filter %.cmx, $^) -o $@

codegen.cma: $(CODEGENLIB_SOURCES:.ml=.cmo) ringbuf/libringbuf.a wrap_ringbuf.o colcomp/libcolcomp.a
	$(OCAMLC)   $(OCAMLFLAGS)    -a       $(MOREFLAGS) $(filter %.cmo, $^) -o $@

ringbuf_test.opt: RamenLog.cmx Helpers.cmx RamenSharedTypes.cmx RingBufLib.cmx RingBuf.cmx ringbuf_test.cmx ringbuf/libringbuf.a wrap_ringbuf.o
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) $(filter %.cmx, $^) -o $@

ramen_configurator: $(CONFIGURATOR_SOURCES:.ml=.cmx)
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) $(filter %.cmx, $^) -o $@

ramen_alerter: $(ALERTER_SOURCES:.ml=.cmx)
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) $(filter %.cmx, $^) -o $@

all_tests.opt: ringbuf/libringbuf.a wrap_ringbuf.o colcomp/libcolcomp.a $(LINKED_FOR_TESTS:.ml=.cmx) all_tests.ml
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) -package qcheck $(filter %.cmx, $^) $(filter %.ml, $^) -o $@

# Web thingies

clean-spec:
	$(RM) ringbuf/*.o colcomp/*.o colcomp/colcomp_test

distclean-spec:
	$(RM) ramen codegen.cmxa ringbuf/*.a colcomp/*.a

colcomp/colcomp_test: colcomp/colcomp_test.o colcomp/libcolcomp.a
	$(CC) $(LDFLAGS) $(LOADLIBES) $(LDLIBS) $^ -o $@

check-spec: ringbuf_test.opt colcomp/colcomp_test
	@./ringbuf_test.opt || echo "FAILURE (ringbuf_test)"
	@colcomp/colcomp_test || echo "FAILURE (colcomp_test)"

long-check: ramen
	@$(MAKE) check
	@./ringbuf_test.opt || echo "FAILURE"
	./tests/tops
	./tests/basic_aggr
	./tests/count_lines
	./tests/sliding_window
	./tests/fun_with_funcs
	./tests/lag
	./tests/word_split
	./tests/case
	./tests/season

include $(top_srcdir)/make.common

# Dependencies

include .depend
