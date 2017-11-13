top_srcdir = ../

ifeq ($(shell uname),Darwin)
FILE_NOTIFIER = RamenFileNotify_Poller.ml
else
FILE_NOTIFIER = RamenFileNotify_Inotify.ml
endif

TESTABLE_SOURCES = \
	EthAddr.ml Ipv4.ml Ipv6.ml Lang.ml Compiler.ml \
	Helpers.ml RamenBloomFilter.ml Globs.ml CodeGen_OCaml.ml

# For the actual command line building all_tests.opt:
LINKED_FOR_TESTS = \
	RamenLog.ml Consts.ml Helpers.ml \
	RamenSharedTypesJS.ml RamenSharedTypes.ml \
	RingBufLib.ml RamenParsing.ml \
	EthAddr.ml Ipv4.ml Ipv6.ml \
	RamenCollectd.ml RamenProtocols.ml Lang.ml \
	RingBuf.ml RamenConf.ml Globs.ml CodeGen_OCaml.ml Compiler.ml \
	RamenBloomFilter.ml

RAMEN_SOURCES = \
	Consts.ml RamenLog.ml Helpers.ml RamenBitmask.ml \
	RamenSharedTypesJS.ml RamenSharedTypes.ml \
	RamenCollectd.ml RamenProtocols.ml \
	RamenParsing.ml EthAddr.ml Ipv4.ml Ipv6.ml Lang.ml \
	RingBufLib.ml RingBuf.ml \
	RamenConf.ml RamenExport.ml RamenProcesses.ml \
	Globs.ml CodeGen_OCaml.ml Compiler.ml \
	RamenGui.ml HttpSrv.ml ApiCmd.ml RingBufCmd.ml \
	ramen.ml

CODEGENLIB_SOURCES = \
	Consts.ml RamenLog.ml Helpers.ml Globs.ml \
	RamenParsing.ml EthAddr.ml Ipv4.ml Ipv6.ml \
	RamenSharedTypesJS.ml RamenSharedTypes.ml \
	RamenCollectd.ml RamenProtocols.ml \
	RingBufLib.ml RingBuf.ml \
	RamenBloomFilter.ml RamenFileNotify.ml \
	CodeGenLib_IO.ml CodeGenLib_State.ml CodeGenLib.ml

LIBRINGBUF_SOURCES = \
	ringbuf/ringbuf.h ringbuf/ringbuf.c \
	ringbuf/wrappers.c

LIBCOLCOMP_SOURCES = \
	colcomp/growblock.h colcomp/growblock.c \
	colcomp/colcomp.h colcomp/colcomp.c

LIBCOLLECTD_SOURCES = \
	collectd/collectd.h collectd/collectd.c \
	collectd/wrappers.c

CONFIGURATOR_SOURCES = \
	Consts.ml RamenLog.ml Helpers.ml \
	RamenSharedTypesJS.ml RamenSharedTypes.ml \
	SqliteHelpers.ml Conf_of_sqlite.ml \
	ramen_configurator.ml

ALERTER_SOURCES = \
	Consts.ml RamenLog.ml Helpers.ml SqliteHelpers.ml \
	ramen_alerter.ml

SOURCES_ = \
	$(RAMEN_SOURCES) $(CODEGENLIB_SOURCES) \
	$(LIBRINGBUF_SOURCES) $(LIBRINGBUF_OCAML_SOURCES) \
	$(LIBCOLCOMP_SOURCES) \
	$(CONFIGURATOR_SOURCES) $(ALERTER_SOURCES) \
	ringbuf_test.ml colcomp/colcomp_test.c
# Do not take into account generated code that depends on compilation:
SOURCES = $(filter-out RamenGui.ml,$(SOURCES_))

PACKAGES = \
	ppp lwt.ppx batteries cmdliner stdint parsercombinator \
	cohttp-lwt-unix num inotify.lwt binocle unix cryptohash owl

INSTALLED_BIN = ramen ramen_configurator ramen_alerter
INSTALLED_LIB = \
  META codegen.cmxa codegen.a \
  $(CODEGENLIB_SOURCES:.ml=.cmi) $(CODEGENLIB_SOURCES:.ml=.cmx) \
  libringbuf.a libcolcomp.a libcollectd.a
INSTALLED = $(INSTALLED_BIN) $(INSTALLED_LIB)

bin_dir ?= /usr/local/bin

all: $(INSTALLED) ringbuf_test.opt
	@true

doc: HELP.html
	@true

RamenFileNotify.ml: $(FILE_NOTIFIER)
	ln -sf $< $@
	touch $@

libringbuf.a: ringbuf/ringbuf.o ringbuf/wrappers.o
	$(AR) rs $@ $^

libcolcomp.a: colcomp/growblock.o colcomp/colcomp.o
	$(AR) rs $@ $^

libcollectd.a: collectd/collectd.o collectd/wrappers.o
	$(AR) rs $@ $^

# We have to force -cclib -lstdint_stubs right after -cclib wrap_ringbuf.o
# otherwise -package stdint would put it before and gcc would not include the
# symbols we need as we are the only users.
MOREFLAGS = \
	-package "$(PACKAGES)" \
	-cclib -L. \
	-cclib -lstdint_stubs \
	-cclib -lringbuf \
	-cclib -lcollectd

ramen: $(RAMEN_SOURCES:.ml=.cmx) libringbuf.a libcollectd.a
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) $(filter %.cmx, $^) -o $@

codegen.cmxa: $(CODEGENLIB_SOURCES:.ml=.cmx) libringbuf.a libcollectd.a
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -a $(MOREFLAGS) $(filter %.cmx, $^) -o $@

codegen.cma: $(CODEGENLIB_SOURCES:.ml=.cmo) libringbuf.a libcollectd.a
	$(OCAMLC) $(OCAMLFLAGS) -a $(MOREFLAGS) $(filter %.cmo, $^) -o $@

ringbuf_test.opt: RamenLog.cmx Consts.cmx Helpers.cmx RamenSharedTypesJS.cmx RamenSharedTypes.cmx RingBufLib.cmx RingBuf.cmx ringbuf_test.cmx libringbuf.a
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) $(filter %.cmx, $^) -o $@

ramen_configurator: $(CONFIGURATOR_SOURCES:.ml=.cmx)
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) -package sqlite3 $(filter %.cmx, $^) -o $@

ramen_alerter: $(ALERTER_SOURCES:.ml=.cmx)
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) -package "sqlite3 syslog" $(filter %.cmx, $^) -o $@

all_tests.opt: libringbuf.a libcolcomp.a $(LINKED_FOR_TESTS:.ml=.cmx) all_tests.ml
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) -package qcheck $(filter %.cmx, $^) $(filter %.ml, $^) -o $@

# configurator/alerter specific sources with more packages

SqliteHelpers.cmx: SqliteHelpers.ml
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -package "$(PACKAGES) sqlite3" -c $<

Conf_of_sqlite.cmx: Conf_of_sqlite.ml
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -package "$(PACKAGES) sqlite3" -c $<

ramen_alerter.cmx: ramen_alerter.ml
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -package "$(PACKAGES) sqlite3 syslog" -c $<

# Web thingies

WEB_PACKAGES = js_of_ocaml js_of_ocaml.ppx

web/engine.cmo: web/engine.ml Consts.cmo
	$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I web -c $<

web/color.cmo: web/color.ml
	$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I web -c $<

web/formats.cmo: web/formats.ml web/engine.cmo
	$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I web -c $<

web/chart.cmo: web/chart.ml web/engine.cmo web/color.cmo web/formats.cmo
	$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I web -c $<

RamenSharedTypesJS_noPPP.cmo: RamenSharedTypesJS.ml
	$(OCAMLC) $(OCAMLFLAGS) -c $< -o $@

web/app.cmo: web/app.ml RamenSharedTypesJS_noPPP.cmo web/engine.cmo web/color.cmo web/formats.cmo web/chart.cmo
	$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I web -c $<

web/script.byte: Consts.cmo RamenSharedTypesJS_noPPP.cmo web/engine.cmo web/color.cmo web/formats.cmo web/chart.cmo web/app.cmo
	$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -linkpkg $^ -o $@

web/script.js: web/script.byte
	$(JSOO) --pretty --opt=3 $^ -o $@

web/script.min.js: web/script.js
	$(JS_MINIFY) $<

RamenGui.ml: web/script.js web/script.min.js web/style.css Makefile
	@echo '(* Generated by Makefile - edition is futile *)' > $@
	@echo 'let without_link = {lookmadelimiter|<html>' >> $@
	@echo '<head><meta charset="UTF-8">' >> $@
	@echo '<title>Ramen</title><style media="all">' >> $@
	@cat web/style.css >> $@
	@echo '</style><script>' >> $@
	@cat web/script.min.js >> $@
	@echo '</script></head>' >> $@
	@echo '<body><div id="application"></div></body>' >> $@
	@echo '</html>|lookmadelimiter}' >> $@
	@echo 'let with_links = {lookmadelimiter|<html>' >> $@
	@echo '<head><meta charset="UTF-8">' >> $@
	@echo '<title>Ramen</title>' >> $@
	@echo '<link href="style.css" rel="stylesheet" media="all"/>' >> $@
	@echo '<script src="script.js"></script>' >> $@
	@echo '</head>' >> $@
	@echo '<body><div id="application"></div></body>' >> $@
	@echo '</html>|lookmadelimiter}' >> $@

# Boring stuff

clean-spec:
	$(RM) ringbuf/*.o colcomp/*.o colcomp/colcomp_test

distclean-spec:
	$(RM) ramen codegen.cmxa libringbuf.a libcolcomp.a libcollectd.a RamenFileNotify.ml

install-spec: $(INSTALLED)
	ocamlfind install ramen $(INSTALLED_LIB)
	install $(INSTALLED_BIN) $(prefix)$(bin_dir)/

uninstall-spec:
	ocamlfind remove ramen
	$(RM) $(prefix)$(bin_dir)/ramen

colcomp/colcomp_test: colcomp/colcomp_test.o libcolcomp.a
	$(CC) $(LDFLAGS) $(LOADLIBES) $(LDLIBS) $^ -o $@

check-spec: all_tests.opt ringbuf_test.opt
	@./ringbuf_test.opt || echo "FAILURE (ringbuf_test)"
	@OCAMLRUNPARAM=b ./all_tests.opt -bt || echo "FAILURE"

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
	./tests/fit_multi
	./tests/upload
	./tests/word_count
	./tests/tuples

include $(top_srcdir)/make.common

# Dependencies

include .depend
