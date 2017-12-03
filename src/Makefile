top_srcdir = ../

CPPFLAGS += -I $(shell ocamlfind query stdint)

ifeq ($(shell uname),Darwin)
FILE_NOTIFIER = RamenFileNotify_Poller.ml
else
FILE_NOTIFIER = RamenFileNotify_Inotify.ml
endif

TESTABLE_SOURCES = \
	EthAddr.ml Ipv4.ml Ipv6.ml Lang.ml Compiler.ml \
	Helpers.ml RamenBloomFilter.ml Globs.ml CodeGen_OCaml.ml \
	RamenHtml.ml

# For the actual command line building all_tests.opt:
LINKED_FOR_TESTS = \
	RamenLog.ml Consts.ml Helpers.ml RWLock.ml \
	RamenSharedTypesJS.ml RamenSharedTypes.ml \
	RingBufLib.ml RamenParsing.ml \
	EthAddr.ml Ipv4.ml Ipv6.ml \
	RamenCollectd.ml RamenNetflow.ml RamenProtocols.ml Lang.ml \
	RingBuf.ml RamenConf.ml Globs.ml CodeGen_OCaml.ml Compiler.ml \
	RamenBloomFilter.ml \
	RamenHtml.ml

RAMEN_SOURCES = \
	Consts.ml RamenLog.ml Helpers.ml RamenBitmask.ml \
	RWLock.ml RamenSharedTypesJS.ml RamenSharedTypes.ml \
	RamenCollectd.ml RamenNetflow.ml RamenProtocols.ml \
	RamenParsing.ml EthAddr.ml Ipv4.ml Ipv6.ml Lang.ml \
	RingBufLib.ml RingBuf.ml \
	RamenConf.ml RamenExport.ml RamenProcesses.ml \
	Globs.ml CodeGen_OCaml.ml Compiler.ml \
	RamenHtml.ml RamenColor.ml RamenFormats.ml RamenChart.ml \
	RamenGui.ml RamenHttpHelpers.ml HttpSrv.ml \
	ApiCmd.ml RingBufCmd.ml ramen.ml

CODEGENLIB_SOURCES = \
	Consts.ml RamenLog.ml Helpers.ml Globs.ml \
	RamenParsing.ml EthAddr.ml Ipv4.ml Ipv6.ml \
	RamenSharedTypesJS.ml RamenSharedTypes.ml \
	RamenCollectd.ml RamenNetflow.ml RamenProtocols.ml \
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

LIBNETFLOW_SOURCES = \
	netflow/v5.c

CONFIGURATOR_SOURCES = \
	Consts.ml RamenLog.ml Helpers.ml \
	RamenSharedTypesJS.ml RamenSharedTypes.ml \
	SqliteHelpers.ml Conf_of_sqlite.ml \
	ramen_configurator.ml

ALERTER_SOURCES = \
	Consts.ml RamenLog.ml Helpers.ml SqliteHelpers.ml \
	AlerterSharedTypesJS.ml AlerterGui.ml RamenHttpHelpers.ml \
	ramen_alerter.ml

WEB_SOURCES = \
	web/WebHelpers.ml web/JsHelpers.ml web/engine.ml \
	web/ramen_app.ml web/alerter_app.ml

SOURCES_ = \
	$(RAMEN_SOURCES) $(CODEGENLIB_SOURCES) \
	$(LIBRINGBUF_SOURCES) $(LIBRINGBUF_OCAML_SOURCES) \
	$(LIBCOLLECTD_SOURCES) $(LIBNETFLOW_SOURCES) $(WEB_SOURCES) \
	$(CONFIGURATOR_SOURCES) $(ALERTER_SOURCES) \
	ringbuf_test.ml colcomp/colcomp_test.c
# Do not take into account generated code that depends on compilation:
SOURCES = $(filter-out AlerterGui.ml, $(filter-out RamenGui.ml,$(SOURCES_)))

PACKAGES = \
	ppp lwt.ppx batteries cmdliner stdint parsercombinator \
	cohttp-lwt-unix num inotify.lwt binocle unix cryptohash owl

INSTALLED_BIN = ramen ramen_configurator ramen_alerter
INSTALLED_LIB = \
  META codegen.cmxa codegen.a \
  $(CODEGENLIB_SOURCES:.ml=.cmi) $(CODEGENLIB_SOURCES:.ml=.cmx) \
  libringbuf.a libcolcomp.a libcollectd.a libnetflow.a
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

libnetflow.a: netflow/v5.o
	$(AR) rs $@ $^

# We have to force -cclib -lstdint_stubs right after -cclib wrap_ringbuf.o
# otherwise -package stdint would put it before and gcc would not include the
# symbols we need as we are the only users.
MOREFLAGS = \
	-package "$(PACKAGES)" \
	-cclib -L. \
	-cclib -lstdint_stubs \
	-cclib -lringbuf \
	-cclib -lcollectd \
	-cclib -lnetflow

ramen: $(RAMEN_SOURCES:.ml=.cmx) libringbuf.a libcollectd.a libnetflow.a
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) $(filter %.cmx, $^) -o $@

codegen.cmxa: $(CODEGENLIB_SOURCES:.ml=.cmx) libringbuf.a libcollectd.a libnetflow.a
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -a $(MOREFLAGS) $(filter %.cmx, $^) -o $@

codegen.cma: $(CODEGENLIB_SOURCES:.ml=.cmo) libringbuf.a libcollectd.a libnetflow.a
	$(OCAMLC) $(OCAMLFLAGS) -a $(MOREFLAGS) $(filter %.cmo, $^) -o $@

ringbuf_test.opt: RamenLog.cmx Consts.cmx Helpers.cmx RamenSharedTypesJS.cmx RamenSharedTypes.cmx RingBufLib.cmx RingBuf.cmx ringbuf_test.cmx libringbuf.a libcollectd.a libnetflow.a
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) $(filter %.cmx, $^) -o $@

ramen_configurator: $(CONFIGURATOR_SOURCES:.ml=.cmx)
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg -package "$(PACKAGES) sqlite3" $(filter %.cmx, $^) -o $@

ramen_alerter: $(ALERTER_SOURCES:.ml=.cmx)
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg -package "$(PACKAGES) sqlite3 syslog net_codecs" $(filter %.cmx, $^) -o $@

all_tests.opt: libringbuf.a libcollectd.a libnetflow.a $(LINKED_FOR_TESTS:.ml=.cmx) all_tests.ml
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) -package qcheck -I web $(filter %.cmx, $^) $(filter %.ml, $^) -o $@

# configurator/alerter specific sources with more packages

SqliteHelpers.cmx: SqliteHelpers.ml
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -package "$(PACKAGES) sqlite3" -c $<

Conf_of_sqlite.cmx: Conf_of_sqlite.ml
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -package "$(PACKAGES) sqlite3" -c $<

ramen_alerter.cmx: ramen_alerter.ml
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -package "$(PACKAGES) sqlite3 syslog net_codecs" -c $<

# Web thingies

WEB_PACKAGES = js_of_ocaml js_of_ocaml.ppx

web/JsHelpers.cmo: web/JsHelpers.ml web/WebHelpers.cmo
	$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I web -c $<

web/WebHelpers.cmo: web/WebHelpers.ml RamenChart.cmo
	$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I web -c $<

web/engine.cmo: web/engine.ml web/WebHelpers.cmo web/JsHelpers.cmo RamenHtml.cmo
	$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I web -c $<

RamenSharedTypesJS_noPPP.cmo: RamenSharedTypesJS.ml
	$(OCAMLC) $(OCAMLFLAGS) -c $< -o $@

web/ramen_app.cmo: web/ramen_app.ml RamenSharedTypesJS_noPPP.cmo RamenHtml.cmo web/WebHelpers.cmo web/JsHelpers.cmo web/engine.cmo RamenColor.cmo RamenFormats.cmo RamenChart.cmo
	$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I web -c $<

web/ramen_script.byte: Consts.cmo RamenHtml.cmo RamenSharedTypesJS_noPPP.cmo web/WebHelpers.cmo web/JsHelpers.cmo web/engine.cmo RamenColor.cmo RamenFormats.cmo RamenChart.cmo web/ramen_app.cmo
	$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -linkpkg $^ -o $@

web/ramen_script.js: web/ramen_script.byte
	$(JSOO) --pretty --opt=3 $^ -o $@

web/ramen_script.min.js: web/ramen_script.js
	$(JS_MINIFY) $<

RamenGui.ml: web/ramen_script.js web/ramen_script.min.js web/style.css Makefile
	@echo '(* Generated by Makefile - edition is futile *)' > $@
	@echo 'let without_link = {lookmadelimiter|<html>' >> $@
	@echo '<head><meta charset="UTF-8">' >> $@
	@echo '<title>Ramen</title><style media="all">' >> $@
	@cat web/style.css >> $@
	@echo '</style><script>' >> $@
	@cat web/ramen_script.min.js >> $@
	@echo '</script></head>' >> $@
	@echo '<body><div id="application"></div></body>' >> $@
	@echo '</html>|lookmadelimiter}' >> $@
	@echo 'let with_links = {lookmadelimiter|<html>' >> $@
	@echo '<head><meta charset="UTF-8">' >> $@
	@echo '<title>Ramen</title>' >> $@
	@echo '<link href="style.css" rel="stylesheet" media="all"/>' >> $@
	@echo '<script src="ramen_script.js"></script>' >> $@
	@echo '</head>' >> $@
	@echo '<body><div id="application"></div></body>' >> $@
	@echo '</html>|lookmadelimiter}' >> $@

AlerterSharedTypesJS_noPPP.cmo: AlerterSharedTypesJS.ml
	$(OCAMLC) $(OCAMLFLAGS) -c $< -o $@

web/alerter_app.cmo: web/alerter_app.ml AlerterSharedTypesJS_noPPP.cmo RamenHtml.cmo web/WebHelpers.cmo web/JsHelpers.cmo web/engine.cmo RamenColor.cmo RamenFormats.cmo RamenChart.cmo
	$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I web -c $<

web/alerter_script.byte: AlerterSharedTypesJS_noPPP.cmo RamenHtml.cmo Consts.cmo web/WebHelpers.cmo web/JsHelpers.cmo web/engine.cmo RamenColor.cmo RamenFormats.cmo RamenChart.cmo web/alerter_app.cmo
	$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -linkpkg $^ -o $@

web/alerter_script.js: web/alerter_script.byte
	$(JSOO) --pretty --opt=3 $^ -o $@

web/alerter_script.min.js: web/alerter_script.js
	$(JS_MINIFY) $<

AlerterGui.ml: web/alerter_script.js web/alerter_script.min.js web/style.css Makefile
	@echo '(* Generated by Makefile - edition is futile *)' > $@
	@echo 'let without_link = {lookmadelimiter|<html>' >> $@
	@echo '<head><meta charset="UTF-8">' >> $@
	@echo '<title>Alerter</title><style media="all">' >> $@
	@cat web/style.css >> $@
	@echo '</style><script>' >> $@
	@cat web/alerter_script.min.js >> $@
	@echo '</script></head>' >> $@
	@echo '<body><div id="application"></div></body>' >> $@
	@echo '</html>|lookmadelimiter}' >> $@
	@echo 'let with_links = {lookmadelimiter|<html>' >> $@
	@echo '<head><meta charset="UTF-8">' >> $@
	@echo '<title>Alerter</title>' >> $@
	@echo '<link href="style.css" rel="stylesheet" media="all"/>' >> $@
	@echo '<script src="alerter_script.js"></script>' >> $@
	@echo '</head>' >> $@
	@echo '<body><div id="application"></div></body>' >> $@
	@echo '</html>|lookmadelimiter}' >> $@

# Boring stuff

clean-spec:
	$(RM) ringbuf/*.o colcomp/*.o colcomp/colcomp_test

distclean-spec:
	$(RM) ramen codegen.cmxa RamenFileNotify.ml
	$(RM) libringbuf.a libcolcomp.a libcollectd.a libnetflow.a

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
	./tests/commit_before

check-long: long-check

include $(top_srcdir)/make.common

# Dependencies

include .depend
