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
	RamenVersions.ml RamenLog.ml Consts.ml Helpers.ml RWLock.ml RamenOutRef.ml \
	RamenSharedTypesJS.ml RamenSharedTypes.ml AlerterSharedTypesJS.ml \
	RingBufLib.ml RamenParsing.ml \
	EthAddr.ml Ipv4.ml Ipv6.ml \
	RamenCollectd.ml RamenNetflow.ml RamenProtocols.ml Lang.ml \
	RingBuf.ml RamenConf.ml Globs.ml \
	RamenCompilConfig.ml RamenDepLibs.ml RamenOCamlCompiler.ml CodeGen_OCaml.ml \
	RamenBinocle.ml RamenBitmask.ml RamenExport.ml \
	RamenHttpHelpers.ml RamenProcesses.ml \
	Compiler.ml RamenBloomFilter.ml RamenHtml.ml

RAMEN_SOURCES = \
	RamenVersions.ml Consts.ml RamenLog.ml Helpers.ml RamenBitmask.ml \
	RWLock.ml RamenOutRef.ml \
	RamenSharedTypesJS.ml AlerterSharedTypesJS.ml RamenSharedTypes.ml \
	RamenCollectd.ml RamenNetflow.ml RamenProtocols.ml RamenParsing.ml \
	EthAddr.ml Ipv4.ml Ipv6.ml RingBufLib.ml Lang.ml RingBuf.ml RamenConf.ml \
	RamenBinocle.ml RamenExport.ml \
	RamenHttpHelpers.ml RamenProcesses.ml Globs.ml \
	RamenCompilConfig.ml RamenDepLibs.ml RamenOCamlCompiler.ml CodeGen_OCaml.ml \
	Compiler.ml RamenHtml.ml RamenColor.ml RamenFormats.ml \
	RamenChart.ml RamenGui.ml \
	SqliteHelpers.ml RamenAlerter.ml HttpSrv.ml ApiCmd.ml RingBufCmd.ml \
	ramen.ml

CODEGENLIB_SOURCES = \
	Consts.ml RamenLog.ml Helpers.ml Globs.ml RWLock.ml \
	RamenOutRef.ml RamenParsing.ml EthAddr.ml Ipv4.ml Ipv6.ml \
	RamenSharedTypesJS.ml RamenSharedTypes.ml \
	RamenCollectd.ml RamenNetflow.ml RamenProtocols.ml \
	RingBufLib.ml RingBuf.ml RamenBinocle.ml \
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

WEB_SOURCES = \
	web/WebHelpers.ml web/JsHelpers.ml web/engine.ml web/gui.ml \
	web/style.ml web/ramen_app.ml web/alerter_app.ml

SOURCES_ = \
	$(RAMEN_SOURCES) $(CODEGENLIB_SOURCES) \
	$(LIBRINGBUF_SOURCES) $(LIBRINGBUF_OCAML_SOURCES) \
	$(LIBCOLLECTD_SOURCES) $(LIBNETFLOW_SOURCES) $(WEB_SOURCES) \
	$(CONFIGURATOR_SOURCES) \
	ringbuf_test.ml colcomp/colcomp_test.c
# Do not take into account generated code that depends on compilation:
SOURCES = $(filter-out RamenGui.ml,$(SOURCES_))

PACKAGES = \
	ppp lwt.ppx batteries cmdliner stdint parsercombinator syslog sqlite3 \
	cohttp-lwt-unix num inotify.lwt binocle unix lacaml net_codecs \
	compiler-libs compiler-libs.common compiler-libs.bytecomp compiler-libs.optcomp

INSTALLED_BIN = ramen ramen_configurator
INSTALLED_LIB = \
  META codegen.cmxa codegen.a \
  $(CODEGENLIB_SOURCES:.ml=.cmi) $(CODEGENLIB_SOURCES:.ml=.cmx) \
  libringbuf.a libcolcomp.a libcollectd.a libnetflow.a
INSTALLED = $(INSTALLED_BIN) $(INSTALLED_LIB)

bin_dir ?= /usr/local/bin
lib_dir ?= /usr/local/lib/ramen

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

ringbuf_test.opt: RamenLog.cmx Consts.cmx Helpers.cmx RamenSharedTypesJS.cmx RamenSharedTypes.cmx RWLock.cmx RamenOutRef.cmx RingBufLib.cmx RingBuf.cmx ringbuf_test.cmx libringbuf.a libcollectd.a libnetflow.a
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) $(filter %.cmx, $^) -o $@

ramen_configurator: $(CONFIGURATOR_SOURCES:.ml=.cmx)
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg -package "$(PACKAGES) sqlite3" $(filter %.cmx, $^) -o $@

all_tests.opt: libringbuf.a libcollectd.a libnetflow.a $(LINKED_FOR_TESTS:.ml=.cmx) all_tests.ml
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) -package qcheck -I web $(filter %.cmx, $^) $(filter %.ml, $^) -o $@

# configurator/alerter specific sources with more packages

SqliteHelpers.cmx: SqliteHelpers.ml
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -package "$(PACKAGES) sqlite3" -c $<

Conf_of_sqlite.cmx: Conf_of_sqlite.ml
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -package "$(PACKAGES) sqlite3" -c $<

# embedded compiler version: build a bundle of all libraries
DEP_DIRS = $(shell ocamlfind query -recursive -predicates native -format '%d' ramen | sort -u | grep -v /findlib)
DEP_LIBS = $(shell ocamlfind query -recursive -predicates native -format '%+a' ramen | uniq | grep -v /findlib)
OCAML_WHERE = $(shell dirname $(shell ocamlfind ocamlc -where))
# cut -c X- means to takes all from X, 1st char being 1, thus the leading _:
OCAML_WHERE_LEN = $(shell printf '_%s/' $(OCAML_WHERE) | wc -m | xargs echo)

BUNDLE_DIR = bundle

bundle:
	@echo "Bundling libs together into $(BUNDLE_DIR)"
	@$(RM) -r '$(BUNDLE_DIR)'
	@mkdir -p '$(BUNDLE_DIR)'
	@for d in $(DEP_DIRS) ; do \
		echo $$d ;\
		cp -r "$$d" '$(BUNDLE_DIR)/' ;\
	done
	@find '$(BUNDLE_DIR)' -type f -not '-(' \
			-name '*.cmx*' -o -name '*.cmi' -o -name '*.a' -o \
			-name '*.so' -o -name '*.o' \
		'-)' -delete
	@find '$(BUNDLE_DIR)' -empty -delete
	@# Delete some of the biggest unused files:
	@$(RM) $(BUNDLE_DIR)/batteries/batteriesExceptionless.cm*
	@$(RM) -r $(BUNDLE_DIR)/ocaml/compiler-libs
	@$(RM) -r $(BUNDLE_DIR)/ocaml/camlp4
	@$(RM) -r $(BUNDLE_DIR)/ocaml/ocamldoc
	@$(RM) -r $(BUNDLE_DIR)/ocaml/thread
	@$(RM) -r $(BUNDLE_DIR)/ocaml/ocplib*
	@$(RM) $(BUNDLE_DIR)/ocaml/camlinternalFormat.*
	@$(RM) $(BUNDLE_DIR)/ocaml/*spacetime*
	@# to clear a clang warning:
	@mkdir $(BUNDLE_DIR)/num
	@mkdir $(BUNDLE_DIR)/bytes

# Bootstrapping this is a bit special as ocamlfind needs to know the ramen
# package first. Therefore this file is included with the source package for
# simplicity.
RamenDepLibs.ml:
	@echo '(* Generated by Makefile - edition is futile *)' > $@
	@echo 'let incdirs = [' >> $@
	@for d in $(DEP_DIRS) ; do \
		echo $$d | cut -c $(OCAML_WHERE_LEN)- | \
		sed -e 's,^\(.*\)$$,  "\1" ;,' >> $@ ;\
	done ;
	@echo "]" >> $@
	@echo "let objfiles = [" >> $@
	@for d in $(DEP_LIBS) ; do \
		echo $$d | cut -c $(OCAML_WHERE_LEN)- | \
		sed -e 's,^\(.*\)$$,  "\1" ;,' >> $@ ;\
	done ;
	@echo "]" >> $@

# At the contrary, this one has to be generated at every build:
RamenCompilConfig.ml:
	@echo '(* Generated by Makefile - edition is futile *)' > $@
	@echo 'let default_bundle_dir = "$(lib_dir)/$(BUNDLE_DIR)/"' >> $@

# Web thingies

WEB_PACKAGES = js_of_ocaml js_of_ocaml.ppx

web/JsHelpers.cmo: web/JsHelpers.ml web/WebHelpers.cmo
	$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I web -c $<

web/WebHelpers.cmo: web/WebHelpers.ml RamenChart.cmo
	$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I web -c $<

web/engine.cmo: web/engine.ml web/WebHelpers.cmo web/JsHelpers.cmo RamenHtml.cmo
	$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I web -c $<

web/gui.cmo: web/gui.ml web/engine.cmo RamenHtml.cmo
	$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I web -c $<

web/style.cmo: web/style.ml RamenHtml.cmo
	$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I web -c $<

AlerterSharedTypesJS_noPPP.cmo: AlerterSharedTypesJS.ml
	$(OCAMLC) $(OCAMLFLAGS) -c $< -o $@

web/alerter_app.cmo: web/alerter_app.ml AlerterSharedTypesJS_noPPP.cmo RamenHtml.cmo web/WebHelpers.cmo web/JsHelpers.cmo web/engine.cmo web/gui.cmo web/style.cmo RamenColor.cmo RamenFormats.cmo RamenChart.cmo
	$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I web -c $<

RamenSharedTypesJS_noPPP.cmo: RamenSharedTypesJS.ml
	$(OCAMLC) $(OCAMLFLAGS) -c $< -o $@

web/ramen_app.cmo: web/ramen_app.ml RamenSharedTypesJS_noPPP.cmo RamenHtml.cmo web/WebHelpers.cmo web/JsHelpers.cmo web/engine.cmo web/gui.cmo web/style.cmo RamenColor.cmo RamenFormats.cmo RamenChart.cmo web/alerter_app.cmo
	$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I web -c $<

web/ramen_script.byte: Consts.cmo RamenHtml.cmo RamenSharedTypesJS_noPPP.cmo web/WebHelpers.cmo web/JsHelpers.cmo web/engine.cmo web/gui.cmo web/style.cmo RamenColor.cmo RamenFormats.cmo RamenChart.cmo AlerterSharedTypesJS_noPPP.cmo web/alerter_app.cmo web/ramen_app.cmo
	$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -linkpkg $^ -o $@

web/ramen_script.js: web/ramen_script.byte
	$(JSOO) --pretty --opt=3 $^ -o $@

web/ramen_script.min.js: web/ramen_script.js
	$(JS_MINIFY) $<

web/style.css: web/style.css.m4 macs.m4
	@m4 -P macs.m4 $< > $@

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
	@echo '<link href="/style.css" rel="stylesheet" media="all"/>' >> $@
	@echo '<script src="/ramen_script.js"></script>' >> $@
	@echo '</head>' >> $@
	@echo '<body><div id="application"></div></body>' >> $@
	@echo '</html>|lookmadelimiter}' >> $@

# Boring stuff

clean-spec:
	@$(RM) ringbuf/*.o colcomp/*.o colcomp/colcomp_test
	@$(RM) RamenDepLibs.ml RamenCompilConfig.ml

distclean-spec:
	@$(RM) ramen codegen.cmxa RamenFileNotify.ml
	@$(RM) libringbuf.a libcolcomp.a libcollectd.a libnetflow.a
	@$(RM) -r $(BUNDLE_DIR)

install-spec: $(INSTALLED)
	@ocamlfind install ramen $(INSTALLED_LIB)
	@install -d $(prefix)$(bin_dir)
	@install $(INSTALLED_BIN) $(prefix)$(bin_dir)/

# We need ocamlfind to find ramen package before we can install the lib bundle.
# Therefore we simply build it as a separate, optional step (that's only
# required if you intend to use the embedded compiler).
install-bundle: bundle
	@install -d $(prefix)$(lib_dir)
	@cp -r $(BUNDLE_DIR) $(prefix)$(lib_dir)/

uninstall-spec:
	@ocamlfind remove ramen
	@$(RM) $(prefix)$(bin_dir)/ramen
	@$(RM) -r $(prefix)$(lib_dir)

colcomp/colcomp_test: colcomp/colcomp_test.o libcolcomp.a
	$(CC) $(LDFLAGS) $(LOADLIBES) $(LDLIBS) $^ -o $@

check-spec: all_tests.opt ringbuf_test.opt
	@./ringbuf_test.opt || echo "FAILURE (ringbuf_test)"
	@OCAMLRUNPARAM=b ./all_tests.opt -bt || echo "FAILURE"

long-check: ramen
	@$(MAKE) check
	@./ringbuf_test.opt || echo "FAILURE"
	@if test -e /tmp/ramen_tests ; then \
		echo "Warning: /tmp/ramen_tests exists already!" ;\
		echo "If subdirs are reused result of tests should not be relied upon." ;\
	fi
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
	./tests/from

check-long: long-check

include $(top_srcdir)/make.common

# Dependencies

include .depend
