# Configuration

VERSION = 1.2.6

DUPS_IN = $(shell ocamlfind ocamlc -where)/compiler-libs
OCAMLC     = OCAMLPATH=$(OCAMLPATH) OCAMLRUNPARAM= OCAMLFIND_IGNORE_DUPS_IN="$(DUPS_IN)" ocamlfind ocamlc
OCAMLOPT   = OCAMLPATH=$(OCAMLPATH) OCAMLRUNPARAM= OCAMLFIND_IGNORE_DUPS_IN="$(DUPS_IN)" ocamlfind ocamlopt
OCAMLDEP   = OCAMLPATH=$(OCAMLPATH) OCAMLRUNPARAM= OCAMLFIND_IGNORE_DUPS_IN="$(DUPS_IN)" ocamlfind ocamldep
OCAMLMKTOP = OCAMLPATH=$(OCAMLPATH) OCAMLRUNPARAM= OCAMLFIND_IGNORE_DUPS_IN="$(DUPS_IN)" ocamlfind ocamlmktop
QTEST      = qtest
JSOO       = js_of_ocaml
JS_MINIFY  = jsoo_minify
WARNS      = -w -40
override OCAMLOPTFLAGS += -I src $(WARNS) -g -annot -O2 -S
override OCAMLFLAGS    += -I src $(WARNS) -g -annot
override CFLAGS        += --std=c11 -g -O2 -Wall -W -Wno-parentheses -fPIC
override CPPFLAGS      += --std=c11 -D_GNU_SOURCE \
                          -I $(shell ocamlfind ocamlc -where) \
                          -DHAVE_INT128 -I $(shell ocamlfind query stdint)

ifeq ($(shell uname),Darwin)
FILE_NOTIFIER = src/RamenFileNotify_Poller.ml
else
FILE_NOTIFIER = src/RamenFileNotify_Inotify.ml
endif

PACKAGES = \
	ppp ppp.unix lwt_ppx batteries cmdliner stdint parsercombinator \
	syslog sqlite3 \
	cohttp-lwt-unix num inotify.lwt binocle unix lacaml net_codecs \
	compiler-libs compiler-libs.common compiler-libs.bytecomp \
	compiler-libs.optcomp

INSTALLED_BIN = src/ramen src/ramen_configurator
INSTALLED_LIB = \
  META src/codegen.cmxa src/codegen.a \
  $(CODEGENLIB_SOURCES:.ml=.cmi) $(CODEGENLIB_SOURCES:.ml=.cmx) \
  src/libringbuf.a src/libcollectd.a src/libnetflow.a
INSTALLED = $(INSTALLED_BIN) $(INSTALLED_LIB)

bin_dir ?= /usr/bin/
# Where the deb (or other non-opam) version of ramen will get the libraries
# bundle from. This is important that the ramen package is compiled with the
# correct path as we use opam installed libs to build the non-opam bundle,
# or RamenCompilConfig will be incompatible).
lib_dir ?= /usr/lib/ramen

all: $(INSTALLED)

# Generic rules

.SUFFIXES: .ml .mli .cmo .cmi .cmx .cmxs .annot .top .js .html .adoc
.PHONY: clean all check dep install uninstall reinstall \
        bundle doc deb \
        docker-latest docker-build-image docker-build-builder docker-push

%.cmo: %.ml
	@echo "Compiling $@ (bytecode)"
	@$(OCAMLC) $(OCAMLFLAGS) -package "$(PACKAGES)" -c $<

%.cmx %.annot: %.ml
	@echo "Compiling $@ (native code)"
	@$(OCAMLOPT) $(OCAMLOPTFLAGS) -package "$(PACKAGES)" -c $<

%.top: %.cma
	@echo "Building $@ top level"
	@$(OCAMLMKTOP) $(OCAMLFLAGS) -custom -package "$(PACKAGES)" $< -o $@

%.html: %.adoc
	@echo "Building documentation $@"
	@asciidoc -a data-uri -a icons -a toc -a max-width=55em --theme volnitsky -o $@ $<

# Dependencies

RAMEN_SOURCES = \
	src/RamenVersions.ml src/Consts.ml src/RamenLog.ml src/Helpers.ml \
	src/RamenBitmask.ml src/RWLock.ml src/RamenOutRef.ml \
	src/RamenSharedTypesJS.ml src/AlerterSharedTypesJS.ml \
	src/RamenParsing.ml src/EthAddr.ml src/Ipv4.ml src/Ipv6.ml \
	src/RamenSharedTypes.ml src/RamenCollectd.ml src/RamenNetflow.ml \
	src/RamenProtocols.ml src/RingBufLib.ml src/RamenTypeConverters.ml \
	src/Lang.ml src/RamenScalar.ml src/RamenTuple.ml src/RamenExpr.ml src/RamenOperation.ml src/RamenProgram.ml \
	src/RingBuf.ml src/RamenSerialization.ml \
	src/RamenConf.ml src/RamenBinocle.ml src/RamenExport.ml \
	src/RamenHttpHelpers.ml src/RamenProcesses.ml src/Globs.ml \
	src/RamenCompilConfig.ml src/RamenDepLibs.ml src/RamenOCamlCompiler.ml \
	src/CodeGen_OCaml.ml src/Compiler.ml src/RamenHtml.ml src/RamenColor.ml \
	src/RamenFormats.ml src/RamenChart.ml src/RamenGui.ml \
	src/SqliteHelpers.ml src/RamenAlerter.ml \
	src/RamenOps.ml src/RamenTests.ml \
	src/HttpSrv.ml src/TermTable.ml src/ApiCmd.ml \
	src/RingBufCmd.ml src/ramen.ml

CODEGENLIB_SOURCES = \
	src/Consts.ml src/RamenLog.ml src/Helpers.ml src/Globs.ml src/RWLock.ml \
	src/RamenOutRef.ml src/RamenParsing.ml src/EthAddr.ml src/Ipv4.ml \
	src/Ipv6.ml \
	src/RamenCollectd.ml src/RamenNetflow.ml \
	src/RingBufLib.ml src/RingBuf.ml src/RamenBinocle.ml \
	src/RamenBloomFilter.ml src/RamenFileNotify.ml src/CodeGenLib_IO.ml \
	src/CodeGenLib_State.ml src/RamenHeap.ml src/RamenSortBuf.ml \
	src/CodeGenLib.ml src/RamenTypeConverters.ml

LIBRINGBUF_SOURCES = \
	src/ringbuf/ringbuf.h src/ringbuf/ringbuf.c src/ringbuf/wrappers.c

LIBCOLCOMP_SOURCES = \
	src/colcomp/growblock.h src/colcomp/growblock.c src/colcomp/colcomp.h \
	src/colcomp/colcomp.c

LIBCOLLECTD_SOURCES = \
	src/collectd/collectd.h src/collectd/collectd.c src/collectd/wrappers.c

LIBNETFLOW_SOURCES = \
	src/netflow/v5.c

CONFIGURATOR_SOURCES = \
	src/Consts.ml src/RamenLog.ml src/Helpers.ml src/RamenSharedTypesJS.ml \
	src/RamenSharedTypes.ml src/SqliteHelpers.ml src/Conf_of_sqlite.ml \
	src/ramen_configurator.ml

WEB_SOURCES = \
	src/web/WebHelpers.ml src/web/JsHelpers.ml src/web/engine.ml \
	src/web/gui.ml src/web/style.ml src/web/ramen_app.ml \
	src/web/alerter_app.ml

TESTONLY_SOURCES = \
	src/TestHelpers.ml

SOURCES_ = \
	$(RAMEN_SOURCES) $(CODEGENLIB_SOURCES) \
	$(LIBRINGBUF_SOURCES) $(LIBRINGBUF_OCAML_SOURCES) \
	$(LIBCOLLECTD_SOURCES) $(LIBNETFLOW_SOURCES) $(WEB_SOURCES) \
	$(CONFIGURATOR_SOURCES) $(TESTONLY_SOURCES) \
	src/ringbuf_test.ml src/colcomp/colcomp_test.c
# Do not take into account generated code that depends on compilation:
SOURCES = $(filter-out src/RamenGui.ml,$(SOURCES_))

dep:
	@$(RM) .depend
	@$(MAKE) .depend

.depend: $(SOURCES)
	@$(OCAMLDEP) -I src -package "$(PACKAGES)" $(filter %.ml, $(SOURCES)) $(filter %.mli, $(SOURCES)) > $@
	@for f in $(filter %.c, $(SOURCES)); do \
		$(CC) $(CPPFLAGS) -MM -MT "$$(dirname $$f)/$$(basename $$f .c).o" $$f >> $@; \
	done

include .depend

# Compile Ramen

src/RamenFileNotify.ml: $(FILE_NOTIFIER)
	@echo "Using implementation $(FILE_NOTIFIER) for new file notifications"
	@ln -sf $(notdir $<) $@
	@touch $@

src/libringbuf.a: src/ringbuf/ringbuf.o src/ringbuf/wrappers.o
	@echo "Building ringbuf library"
	@sleep 1 # ar truncate mtime !?
	@$(AR) rs $@ $^ >/dev/null

src/libcolcomp.a: src/colcomp/growblock.o src/colcomp/colcomp.o
	@echo "Building colcomp library"
	@sleep 1 # ar truncate mtime !?
	@$(AR) rs $@ $^ >/dev/null

src/libcollectd.a: src/collectd/collectd.o src/collectd/wrappers.o
	@echo "Building collectd library"
	@sleep 1 # ar truncate mtime !?
	@$(AR) rs $@ $^ >/dev/null

src/libnetflow.a: src/netflow/v5.o
	@echo "Building netflow library"
	@sleep 1 # ar truncate mtime !?
	@$(AR) rs $@ $^ >/dev/null

# We have to force -cclib -lstdint_stubs right after -cclib wrap_ringbuf.o
# otherwise -package stdint would put it before and gcc would not include the
# symbols we need as we are the only users.
MOREFLAGS = \
	-package "$(PACKAGES)" \
	-cclib -Lsrc \
	-cclib -lstdint_stubs \
	-cclib -lringbuf \
	-cclib -lcollectd \
	-cclib -lnetflow

src/ramen: $(RAMEN_SOURCES:.ml=.cmx) src/libringbuf.a src/libcollectd.a src/libnetflow.a
	@echo "Linking $@"
	@$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) $(filter %.cmx, $^) -o $@

src/codegen.cmxa: $(CODEGENLIB_SOURCES:.ml=.cmx) src/libringbuf.a src/libcollectd.a src/libnetflow.a
	@echo "Linking runtime library (native code) $@"
	@$(OCAMLOPT) $(OCAMLOPTFLAGS) -a $(MOREFLAGS) $(filter %.cmx, $^) -o $@

src/codegen.cma: $(CODEGENLIB_SOURCES:.ml=.cmo) src/libringbuf.a src/libcollectd.a src/libnetflow.a
	@echo "Linking runtime library (byte code) $@"
	@$(OCAMLC) $(OCAMLFLAGS) -a $(MOREFLAGS) $(filter %.cmo, $^) -o $@

# configurator/alerter specific sources with more packages

src/SqliteHelpers.cmx: src/SqliteHelpers.ml
	@echo "Compiling $@ (native code)"
	@$(OCAMLOPT) $(OCAMLOPTFLAGS) -package "$(PACKAGES) sqlite3" -c $<

src/Conf_of_sqlite.cmx: src/Conf_of_sqlite.ml
	@echo "Compiling $@ (native code)"
	@$(OCAMLOPT) $(OCAMLOPTFLAGS) -package "$(PACKAGES) sqlite3" -c $<

src/ramen_configurator: $(CONFIGURATOR_SOURCES:.ml=.cmx)
	@echo "Linking $@"
	@$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg -package "$(PACKAGES) sqlite3" $(filter %.cmx, $^) -o $@

# embedded compiler version: build a bundle of all libraries
OCAML_WHERE = $(shell dirname $(shell ocamlfind ocamlc -where))
# cut -c X- means to takes all from X, 1st char being 1, thus the leading _:
OCAML_WHERE_LEN = $(shell printf '_%s/' $(OCAML_WHERE) | wc -m | xargs echo)

BUNDLE_DIR = bundle

bundle: bundle/date

bundle/date: src/codegen.cmxa
	@echo "Bundling libs together into $(BUNDLE_DIR)"
	@$(RM) -r '$(BUNDLE_DIR)'
	@mkdir -p '$(BUNDLE_DIR)'
	@set -e ; for d in $$(ocamlfind query -recursive -predicates native -format '%d' ramen | sort -u | grep -v /findlib) ; do \
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
	@touch $@

# Bootstrapping this is a bit special as ocamlfind needs to know the ramen
# package first. Therefore this file is included with the source package for
# simplicity.
src/RamenDepLibs.ml:
	@echo '(* Generated by Makefile - edition is futile *)' > $@
	@echo 'let incdirs = [' >> $@
	@set -e ; for d in $$(ocamlfind query -recursive -predicates native -format '%d' ramen | sort -u | grep -v /findlib) ; do \
		echo $$d | cut -c $(OCAML_WHERE_LEN)- | \
		sed -e 's,^\(.*\)$$,  "\1" ;,' >> $@ ;\
	done ;
# Equivalent to -threads:
	@echo '  "ocaml/threads" ;' >> $@
	@echo "]" >> $@
	@echo "let objfiles = [" >> $@
	@set -e ; for d in $$(ocamlfind query -recursive -predicates native -format '%+a' ramen | uniq | grep -v /findlib) ; do \
		echo $$d | cut -c $(OCAML_WHERE_LEN)- | \
		sed -e 's,^\(.*\)$$,  "\1" ;,' >> $@ ;\
	done ;
	@echo "]" >> $@
# Equivalent to -threads, must come right after unix.cmxa:
	@sed -i -e '/ocaml\/unix.cmxa/a \ \ "ocaml/threads/threads.cmxa" ;' $@

# At the contrary, this one has to be generated at every build:
src/RamenCompilConfig.ml:
	@echo '(* Generated by Makefile - edition is futile *)' > $@
	@echo 'let default_bundle_dir = "$(lib_dir)/$(BUNDLE_DIR)/"' >> $@

# Web thingies

WEB_PACKAGES = js_of_ocaml js_of_ocaml.ppx

src/web/JsHelpers.cmo: src/web/JsHelpers.ml src/web/WebHelpers.cmo
	@echo "Compiling $@ (byte code for JS)"
	@$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I src/web -c $<

src/web/WebHelpers.cmo: src/web/WebHelpers.ml src/RamenChart.cmo
	@echo "Compiling $@ (byte code for JS)"
	@$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I src/web -c $<

src/web/engine.cmo: src/web/engine.ml src/web/WebHelpers.cmo src/web/JsHelpers.cmo src/RamenHtml.cmo
	@echo "Compiling $@ (byte code for JS)"
	@$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I src/web -c $<

src/web/gui.cmo: src/web/gui.ml src/web/engine.cmo src/RamenHtml.cmo
	@echo "Compiling $@ (byte code for JS)"
	@$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I src/web -c $<

src/web/style.cmo: src/web/style.ml src/RamenHtml.cmo
	@echo "Compiling $@ (byte code for JS)"
	@$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I src/web -c $<

src/AlerterSharedTypesJS_noPPP.cmo: src/AlerterSharedTypesJS.ml
	@echo "Compiling $@ (byte code for JS)"
	@$(OCAMLC) $(OCAMLFLAGS) -c $< -o $@

src/web/alerter_app.cmo: src/web/alerter_app.ml src/AlerterSharedTypesJS_noPPP.cmo src/RamenHtml.cmo src/web/WebHelpers.cmo src/web/JsHelpers.cmo src/web/engine.cmo src/web/gui.cmo src/web/style.cmo src/RamenColor.cmo src/RamenFormats.cmo src/RamenChart.cmo
	@echo "Compiling $@ (byte code for JS)"
	@$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I src/web -c $<

src/RamenSharedTypesJS_noPPP.cmo: src/RamenSharedTypesJS.ml
	@echo "Compiling $@ (byte code for JS)"
	@$(OCAMLC) $(OCAMLFLAGS) -c $< -o $@

src/web/ramen_app.cmo: src/web/ramen_app.ml src/RamenSharedTypesJS_noPPP.cmo src/RamenHtml.cmo src/web/WebHelpers.cmo src/web/JsHelpers.cmo src/web/engine.cmo src/web/gui.cmo src/web/style.cmo src/RamenColor.cmo src/RamenFormats.cmo src/RamenChart.cmo src/web/alerter_app.cmo
	@echo "Compiling $@ (byte code for JS)"
	@$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -I src/web -c $<

src/web/ramen_script.byte: src/Consts.cmo src/RamenHtml.cmo src/RamenSharedTypesJS_noPPP.cmo src/web/WebHelpers.cmo src/web/JsHelpers.cmo src/web/engine.cmo src/web/gui.cmo src/web/style.cmo src/RamenColor.cmo src/RamenFormats.cmo src/RamenChart.cmo src/AlerterSharedTypesJS_noPPP.cmo src/web/alerter_app.cmo src/web/ramen_app.cmo
	@echo "Linking byte code library $@ for JS"
	@$(OCAMLC) $(OCAMLFLAGS) -package "$(WEB_PACKAGES)" -linkpkg $^ -o $@

src/web/ramen_script.js: src/web/ramen_script.byte
	@echo "Converting byte code $< into JS $@"
	@$(JSOO) --pretty --opt=3 $^ -o $@

src/web/ramen_script.min.js: src/web/ramen_script.js
	@echo "Converting byte code $< into JS $@"
	@$(JS_MINIFY) $<

src/web/style.css: src/web/style.css.m4 src/macs.m4
	@echo "Expanding CSS into $@"
	@m4 -P src/macs.m4 $< > $@

src/RamenGui.ml: src/web/ramen_script.js src/web/ramen_script.min.js src/web/style.css
	@echo "Generating $@ with embedded JS"
	@echo '(* Generated by Makefile - edition is futile *)' > $@
	@echo 'let without_link = {lookmadelimiter|<html>' >> $@
	@echo '<head><meta charset="UTF-8">' >> $@
	@echo '<title>Ramen</title><style media="all">' >> $@
	@cat src/web/style.css >> $@
	@echo '</style><script>' >> $@
	@cat src/web/ramen_script.min.js >> $@
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

# Tests

TESTABLE_SOURCES = \
	src/EthAddr.ml src/Ipv4.ml src/Ipv6.ml \
	src/Lang.ml src/RamenScalar.ml src/RamenExpr.ml \
	src/RamenOperation.ml src/RamenProgram.ml \
	src/Compiler.ml \
	src/Helpers.ml src/RamenBloomFilter.ml src/Globs.ml src/CodeGen_OCaml.ml \
	src/RamenHtml.ml \
	src/RamenSortBuf.ml

# For the actual command line building all_tests.opt:
LINKED_FOR_TESTS = \
	src/RamenVersions.ml src/RamenLog.ml src/Consts.ml src/Helpers.ml \
	src/RWLock.ml src/RamenOutRef.ml src/RamenSharedTypesJS.ml \
	src/RamenSharedTypes.ml src/AlerterSharedTypesJS.ml src/RingBufLib.ml \
	src/RamenParsing.ml src/EthAddr.ml src/Ipv4.ml src/Ipv6.ml \
	src/RamenCollectd.ml src/RamenNetflow.ml src/RamenProtocols.ml \
	src/RamenTypeConverters.ml \
	src/Lang.ml src/RamenScalar.ml src/RamenTuple.ml src/RamenExpr.ml src/RamenOperation.ml src/RamenProgram.ml \
	src/RingBuf.ml src/RamenConf.ml \
	src/Globs.ml \
	src/RamenCompilConfig.ml src/RamenDepLibs.ml src/RamenOCamlCompiler.ml \
	src/RamenHeap.ml src/RamenSortBuf.ml \
	src/CodeGen_OCaml.ml src/RamenBinocle.ml src/RamenBitmask.ml \
	src/RamenSerialization.ml src/RamenExport.ml \
	src/RamenHttpHelpers.ml src/RamenProcesses.ml \
	src/Compiler.ml src/RamenBloomFilter.ml src/RamenHtml.ml \
	src/TestHelpers.ml

src/all_tests.ml: $(TESTABLE_SOURCES)
	@echo "Generating unit tests into $@"
	@$(QTEST) --shuffle -o $@ extract $^

all_tests.opt: src/libringbuf.a src/libcollectd.a src/libnetflow.a $(LINKED_FOR_TESTS:.ml=.cmx) src/all_tests.ml
	@echo "Building unit tests into $@"
	@$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) -package qcheck -I src/web $(filter %.cmx, $^) $(filter %.ml, $^) -o $@

ringbuf_test.opt: src/RamenLog.cmx src/Consts.cmx src/Helpers.cmx src/RamenSharedTypesJS.cmx src/RamenSharedTypes.cmx src/RWLock.cmx src/RamenOutRef.cmx src/RingBufLib.cmx src/RingBuf.cmx src/ringbuf_test.cmx src/libringbuf.a src/libcollectd.a src/libnetflow.a
	@echo "Building ringbuf tests into $@"
	@$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) $(filter %.cmx, $^) -o $@

colcomp/colcomp_test: colcomp/colcomp_test.o libcolcomp.a
	@echo "Building colcomp tests into $@"
	@$(CC) $(LDFLAGS) $(LOADLIBES) $(LDLIBS) $^ -o $@

check: all_tests.opt ringbuf_test.opt
	@echo "Running unit tests..."
	@./ringbuf_test.opt || echo "FAILURE (ringbuf_test)"
	@OCAMLRUNPARAM=b ./all_tests.opt -bt || echo "FAILURE"

long-check: src/ramen check
	@echo "Running all tests..."
	@if test -e /tmp/ramen_tests ; then \
		echo "Warning: /tmp/ramen_tests exists already!" ;\
		echo "If subdirs are reused result of tests should not be relied upon." ;\
	fi
	./src/tests/tops
	./src/tests/basic_aggr
	./src/tests/count_lines
	./src/tests/sliding_window
	./src/tests/fun_with_funcs
	./src/tests/lag
	./src/tests/word_split
	./src/tests/case
	./src/tests/season
	./src/tests/fit_multi
	./src/tests/upload
	./src/tests/word_count
	./src/tests/tuples
	./src/tests/commit_before
	./src/tests/from
	./src/tests/sort
	./src/tests/merge
	./src/tests/params

check-long: long-check

# Documentation

doc: \
	docs/tutorial_network_monitoring.html docs/tutorial_counting_words.html \
	docs/manual.html docs/roadmap.html docs/alerter.html

docs/tutorial_network_monitoring.html: docs/tutorial_group_by.svg docs/sample_chart1.svg

# Installation

install: $(INSTALLED)
	@ocamlfind install ramen $(INSTALLED_LIB)
	@echo "Installing binaries into $(prefix)$(bin_dir)"
	@install -d $(prefix)$(bin_dir)
	@install $(INSTALLED_BIN) $(prefix)$(bin_dir)/

# We need ocamlfind to find ramen package before we can install the lib bundle.
# Therefore we simply build it as a separate, optional step (that's only
# required if you intend to use the embedded compiler).
install-bundle: bundle
	@echo "Installing libraries bundle into $(DESTDIR)$(lib_dir)"
	@install -d $(DESTDIR)$(lib_dir)
	@cp -r $(BUNDLE_DIR) $(DESTDIR)$(lib_dir)/

uninstall:
	@ocamlfind remove ramen
	@echo "Uninstalling binaries and libraries bundle"
	@$(RM) $(prefix)$(bin_dir)/ramen $(prefix)$(bin_dir)/ramen_configurator

uninstall-bundle:
	@echo "Uninstalling libraries bundle"
	@$(RM) -r $(DESTDIR)$(lib_dir)/$(BUNDLE_DIR)

reinstall: uninstall install

# Debian package

deb: ramen.$(VERSION).deb

ramen.$(VERSION).deb: $(INSTALLED) bundle/date debian.control
	@echo "Building debian package $@"
	@sudo rm -rf debtmp
	@install -d debtmp/usr/bin debtmp/$(lib_dir)
	@install $(INSTALLED_BIN) debtmp/usr/bin
	@cp -r $(BUNDLE_DIR) debtmp/$(lib_dir)
	@mkdir -p debtmp/DEBIAN
	@cp debian.control debtmp/DEBIAN/control
	@chmod a+x -R debtmp/usr
	@sudo chown root: -R debtmp/usr
	@dpkg --build debtmp
	@mv debtmp.deb $@

# Docker image

docker-latest: docker/Dockerfile docker/ramen.$(VERSION).deb
	@echo "Building docker image for testing DEB version"
	@docker build -t rixed/ramen:latest --squash -f $< docker/

docker-build-builder: docker/Dockerfile-builder
	@for distrib in jessie stretch ; do \
		echo "Building docker image for building the DEB packages for $$distrib" ;\
		echo 'FROM debian:'$$distrib > docker/Dockerfile-builder.$$distrib ;\
		cat docker/Dockerfile-builder >> docker/Dockerfile-builder.$$distrib ;\
		docker build -t rixed/ramen-builder:$$distrib \
		             -f docker/Dockerfile-builder.$$distrib docker/ ;\
		$(RM) docker/Dockerfile-builder.$$distrib ;\
	done

docker-build-image: docker-latest
	@echo "Tagging latest docker image to v$(VERSION)"
	@docker tag rixed/ramen:latest rixed/ramen:v$(VERSION)

docker-push:
	@echo "Uploading docker images"
	@docker push rixed/ramen:latest
	@docker push rixed/ramen:v$(VERSION)
	@docker push rixed/ramen-builder:jessie
	@docker push rixed/ramen-builder:stretch

# Cleaning

clean:
	@echo "Cleaning all build files"
	@$(RM) src/*.cmo src/*.s src/*.annot src/*.o
	@$(RM) src/web/*.cmo src/web/*.annot src/web/*.byte
	@$(RM) *.opt src/all_tests.* perf.data* gmon.out
	@$(RM) src/ringbuf/*.o src/colcomp/*.o src/colcomp/colcomp_test
	@$(RM) src/*.cma src/*.cmx src/*.cmxa src/*.cmxs src/*.cmi
	@$(RM) src/web/*.cmi src/web/*.js src/web/style.css
	@$(RM) oUnit-anon.cache qtest.targets.log
	@$(RM) .depend src/*.opt src/*.byte src/*.top
	@$(RM) doc/tutorial.html doc/manual.html doc/roadmap.html
	@$(RM) src/ramen src/codegen.cmxa src/RamenFileNotify.ml src/libringbuf.a
	@$(RM) src/libcolcomp.a src/libcollectd.a src/libnetflow.a
	@$(RM) -r $(BUNDLE_DIR)
	@sudo rm -rf debtmp
	@$(RM) ramen.*.deb
