# Configuration

VERSION = 3.0.1

DUPS_IN  = $(shell ocamlfind query compiler-libs):$(shell ocamlfind query lwt_ppx)
OCAMLOPT = OCAMLPATH=$(OCAMLPATH) OCAMLRUNPARAM= OCAMLFIND_IGNORE_DUPS_IN="$(DUPS_IN)" ocamlfind ocamlopt
OCAMLDEP = OCAMLPATH=$(OCAMLPATH) OCAMLRUNPARAM= OCAMLFIND_IGNORE_DUPS_IN="$(DUPS_IN)" ocamlfind ocamldep
QTEST    = qtest
WARNS    = -w -40-58
override OCAMLOPTFLAGS += -thread -I src $(WARNS) -g -annot -S
override CFLAGS        += --std=gnu11 -g -O2 -Wall -W -Wno-parentheses -fPIC
override CPPFLAGS      += --std=gnu11 -D_GNU_SOURCE \
                          -I $(shell ocamlfind ocamlc -where) \
                          -DHAVE_INT128 -I $(shell ocamlfind query stdint)

ifdef NDEBUG
OCAMLOPTFLAGS += -noassert -O2
CPPFLAGS += -DNDEBUG -O2
endif

ifeq ($(shell uname),Darwin)
FILE_NOTIFIER = src/RamenFileNotify_Poller.ml
else
FILE_NOTIFIER = src/RamenFileNotify_Inotify.ml
endif

PACKAGES = \
	ppp ppp.unix lwt_ppx batteries cmdliner stdint parsercombinator \
	syslog cohttp-lwt-unix num inotify.lwt binocle unix lacaml \
	compiler-libs compiler-libs.common compiler-libs.bytecomp \
	compiler-libs.optcomp net_codecs sqlite3

INSTALLED_BIN = src/ramen
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

# Where examples are installed
sample_dir ?= /var/lib/ramen

all: $(INSTALLED)

# Generic rules

.SUFFIXES: .ml .mli .cmi .cmx .cmxs .annot .top .html .adoc .ramen .x .test .success
.PHONY: clean clean-temp all check func-check unit-check cli-check err-check \
        dep install uninstall reinstall bundle doc deb tarball \
        docker-latest docker-build-image docker-build-builder docker-circleci docker-push

%.cmi: %.mli
	@echo 'Compiling $@ (interface)'
	@$(OCAMLOPT) $(OCAMLOPTFLAGS) -package "$(PACKAGES)" -c $<

%.cmx %.annot: %.ml
	@echo 'Compiling $@'
	@$(OCAMLOPT) $(OCAMLOPTFLAGS) -package "$(PACKAGES)" -c $<

%.html: %.adoc
	@echo 'Building documentation $@'
	@asciidoc -a data-uri -a icons -a toc -a max-width=55em --theme volnitsky -o $@ $<

# Dependencies

RAMEN_SOURCES = \
	src/RamenVersions.ml \
	src/RamenConsts.ml \
	src/RamenLog.ml \
	src/RamenNullable.ml \
	src/RamenHelpers.ml \
	src/RamenExperiments.ml \
	src/RamenBitmask.ml \
	src/RamenAdvLock.ml \
	src/RamenOutRef.ml \
	src/RamenParsing.ml \
	src/RamenEthAddr.ml \
	src/RamenIpv4.ml \
	src/RamenIpv6.ml \
	src/RamenIp.ml \
	src/RamenEventTime.ml \
	src/RamenUnits.ml \
	src/RamenCollectd.ml \
	src/RamenNetflow.ml \
	src/RamenProtocols.ml \
	src/RamenTypeConverters.ml \
	src/RamenTypes.ml \
	src/RamenTuple.ml \
	src/RamenName.mli \
	src/RamenName.ml \
	src/RamenLang.ml \
	src/RingBuf.ml \
	src/RingBufLib.ml \
	src/RamenBinocle.ml \
	src/RamenNotification.ml \
	src/RamenExpr.ml \
	src/RamenConf.ml \
	src/RamenOperation.ml \
	src/RamenSerialization.ml \
	src/RamenProgram.ml \
	src/RamenWatchdog.ml \
	src/RamenExport.ml \
	src/HeavyHitters.ml \
	src/RamenTimeseries.ml \
	src/RamenHttpHelpers.ml \
	src/RamenProcesses.ml \
	src/RamenGc.ml \
	src/Globs.ml \
	src/RamenCompilConfig.ml \
	src/RamenDepLibs.ml \
	src/RamenOCamlCompiler.ml \
	src/CodeGen_OCaml.ml \
	src/RamenTypingHelpers.ml \
	src/RamenSmtParser.ml \
	src/RamenSmtErrors.ml \
	src/RamenSmtTyping.ml \
	src/RamenCompiler.ml \
	src/RamenRun.ml \
	src/RamenGraphite.ml \
	src/RamenApi.ml \
	src/RamenHeap.ml \
	src/SqliteHelpers.ml \
	src/RamenNotifier.ml \
	src/TermTable.ml \
	src/RamenCliCmd.ml \
	src/RingBufCmd.ml \
	src/RamenCompletion.ml \
	src/RamenTests.ml \
	src/ramen.ml

CODEGENLIB_SOURCES = \
	src/RamenConsts.ml \
	src/RamenLog.ml \
	src/RamenNullable.ml \
	src/RamenHelpers.ml \
	src/Globs.ml \
	src/RamenAdvLock.ml \
	src/RamenWatchdog.ml \
	src/RamenOutRef.ml \
	src/RamenParsing.ml \
	src/RamenEthAddr.ml \
	src/RamenIpv4.ml \
	src/RamenIpv6.ml \
	src/RamenIp.ml \
	src/RamenEventTime.ml \
	src/RamenUnits.ml \
	src/RamenCollectd.ml \
	src/RamenNetflow.ml \
	src/RingBuf.ml \
	src/RamenTypeConverters.ml \
	src/RingBufLib.ml \
	src/RamenBinocle.ml \
	src/RamenNotification.ml \
	src/RamenBloomFilter.ml \
	src/RamenSampling.ml \
	src/RamenFileNotify.ml \
	src/CodeGenLib_IO.ml \
	src/CodeGenLib_State.ml \
	src/CodeGenLib_Casing.ml \
	src/RamenHeap.ml \
	src/RamenSortBuf.ml \
	src/CodeGenLib_Skeletons.ml \
	src/HeavyHitters.ml \
	src/CodeGenLib.ml

LIBRINGBUF_SOURCES = \
	src/ringbuf/ringbuf.h \
	src/ringbuf/ringbuf.c \
	src/ringbuf/wrappers.c

LIBCOLLECTD_SOURCES = \
	src/collectd/collectd.h \
	src/collectd/collectd.c \
	src/collectd/wrappers.c

LIBNETFLOW_SOURCES = \
	src/netflow/v5.c

TESTONLY_SOURCES = \
	src/TestHelpers.ml

SOURCES = \
	$(RAMEN_SOURCES) $(CODEGENLIB_SOURCES) \
	$(LIBRINGBUF_SOURCES) $(LIBRINGBUF_OCAML_SOURCES) \
	$(LIBCOLLECTD_SOURCES) $(LIBNETFLOW_SOURCES) \
	$(TESTONLY_SOURCES) \
	src/ringbuf_test.ml

dep:
	@$(RM) .depend
	@$(MAKE) .depend

.depend: $(SOURCES)
	@$(OCAMLDEP) -native -I src -package "$(PACKAGES)" $(filter %.ml, $(SOURCES)) $(filter %.mli, $(SOURCES)) > $@
	@for f in $(filter %.c, $(SOURCES)); do \
		$(CC) $(CPPFLAGS) -MM -MT "$$(dirname $$f)/$$(basename $$f .c).o" $$f >> $@; \
	done

include .depend

# Compile Ramen

src/RamenFileNotify.ml: $(FILE_NOTIFIER)
	@echo 'Using implementation $(FILE_NOTIFIER) for new file notifications'
	@ln -sf $(notdir $<) $@
	@touch $@

src/libringbuf.a: src/ringbuf/ringbuf.o src/ringbuf/wrappers.o
	@echo 'Building ringbuf library'
	@sleep 1 # ar truncate mtime !?
	@$(AR) rs $@ $^ >/dev/null

src/libcollectd.a: src/collectd/collectd.o src/collectd/wrappers.o
	@echo 'Building collectd library'
	@sleep 1 # ar truncate mtime !?
	@$(AR) rs $@ $^ >/dev/null

src/libnetflow.a: src/netflow/v5.o
	@echo 'Building netflow library'
	@sleep 1 # ar truncate mtime !?
	@$(AR) rs $@ $^ >/dev/null

# We have to force -cclib -lstdint_stubs right after -cclib wrap_ringbuf.o
# otherwise -package stdint would put it before and gcc would not include the
# symbols we need as we are the only users.
MOREFLAGS = \
	-package "$(PACKAGES)" \
	-cclib -lstdint_stubs \
	-cclib -lringbuf \
	-cclib -lcollectd \
	-cclib -lnetflow

src/ramen: \
	$(patsubst %.mli,%.cmi,$(filter %.mli, $(RAMEN_SOURCES))) \
	$(patsubst %.ml,%.cmx,$(filter %.ml, $(RAMEN_SOURCES))) \
	src/libringbuf.a src/libcollectd.a \
	src/libnetflow.a
	@echo 'Linking $@'
	@$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) $(filter %.cmx, $^) -o $@

src/codegen.cmxa: $(CODEGENLIB_SOURCES:.ml=.cmx) src/libringbuf.a src/libcollectd.a src/libnetflow.a
	@echo 'Linking runtime library $@'
	@$(OCAMLOPT) $(OCAMLOPTFLAGS) -a $(MOREFLAGS) $(filter %.cmx, $^) -o $@

# embedded compiler version: build a bundle of all libraries
OCAML_WHERE = $(shell dirname $(shell ocamlfind ocamlc -where))
# cut -c X- means to takes all from X, 1st char being 1, thus the leading _:
OCAML_WHERE_LEN = $(shell printf '_%s/' $(OCAML_WHERE) | wc -m | xargs echo)

BUNDLE_DIR = bundle

bundle: bundle/date

bundle/date: src/codegen.cmxa
	@echo 'Bundling libs together into $(BUNDLE_DIR)'
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
	@set -e ; for d in $$(ocamlfind query -recursive -predicates native -format '%d' ramen | sort -u | grep -v -e compiler-libs -e ppx_tools -e /findlib) ; do \
		echo $$d | cut -c $(OCAML_WHERE_LEN)- | \
		sed -e 's,^\(.*\)$$,  "\1" ;,' >> $@ ;\
	done ;
# Equivalent to -threads:
	@echo '  "ocaml/threads" ;' >> $@
	@echo ']' >> $@
	@echo 'let objfiles = [' >> $@
	@set -e ; for d in $$(ocamlfind query -recursive -predicates native -format '%+a' ramen | uniq | grep -v -e compiler-libs -e ppx_tools -e /findlib) ; do \
		echo $$d | cut -c $(OCAML_WHERE_LEN)- | \
		sed -e 's,^\(.*\)$$,  "\1" ;,' >> $@ ;\
	done ;
	@echo ']' >> $@
# Equivalent to -threads, must come right after unix.cmxa:
	@sed -i -e '/ocaml\/unix.cmxa/a \ \ "ocaml/threads/threads.cmxa" ;' $@

# At the contrary, this one has to be generated at every build:
src/RamenCompilConfig.ml:
	@echo '(* Generated by Makefile - edition is futile *)' > $@
	@echo 'let default_bundle_dir = "$(lib_dir)/$(BUNDLE_DIR)/"' >> $@

# Tests

TESTABLE_SOURCES = \
	src/RamenParsing.ml \
	src/RamenEthAddr.ml \
	src/RamenIpv4.ml \
	src/RamenIpv6.ml \
	src/RamenIp.ml \
	src/RamenTypes.ml \
	src/RamenExpr.ml \
	src/RamenOperation.ml \
	src/RamenProgram.ml \
	src/RamenTypingHelpers.ml \
	src/HeavyHitters.ml \
	src/RamenHelpers.ml \
	src/RamenBloomFilter.ml \
	src/Globs.ml \
	src/CodeGen_OCaml.ml \
	src/RamenSortBuf.ml \
	src/RamenGraphite.ml \
	src/RingBufLib.ml \
	src/RamenSerialization.ml \
	src/RamenSmtParser.ml \
	src/RamenUnits.ml

# For the actual command line building all_tests.opt:
LINKED_FOR_TESTS = \
	src/RamenVersions.ml \
	src/RamenLog.ml \
	src/RamenConsts.ml \
	src/RamenHelpers.ml \
	src/HeavyHitters.ml \
	src/RamenExperiments.ml \
	src/RamenAdvLock.ml \
	src/RamenOutRef.ml \
	src/RamenParsing.ml \
	src/RamenEthAddr.ml \
	src/RamenIpv4.ml \
	src/RamenIpv6.ml \
	src/RamenIp.ml \
	src/RamenEventTime.ml \
	src/RamenUnits.ml \
	src/RamenCollectd.ml \
	src/RamenNetflow.ml \
	src/RamenProtocols.ml \
	src/RamenTypeConverters.ml \
	src/RamenTypes.ml \
	src/RamenTuple.ml \
	src/RamenName.mli \
	src/RamenName.ml \
	src/RamenLang.ml \
	src/RamenExpr.ml \
	src/RamenWatchdog.ml \
	src/RingBuf.ml \
	src/RingBufLib.ml \
	src/RamenBinocle.ml \
	src/RamenNotification.ml \
	src/RamenConf.ml \
	src/RamenOperation.ml \
	src/RamenProgram.ml \
	src/Globs.ml \
	src/RamenCompilConfig.ml \
	src/RamenDepLibs.ml \
	src/RamenOCamlCompiler.ml \
	src/RamenHeap.ml \
	src/RamenSortBuf.ml \
	src/CodeGen_OCaml.ml \
	src/RamenBitmask.ml \
	src/RamenSerialization.ml \
	src/RamenExport.ml \
	src/HeavyHitters.ml \
	src/RamenTimeseries.ml \
	src/RamenHttpHelpers.ml \
	src/RamenProcesses.ml \
	src/RamenTypingHelpers.ml \
	src/RamenBloomFilter.ml \
	src/RamenGraphite.ml \
	src/TestHelpers.ml \
	src/RamenSmtParser.ml

src/all_tests.ml: $(TESTABLE_SOURCES)
	@echo 'Generating unit tests into $@'
	@$(QTEST) --shuffle -o $@ extract $^

all_tests.opt: \
	src/libringbuf.a src/libcollectd.a src/libnetflow.a \
	$(LINKED_FOR_TESTS:.ml=.cmx) src/all_tests.ml
	@echo 'Building unit tests into $@'
	@$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) -package qcheck $(filter %.cmx, $^) $(filter %.a, $^) $(filter %.ml, $^) -o $@

ringbuf_test.opt: \
	src/RamenLog.cmx src/RamenConsts.cmx src/RamenHelpers.cmx \
	src/RamenAdvLock.cmx \
	src/RamenOutRef.cmx src/RamenParsing.cmx \
	src/RamenEthAddr.cmx src/RamenIpv4.cmx src/RamenIpv6.cmx src/RamenIp.cmx \
	src/RamenTypeConverters.cmx src/RamenTypes.cmx \
	src/RamenUnits.cmx src/RamenTuple.cmx src/RingBuf.cmx src/RingBufLib.cmx \
	src/ringbuf_test.cmx src/libringbuf.a src/libcollectd.a src/libnetflow.a
	@echo 'Building ringbuf tests into $@'
	@$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg $(MOREFLAGS) $(filter %.cmx, $^) -o $@

check: unit-check cli-check func-check err-check

unit-check: all_tests.opt ringbuf_test.opt
	@echo 'Running unit tests...'
	@OCAMLRUNPARAM=b ./all_tests.opt -bt
	@./ringbuf_test.opt

cli-check:
	@echo 'Running CLI tests...'
	@cd tests && cucumber

RAMEN_TESTS = $(wildcard tests/*.test)

RAMEN_TESTS_SOURCES = \
	$(RAMEN_TESTS:.test=.ramen) \
	tests/fixtures/n123.ramen

%.x: %.ramen src/ramen src/codegen.cmxa
	@echo 'Compiling ramen program $@'
	@src/ramen compile --bundle=bundle --root=./ $<

%.success: %.test src/ramen
	@echo 'Running test $(basename $< .test)'
	@$(RM) $@
	@src/ramen test --root=./ $< && touch $@

# One day we will have `ramen makedep`:
tests/lag.x: tests/fixtures/n123.x
tests/count_lines.x: tests/fixtures/n123.x
tests/params.x: tests/fixtures/earthquakes.x
tests/sort.x: tests/fixtures/earthquakes.x
tests/fit_multi.x: tests/fixtures/cars.x
tests/top_expr.x: tests/fixtures/cars.x
tests/season.x: tests/fixtures/earthquakes.x
tests/previous_and_null.x: tests/fixtures/n123.x
tests/fun_with_funcs.x: tests/fixtures/cars.x
tests/case.x: tests/fixtures/n123.x
tests/commit_before.x: tests/fixtures/n123.x tests/fixtures/cars.x
tests/basic_aggr.x: tests/fixtures/n123.x tests/fixtures/cars.x
tests/tuples.x: tests/fixtures/n123.x
tests/port_scan.x: tests/fixtures/port_scan.x
tests/ip.x: tests/fixtures/mixture.x
tests/histogram.x: tests/fixtures/cars.x
tests/last.x: tests/fixtures/earthquakes.x tests/fixtures/cars.x
tests/sample.x: tests/fixtures/n123.x
tests/lag.success: tests/lag.x tests/fixtures/n123.x
tests/count_lines.success: tests/count_lines.x tests/fixtures/n123.x
tests/params.success: tests/params.x tests/fixtures/earthquakes.x
tests/sort.success: tests/sort.x tests/fixtures/earthquakes.x
tests/fit_multi.success: tests/fit_multi.x tests/fixtures/cars.x
tests/top_expr.success: tests/top_expr.x tests/fixtures/cars.x
tests/season.success: tests/season.x tests/fixtures/earthquakes.x
tests/merge.success: tests/merge.x
tests/from.success: tests/from.x
tests/previous_and_null.success: tests/previous_and_null.x tests/fixtures/n123.x
tests/fun_with_funcs.success: tests/fun_with_funcs.x tests/fixtures/cars.x
tests/case.success: tests/case.x tests/fixtures/n123.x
tests/commit_before.success: tests/commit_before.x tests/fixtures/n123.x tests/fixtures/cars.x
tests/word_count.success: tests/word_count.x
tests/word_split.success: tests/word_split.x
tests/basic_aggr.success: tests/basic_aggr.x tests/fixtures/n123.x tests/fixtures/cars.x
tests/tuples.success: tests/tuples.x tests/fixtures/n123.x
tests/port_scan.success: tests/port_scan.x tests/fixtures/port_scan.x
tests/ip.success: tests/ip.x tests/fixtures/mixture.x
tests/min_max.success: tests/min_max.x
tests/nulls.success: tests/nulls.x
tests/constructed_types.success: tests/constructed_types.x
tests/histogram.success: tests/histogram.x tests/fixtures/cars.x
tests/strings.success: tests/strings.x
tests/in.success: tests/in.x
tests/time.success: tests/time.x
tests/last.success: tests/last.x tests/fixtures/earthquakes.x tests/fixtures/cars.x
tests/event_time.success: tests/event_time.x
tests/sample.success: tests/sample.x tests/fixtures/n123.x
tests/stress.success: tests/stress.x

func-check: $(RAMEN_TESTS:.test=.success)

err-check: $(wildcard tests/errors/*.ramen) $(wildcard tests/errors/*.err)
	@echo 'Running parsing/typing error tests'
	@for f in $(wildcard tests/errors/*.ramen); do \
		t=$$(mktemp) ;\
		src/ramen compile --quiet $$f 2>&1 >/dev/null | sed 1d > $$t ;\
		if ! diff -q $$f.err $$t ; then \
			echo "FAILURE: $$f" ;\
			exit 1 ;\
		fi ;\
		$(RM) $$t ;\
	done

# Documentation

doc: \
	docs/tutorial.html docs/manual.html docs/roadmap.html

docs/tutorial.html: \
	docs/tutorial_group_by.svg docs/sample_chart1.svg

# Installation

install: $(INSTALLED)
	@ocamlfind install ramen $(INSTALLED_LIB)
	@echo 'Installing binaries into $(prefix)$(bin_dir)'
	@install -d $(prefix)$(bin_dir)
	@install $(INSTALLED_BIN) $(prefix)$(bin_dir)/
	@for f in $(INSTALLED_BIN); do strip $(prefix)$(bin_dir)/$$(basename $$f); done

# We need ocamlfind to find ramen package before we can install the lib bundle.
# Therefore we simply build it as a separate, optional step (that's only
# required if you intend to use the embedded compiler).
install-bundle: bundle
	@echo 'Installing libraries bundle into $(DESTDIR)$(lib_dir)'
	@install -d $(DESTDIR)$(lib_dir)
	@cp -r $(BUNDLE_DIR) $(DESTDIR)$(lib_dir)/

install-examples:
	@echo 'Installing examples into $(DESTDIR)$(sample_dir)'
	@install -d $(DESTDIR)$(lib_dir)
	@cd examples/programs && \
	 find . -type f -name '*.ramen' \
	   -exec sh -c 'mkdir -p $(DESTDIR)$(sample_dir)/$$(dirname {})' \; \
	   -exec sh -c 'cp {} $(DESTDIR)$(sample_dir)/$$(dirname {})' \;

uninstall:
	@ocamlfind remove ramen
	@echo 'Uninstalling binaries and libraries bundle'
	@$(RM) $(prefix)$(bin_dir)/ramen

uninstall-bundle:
	@echo 'Uninstalling libraries bundle'
	@$(RM) -r $(DESTDIR)$(lib_dir)/$(BUNDLE_DIR)

reinstall: uninstall install

# Debian package

deb: ramen.$(VERSION).deb

tarball: ramen.$(VERSION).tgz

ramen.$(VERSION).deb: $(INSTALLED) bundle/date debian.control
	@echo 'Building debian package $@'
	@sudo rm -rf debtmp
	@install -d debtmp/usr/bin
	@install $(INSTALLED_BIN) debtmp/usr/bin
	@for f in $(INSTALLED_BIN); do strip debtmp/usr/bin/$$(basename $$f); done
	@$(MAKE) DESTDIR=$(PWD)/debtmp/ install-examples
	@$(MAKE) DESTDIR=$(PWD)/debtmp/ install-bundle
	@mkdir -p debtmp/DEBIAN
	@cp debian.control debtmp/DEBIAN/control
	@chmod -R a+x debtmp/usr
	@sudo chown root: -R debtmp/usr
	@dpkg --build debtmp
	@mv debtmp.deb $@

ramen.$(VERSION).tgz: $(INSTALLED_BIN) bundle/date
	@echo 'Building tarball $@'
	@$(RM) -r tmp/ramen
	@install -d tmp/ramen
	@install $(INSTALLED_BIN) tmp/ramen/
	@install /usr/bin/z3 tmp/ramen/
	@for f in $(INSTALLED_BIN) z3; do strip tmp/ramen/$$(basename $$f); done
	@chmod -R a+x tmp/ramen/*
	@$(MAKE) DESTDIR=$(PWD)/tmp/ramen/ lib_dir=/ sample_dir=/examples install-examples
	@$(MAKE) DESTDIR=$(PWD)/tmp/ramen/ lib_dir=/ sample_dir=/examples install-bundle
	@tar c -C tmp ramen | gzip > $@

# Docker images

docker-latest: docker/Dockerfile docker/ramen.$(VERSION).deb
	@echo 'Building docker image for testing DEB version'
	@docker build -t rixed/ramen:latest --squash -f $< docker/

docker-build-builder: docker/Dockerfile-builder
	@for distrib in jessie stretch ; do \
		echo 'Building docker image for building the DEB packages for $$distrib' ;\
		echo 'FROM debian:'$$distrib > docker/Dockerfile-builder.$$distrib ;\
		cat docker/Dockerfile-builder >> docker/Dockerfile-builder.$$distrib ;\
		docker build -t rixed/ramen-builder:$$distrib \
		             -f docker/Dockerfile-builder.$$distrib docker/ ;\
		$(RM) docker/Dockerfile-builder.$$distrib ;\
	done

docker-build-image: docker-latest
	@echo 'Tagging latest docker image to v$(VERSION)'
	@docker tag rixed/ramen:latest rixed/ramen:v$(VERSION)

docker-circleci: docker/Dockerfile-circleci
	@echo 'Building docker image for CircleCi'
	@docker build -t rixed/ramen-circleci -f $< docker/

docker-push:
	@echo 'Uploading docker images'
	@docker push rixed/ramen:latest
	@docker push rixed/ramen:v$(VERSION)
	@docker push rixed/ramen-builder:jessie
	@docker push rixed/ramen-builder:stretch
	@docker push rixed/ramen-circleci

# Cleaning

clean-temp:
	@echo 'Cleaning ramen compiler temp files'
	@for d in tests examples ; do\
	   find $$d -\( -name '*.ml' -o -name '*.cmx' -o -name '*.o' \
	                -o -name '*.annot' -o -name '*.x'  -o -name '*.s' \
	                -o -name '*.cmi' -\) -delete ;\
	 done

clean: clean-temp
	@echo 'Cleaning all build files'
	@$(RM) src/*.s src/*.annot src/*.o
	@$(RM) *.opt src/all_tests.* perf.data* gmon.out
	@$(RM) src/ringbuf/*.o
	@$(RM) src/*.cmx src/*.cmxa src/*.cmxs src/*.cmi
	@$(RM) src/oUnit-anon.cache src/qtest.targets.log
	@$(RM) .depend src/*.opt src/*.byte
	@$(RM) doc/tutorial.html doc/manual.html doc/roadmap.html
	@$(RM) src/ramen src/codegen.cmxa src/RamenFileNotify.ml src/libringbuf.a
	@$(RM) src/libcollectd.a src/libnetflow.a src/codegen.a
	@find tests -name '*.success' -delete
	@$(RM) -r $(BUNDLE_DIR)
	@sudo rm -rf debtmp
	@$(RM) -r tmp
	@$(RM) ramen.*.deb ramen.*.tgz
