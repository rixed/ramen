top_srcdir = .
OCAMLC     = ocamlfind ocamlc
OCAMLOPT   = ocamlfind ocamlopt
OCAMLDEP   = ocamlfind ocamldep
QTEST      = qtest
WARNS      = Ael-31-41-42-44-45-48
override OCAMLOPTFLAGS += $(INCS) -w $(WARNS) -g -annot -I $(top_srcdir) -O2
override OCAMLFLAGS    += $(INCS) -w $(WARNS) -g -annot -I $(top_srcdir)

LIB_SOURCES = \
	Alarm.ml IO.ml \
	Configuration.ml Engine.ml Model.ml \
	ReifyEngine.ml ExecuteEngine.ml

TESTABLE_SOURCES = \
	$(LIB_SOURCES) \
	EventTypes.ml

SOURCES = \
	$(TESTABLE_SOURCES) \
	alerting_conf.ml rigati.ml

# Common rules
.SUFFIXES: .ml .mli .cmo .cmi .cmx .cmxs .annot
.PHONY: clean distclean all check dep install uninstall reinstall

PACKAGES = lwt.ppx lwt.unix batteries unix ppp cmdliner dynlink inotify
LIB_NAME = ramen

all: .depend $(LIB_NAME).cmxa rigati alerting_conf.cmxs

%.cmo: %.ml
	$(OCAMLC) $(OCAMLFLAGS) -package "$(PACKAGES)" -c $<

%.cmx %.annot: %.ml
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -package "$(PACKAGES)" -c $<

%.cmxs: %.cmx $(LIB_NAME).cmxa
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -package "$(PACKAGES)" -o $@ -shared $<

$(LIB_NAME).cmxa: $(LIB_SOURCES:.ml=.cmx)
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -a -package "$(PACKAGES)" $^ -o $@

$(LIB_NAME).cma: $(LIB_SOURCES:.ml=.cmo)
	$(OCAMLC) $(OCAMLFLAGS) -a -linkpkg -package "$(PACKAGES)" -custom $^ -o $@

alerting_conf.cmxs: EventTypes.cmx alerting_conf.cmx $(LIB_NAME).cmxa
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg -package "$(PACKAGES)" -o $@ -shared EventTypes.cmx alerting_conf.cmx

rigati: $(LIB_NAME).cmxa rigati.cmx
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg -package "$(PACKAGES)" $^ -o $@

clean:
	$(RM) *.cm[iox] *.cmxs *.a *.s *.o .depend *.annot all_tests.* perf.data* gmon.out

distclean: clean
	$(RM) *.cma *.cmxa oUnit-anon.cache qtest.targets.log *.opt rigati

# Tests

all_tests.ml: $(TESTABLE_SOURCES)
	$(QTEST) --shuffle -o $@ extract $^

all_tests.opt: $(TESTABLE_SOURCES:.ml=.cmx) all_tests.ml
	$(OCAMLOPT) -o $@ $(SYNTAX) -package "$(PACKAGES)" -package qcheck -linkpkg $(OCAMLOPTFLAGS) $^

check: all_tests.opt
	@./all_tests.opt || echo "FAILURE"

# Installation

INSTALLED = \
	META $(LIB_NAME).cmxa $(LIB_NAME).a *.cmxs \
	$(LIB_SOURCES:.ml=.cmx) $(LIB_SOURCES:.ml=.cmi)

install: $(INSTALLED)
	ocamlfind install $(LIB_NAME) $^

uninstall:
	ocamlfind remove $(LIB_NAME)

reinstall: uninstall install

# Dependencies

dep:
	$(RM) .depend
	$(MAKE) .depend

.depend: $(SOURCES)
	$(OCAMLDEP) -package "$(PACKAGES)" $(filter %.ml, $(SOURCES)) $(filter %.mli, $(SOURCES)) > $@

-include .depend
