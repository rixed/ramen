top_srcdir = ..

TESTABLE_SOURCES = \
	EventTypes.ml conf_of_sqlite.ml

SOURCES = \
	$(TESTABLE_SOURCES) \
	conf_of_sqlite.ml \
	alerting_conf.ml

PACKAGES = sqlite3 ramen

INSTALLED = \
	*.cmxs \
	$(LIB_SOURCES:.ml=.cmx) $(LIB_SOURCES:.ml=.cmi)

all: .depend alerting_conf.cmxs

alerting_conf.cmx: ../ramen/ramen.cmxa ../ramen/rigati

alerting_conf.cmxs: EventTypes.cmx conf_of_sqlite.cmx alerting_conf.cmx
	@# Do not ever add "ramen" in the -package here or fear the wrath of dynlink symbol resolution bug!
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg -package sqlite3 -o $@ -shared EventTypes.cmx conf_of_sqlite.cmx alerting_conf.cmx

alerting_conf.cma: EventTypes.cmo alerting_conf.cmo $(LIB_NAME).cma
	$(OCAMLC) $(OCAMLFLAGS) -a -linkpkg -package sqlite3 $(LIB_NAME).cma EventTypes.cmo alerting_conf.cmo -o $@

clean-spec:

distclean-spec:

include $(top_srcdir)/make.common

# Dependencies

include .depend
