NAME = ramen

top_srcdir = ..

LIB_SOURCES = \
	Globs.ml Alarm.ml IO.ml \
	Configuration.ml Engine.ml Model.ml \
	ReifyEngine.ml ExecuteEngine.ml

TESTABLE_SOURCES = \
	$(LIB_SOURCES)

SOURCES = \
	$(TESTABLE_SOURCES) \
	rigati.ml

PACKAGES = lwt.ppx batteries ppp cmdliner dynlink inotify.lwt
LIB_NAME = ramen

INSTALLED = \
	META $(LIB_NAME).cmxa $(LIB_NAME).a \
	$(LIB_SOURCES:.ml=.cmx) $(LIB_SOURCES:.ml=.cmi)

all: .depend $(LIB_NAME).cmxa rigati

$(LIB_NAME).cmxa: $(LIB_SOURCES:.ml=.cmx)
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -a -package "$(PACKAGES)" $^ -o $@

$(LIB_NAME).cma: $(LIB_SOURCES:.ml=.cmo)
	$(OCAMLC) $(OCAMLFLAGS) -a -linkpkg -package "$(PACKAGES)" -custom $^ -o $@

rigati: $(LIB_NAME).cmxa rigati.cmx
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg -package "$(PACKAGES)" $^ -o $@

rigati.byte: $(LIB_NAME).cma rigati.cmo
	$(OCAMLC) $(OCAMLFLAGS) -linkpkg -package "$(PACKAGES)" $^ -o $@

clean-spec:

distclean-spec:
	$(RM) rigati

include $(top_srcdir)/make.common

# Dependencies

include .depend
