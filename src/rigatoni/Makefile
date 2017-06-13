top_srcdir = ..

LINKED_WITH_TESTS = \
	Log.ml CodeGenLib_Misc.ml CodeGen_OCaml.ml RamenConf.ml

TESTABLE_SOURCES = \
	Lang.ml HttpSrv.ml

RIGATONI_SOURCES = \
	Log.ml Lang.ml \
	CodeGenLib_Misc.ml CodeGen_OCaml.ml \
	RamenConf.ml HttpSrv.ml \
	RingBuf.ml \
	rigatoni.ml

CODEGENLIB_SOURCES = \
	Log.ml RingBuf.ml CodeGenLib_Misc.ml CodeGenLib_IO.ml CodeGenLib_RingBuf.ml CodeGenLib.ml

LIBRINGBUF_SOURCES = \
	ringbuf/ringbuf.c ringbuf/ringbuf.h

SOURCES = \
	$(RIGATONI_SOURCES) wrap_ringbuf.c $(CODEGENLIB_SOURCES) \
	$(LIBRINGBUF_SOURCES) $(LIBRINGBUF_OCAML_SOURCES) \

PACKAGES = ppp lwt.ppx batteries cmdliner stdint parsercombinator cohttp.lwt

INSTALLED = \
  rigatoni codegen.cmxa ringbuf/libringbuf.a

all: $(INSTALLED) ringbuf_test.opt ringbuf_ctl

ringbuf/libringbuf.a: ringbuf/ringbuf.o
	$(AR) rs $@ $^

rigatoni: $(RIGATONI_SOURCES:.ml=.cmx) ringbuf/libringbuf.a wrap_ringbuf.o
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg -package "$(PACKAGES)" -I ringbuf -cclib wrap_ringbuf.o -cclib -Lringbuf -cclib -lringbuf $(filter %.cmx, $^) -o $@

codegen.cmxa: $(CODEGENLIB_SOURCES:.ml=.cmx) ringbuf/libringbuf.a wrap_ringbuf.o
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -a       -package "$(PACKAGES)" -I ringbuf -cclib wrap_ringbuf.o -cclib -Lringbuf -cclib -lringbuf $(filter %.cmx, $^) -o $@

ringbuf_test.opt: RingBuf.cmx CodeGenLib_Misc.cmx ringbuf_test.cmx ringbuf/libringbuf.a wrap_ringbuf.o
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg -package "$(PACKAGES)" -I ringbuf -cclib wrap_ringbuf.o -cclib -Lringbuf -cclib -lringbuf $(filter %.cmx, $^) -o $@

ringbuf_ctl: RingBuf.cmx ringbuf_ctl.cmx ringbuf/libringbuf.a wrap_ringbuf.o
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg -package "$(PACKAGES)" -I ringbuf -cclib wrap_ringbuf.o -cclib -Lringbuf -cclib -lringbuf $(filter %.cmx, $^) -o $@

clean-spec:
	$(RM) ringbuf/libringbuf.a

distclean-spec:
	$(RM) rigatoni codegen.cmxa

include $(top_srcdir)/make.common

# Dependencies

include .depend
