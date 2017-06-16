top_srcdir = ..

LINKED_WITH_TESTS = \
	Log.ml Lang.ml RingBufLib.ml CodeGen_OCaml.ml RamenConf.ml

TESTABLE_SOURCES = \
	Lang.ml HttpSrv.ml

RIGATONI_SOURCES = \
	Log.ml Lang.ml \
	RingBufLib.ml \
	CodeGen_OCaml.ml \
	RamenConf.ml HttpSrv.ml \
	rigatoni.ml

CODEGENLIB_SOURCES = \
	Log.ml RingBuf.ml RingBufLib.ml CodeGenLib_IO.ml CodeGenLib.ml

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

# We have to foce -cclib -lstdint_stubs right after -cclib wrap_ringbuf.o otherwise -package stdint would
# put it before and gcc would not include the symbols we need as we are the only users.
rigatoni: $(RIGATONI_SOURCES:.ml=.cmx) ringbuf/libringbuf.a wrap_ringbuf.o
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg -package "$(PACKAGES)" -I ringbuf -cclib wrap_ringbuf.o -cclib -lstdint_stubs -cclib -Lringbuf -cclib -lringbuf $(filter %.cmx, $^) -o $@

codegen.cmxa: $(CODEGENLIB_SOURCES:.ml=.cmx) ringbuf/libringbuf.a wrap_ringbuf.o
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -a       -package "$(PACKAGES)" -I ringbuf -cclib wrap_ringbuf.o -cclib -lstdint_stubs -cclib -Lringbuf -cclib -lringbuf $(filter %.cmx, $^) -o $@

ringbuf_test.opt: RingBuf.cmx RingBufLib.cmx ringbuf_test.cmx ringbuf/libringbuf.a wrap_ringbuf.o
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg -package "$(PACKAGES)" -I ringbuf -cclib wrap_ringbuf.o -cclib -lstdint_stubs -cclib -Lringbuf -cclib -lringbuf $(filter %.cmx, $^) -o $@

ringbuf_ctl: RingBuf.cmx ringbuf_ctl.cmx ringbuf/libringbuf.a wrap_ringbuf.o
	$(OCAMLOPT) $(OCAMLOPTFLAGS) -linkpkg -package "$(PACKAGES)" -I ringbuf -cclib wrap_ringbuf.o -cclib -lstdint_stubs -cclib -Lringbuf -cclib -lringbuf $(filter %.cmx, $^) -o $@

clean-spec:
	$(RM) ringbuf/libringbuf.a

distclean-spec:
	$(RM) rigatoni codegen.cmxa

include $(top_srcdir)/make.common

# Dependencies

include .depend
