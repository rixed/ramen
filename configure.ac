AC_INIT(ramen, 3.0.40)
m4_include([m4/ocaml.m4])

AC_PROG_CC
AC_CONFIG_HEADERS([src/config.h])

AC_CHECK_FUNCS([fdatasync])

AC_PROG_OCAML
if test "$OCAMLC" = "no"; then
  AC_MSG_ERROR([You must install the OCaml compiler])
fi

AC_PROG_FINDLIB
if test "$OCAMLFIND" = "no"; then
  AC_MSG_ERROR([You must install OCaml findlib (the ocamlfind command)])
fi

AC_CHECK_OCAML_PKG(batteries stdint parsercombinator binocle lacaml num ppp cmdliner syslog sqlite3 net_codecs)

AC_CONFIG_FILES([
  Makefile
  META
  opam
  src/RamenVersions.ml
  tests/features/api.feature
  debian.control
  docker/Dockerfile-builder
  docker/Dockerfile
])
AC_OUTPUT
