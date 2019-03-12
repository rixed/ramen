AC_INIT(ramen, 3.3.0)
m4_include([m4/ocaml.m4])

AC_PROG_CC
AC_PROG_CXX
AC_CONFIG_HEADERS([src/config.h])

AC_CHECK_FUNCS([fdatasync renamex_np renameat2])

AC_PROG_OCAML
if test "$OCAMLC" = "no"; then
  AC_MSG_ERROR([You must install the OCaml compiler])
fi

AC_PROG_FINDLIB
if test "$OCAMLFIND" = "no"; then
  AC_MSG_ERROR([You must install OCaml findlib (the ocamlfind command)])
fi

AC_CHECK_OCAML_PKG(batteries stdint parsercombinator binocle lacaml num ppp cmdliner syslog sqlite3 net_codecs)

AC_LANG([C++])
CXXFLAGS="$CFLAGS -std=c++17"

AC_CONFIG_FILES([
  Makefile
  opam
  src/RamenVersions.ml
  tests/features/api.feature
  debian.control
  docker/Dockerfile-builder
  docker/Dockerfile
])
AC_OUTPUT
