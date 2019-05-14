AC_INIT(ramen, 3.4.0)
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

AC_CHECK_OCAML_PKG(batteries)
AC_CHECK_OCAML_PKG(stdint)
AC_CHECK_OCAML_PKG(parsercombinator)
AC_CHECK_OCAML_PKG(binocle)
AC_CHECK_OCAML_PKG(lacaml)
AC_CHECK_OCAML_PKG(num)
AC_CHECK_OCAML_PKG(ppp)
AC_CHECK_OCAML_PKG(cmdliner)
AC_CHECK_OCAML_PKG(syslog)
AC_CHECK_OCAML_PKG(sqlite3)
AC_CHECK_OCAML_PKG(net_codecs)
AC_CHECK_OCAML_PKG(zmq)

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
  rmadmin/rmadmin.pro
])
AC_OUTPUT
