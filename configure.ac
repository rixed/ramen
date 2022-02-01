AC_INIT(ramen, 6.16.4)
m4_include([m4/ocaml.m4])
m4_include([m4/z3.m4])

AC_ARG_VAR(OCAMLOPTFLAGS, [Additional flags to pass to the ocaml compiler])
AC_ARG_VAR(OCAMLCFLAGS, [Additional flags to pass to the ocaml bytecode compiler])

AC_PROG_CC
AC_PROG_CXX
AC_CONFIG_HEADERS([src/config.h])

AC_CHECK_HEADERS([execinfo.h])
AC_CHECK_FUNCS([fdatasync renamex_np renameat2])

AC_PROG_OCAML
AS_IF(
  [test "$OCAMLC" = no],
  AC_MSG_ERROR([You must install the OCaml compiler.]))

AC_PROG_FINDLIB
AS_IF(
  [test "$OCAMLFIND" = no],
  AC_MSG_ERROR([You must install OCaml findlib (the ocamlfind command).]))

AC_CHECK_OCAML_PKG(batteries)
AC_CHECK_OCAML_PKG(binocle)
AC_CHECK_OCAML_PKG(cmdliner)
AC_CHECK_OCAML_PKG(csv)
AC_CHECK_OCAML_PKG(dessser)
AC_CHECK_OCAML_PKG(ipaddr)
AC_CHECK_OCAML_PKG(kafka)
AC_CHECK_OCAML_PKG(lacaml)
AC_CHECK_OCAML_PKG(lmdb)
AC_CHECK_OCAML_PKG(net_codecs)
AC_CHECK_OCAML_PKG(num)
AC_CHECK_OCAML_PKG(parsercombinator)
AC_CHECK_OCAML_PKG(pfds)
AC_CHECK_OCAML_PKG(ppp)
AC_CHECK_OCAML_PKG(qcheck)
AC_CHECK_OCAML_PKG(qtest)
AC_CHECK_OCAML_PKG(smt2)
AC_CHECK_OCAML_PKG(sodium)
AC_CHECK_OCAML_PKG(sqlite3)
AC_CHECK_OCAML_PKG(stdint)
AC_CHECK_OCAML_PKG(syslog)
AC_CHECK_OCAML_PKG(uuidm)

AC_CHECK_Z3

AC_LANG([C++])
CXXFLAGS="$CFLAGS -std=c++17"
AC_MSG_CHECKING([local orc is installed.])
LIBS="$LIBS -lorc -lprotobuf -lsnappy -lz -llz4 -lzstd -pthread"
AC_LINK_IFELSE(
  [AC_LANG_PROGRAM([#include <orc/OrcFile.hh>],
    [[auto t = orc::Type::buildTypeFromString("test");]],
    [[return 0;]])],
  [AC_MSG_RESULT([Will use local orc.]) && HAVE_ORC=1],
  [AC_MSG_RESULT([Local orc not found. Will compile orc.])])
AC_SUBST([HAVE_ORC])

AC_CONFIG_FILES([
  Makefile
  opam
  src/RamenVersions.ml
  debian.control
  tests/features/api.feature
  docker/Dockerfile
  docker/Dockerfile-dev
])
AC_OUTPUT
