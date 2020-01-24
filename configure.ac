AC_INIT(ramen, 4.3.22)
m4_include([m4/ocaml.m4])

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
AC_CHECK_OCAML_PKG(stdint)
AC_CHECK_OCAML_PKG(parsercombinator)
AC_CHECK_OCAML_PKG(binocle)
AC_CHECK_OCAML_PKG(lacaml)
AC_CHECK_OCAML_PKG(lmdb)
AC_CHECK_OCAML_PKG(num)
AC_CHECK_OCAML_PKG(ppp)
AC_CHECK_OCAML_PKG(cmdliner)
AC_CHECK_OCAML_PKG(sodium)
AC_CHECK_OCAML_PKG(syslog)
AC_CHECK_OCAML_PKG(sqlite3)
AC_CHECK_OCAML_PKG(net_codecs)
AC_CHECK_OCAML_PKG(zmq)
AC_CHECK_OCAML_PKG(kafka)
AC_CHECK_OCAML_PKG(pfds)
AC_CHECK_OCAML_PKG(dessser)

AC_PATH_PROG(Z3, z3)
AS_IF(
  [test -z "$Z3"],
  [AC_MSG_ERROR([Cannot find z3 in the path.])])

# For rmadmin:
# Although ramen uses Z3's C++ API, we are lucky that the same library file contains
# the C API, much easier to check:
AC_CHECK_LIB(z3, Z3_algebraic_is_value)
AC_CHECK_LIB(zmq, zmq_bind)

AC_PATH_PROG(QMAKE, qmake)
AS_IF(
  [test -z "$QMAKE"],
  [AC_MSG_WARN([Cannot find qmake, will not build rmadmin.])])
AC_PATH_PROG(CONVERT, convert)
AS_IF(
  [test -z "$CONVERT"],
  [AC_MSG_WARN([Cannot find convert, will not build rmadmin.])])

AC_LANG([C++])
CXXFLAGS="$CFLAGS -std=c++17"

AC_CONFIG_FILES([
  Makefile
  opam
  src/RamenVersions.ml
  tests/features/api.feature
  debian.control
  docker/Dockerfile-dev
  docker/Dockerfile
  rmadmin/rmadmin.pro
])
AC_OUTPUT
