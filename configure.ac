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
# We will need to find the given liborc at runtime, so just ask for where it is:
AC_MSG_CHECKING([for liborc])
AC_ARG_WITH([liborc],
  AS_HELP_STRING([--with-liborc=path], [Where liborc is located]))
AS_IF([test "x$with_liborc" != "xno"], [
  AS_IF([test -e "$with_liborc/liborc.a"], [
    RUNTIME_LIBDIRS="$with_liborc"
    LDFLAGS="$LDFLAGS -L$with_liborc"
    AC_MSG_RESULT([found in $with_liborc])
  ], [
    AC_MSG_ERROR([Cannot find liborc.a in $with_liborc])
  ])
], [
  # No location given: just see if we can link
  SAVED_LDFLAGS="$LDFLAGS"
  LDFLAGS="$LDFLAGS -lorc"
  AC_TRY_LINK([#include <orc/Common.hh>], [orc::CompressionKind dummy],
    [TEST_LIBS="$TEST_LIBS -lorc"]
    AC_MSG_RESULT([found])
    ,
    [AC_MSG_ERROR([Not found!])])
  LDFLAGS="$SAVED_LDFLAGS"
])

# Similarly, we could be given the path of other libs (sasl2, openssl...),
# that are required at runtime:
AC_ARG_WITH([runtime-libdirs],
  AS_HELP_STRING([--with-runtime-libdirs=path1,path2,...],
                 [Additional libdirs to use at runtime]))
AS_IF([test "x$with_runtime_libdirs" != "xno"], [
  RUNTIME_LIBDIRS="$RUNTIME_LIBDIRS,$with_runtime_libdirs"
])
AC_SUBST([RUNTIME_LIBDIRS])

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
