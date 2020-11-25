AC_DEFUN([AC_CHECK_Z3],
[dnl
  # The given location is the one we are going to copy z3 from and this
  # location will be recorded in ramen to be the default location.
  # When building the docker image it is merely copied into /usr/bin/z3 and
  # the RAMEN_SMT_SOLVER envvar set to that.
  AC_ARG_WITH([z3],
    [AS_HELP_STRING([--with-z3],
      [location of the z3 solver @<:@default=check@:>@])],
    [],
    [with_z3=no])

  # Check either the given location or the PATH:
  AS_IF([test "$with_z3" != no],
    [AS_IF([test -x "$with_z3"],
      [Z3="$with_z3"],
      [AC_MSG_ERROR([$with_z3 is not executable])])],
    [AC_PATH_PROG(Z3, z3)])

  # Check z3 path
  AS_IF([test -z "$Z3"],
    [AC_MSG_ERROR([Cannot find z3 in the path.])])

  # Check z3 version
  Z3VERSION=$($Z3 --version | sed -n -e 's+^.*\([[0-9]].[[0-9]].[[0-9]]\).*$+\1+p')
  AC_SUBST([Z3VERSION])

  Z3_MIN_VERSION="4.6.0"
  AS_VERSION_COMPARE([$Z3_MIN_VERSION],[$Z3VERSION],
    AC_MSG_NOTICE([z3 version $Z3VERSION is newer than the minimun required $Z3_MIN_VERSION]),
    AC_MSG_NOTICE([z3 version $Z3VERSION is the minimun required $Z3_MIN_VERSION]),
    AC_MSG_ERROR([Bad z3 version. Minimun version is $Z3_MIN_VERSION]))

  # For rmadmin:
  # Although ramen uses Z3's C++ API, we are lucky that the same library file contains
  # the C API, much easier to check:
  AC_CHECK_LIB(z3, Z3_algebraic_is_value)
])
