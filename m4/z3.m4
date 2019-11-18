AC_DEFUN([AC_CHECK_Z3],
[dnl
  #check z3 path
  AC_PATH_PROG(Z3, z3)
  AS_IF(
    [test -z "$Z3"],
    [AC_MSG_ERROR([Cannot find z3 in the path.])])

  # check z3 version
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
