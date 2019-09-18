// vim: ft=c bs=2 ts=2 sts=2 sw=2 expandtab
#ifndef MISCMACS_H_20190918_H
#define MISCMACS_H_20190918_H
#include "../config.h"

#define NUM_ITEMS(array) (sizeof array / sizeof array[0])

#if defined(HAVE_EXECINFO_H)
# include <execinfo.h>
# define DUMP_BACKTRACE() do {\
    void *frames[8]; \
    size_t size = backtrace(frames, NUM_ITEMS(frames)); \
    char **strings = backtrace_symbols(frames, size); \
    for (size_t i = 0; i < size; i++) \
      printf ("%s\n", strings[i]); \
    free(strings); \
  } while (0)
#else
# define DUMP_BACKTRACE() do {} while (0)
#endif

#endif
