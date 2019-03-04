// vim: ft=c bs=2 ts=2 sts=2 sw=2 expandtab
/* The purpose of this small library is merely to move archive files into
 * the archive subdirectory, with a name depending on the time range
 * (with a trailing suffix to uniquify the files).
 * It tries to do so with no locks.
 * Later this could grow to allow more flexible behavior (such as uploading
 * on HDFS or some cloud). */
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <assert.h>
#include <limits.h>

#include "archive.h"
#include "../config.h"

#ifndef HAVE_RENAMEX_NP
# ifndef HAVE_RENAMEAT2
    // Assuming we are building on an older Linux,
    // let's try to call the syscall manually
#   include <sys/syscall.h>
#   include <linux/fs.h>
# endif
#endif

static void fill_with_printable_random(char *dst, size_t sz)
{
  assert(sz > 0); // or we couldn't nul-term dst

  static char printable[] =
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789";
  int r = 0;
  while (--sz > 0) {
    if (r <= 255) r = rand();
    *dst = printable[ r % sizeof(printable) ];
    r >>= 8;
  }
  *dst = '\0';
}

// Either overwrite dirname with the directory name of fname, or leave
// it untouched.
void dirname_of_fname(char *dirname, size_t sz, char const *fname)
{
  // Build the name of the archive from the dirname of fname:
  char const *last_slash = NULL;
  for (char const *c = fname; *c != '\0'; c++) {
    if (*c == '/') last_slash = c;
  }
  if (last_slash) {
    size_t len = last_slash - fname;
    if (len < sz) {
      memcpy(dirname, fname, len);
      dirname[len] = '\0';
    }
  }
}

int ramen_archive(char const *fname, double start, double stop)
{
  int ret = -1;

  printf("Archiving file %s\n", fname);

  char dirname[PATH_MAX] = "./";
  dirname_of_fname(dirname, sizeof(dirname), fname);

  char arc_fname[PATH_MAX];
  char rnd[6+1];
  unsigned nb_tries = 0;

  while (1) {
    fill_with_printable_random(rnd, sizeof(rnd));
    if ((size_t)snprintf(arc_fname, sizeof(arc_fname), "%s/arc/%a_%a_%s",
          dirname, start, stop, rnd) >= PATH_MAX) {
      fprintf(stderr, "Archive file name truncated: '%s'\n", arc_fname);
      goto err0;
    }

    printf("Rename the current output (%s) into the archive (%s)\n",
           fname, arc_fname);

    if (0 ==
#ifdef HAVE_RENAMEX_NP
      renamex_np(fname, arc_fname, RENAME_EXCL)
#else
# ifdef HAVE_RENAMEAT2
      renameat2(AT_FDCWD, fname, AT_FDCWD, arc_fname, RENAME_NOREPLACE)
# else
      syscall(SYS_renameat2, AT_FDCWD, fname, AT_FDCWD, arc_fname, RENAME_NOREPLACE)
# endif
#endif
    ) {
      // Success renaming the file!
      ret = 0;
      break;
    } else {
      // Failure renaming the file.
      if (errno != EEXIST || nb_tries++ > 10) {
        fprintf(stderr, "Cannot rename output '%s' into '%s': %s\n",
                fname, arc_fname, strerror(errno));
        goto err0;
      }
    }
  };

err0:
  return ret;
}
