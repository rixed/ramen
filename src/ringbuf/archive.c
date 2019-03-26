// vim: ft=c bs=2 ts=2 sts=2 sw=2 expandtab
/* The purpose of this small library is merely to move archive files into
 * the archive subdirectory, with a name depending on the time range
 * (with a trailing suffix to uniquify the files).
 * It tries to do so with no locks.
 * Later this could grow to allow more flexible behavior (such as uploading
 * on HDFS or some cloud). */
#include <stdlib.h>
#include <stdbool.h>
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

#if !(defined(HAVE_RENAMEX_NP)) && !(defined(HAVE_RENAMEAT2)) && !(defined(__APPLE__))
  // Assuming we are building on an older Linux,
  // let's try to call the syscall manually
# include <sys/syscall.h>
# include <linux/fs.h>
#endif

// Create the directories required to create that file:
int mkdir_for_file(char *fname)
{
  int ret = -1;

  size_t len = strlen(fname);
  ssize_t last_slash;
  for (last_slash = len - 1; last_slash >= 0 && fname[last_slash] != '/'; last_slash--) ;

  if (last_slash <= 0) return 0; // no dir to create (or root)

  fname[last_slash] = '\0';
  if (0 != mkdir(fname, S_IRUSR|S_IWUSR|S_IXUSR)) {
    if (EEXIST == errno) {
      // Assume that's a directory
    } else {
      int ret = -1;
      if (ENOENT == errno) {
        if (mkdir_for_file(fname) < 0) goto err1;
        ret = mkdir(fname, S_IRUSR|S_IWUSR|S_IXUSR);
      }
      if (ret != 0) {
        fprintf(stderr, "Cannot create directory '%s': %s\n", fname, strerror(errno));
        goto err1;
      }
    }
  }

  ret = 0;
err1:
  fname[last_slash] = '/';
  fflush(stdout);
  fflush(stderr);
  return ret;
}

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
    *dst++ = printable[ r % sizeof(printable) ];
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

char const *extension_of_fname(char const *fname)
{
  char const *ret = NULL;
  for (char const *c = fname; *c != '\0'; c++) {
    if (*c == '/') ret = NULL;
    else if (*c == '.') ret = c;
  }

  return ret ? ret : "";
}

// WARNING: If only_if_exist and the lock does not exist, this returns 0.
int lock(char const *rb_fname, int operation /* LOCK_SH|LOCK_EX */, bool only_if_exist)
{
  char fname[PATH_MAX];
  if ((size_t)snprintf(fname, sizeof(fname), "%s.lock", rb_fname) >= sizeof(fname)) {
    fprintf(stderr, "Archive lockfile name truncated: '%s'\n", fname);
    goto err;
  }

  int fd = open(fname, only_if_exist ? 0 : O_CREAT, S_IRUSR|S_IWUSR);
  if (fd < 0) {
    if (errno == ENOENT && only_if_exist) return 0;
    fprintf(stderr, "Cannot create '%s': %s\n", fname, strerror(errno));
    goto err;
  }

  int err = -1;
  do {
    err = flock(fd, operation);
  } while (err < 0 && EINTR == errno);

  if (err < 0) {
    fprintf(stderr, "Cannot lock '%s': %s\n", fname, strerror(errno));
    if (close(fd) < 0) {
      fprintf(stderr, "Cannot close lockfile '%s': %s\n", fname, strerror(errno));
      // so be it
    }
    goto err;
  }

  return fd;
err:
  fflush(stderr);
  return -1;
}

int unlock(int lock_fd)
{
  if (0 == lock_fd) {
    // Assuming lock didn't exist rather than locking stdin:
    return 0;
  }

  while (0 != close(lock_fd)) {
    if (errno != EINTR) {
      fprintf(stderr, "Cannot unlock fd %d: %s\n", lock_fd, strerror(errno));
      fflush(stderr);
      return -1;
    }
  }
  return 0;
}


#if !(defined(HAVE_RENAMEX_NP)) && !(defined(HAVE_RENAMEAT2)) && defined(__APPLE__)
// Poor man version of exclusive rename, using a lock
static int rename_locked(char const *src, char const *dst)
{
  int ret = -1;

  int lock_fd = lock(src, LOCK_EX, true);
  if (lock_fd < 0) goto err0;

  ret = rename(src, dst);

  if (0 != unlock(lock_fd)) {
    ret = -1;
    goto err0;
  }

err0:
  return ret;
}
#endif

int ramen_archive(char const *fname, double start, double stop)
{
  int ret = -1;

  printf("Archiving file %s\n", fname);

  char dirname[PATH_MAX] = ".";
  dirname_of_fname(dirname, sizeof(dirname), fname);

  char arc_fname[PATH_MAX];
  char rnd[6+1];
  unsigned nb_tries = 0;
  char const *ext = extension_of_fname(fname);

  while (1) {
    fill_with_printable_random(rnd, sizeof(rnd));
    if ((size_t)snprintf(arc_fname, sizeof(arc_fname), "%s/arc/%a_%a_%s%s",
          dirname, start, stop, rnd, ext) >= PATH_MAX) {
      fprintf(stderr, "Archive file name truncated: '%s'\n", arc_fname);
      goto err0;
    }

    printf("Rename the current output (%s) into the archive (%s)\n",
           fname, arc_fname);

    if (0 != mkdir_for_file(arc_fname)) goto err0;

    if (0 ==
#ifdef HAVE_RENAMEX_NP
      renamex_np(fname, arc_fname, RENAME_EXCL)
#else
# ifdef HAVE_RENAMEAT2
      renameat2(AT_FDCWD, fname, AT_FDCWD, arc_fname, RENAME_NOREPLACE)
# else
#   ifdef __APPLE__
      rename_locked(fname, arc_fname)
#   else
      syscall(SYS_renameat2, AT_FDCWD, fname, AT_FDCWD, arc_fname, RENAME_NOREPLACE)
#   endif
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
