// vim: ft=c bs=2 ts=2 sts=2 sw=2 expandtab
#include <assert.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>

#include "ringbuf.h"
#include "archive.h"

#ifdef NEED_DATA_CACHE_FLUSH
extern inline void my_cacheflush(void const *p_, size_t sz);
#endif
extern inline void ringbuf_head_unlock(struct ringbuf *);
extern inline void ringbuf_head_lock(struct ringbuf *);
extern inline uint32_t ringbuf_file_num_entries(struct ringbuf_file const *rb, uint32_t, uint32_t);
extern inline uint32_t ringbuf_file_num_free(struct ringbuf_file const *rb, uint32_t, uint32_t);

extern inline enum ringbuf_error ringbuf_enqueue(struct ringbuf *rb, uint32_t const *data, uint32_t num_words, double t_start, double t_stop);

extern inline ssize_t ringbuf_dequeue(struct ringbuf *rb, uint32_t *data, size_t max_size);
extern inline ssize_t ringbuf_read_first(struct ringbuf *rb, struct ringbuf_tx *tx);
extern inline ssize_t ringbuf_read_next(struct ringbuf *rb, struct ringbuf_tx *tx);

static ssize_t really_read(int fd, void *d, size_t sz, char const *fname /* printed */)
{
  size_t rs = 0;
  while (rs < sz) {
    ssize_t ss = read(fd, d + rs, sz - rs);
    if (ss < 0) {
      if (errno != EINTR) {
        fprintf(stderr, "%d: Cannot read '%s': %s\n",
                getpid(), fname, strerror(errno));
        fflush(stderr);
        return -1;
      }
    } else if (ss == 0) {
      break;
    } else {
      rs += ss;
    }
  };
  return rs;
}

static int really_write(int fd, void const *s, size_t sz, char const *fname /* printed */)
{
  size_t ws = 0;
  while (ws < sz) {
    ssize_t ss = write(fd, s + ws, sz - ws);
    if (ss < 0) {
      if (errno != EINTR) {
        fprintf(stderr, "%d: Cannot write '%s': %s\n",
                getpid(), fname, strerror(errno));
        fflush(stderr);
        return -1;
      }
    } else {
      ws += ss;
    }
  }
  return 0;
}

static int read_max_seqnum(char const *bname, uint64_t *first_seq)
{
  int ret = -1;

  char dirname[PATH_MAX] = ".";
  dirname_of_fname(dirname, sizeof(dirname), bname);
  char fname[PATH_MAX];
  if ((size_t)snprintf(fname, PATH_MAX, "%s/arc/max", dirname) >= PATH_MAX) {
    fprintf(stderr, "%d: Archive max seq file name truncated: '%s'\n",
            getpid(), fname);
    goto err0;
  }

  int fd = open(fname, O_RDWR|O_CREAT, S_IRUSR|S_IWUSR);
  if (fd < 0) {
    if (ENOENT == errno) {
      *first_seq = 0;
      ret = 0;
      goto err0;
    } else {
      fprintf(stderr, "%d: Cannot create '%s': %s\n",
              getpid(), fname, strerror(errno));
      goto err0;
    }
  } else {
    ssize_t rs = really_read(fd, first_seq, sizeof(*first_seq), fname);
    if (rs < 0) {
      goto err1;
    } else if (rs == 0) {
      *first_seq = 0;
    } else if ((size_t)rs < sizeof(*first_seq)) {
      fprintf(stderr, "%d: Too short a file for seqnum: %s\n", getpid(),  fname);
      goto err1;
    }
  }

  ret = 0;
err1:
  if (0 != close(fd)) {
    fprintf(stderr, "%d: Cannot close sequence file '%s': %s\n",
            getpid(), fname, strerror(errno));
    ret = -1;
  }
err0:
  fflush(stderr);
  return ret;
}

static int write_max_seqnum(char const *bname, uint64_t seqnum)
{
  int ret = -1;

  // Save the new sequence number:
  char dirname[PATH_MAX] = ".";
  dirname_of_fname(dirname, sizeof(dirname), bname);
  char fname[PATH_MAX];
  if ((size_t)snprintf(fname, PATH_MAX, "%s/arc/max", dirname) >= PATH_MAX) {
    fprintf(stderr, "%d: Archive max seq file name truncated: '%s'\n",
            getpid(), fname);
    goto err0;
  }

  int fd = open(fname, O_WRONLY|O_CREAT, S_IRUSR|S_IWUSR);
  if (fd < 0) {
    if (ENOENT == errno) {
      if (0 != mkdir_for_file(fname)) return -1;
      fd = open(fname, O_WRONLY|O_CREAT, S_IRUSR|S_IWUSR); // retry
    }
    if (fd < 0) {
      fprintf(stderr, "%d: Cannot create '%s': %s\n",
              getpid(), fname, strerror(errno));
      goto err0;
    }
  }

  if (0 != really_write(fd, &seqnum, sizeof(seqnum), fname)) {
    goto err1;
  }

# if defined(HAVE_FDATASYNC) && !(defined(__APPLE__))
  if (0 != fdatasync(fd))
# else
  // Must be a MacOS then:
  if (0 != fcntl(fd, F_FULLFSYNC))
# endif
  {
    fprintf(stderr, "%d: Cannot fdatasync sequence file '%s': %s\n",
            getpid(), fname, strerror(errno));
    // best effort
  }

  ret = 0;

err1:
  if (0 != close(fd)) {
    fprintf(stderr, "%d, Cannot close sequence file '%s': %s\n",
            getpid(), fname, strerror(errno));
    ret = -1;
  }
err0:
  fflush(stderr);
  return ret;
}

// Keep existing files as much as possible:
extern int ringbuf_create_locked(
    uint64_t version, bool wrap, uint32_t num_words, double timeout, char const *fname)
{
  int ret = -1;
  struct ringbuf_file rbf;

  // First try to create the file:
  int fd = open(fname, O_WRONLY|O_CREAT|O_EXCL, S_IRUSR|S_IWUSR);
  if (fd >= 0) {
    // We are the creator. Other creators are waiting for the lock.
    //printf("Creating ringbuffer '%s'\n", fname);

    size_t file_length = sizeof(rbf) + num_words*sizeof(uint32_t);
    if (ftruncate(fd, file_length) < 0) {
      fprintf(stderr, "%d: Cannot ftruncate file '%s': %s\n",
              getpid(), fname, strerror(errno));
      goto err3;
    }

    if (0 != read_max_seqnum(fname, &rbf.first_seq)) goto err3;

    rbf.version = version;
    rbf.num_words = num_words;
    atomic_flag_clear(&rbf.lock);
    // Uh?! Why atomic to write in local rbf?
    atomic_init(&rbf.prod_head, 0);
    atomic_init(&rbf.prod_tail, 0);
    atomic_init(&rbf.cons_head, 0);
    atomic_init(&rbf.cons_tail, 0);
    atomic_init(&rbf.num_allocs, 0);
    atomic_init(&rbf.tmin, 0.);
    atomic_init(&rbf.tmax, 0.);
    rbf.wrap = wrap;
    rbf.timeout = timeout;

    if (0 != really_write(fd, &rbf, sizeof(rbf), fname)) {
      goto err3;
    }
    if (! wrap && 0 != fsync(fd)) {
      fprintf(stderr, "%d: Cannot fsync ringbuf file '%s': %s\n",
              getpid(), fname, strerror(errno));
      // best effort
    }
  } else {
    if (errno == EEXIST) {
      ret = 0;
    } else {
      fprintf(stderr, "%d: Cannot open ring-buffer '%s': %s\n",
              getpid(), fname, strerror(errno));
    }
    goto err0;
  }

  ret = 0;

err3:
  if (ret != 0 && unlink(fname) < 0) {
    fprintf(stderr, "%d: Cannot erase not-created ringbuf '%s': %s\n"
                    "Oh dear!\n", getpid(), fname, strerror(errno));
  }
//err2:
  if (close(fd) < 0) {
    fprintf(stderr, "%d: Cannot close ring-buffer(1) '%s': %s\n",
            getpid(), fname, strerror(errno));
    // so be it
  }
err0:
  fflush(stderr);
  return ret;
}

extern enum ringbuf_error ringbuf_create(
    uint64_t version, bool wrap, uint32_t num_words, double timeout, char const *fname)
{
  enum ringbuf_error err = RB_ERR_FAILURE;

  // We must not try to create a RB while another process is rotating or
  // creating it:
  int lock_fd = lock(fname, LOCK_EX, false);
  if (lock_fd < 0) goto err0;

  if (0 != ringbuf_create_locked(version, wrap, num_words, timeout, fname)) {
    goto err1;
  }

  err = RB_OK;

err1:
  if (0 != unlock(lock_fd)) err = RB_ERR_FAILURE;
err0:
  return err;
}

static bool check_header_eq(char const *fname, char const *what, unsigned expected, unsigned actual)
{
  if (expected == actual) return true;

  fprintf(stderr, "%d: Invalid ring buffer file '%s': %s should be %u but is %u\n",
          getpid(), fname, what, expected, actual);
  fflush(stderr);
  return false;
}

static bool check_header_max(char const *fname, char const *what, unsigned max, unsigned actual)
{
  if (actual < max) return true;

  fprintf(stderr, "%d: Invalid ring buffer file '%s': %s (%u) should be < %u\n",
          getpid(), fname, what, actual, max);
  fflush(stderr);
  return false;
}

static enum ringbuf_error mmap_rb(uint64_t version, struct ringbuf *rb)
{
  enum ringbuf_error err = RB_ERR_FAILURE;

  int fd = open(rb->fname, O_RDWR, S_IRUSR|S_IWUSR);
  if (fd < 0) {
    fprintf(stderr, "%d: Cannot load ring-buffer from file '%s': %s\n",
            getpid(), rb->fname, strerror(errno));
    goto err0;
  }

  off_t file_length = lseek(fd, 0, SEEK_END);
  if (file_length == (off_t)-1) {
    fprintf(stderr, "%d: Cannot lseek into file '%s': %s\n",
            getpid(), rb->fname, strerror(errno));
    goto err1;
  }
  if ((size_t)file_length <= sizeof(*rb->rbf)) {
    fprintf(stderr, "%d: Invalid ring buffer file '%s': Too small.\n",
            getpid(), rb->fname);
    goto err1;
  }

  struct ringbuf_file *rbf =
      mmap(NULL, file_length, PROT_READ|PROT_WRITE, MAP_SHARED, fd, (off_t)0);
  if (rbf == MAP_FAILED) {
    fprintf(stderr, "%d: Cannot mmap file '%s': %s\n",
            getpid(), rb->fname, strerror(errno));
    goto err1;
  }

  // Sanity checks
  if (!(
        check_header_eq(rb->fname, "file size", rbf->num_words*sizeof(uint32_t) + sizeof(*rbf), file_length) &&
        check_header_max(rb->fname, "prod head", rbf->num_words, rbf->prod_head) &&
        check_header_max(rb->fname, "prod tail", rbf->num_words, rbf->prod_tail) &&
        check_header_max(rb->fname, "cons head", rbf->num_words, rbf->cons_head) &&
        check_header_max(rb->fname, "cons tail", rbf->num_words, rbf->cons_tail)
  )) {
    munmap(rbf, file_length);
    goto err1;
  }

  // Check version
  if (rbf->version != version) {
    munmap(rbf, file_length);
    err = RB_ERR_BAD_VERSION;
    goto err1;
  }

  rb->rbf = rbf;
  rb->mmapped_size = file_length;

# ifdef LOCK_WITH_LOCKF
  char fname[PATH_MAX];
  if ((size_t)snprintf(fname, PATH_MAX, "%s.head_lock", rb->fname) >= PATH_MAX) {
    fprintf(stderr, "%d: Archive header lock file name truncated: '%s'\n",
            getpid(), fname);
    goto err1;
  }

  rb->lock_fd = open(fname, O_RDWR|O_CREAT, S_IRUSR|S_IWUSR);
  if (rb->lock_fd < 0) {
    fprintf(stderr, "%d: Cannot open '%s': %s\n", getpid(), fname, strerror(errno));
    goto err1;
  }
# endif

  err = RB_OK;

err1:
  if (close(fd) < 0) {
    fprintf(stderr, "%d: Cannot close ring-buffer(2) '%s': %s\n",
            getpid(), rb->fname, strerror(errno));
    // so be it
  }
err0:
  fflush(stderr);
  return err;
}

extern enum ringbuf_error ringbuf_load(struct ringbuf *rb, uint64_t version, char const *fname)
{
  enum ringbuf_error err = RB_ERR_FAILURE;

  size_t fname_len = strlen(fname);
  if (fname_len + 1 > sizeof(rb->fname)) {
    fprintf(stderr, "%d: Cannot load ring-buffer: Filename too long: %s\n",
            getpid(), fname);
    goto err0;
  }
  memcpy(rb->fname, fname, fname_len + 1);
  rb->rbf = NULL;
  rb->mmapped_size = 0;
# ifdef LOCK_WITH_LOCKF
  rb->lock_fd = -1;
# endif

  // Although we probably just ringbuf_created that file, some other processes
  // might be rotating it already. Note that archived files do not have a lock
  // file, nor do they need one:
  int lock_fd = lock(rb->fname, LOCK_SH, true);
  if (lock_fd < 0) goto err0;

  if ((err = mmap_rb(version, rb)) != RB_OK) {
    goto err1;
  }

  err = RB_OK;

err1:
  if (0 != unlock(lock_fd)) err = RB_ERR_FAILURE;
err0:
  fflush(stderr);
  return err;
}

enum ringbuf_error ringbuf_unload(struct ringbuf *rb)
{
  if (rb->rbf) {
    if (0 != munmap(rb->rbf, rb->mmapped_size)) {
      fprintf(stderr, "%d: Cannot munmap: %s\n", getpid(), strerror(errno));
      fflush(stderr);
      return RB_ERR_FAILURE;
    }
    rb->rbf = NULL;
  }
# ifdef LOCK_WITH_LOCKF
  if (rb->lock_fd >= 0) {
    if (close(rb->lock_fd) < 0) {
      fprintf(stderr, "%d: Cannot close ring-buffer '%s' headlock: %s\n",
              getpid(), rb->fname, strerror(errno));
    }
    rb->lock_fd = -1;
  }
# endif

  rb->mmapped_size = 0;
  return RB_OK;
}

// Called with the lock
static int rotate_file_locked(struct ringbuf *rb)
{
  // Signal the EOF
  atomic_store(rb->rbf->data + (atomic_load(&rb->rbf->prod_head)), UINT32_MAX);

  int ret = -1;

  uint64_t last_seq = rb->rbf->first_seq + rb->rbf->num_allocs;
  if (0 != write_max_seqnum(rb->fname, last_seq)) goto err0;

  // Name the archive according to tuple seqnum included and also with the
  // time range (will be only 0 if no time info is available):
  char dirname[PATH_MAX] = ".";
  dirname_of_fname(dirname, sizeof(dirname), rb->fname);
  char arc_fname[PATH_MAX];
  if ((size_t)snprintf(arc_fname, PATH_MAX,
                       "%s/arc/%016"PRIx64"_%016"PRIx64"_%a_%a.b",
                       dirname, rb->rbf->first_seq, last_seq,
                       rb->rbf->tmin, rb->rbf->tmax) >= PATH_MAX) {
    fprintf(stderr, "%d: Archive file name truncated: '%s'\n", getpid(), arc_fname);
    goto err0;
  }

  // Rename the current rb into the archival name.
  //printf("Rename the current rb (%s) into the archive (%s)\n",
  //       rb->fname, arc_fname);

  // If the archive flag was set, do actually archive that file
  if (0 != rename(rb->fname, arc_fname)) {
    fprintf(stderr, "%d: Cannot rename full buffer '%s' into '%s': %s\n",
            getpid(), rb->fname, arc_fname, strerror(errno));
    goto err0;
  }

  // Regardless of how this rotation went, we must not release the lock without
  // having created a new archive file:
  //printf("Create a new buffer file under the same old name '%s'\n", rb->fname);
  if (0 != ringbuf_create_locked(rb->rbf->version, rb->rbf->wrap,
                                 rb->rbf->num_words, rb->rbf->timeout,
                                 rb->fname)) {
    goto err0;
  }

  ret = 0;

err0:
  fflush(stdout);
  fflush(stderr);
  return ret;
}

enum ringbuf_error rotate_file(struct ringbuf *rb)
{
  struct ringbuf_file *rbf = rb->rbf;
  if (rbf->wrap) return RB_OK;

  //printf("Rotating buffer '%s'!\n", rb->fname);

  enum ringbuf_error err = RB_ERR_FAILURE;

  // We have filled the non-wrapping buffer: rotate the file!
  // We need a lock to ensure no other writers is rotating at the same time
  int lock_fd = lock(rb->fname, LOCK_EX, false);
  if (lock_fd < 0) goto err0;

  // Wait, maybe some other process rotated the file already while we were
  // waiting for that lock? In that case it would have written the EOF:
  if (atomic_load(rbf->data + atomic_load(&rbf->prod_head)) != UINT32_MAX) {
    if (0 != rotate_file_locked(rb)) goto err1;
  } else {
    //printf("...actually not, someone did already.\n");
  }

err1:
  if (0 != unlock(lock_fd)) goto err0;
  err = RB_OK;
  // Too bad we cannot unlink that lockfile without a race condition
err0:
  fflush(stdout);
  fflush(stderr);
  return err;
}

static enum ringbuf_error may_rotate(struct ringbuf *rb, uint32_t num_words)
{
  struct ringbuf_file *rbf = rb->rbf;
  if (rbf->wrap) return RB_OK;

  uint32_t const needed = 1 /* msg size */ + num_words + 1 /* EOF */;
  uint32_t const free =
    ringbuf_file_num_free(rbf, atomic_load(&rbf->cons_tail),
                               atomic_load(&rbf->prod_head));
  if (free >= needed) {
    if (atomic_load(rbf->data + atomic_load(&rbf->prod_head)) == UINT32_MAX) {
      // Another writer might have "closed" this ringbuf already, that's OK.
      // But we still must be close to the actual end, otherwise complain:
      if (free > 2 * needed) {
        fprintf(stderr,
                "%d: Enough place for a new record (%"PRIu32" words, "
                "and %"PRIu32" free) but EOF mark is set\n",
                getpid(), needed, free);
      }
    } else {
      return RB_OK;
    }
  }

  //printf("Rotating buffer '%s'!\n", rb->fname);

  enum ringbuf_error err = RB_ERR_FAILURE;

  // We have filled the non-wrapping buffer: rotate the file!
  // We need a lock to ensure no other writers is rotating at the same time
  int lock_fd = lock(rb->fname, LOCK_EX, false);
  if (lock_fd < 0) goto err0;

  // Wait, maybe some other process rotated the file already while we were
  // waiting for that lock? In that case it would have written the EOF:
  if (atomic_load(rbf->data + atomic_load(&rbf->prod_head)) != UINT32_MAX) {
    if (0 != rotate_file_locked(rb)) goto err1;
  } else {
    //printf("...actually not, someone did already.\n");
  }

  // Remember the version:
  uint64_t version = rbf->version;

  // Unmap rb
  //printf("Unmap rb\n");
  if (RB_OK != ringbuf_unload(rb)) goto err1;

  // Mmap the new file and update rbf.
  //printf("Mmap the new file and update rbf\n");
  if ((err = mmap_rb(version, rb)) != RB_OK) goto err1;

  err = RB_OK;

err1:
  if (0 != unlock(lock_fd)) err = RB_ERR_FAILURE;
  // Too bad we cannot unlink that lockfile without a race condition
err0:
  fflush(stdout);
  fflush(stderr);
  return err;
}

/* ringbuf will have:
 *  word n: num_words
 *  word n+1..n+num_words: allocated.
 *  tx->record_start will point at word n+1 above. */
extern enum ringbuf_error ringbuf_enqueue_alloc(struct ringbuf *rb, struct ringbuf_tx *tx, uint32_t num_words)
{
  // It is currently not possible to have an empty record:
  ASSERT_RB(num_words > 0);

  uint32_t cons_tail;
  uint32_t need_eof = 0;  // 0 never needs an EOF

  enum ringbuf_error err = may_rotate(rb, num_words);
  if (err != RB_OK) return err;

  struct ringbuf_file *rbf = rb->rbf;

# if defined(LOCK_WITH_SPINLOCK) || defined(LOCK_WITH_LOCKF)

  ringbuf_head_lock(rb);

  tx->seen = atomic_load(&rbf->prod_head);
  cons_tail = rbf->cons_tail;
  tx->record_start = tx->seen;
  // We will write the size then the data:
  tx->next = tx->record_start + 1 + num_words;
  uint32_t alloced = 1 + num_words;

  // Avoid wrapping inside the record
  if (tx->next > rbf->num_words) {
    need_eof = tx->seen;
    alloced += rbf->num_words - tx->seen;
    tx->record_start = 0;
    tx->next = 1 + num_words;
    ASSERT_RB(tx->next < rbf->num_words);
  } else if (tx->next == rbf->num_words) {
    //printf("tx->next == rbf->num_words\n");
    tx->next = 0;
  }

  // Enough room?
  if (ringbuf_file_num_free(rbf, cons_tail, tx->seen) <= alloced) {
    /*printf("Ringbuf is full, cannot alloc for enqueue %"PRIu32"/%"PRIu32" tot words, seen=%"PRIu32", cons_tail=%"PRIu32", num_free=%"PRIu32"\n",
           alloced, rbf->num_words, tx->seen, cons_tail, ringbuf_file_num_free(rbf, cons_tail, tx->seen));*/
    ringbuf_head_unlock(rb);
    return RB_ERR_NO_MORE_ROOM;
  }

/*  fprintf(stderr, "%d: enqueue: prod=[%d..%d-%d], tx->seen=%"PRIu32"\n",
          getpid(), rbf->prod_tail, rbf->prod_head, tx->next, tx->seen);*/

  atomic_store(&rbf->prod_head, tx->next);

  if (need_eof) atomic_store(rbf->data + need_eof, UINT32_MAX);
  ASSERT_RB(num_words < MAX_RINGBUF_MSG_WORDS);
  atomic_store(rbf->data + (tx->record_start ++), num_words);
  ringbuf_head_unlock(rb);

# else

  /* Lock-less version: */

  tx->seen = atomic_load(&rbf->prod_head); // will be updated by compare_exchange
  do {
    cons_tail = atomic_load(&rbf->cons_tail);
    tx->record_start = tx->seen;
    // We will write the size then the data:
    tx->next = tx->record_start + 1 + num_words;
    uint32_t alloced = 1 + num_words;
    need_eof = 0;  // 0 never needs an EOF

    // Avoid wrapping inside the record
    if (tx->next > rbf->num_words) {
      need_eof = tx->seen;
      alloced += rbf->num_words - tx->seen;
      tx->record_start = 0;
      tx->next = 1 + num_words;
      ASSERT_RB(tx->next < rbf->num_words);
    } else if (tx->next == rbf->num_words) {
      //printf("tx->next == rbf->num_words\n");
      tx->next = 0;
    }

    // Enough room? (not that cons_tail is a pessimistic estimate at this point)
    if (ringbuf_file_num_free(rbf, cons_tail, tx->seen) <= alloced) {
      /*printf("Ringbuf is full, cannot alloc for enqueue %"PRIu32"/%"PRIu32" tot words, seen=%"PRIu32", cons_tail=%"PRIu32", num_free=%"PRIu32"\n",
             alloced, rbf->num_words, tx->seen, cons_tail, ringbuf_file_num_free(rbf, cons_tail, tx->seen));*/
      return RB_ERR_NO_MORE_ROOM;
    }

    /* So far this was all speculative. Let's check if anybody altered that: */
  } while (! atomic_compare_exchange_weak(&rbf->prod_head, &tx->seen, tx->next));

  /* It is possible that other writers/readers have emptied and refilled the
   * whole ringbuffer after ringbuf_file_num_free was called and then the
   * prod_head might have came back exactly to the same position, but now maybe
   * the ringbuffer cannot fit alloced words.
   * There is no way to know if that occurred. The only prevention against that
   * is to have a ringbuffer much larger than what all the readers/writers can
   * process in such a short amount of time. */

  if (need_eof) atomic_store(rbf->data + need_eof, UINT32_MAX);
  ASSERT_RB(num_words < MAX_RINGBUF_MSG_WORDS);
  atomic_store(rbf->data + (tx->record_start ++), num_words);

# endif

  return RB_OK;
}

/* So we have reserved a TX in the prod area, remembering what the former
 * head of the prod area was before adding our TX on top, and we are now
 * finished serializing the message in that TX and would like to "commit"
 * the TX. We must now wait until the tail of the prod area reaches the
 * head position we observed.
 * If a previous writer died while serializing its own TX, then we are going
 * to wait forever.
 * It's then supervisor's job to detect the deadlock and clean that
 * ringbuffer. */

#define SLEEP_WHEN_WAITING
static void release_cpu(void) {
#ifdef SLEEP_WHEN_WAITING
  /* 66ns is very noticeable with strace */
  static struct timespec const quick = { .tv_sec = 0, .tv_nsec = 66 };
  nanosleep(&quick, NULL);
#else
  sched_yield();
#endif
}

#ifndef CLOCK_MONOTONIC_RAW
# define CLOCK_MONOTONIC_RAW CLOCK_MONOTONIC
#endif

#if defined(LOCK_WITH_SPINLOCK) || defined(LOCK_WITH_LOCKF)
#else
static bool is_after(struct timespec const *t1, struct timespec const *t2)
{
  return t1->tv_sec > t2->tv_sec ||
         t1->tv_sec == t2->tv_sec && t1->tv_nsec > t2->tv_nsec;
}

static uint32_t get_num_words(struct ringbuf_file const *rbf, uint32_t from, uint32_t to)
{
  if (to >= from) return to - from;
  else return (rbf->num_words - from) + to;
}

static int prepare_timeouts(struct ringbuf_file const *rbf, uint32_t current, uint32_t target, struct timespec *start_to_worry, struct timespec *declared_dead)
{
  if (0 != clock_gettime(CLOCK_MONOTONIC_RAW, start_to_worry)) {
    fprintf(stderr, "%d: Cannot clock_gettime: %s\n",
            getpid(), strerror(errno));
    return -1;
  }

  // Timeout after 2s + 500ms*kb, maxed at ~5s:
  uint32_t const offset = get_num_words(rbf, current, target);
  unsigned long us = 2000000 + (500 * offset);
  unsigned long s = us / 1000000;
  s = s > 5 ? 5 : s;
  us -= s * 1000000;
  declared_dead->tv_sec = start_to_worry->tv_sec + s;
  declared_dead->tv_nsec = start_to_worry->tv_nsec + (1000 * us);

/*  fprintf(stderr, "%d: Will timeout after %lus, %lums (offs=%"PRIu32")\n",
          getpid(), s, us, offset);*/

  return 0;
}
#endif

void ringbuf_enqueue_commit(struct ringbuf *rb, struct ringbuf_tx const *tx, double t_start, double t_stop)
{
  struct ringbuf_file *rbf = rb->rbf;

  if (t_start > t_stop) {
    double tmp = t_start;
    t_start = t_stop;
    t_stop = tmp;
  }

# if defined(LOCK_WITH_SPINLOCK) || defined(LOCK_WITH_LOCKF)

  // Update the prod_tail to match the new prod_head.
  // First, wait until the prod_tail reach the head we observed (ie.
  // previously allocated records have been committed).
  unsigned loops = 0;
  while (true) {
    ringbuf_head_lock(rb);
    uint32_t seen_copy = tx->seen;
    if (atomic_compare_exchange_weak(&rbf->prod_tail, &seen_copy, tx->next)) {
      // Once all previous messages are gone, commit that one

      if (ringbuf_file_num_entries(rbf, tx->next, rbf->cons_head) <= 0) {
        fprintf(stderr, "%d: num_entries = %"PRIu32", tx->next = %"PRIu32
                        ", cons_head = %"PRIu32", tx->seen = %"PRIu32"\n",
                getpid(),
                ringbuf_file_num_entries(rbf, tx->next, rbf->cons_head),
                tx->next, rbf->cons_head, tx->seen);
        fflush(stderr);
        ASSERT_RB(ringbuf_file_num_entries(rbf, tx->next, rbf->cons_head) > 0);
      }

      // All we need is for the following prod_tail change to always
      // be visible after the changes to num_allocs and min/max observed t:
      uint32_t prev_num_allocs =
        atomic_fetch_add_explicit(&rbf->num_allocs, 1, memory_order_relaxed);
      if (t_start > 0. || t_stop > 0.) {
        double const tmin = atomic_load_explicit(&rbf->tmin, memory_order_relaxed);
        double const tmax = atomic_load_explicit(&rbf->tmax, memory_order_relaxed);
        if (0 == prev_num_allocs || t_start < tmin) {
            atomic_store_explicit(&rbf->tmin, t_start, memory_order_relaxed);
            //fprintf(stderr, "tmin = %f\n", t_start);
        }
        if (0 == prev_num_allocs || t_stop > tmax) {
            atomic_store_explicit(&rbf->tmax, t_stop, memory_order_relaxed);
            //fprintf(stderr, "tmax = %f\n", t_stop);
        }
      }

      ringbuf_head_unlock(rb);
      break;
    } else {
/*      fprintf(stderr, "%d: prod_tail still %"PRIu32", waiting for seen=%"PRIu32"\n",
              getpid(), rbf->prod_tail, tx->seen);*/
      ringbuf_head_unlock(rb);
      /* Wait until less than ASSUME_KIA_AFTER (which is ~1s) so that other
       * workers can assume this one is indeed KIA: */
      if (++ loops > ASSUME_KIA_AFTER / 2) {
        fprintf(stderr, "%d: prod_tail still %"PRIu32", waiting for seen=%"PRIu32
                        " for too long, aborting!\n",
                getpid(), rbf->prod_tail, tx->seen);
        abort();
      }
      release_cpu();
    }
  }

# else

  /* If a worker died with a region allocated in the producers section, we
   * are in trouble. To mitigate that risk:
   * - after a WAIT_LOOP_YIELD_AFTER spins, suspect a worker died.
   *   Measure the distance from prod_tail, measure time, and start spinning
   *   with sched_yield() (actually, release_cpu() that does nanosleep instead),
   * - after a time proportional to the distance with prod_tail (so earlier
   *   worker should fix the ringbuffer first), forcibly "commit" the whole
   *   beginning of the producers section, by setting its size to whatever is
   *   required to reach our own allocation, write a special header in there
   *   meaning "that data is invalid",
   * - and resume as normal.
   * If there were other workers before us, we will also mark their content as
   * invalid, but that's no big deal. */

# define WAIT_LOOP_YIELD_AFTER 100000

  unsigned loops = 0;
  uint32_t prod_tail, prev_prod_tail;
  struct timespec start_to_worry, declared_dead;
  while ((prod_tail = atomic_load(&rbf->prod_tail)) != tx->seen) {
    /* In case of high contention the previous writer might just need a CPU
     * to run on, but prepare for the worse anyway: */
    if (++loops >= WAIT_LOOP_YIELD_AFTER) {
      if (loops == WAIT_LOOP_YIELD_AFTER) {
        prev_prod_tail = prod_tail;
        if (0 != prepare_timeouts(rbf, prod_tail, tx->seen, &start_to_worry, &declared_dead)) {
          abort(); // Out of idea
        }
      } else {
        if (prod_tail != prev_prod_tail) {
          //fprintf(stderr, "%d: Unblocked itself\n", getpid());
          loops = 0;
        } else {
          struct timespec now;
          if (0 == clock_gettime(CLOCK_MONOTONIC_RAW, &now)) {
            if (is_after(&now, &declared_dead)) {
              /* Time for action: forcibly "commit" the beginning of the queue */
              uint32_t prev_num_words = atomic_load(rbf->data + prod_tail);
              uint32_t new_num_words =
                // Beware that the size written does not include the size itself:
                get_num_words(rbf, prod_tail+1, tx->seen);
              /* No syscall (write, getpid) before all the writes to minimize
               * the risk of having another blocked writer scheduled now */
              if (prev_num_words != new_num_words)
                atomic_store(rbf->data + prod_tail, new_num_words);
              atomic_store(&rbf->prod_tail, tx->seen);
              // TODO: also mark it as invalid!
              if (prev_num_words != new_num_words) {
                fprintf(stderr, "%d: Invalidating frozen msg in prod section @%d, "
                                "overwriting msg size from %"PRIu32" to %"PRIu32" words\n",
                        getpid(), prod_tail, prev_num_words, new_num_words);
              } else {
                fprintf(stderr, "%d: Invalidating frozen msg in prod section @%d, "
                                "keeping msg size at %"PRIu32" words\n",
                        getpid(), prod_tail, prev_num_words);
              }
              /* In case another worker also changed prod_tail, what we just
               * committed will be skipped over. */
              break;
            }
          } else {
            fprintf(stderr, "%d: Cannot clock_gettime: %s\n",
                    getpid(), strerror(errno));
          }
        }
        release_cpu();
      }
    }
  }

  /* Here our record is the next. In theory, next writers are now all
   * waiting for us. */

  //printf("enqueue commit, set prod_tail=%"PRIu32" while cons_head=%"PRIu32"\n", tx->next, rbf->cons_head);
  /* All we need is for the following prod_tail change to always
   * be visible after the changes to num_allocs and tmin/tmax: */
  uint32_t prev_num_allocs = atomic_fetch_add_explicit(&rbf->num_allocs, 1, memory_order_relaxed);
  if (t_start > 0. || t_stop > 0.) {
    double const tmin = atomic_load_explicit(&rbf->tmin, memory_order_relaxed);
    double const tmax = atomic_load_explicit(&rbf->tmax, memory_order_relaxed);
    if (0 == prev_num_allocs || t_start < tmin)
        atomic_store_explicit(&rbf->tmin, t_start, memory_order_relaxed);
    if (0 == prev_num_allocs || t_stop > tmax)
        atomic_store_explicit(&rbf->tmax, t_stop, memory_order_relaxed);
  }
  atomic_store(&rbf->prod_tail, tx->next);
  //print_rb(rb);

# endif
}

ssize_t ringbuf_dequeue_alloc(struct ringbuf *rb, struct ringbuf_tx *tx)
{
  struct ringbuf_file *rbf = rb->rbf;

# if defined(LOCK_WITH_SPINLOCK) || defined(LOCK_WITH_LOCKF)

  /* Try to "reserve" the next record after cons_head by moving rbf->cons_head
   * after it */
  ringbuf_head_lock(rb);

  tx->seen = atomic_load(&rbf->cons_head);
  uint32_t seen_prod_tail = atomic_load(&rbf->prod_tail);
  tx->record_start = tx->seen;

  if (ringbuf_file_num_entries(rbf, seen_prod_tail, tx->seen) < 1) {
    //printf("Not a single word to read; prod_tail=%"PRIu32", cons_head=%"PRIu32".\n", seen_prod_tail, tx->seen);
    ringbuf_head_unlock(rb);
    return -1;
  }

  uint32_t num_words = atomic_load(rbf->data + (tx->record_start ++));  // which may be wrong already
  // Note that num_words = 0 would be invalid, but as long as we haven't
  // successfully written cons_head back to the RB we are not sure this is
  // an actual record size.

  uint32_t dequeued = 1 + num_words;  // How many words we'd like to increment cons_head of

  if (num_words == UINT32_MAX) { // A wrap around marker
    tx->record_start = 0;
    num_words = atomic_load(rbf->data + (tx->record_start ++));
    ASSERT_RB(num_words < MAX_RINGBUF_MSG_WORDS);
    ASSERT_RB(num_words > 0);
    dequeued = 1 + num_words + rbf->num_words - tx->seen;
  }

  tx->next = (tx->record_start + num_words) % rbf->num_words;

/*  fprintf(stderr, "%d: dequeue: cons=[%d..%d-%d]\n",
          getpid(), rbf->cons_tail, rbf->cons_head, tx->next);*/

  if (dequeued > ringbuf_file_num_entries(rbf, seen_prod_tail, tx->seen)) {
    fprintf(stderr, "%d: dequeued = %"PRIu32" > num_entries = %"PRIu32
                    ", seen_prod_tail=%"PRIu32", tx->seen=%"PRIu32"\n",
        getpid(), dequeued,
        ringbuf_file_num_entries(rbf, seen_prod_tail, tx->seen),
        seen_prod_tail, tx->seen);
    fflush(stderr);
    ASSERT_RB(dequeued <= ringbuf_file_num_entries(rbf, seen_prod_tail, tx->seen));
  }

  atomic_store(&rbf->cons_head, tx->next);
  ringbuf_head_unlock(rb);
# else

  /* Lock-less version */

  uint32_t seen_prod_tail, num_words, dequeued;

   /* Try to "reserve" the next record after cons_head by moving rbf->cons_head
    * after it */
  tx->seen = atomic_load(&rbf->cons_head); // compare_exchange will update this
  do {
    seen_prod_tail = atomic_load(&rbf->prod_tail);
    tx->record_start = tx->seen;

    if (ringbuf_file_num_entries(rbf, seen_prod_tail, tx->seen) < 1) {
      //printf("Not a single word to read; prod_tail=%"PRIu32", cons_head=%"PRIu32".\n", seen_prod_tail, tx->seen);
      return -1;
    }

    num_words = atomic_load(rbf->data + (tx->record_start ++));  // which may be wrong already
    // Note that num_words = 0 would be invalid, but as long as we haven't
    // successfully written cons_head back to the RB we are not sure this is
    // an actual record size.

    dequeued = 1 + num_words;  // How many words we'd like to increment cons_head of

    if (num_words == UINT32_MAX) { // A wrap around marker
      tx->record_start = 0;
      num_words = atomic_load(rbf->data + (tx->record_start ++));
      dequeued = 1 + num_words + rbf->num_words - tx->seen;
    }

    tx->next = (tx->record_start + num_words) % rbf->num_words;

    /* So far we have only read stuff. Now we are all set, *if* no other thread
     * changed anything. Let's find out: */
  } while (! atomic_compare_exchange_weak(&rbf->cons_head, &tx->seen, tx->next));

  /* It is possible that by the time we've read num_words and computed dequeued
   * and record_start, other readers have entirely emptied the ringbuffer and
   * writers are put some new content in it, in such a way that cons_head
   * went a full circle and came back at tx->seen, so the CAS succeeded, but
   * num_words and co. are wrong.
   * Let's make sure this have not occurred, or fail: */
  uint32_t num_words2 = atomic_load(rbf->data + tx->seen);
  ASSERT_RB(num_words2 == num_words || num_words2 == UINT32_MAX);

  if (num_words >= MAX_RINGBUF_MSG_WORDS)
    fprintf(stderr, "num_words = 0x%"PRIx32"!?, record_start=%"PRIu32", dequeued=%"PRIu32", next=%"PRIu32"\n",
            num_words, tx->record_start, dequeued, tx->next);
  ASSERT_RB(num_words < MAX_RINGBUF_MSG_WORDS);
  ASSERT_RB(num_words > 0);

  /* If the CAS succeeded it means nobody altered the indexes while we were
   * reading, therefore nobody wrote something silly in place of the number
   * of words present, so we are all good. */

# endif
  // It is currently not possible to have an empty record:
  if (num_words <= 0) {
    fprintf(stderr, "%d: num_words = %"PRIu32", read before %"PRIu32
                    ", dequeued=%"PRIu32"\n",
            getpid(), num_words, tx->record_start, dequeued);
    fflush(stderr);
    ASSERT_RB(num_words > 0);
  }

  return num_words*sizeof(uint32_t);
}

void ringbuf_dequeue_commit(struct ringbuf *rb, struct ringbuf_tx const *tx)
{
  struct ringbuf_file *rbf = rb->rbf;

# if defined(LOCK_WITH_SPINLOCK) || defined(LOCK_WITH_LOCKF)

  unsigned loops = 0;
  while (true) {
    ringbuf_head_lock(rb);
    uint32_t seen_copy = tx->seen;
    if (atomic_compare_exchange_weak(&rbf->cons_tail, &seen_copy, tx->next)) {
      ringbuf_head_unlock(rb);
      break;
    } else {
      ringbuf_head_unlock(rb);
      if (++ loops > ASSUME_KIA_AFTER / 2) {
        fprintf(stderr, "%d: cons_tail still %"PRIu32", waiting for seen=%"PRIu32
                        " for too long, aborting!\n",
                getpid(), rbf->cons_tail, tx->seen);
        abort();
      }
      release_cpu();
    }
  }

# else

  unsigned loops = 0;
  uint32_t cons_tail, prev_cons_tail;
  struct timespec start_to_worry, declared_dead;
  while ((cons_tail = atomic_load(&rbf->cons_tail)) != tx->seen) {
    if (++loops >= WAIT_LOOP_YIELD_AFTER) {
      if (loops == WAIT_LOOP_YIELD_AFTER) {
        prev_cons_tail = cons_tail;
        if (0 != prepare_timeouts(rbf, cons_tail, tx->seen, &start_to_worry, &declared_dead)) {
          abort(); // Out of idea
        }
      } else {
        if (cons_tail != prev_cons_tail) {
          //fprintf(stderr, "%d: Unblocked itself\n", getpid());
          loops = 0;
        } else {
          struct timespec now;
          if (0 == clock_gettime(CLOCK_MONOTONIC_RAW, &now)) {
            if (is_after(&now, &declared_dead)) {
              /* Time for action: forcibly "commit" the beginning of the queue */
              uint32_t prev_num_words = atomic_load(rbf->data + cons_tail);
              uint32_t new_num_words =
                // Beware that the size written does not include the size itself:
                get_num_words(rbf, cons_tail+1, tx->seen);
              /* No syscall (write, getpid) before all the writes to minimize
               * the risk of having another blocked reader scheduled now */
              if (prev_num_words != new_num_words)
                atomic_store(rbf->data + cons_tail, new_num_words);
              atomic_store(&rbf->cons_tail, tx->seen);
              // Note: no need to mask it as invalid, the consumer is already dead
              if (prev_num_words != new_num_words) {
                fprintf(stderr, "%d: Invalidating frozen msg in cons section @%d, "
                                "overwriting msg size from %"PRIu32" to %"PRIu32" words\n",
                        getpid(), cons_tail, prev_num_words, new_num_words);
              } else {
                fprintf(stderr, "%d: Invalidating frozen msg in cons section @%d, "
                                "keeping msg size at %"PRIu32" words\n",
                        getpid(), cons_tail, prev_num_words);
              }
              /* In case another worker also changed prod_tail, what we just
               * committed will be skipped over. */
              break;
            }
          } else {
            fprintf(stderr, "%d: Cannot clock_gettime: %s\n",
                    getpid(), strerror(errno));
          }
        }
        release_cpu();
      }
    }
  }

  //printf("dequeue commit, set const_tail=%"PRIu32" while prod_head=%"PRIu32"\n", tx->next, rbf->prod_head);
  atomic_store(&rbf->cons_tail, tx->next);
  //print_rb(rb);

# endif
}

ssize_t ringbuf_read_first(struct ringbuf *rb, struct ringbuf_tx *tx)
{
  struct ringbuf_file *rbf = rb->rbf;

  tx->seen = 0; // unused
  tx->record_start = 0;
  uint32_t num_words = atomic_load(rbf->data + (tx->record_start ++));
  if (num_words == 0) return -1;
  tx->next = tx->record_start + num_words;
  /*printf("read_first: num_words=%"PRIu32", record_start=%"PRIu32", next=%"PRIu32"\n",
         num_words, tx->record_start, tx->next);*/
  // Sanity checks:
  if (num_words == UINT32_MAX || num_words >= rbf->num_words) return -2;
  return num_words*sizeof(uint32_t);
}

ssize_t ringbuf_read_next(struct ringbuf *rb, struct ringbuf_tx *tx)
{
  struct ringbuf_file *rbf = rb->rbf;

  ASSERT_RB(tx->record_start < tx->next); // Or we have read the whole of it already
  if (tx->next == rbf->num_words) return 0; // Same as EOF
  uint32_t num_words = atomic_load(rbf->data + tx->next);
  if (num_words == 0) return -1; // new file past the prod cursor
  if (num_words == UINT32_MAX) return 0; // EOF
  // Has to be tested *after* EOF:
  if (tx->next >= atomic_load(&rbf->prod_tail)) return -1; // Have to wait
  tx->record_start = tx->next + 1;
  tx->next = (tx->record_start + num_words) % rbf->num_words;
  /*printf("read_next: record_start=%"PRIu32", next=%"PRIu32"\n",
         tx->record_start, tx->next);
  fflush(stdout);*/
  return num_words*sizeof(uint32_t);
}

static bool really_are_different(uint32_t _Atomic *a, uint32_t _Atomic *b)
{
  uint32_t d = atomic_load(a) - atomic_load(b);
  if (d == 0) return false;

  unsigned loops;
  for (loops = 0; loops < ASSUME_KIA_AFTER; loops ++) {
      if (atomic_load(a) - atomic_load(b) != d) return false;
      release_cpu();
  }

  time_t now = time(NULL);
  struct tm const *tm = localtime(&now);
  fprintf(stderr,
    "%d: %04d-%02d-%02d %02d:%02d:%02d, "
    "waited in really_are_different for %u loops; has a reader/writer died?\n",
    getpid(),
    tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
    tm->tm_hour, tm->tm_min, tm->tm_sec,
    loops);
  fflush(stderr);

  return true;
}

/* At start we suppose the RB has head=tail for both prod and cons.
 * If not we force it so. Be wary that some writers/readers might have
 * been started already. */
bool ringbuf_repair(struct ringbuf *rb)
{
  struct ringbuf_file *rbf = rb->rbf;
  bool was_needed = false;

  // Avoid writing in this mmaped page for no good reason:
  if (really_are_different(&rbf->prod_head, &rbf->prod_tail)) {
    atomic_store(&rbf->prod_head, atomic_load(&rbf->prod_tail));
    was_needed = true;
  }

  if (really_are_different(&rbf->cons_head, &rbf->cons_tail)) {
    atomic_store(&rbf->cons_head, atomic_load(&rbf->cons_tail));
    was_needed = true;
  }

  // Just in case, clear the lock as well:
  if (was_needed) {
    atomic_flag_clear_explicit(&rbf->lock, memory_order_release);
  }

  return was_needed;
}
