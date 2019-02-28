// vim: ft=c bs=2 ts=2 sts=2 sw=2 expandtab
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
#include "../config.h"
#include "archive.h"

extern inline uint32_t ringbuf_file_num_entries(struct ringbuf_file const *rb, uint32_t, uint32_t);
extern inline uint32_t ringbuf_file_num_free(struct ringbuf_file const *rb, uint32_t, uint32_t);

extern inline enum ringbuf_error ringbuf_enqueue(struct ringbuf *rb, uint32_t const *data, uint32_t num_words, double t_start, double t_stop);

extern inline ssize_t ringbuf_dequeue_alloc(struct ringbuf *rb, struct ringbuf_tx *tx);
extern inline ssize_t ringbuf_dequeue(struct ringbuf *rb, uint32_t *data, size_t max_size);
extern inline ssize_t ringbuf_read_first(struct ringbuf *rb, struct ringbuf_tx *tx);
extern inline ssize_t ringbuf_read_next(struct ringbuf *rb, struct ringbuf_tx *tx);

// Create the directories required to create that file:
static int mkdir_for_file(char *fname)
{
  int ret = -1;

  size_t len = strlen(fname);
  ssize_t last_slash;
  for (last_slash = len - 1; last_slash >= 0 && fname[last_slash] != '/'; last_slash--) ;

  if (last_slash <= 0) return 0; // no dir to create (or root)

  fname[last_slash] = '\0';
  if (0 != mkdir(fname, S_IRUSR|S_IWUSR|S_IXUSR)) {
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

  ret = 0;
err1:
  fname[last_slash] = '/';
  fflush(stdout);
  fflush(stderr);
  return ret;
}

static ssize_t really_read(int fd, void *d, size_t sz, char const *fname /* printed */)
{
  size_t rs = 0;
  while (rs < sz) {
    ssize_t ss = read(fd, d + rs, sz - rs);
    if (ss < 0) {
      if (errno != EINTR) {
        fprintf(stderr, "Cannot read '%s': %s\n", fname, strerror(errno));
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
        fprintf(stderr, "Cannot write '%s': %s\n", fname, strerror(errno));
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

  char fname[PATH_MAX];
  if ((size_t)snprintf(fname, PATH_MAX, "%s.arc/max", bname) >= PATH_MAX) {
    fprintf(stderr, "Archive max seq file name truncated: '%s'\n", fname);
    goto err0;
  }

  int fd = open(fname, O_RDWR|O_CREAT, S_IRUSR|S_IWUSR);
  if (fd < 0) {
    if (ENOENT == errno) {
      *first_seq = 0;
      ret = 0;
      goto err0;
    } else {
      fprintf(stderr, "Cannot create '%s': %s\n", fname, strerror(errno));
      goto err0;
    }
  } else {
    ssize_t rs = really_read(fd, first_seq, sizeof(*first_seq), fname);
    if (rs < 0) {
      goto err1;
    } else if (rs == 0) {
      *first_seq = 0;
    } else if ((size_t)rs < sizeof(first_seq)) {
      fprintf(stderr, "Too short a file for seqnum: %s\n", fname);
      goto err1;
    }
  }

  ret = 0;
err1:
  if (0 != close(fd)) {
    fprintf(stderr, "Cannot close sequence file '%s': %s\n",
            fname, strerror(errno));
    ret = -1;
  }
err0:
  fflush(stderr);
  return ret;
}

static int write_max_seqnum(char const *bname, uint64_t seqnum)
{
  int ret = -1;

  char fname[PATH_MAX];
  // Save the new sequence number:
  if ((size_t)snprintf(fname, PATH_MAX, "%s.arc/max", bname) >= PATH_MAX) {
    fprintf(stderr, "Archive max seq file name truncated: '%s'\n", fname);
    goto err0;
  }

  int fd = open(fname, O_WRONLY|O_CREAT, S_IRUSR|S_IWUSR);
  if (fd < 0) {
    if (ENOENT == errno) {
      if (0 != mkdir_for_file(fname)) return -1;
      fd = open(fname, O_WRONLY|O_CREAT, S_IRUSR|S_IWUSR); // retry
    }
    if (fd < 0) {
      fprintf(stderr, "Cannot create '%s': %s\n", fname, strerror(errno));
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
    fprintf(stderr, "Cannot fdatasync sequence file '%s': %s\n",
            fname, strerror(errno));
    // best effort
  }

  ret = 0;

err1:
  if (0 != close(fd)) {
    fprintf(stderr, "Cannot close sequence file '%s': %s\n",
            fname, strerror(errno));
    ret = -1;
  }
err0:
  fflush(stderr);
  return ret;
}

// WARNING: If only_if_exist and the lock does not exist, this returns 0.
static int lock(char const *rb_fname, int operation /* LOCK_SH|LOCK_EX */, bool only_if_exist)
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

static int unlock(int lock_fd)
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

// Keep existing files as much as possible:
extern int ringbuf_create_locked(
  uint64_t version, bool wrap, char const *fname, uint32_t num_words)
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
      fprintf(stderr, "Cannot ftruncate file '%s': %s\n", fname, strerror(errno));
      goto err3;
    }

    if (0 != read_max_seqnum(fname, &rbf.first_seq)) goto err3;

    rbf.version = version;
    rbf.num_words = num_words;
    atomic_init(&rbf.prod_head, 0);
    atomic_init(&rbf.prod_tail, 0);
    atomic_init(&rbf.cons_head, 0);
    atomic_init(&rbf.cons_tail, 0);
    atomic_init(&rbf.num_allocs, 0);
    atomic_init(&rbf.tmin, 0.);
    atomic_init(&rbf.tmax, 0.);
    rbf.wrap = wrap;

    if (0 != really_write(fd, &rbf, sizeof(rbf), fname)) {
      goto err3;
    }
    if (! wrap && 0 != fsync(fd)) {
      fprintf(stderr, "Cannot fsync ringbug file '%s': %s\n",
              fname, strerror(errno));
      // best effort
    }
  } else {
    if (errno == EEXIST) {
      ret = 0;
    } else {
      fprintf(stderr, "Cannot open ring-buffer '%s': %s\n", fname, strerror(errno));
    }
    goto err0;
  }

  ret = 0;

err3:
  if (ret != 0 && unlink(fname) < 0) {
    fprintf(stderr, "Cannot erase not-created ringbuf '%s': %s\n"
                    "Oh dear!\n", fname, strerror(errno));
  }
//err2:
  if (close(fd) < 0) {
    fprintf(stderr, "Cannot close ring-buffer(1) '%s': %s\n", fname, strerror(errno));
    // so be it
  }
err0:
  fflush(stderr);
  return ret;
}

extern enum ringbuf_error ringbuf_create(uint64_t version, bool wrap, uint32_t num_words, char const *fname)
{
  enum ringbuf_error err = RB_ERR_FAILURE;

  // We must not try to create a RB while another process is rotating or
  // creating it:
  int lock_fd = lock(fname, LOCK_EX, false);
  if (lock_fd < 0) goto err0;

  if (0 != ringbuf_create_locked(version, wrap, fname, num_words)) {
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

  fprintf(stderr, "Invalid ring buffer file '%s': %s should be %u but is %u\n",
          fname, what, expected, actual);
  fflush(stderr);
  return false;
}

static bool check_header_max(char const *fname, char const *what, unsigned max, unsigned actual)
{
  if (actual < max) return true;

  fprintf(stderr, "Invalid ring buffer file '%s': %s (%u) should be < %u\n",
          fname, what, actual, max);
  fflush(stderr);
  return false;
}

static enum ringbuf_error mmap_rb(uint64_t version, struct ringbuf *rb)
{
  enum ringbuf_error err = RB_ERR_FAILURE;

  int fd = open(rb->fname, O_RDWR, S_IRUSR|S_IWUSR);
  if (fd < 0) {
    fprintf(stderr, "Cannot load ring-buffer from file '%s': %s\n", rb->fname, strerror(errno));
    goto err0;
  }

  off_t file_length = lseek(fd, 0, SEEK_END);
  if (file_length == (off_t)-1) {
    fprintf(stderr, "Cannot lseek into file '%s': %s\n", rb->fname, strerror(errno));
    goto err1;
  }
  if ((size_t)file_length <= sizeof(*rb->rbf)) {
    fprintf(stderr, "Invalid ring buffer file '%s': Too small.\n", rb->fname);
    goto err1;
  }

  struct ringbuf_file *rbf =
      mmap(NULL, file_length, PROT_READ|PROT_WRITE, MAP_SHARED, fd, (off_t)0);
  if (rbf == MAP_FAILED) {
    fprintf(stderr, "Cannot mmap file '%s': %s\n", rb->fname, strerror(errno));
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
  err = RB_OK;

err1:
  if (close(fd) < 0) {
    fprintf(stderr, "Cannot close ring-buffer(2) '%s': %s\n", rb->fname, strerror(errno));
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
    fprintf(stderr, "Cannot load ring-buffer: Filename too long: %s\n", fname);
    goto err0;
  }
  memcpy(rb->fname, fname, fname_len + 1);
  rb->rbf = NULL;
  rb->mmapped_size = 0;

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
      fprintf(stderr, "Cannot munmap: %s\n", strerror(errno));
      fflush(stderr);
      return RB_ERR_FAILURE;
    }
    rb->rbf = NULL;
  }
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
  char arc_fname[PATH_MAX];
  if ((size_t)snprintf(arc_fname, PATH_MAX,
                       "%s.arc/%016"PRIx64"_%016"PRIx64"_%a_%a.b",
                       rb->fname, rb->rbf->first_seq, last_seq,
                       rb->rbf->tmin, rb->rbf->tmax) >= PATH_MAX) {
    fprintf(stderr, "Archive file name truncated: '%s'\n", arc_fname);
    goto err0;
  }

  // Rename the current rb into the archival name.
  //printf("Rename the current rb (%s) into the archive (%s)\n",
  //       rb->fname, arc_fname);
  if (0 != rename(rb->fname, arc_fname)) {
    fprintf(stderr, "Cannot rename full buffer '%s' into '%s': %s\n",
            rb->fname, arc_fname, strerror(errno));
    goto err0;
  }

  // Regardless of how this rotation went, we must not release the lock without
  // having created a new archive file:
  //printf("Create a new buffer file under the same old name '%s'\n", rb->fname);
  if (0 != ringbuf_create_locked(rb->rbf->version, rb->rbf->wrap,
                                 rb->fname, rb->rbf->num_words)) {
    goto err0;
  }

  ret = 0;

err0:
  fflush(stdout);
  fflush(stderr);
  return ret;
}

static enum ringbuf_error may_rotate(struct ringbuf *rb, uint32_t num_words)
{
  struct ringbuf_file *rbf = rb->rbf;
  if (rbf->wrap) return RB_OK;

  uint32_t const needed = 1 /* msg size */ + num_words + 1 /* EOF */;
  uint32_t const free = ringbuf_file_num_free(rbf, rbf->cons_tail, rbf->prod_head);
  if (free >= needed) {
    if (atomic_load(rbf->data + atomic_load(&rbf->prod_head)) == UINT32_MAX) {
      // Another writer might have "closed" this ringbuf already, that's OK.
      // But we still must be close to the actual end, otherwise complain:
      if (free > 2 * needed) {
        fprintf(stderr,
                "Enough place for a new record (%"PRIu32" words, "
                "and %"PRIu32" free) but EOF mark is set\n", needed, free);
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

  do {
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
      return RB_ERR_NO_MORE_ROOM;
    }

  } while (! atomic_compare_exchange_weak(&rbf->prod_head, &tx->seen, tx->next));

  if (need_eof) atomic_store(rbf->data + need_eof, UINT32_MAX);
  atomic_store(rbf->data + (tx->record_start ++), num_words);

  return RB_OK;
}

static struct timespec const quick = { .tv_sec = 0, .tv_nsec = 666 };

void ringbuf_enqueue_commit(struct ringbuf *rb, struct ringbuf_tx const *tx, double t_start, double t_stop)
{
  struct ringbuf_file *rbf = rb->rbf;

  if (t_start > t_stop) {
    double tmp = t_start;
    t_start = t_stop;
    t_stop = tmp;
  }

  // Update the prod_tail to match the new prod_head.
  // First, wait until the prod_tail reach the head we observed (ie.
  // previously allocated records have been committed).
  unsigned num_loops = 0;
  uint32_t init_prod_tail = rbf->prod_tail;

  while (atomic_load_explicit(&rbf->prod_tail, memory_order_acquire) != tx->seen) {
    num_loops ++;
    nanosleep(&quick, NULL);
    //sched_yield();
  }
# define MAX_WAIT_LOOP 1000
  if (num_loops > MAX_WAIT_LOOP) {
    PRINT_RB(rb,
      "waited for prod_tail to advance from %"PRIu32" to %"PRIu32
      " for %u loops; has another writer died?\n",
      init_prod_tail, tx->seen, num_loops);
  }

  // Here our record is the next. In theory, next writers are now all
  // waiting for us.

  //printf("enqueue commit, set prod_tail=%"PRIu32" while cons_head=%"PRIu32"\n", tx->next, rbf->cons_head);
  ASSERT_RB(ringbuf_file_num_entries(rbf, tx->next, rbf->cons_head) > 0);
  // All we need is for the following prod_tail change to always
  // be visible after the changes to num_allocs and min/max observed t:
  uint32_t prev_num_allocs = atomic_fetch_add_explicit(&rbf->num_allocs, 1, memory_order_relaxed);
  if (t_start > 0. || t_stop > 0.) {
    double tmin = atomic_load_explicit(&rbf->tmin, memory_order_relaxed);
    double tmax = atomic_load_explicit(&rbf->tmax, memory_order_relaxed);
    if (0 == prev_num_allocs || t_start < tmin)
        atomic_store_explicit(&rbf->tmin, t_start, memory_order_relaxed);
    if (0 == prev_num_allocs || t_stop > tmax)
        atomic_store_explicit(&rbf->tmax, t_stop, memory_order_relaxed);
  }
  atomic_store_explicit(&rbf->prod_tail, tx->next, memory_order_release);
  //print_rb(rb);

# ifdef NEED_DATA_CACHE_FLUSH
  my_cacheflush(rbf->data + tx->record_start, (tx->next - tx->record_start) * sizeof(rbf->data[0]));
# endif
}

void ringbuf_dequeue_commit(struct ringbuf *rb, struct ringbuf_tx const *tx)
{
  struct ringbuf_file *rbf = rb->rbf;

  unsigned num_loops = 0;
  uint32_t const init_cons_tail = rbf->cons_tail;
  while (rbf->cons_tail != tx->seen) {
    num_loops ++;
    nanosleep(&quick, NULL);
    //sched_yield();
  }
  if (num_loops > MAX_WAIT_LOOP) {
    PRINT_RB(rb,
      "waited for cons_tail to advance from %"PRIu32" to %"PRIu32
      " for %u loops; has another reader died?\n",
      init_cons_tail, tx->seen, num_loops);
  }

  //printf("dequeue commit, set const_taill=%"PRIu32" while prod_head=%"PRIu32"\n", tx->next, rbf->prod_head);
  rbf->cons_tail = tx->next;
  //print_rb(rb);
}

static bool really_is_different(uint32_t _Atomic *a, uint32_t _Atomic *b)
{
  uint32_t d = atomic_load(a) - atomic_load(b);
  if (d == 0) return false;

  for (unsigned try = 0; try < MAX_WAIT_LOOP; try ++) {
      if (atomic_load(a) - atomic_load(b) != d) return false;
      nanosleep(&quick, NULL);
  }

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
  if (really_is_different(&rbf->prod_head, &rbf->prod_tail)) {
    atomic_store(&rbf->prod_head, atomic_load(&rbf->prod_tail));
    was_needed = true;
  }

  if (really_is_different(&rbf->cons_head, &rbf->cons_tail)) {
    atomic_store(&rbf->cons_head, atomic_load(&rbf->cons_tail));
    was_needed = true;
  }

  return was_needed;
}
