#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <fcntl.h>
#include <assert.h>
#include <errno.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <sched.h>

#include "ringbuf.h"

extern inline uint32_t ringbuf_file_nb_entries(struct ringbuf_file const *rb, uint32_t, uint32_t);
extern inline uint32_t ringbuf_file_nb_free(struct ringbuf_file const *rb, uint32_t, uint32_t);

extern inline void ringbuf_enqueue_commit(struct ringbuf *rb, struct ringbuf_tx const *tx, double t_start, double t_stop);
extern inline int ringbuf_enqueue(struct ringbuf *rb, uint32_t const *data, uint32_t nb_words, double t_start, double t_stop);

extern inline ssize_t ringbuf_dequeue_alloc(struct ringbuf *rb, struct ringbuf_tx *tx);
extern inline void ringbuf_dequeue_commit(struct ringbuf *rb, struct ringbuf_tx const *tx);
extern inline ssize_t ringbuf_dequeue(struct ringbuf *rb, uint32_t *data, size_t max_size);
extern inline bool ringbuf_repair(struct ringbuf *rb);
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
        return -1;
      }
    } else {
      ws += ss;
    }
  }
  return 0;
}

static void rand_printable_chars(char *dst, size_t len)
{
  static char chrs[] = "abcdefghijklmnopqrstuvwxyz"
                       "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                       "0123456789-_"; // 64 chars
  while (len--) dst[len] = chrs[rand() % (sizeof(chrs)-1)];
}

static int read_max_seqnum(char const *bname, uint64_t *first_seq)
{
  int ret = -1;

  char fname[PATH_MAX];
  if ((size_t)snprintf(fname, PATH_MAX, "%s.per_seq/max", bname) >= PATH_MAX) {
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
      goto err1;
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
  return ret;
}

static int write_max_seqnum(char const *bname, uint64_t seqnum)
{
  int ret = -1;

  char fname[PATH_MAX];
  // Save the new sequence number:
  if ((size_t)snprintf(fname, PATH_MAX, "%s.per_seq/max", bname) >= PATH_MAX) {
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

  ret = 0;

err1:
  if (0 != close(fd)) {
    fprintf(stderr, "Cannot close sequence file '%s': %s\n",
            fname, strerror(errno));
    ret = -1;
  }
err0:
  return ret;
}

// WARNING: If only_if_exist and the lock does not exist, this returns 0.
static int lock(char const *rb_fname, int operation /* LOCK_SH|LOCK_EX */, bool only_if_exist)
{
  char fname[PATH_MAX];
  if ((size_t)snprintf(fname, sizeof(fname), "%s.lock", rb_fname) >= sizeof(fname)) {
    fprintf(stderr, "Archive lockfile name truncated: '%s'\n", fname);
    return -1;
  }

  int fd = open(fname, only_if_exist ? 0 : O_CREAT, S_IRUSR|S_IWUSR);
  if (fd < 0) {
    if (errno == ENOENT && only_if_exist) return 0;
    fprintf(stderr, "Cannot create '%s': %s\n", fname, strerror(errno));
    return -1;
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
    return -1;
  }

  return fd;
}

static int unlock(int lock_fd)
{
  if (0 == lock_fd) {
    // Assuming lock didn't exist rather than locking stdin:
    return 0;
  }

  if (0 != close(lock_fd)) {
    fprintf(stderr, "Cannot unlock fd %d: %s\n", lock_fd, strerror(errno));
    return -1;
  }
  return 0;
}

// Keep existing files as much as possible:
extern int ringbuf_create_locked(bool wrap, char const *fname, uint32_t nb_words)
{
  int ret = -1;
  struct ringbuf_file rbf;

  // First try to create the file:
  int fd = open(fname, O_WRONLY|O_CREAT|O_EXCL, S_IRUSR|S_IWUSR);
  if (fd >= 0) {
    // We are the creator. Other creators are waiting for the lock.
    //printf("Creating ringbuffer '%s'\n", fname);

    size_t file_length = sizeof(rbf) + nb_words*sizeof(uint32_t);
    if (ftruncate(fd, file_length) < 0) {
      fprintf(stderr, "Cannot ftruncate file '%s': %s\n", fname, strerror(errno));
      goto err3;
    }

    if (0 != read_max_seqnum(fname, &rbf.first_seq)) goto err3;

    rbf.nb_words = nb_words;
    rbf.prod_head = rbf.prod_tail = 0;
    rbf.cons_head = rbf.cons_tail = 0;
    rbf.nb_allocs = 0;
    rbf.wrap = wrap;

    if (0 != really_write(fd, &rbf, sizeof(rbf), fname)) {
      goto err3;
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
  return ret;
}

extern int ringbuf_create(bool wrap, char const *fname, uint32_t nb_words)
{
  int ret = -1;

  // We must not try to create a RB while another process is rotating or
  // creating it:
  int lock_fd = lock(fname, LOCK_EX, false);
  if (lock_fd < 0) goto err0;

  if (0 != ringbuf_create_locked(wrap, fname, nb_words)) {
    goto err1;
  }

  ret = 0;

err1:
  if (0 != unlock(lock_fd)) ret = -1;
err0:
  return ret;
}

static bool check_header_eq(char const *fname, char const *what, unsigned expected, unsigned actual)
{
  if (expected == actual) return true;

  fprintf(stderr, "Invalid ring buffer file '%s': %s should be %u but is %u\n",
          fname, what, expected, actual);
  return false;
}

static bool check_header_max(char const *fname, char const *what, unsigned max, unsigned actual)
{
  if (actual < max) return true;

  fprintf(stderr, "Invalid ring buffer file '%s': %s (%u) should be < %u\n",
          fname, what, actual, max);
  return false;
}

static int mmap_rb(struct ringbuf *rb)
{
  int ret = -1;

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
        check_header_eq(rb->fname, "file size", rbf->nb_words*sizeof(uint32_t) + sizeof(*rbf), file_length) &&
        check_header_max(rb->fname, "prod head", rbf->nb_words, rbf->prod_head) &&
        check_header_max(rb->fname, "prod tail", rbf->nb_words, rbf->prod_tail) &&
        check_header_max(rb->fname, "cons head", rbf->nb_words, rbf->cons_head) &&
        check_header_max(rb->fname, "cons tail", rbf->nb_words, rbf->cons_tail)
  )) {
    munmap(rbf, file_length);
    goto err1;
  }

  rb->rbf = rbf;
  rb->mmapped_size = file_length;
  ret = 0;

err1:
  if (close(fd) < 0) {
    fprintf(stderr, "Cannot close ring-buffer(2) '%s': %s\n", rb->fname, strerror(errno));
    // so be it
  }
err0:
  return ret;
}

extern int ringbuf_load(struct ringbuf *rb, char const *fname)
{
  int ret = -1;

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

  if (0 != mmap_rb(rb)) {
    ret = -1;
    goto err1;
  }

  ret = 0;

err1:
  if (0 != unlock(lock_fd)) ret = -1;
err0:
  return ret;
}

int ringbuf_unload(struct ringbuf *rb)
{
  if (rb->rbf) {
    if (0 != munmap(rb->rbf, rb->mmapped_size)) {
      fprintf(stderr, "Cannot munmap: %s\n", strerror(errno));
      return -1;
    }
    rb->rbf = NULL;
  }
  rb->mmapped_size = 0;
  return 0;
}

// Called with the lock
static int rotate_file(struct ringbuf *rb)
{
  // Signal the EOF
  rb->rbf->data[rb->rbf->prod_head] = UINT32_MAX;

  int ret = -1;

  uint64_t last_seq = rb->rbf->first_seq + rb->rbf->nb_allocs;
  if (0 != write_max_seqnum(rb->fname, last_seq)) goto err0;

  // Note: let's use different subdirs per_seq/per_time etc for different
  // indexes so that directories are smaller when searching:
  char arc_fname[PATH_MAX];
  if ((size_t)snprintf(arc_fname, PATH_MAX, "%s.per_seq/%016"PRIx64"-%016"PRIx64".b",
                       rb->fname, rb->rbf->first_seq, last_seq) >= PATH_MAX) {
    fprintf(stderr, "Archive file name truncated: '%s'\n", arc_fname);
    goto err0;
  }

  // Rename the current rb into the archival name.
  printf("Rename the current rb (%s) into the archive (%s)\n",
         rb->fname, arc_fname);
  if (0 != rename(rb->fname, arc_fname)) {
    fprintf(stderr, "Cannot rename full buffer '%s' into '%s': %s\n",
            rb->fname, arc_fname, strerror(errno));
    goto err0;
  }

  // Also link time indexed file to the archive.
  // NOTE: There can be several files with same time range so suffix
  //       with a random string.
  char rstr[8+1];
  rand_printable_chars(rstr, sizeof(rstr)-1);
  rstr[sizeof(rstr)-1] = '\0';
  char arc2_fname[PATH_MAX];
  if ((size_t)snprintf(arc2_fname, PATH_MAX, "%s.per_time/%a-%a.%s.b",
                       rb->fname, rb->rbf->tmin, rb->rbf->tmax, rstr) >= PATH_MAX) {
    fprintf(stderr, "Archive file name truncated: '%s'\n", arc2_fname);
    goto err0;
  }
  printf("Link to time archive (%s)\n", arc2_fname);
  if (0 != link(arc_fname, arc2_fname)) {
    int ret = -1;
    if (ENOENT == errno) {
      if (0 != mkdir_for_file(arc2_fname)) goto err0;
      ret = link(arc_fname, arc2_fname); // retry
    }
    if (ret < 0) {
      fprintf(stderr, "Cannot create '%s': %s\n", arc2_fname, strerror(errno));
      goto err0;
    }
  }

  // Create a new buffer file under the same old name:
  printf("Create a new buffer file under the same old name '%s'\n", rb->fname);
  if (0 != ringbuf_create_locked(rb->rbf->wrap, rb->fname, rb->rbf->nb_words)) {
    goto err0;
  }

  ret = 0;
err0:
  return ret;
}

static int may_rotate(struct ringbuf *rb, uint32_t nb_words)
{
  struct ringbuf_file *rbf = rb->rbf;
  if (rbf->wrap) return 0;

  uint32_t const needed = 1 /* msg size */ + nb_words + 1 /* EOF */;
  if (ringbuf_file_nb_free(rbf, rbf->cons_tail, rbf->prod_head) >= needed)
    return 0;

  printf("Rotating buffer '%s'!\n", rb->fname);

  int ret = -1;

  // We have filled the non-wrapping buffer: rotate the file!
  // We need a lock to ensure no other writers is rotating at the same time
  int lock_fd = lock(rb->fname, LOCK_EX, false);
  if (lock_fd < 0) goto err0;

  // Wait, maybe some other process rotated the file already while we were
  // waiting for that lock? In that case it would have written the EOF:
  if (rb->rbf->data[rb->rbf->prod_head] != UINT32_MAX) {
    if (0 != rotate_file(rb)) goto err1;
  } else {
    printf("...actually not, someone did already.\n");
  }

  // Unmap rb
  printf("Unmap rb\n");
  if (0 != ringbuf_unload(rb)) goto err1;

  // Mmap the new file and update rbf.
  printf("Mmap the new file and update rbr and rb\n");
  if (0 != mmap_rb(rb)) goto err1;

  ret = 0;

err1:
  if (0 != unlock(lock_fd)) ret = -1;
  // Too bad we cannot unlink that lockfile without a race condition
err0:
  return ret;
}

/* ringbuf will have:
 *  word n: nb_words
 *  word n+1..n+nb_words: allocated.
 *  tx->record_start will point at word n+1 above. */
extern int ringbuf_enqueue_alloc(struct ringbuf *rb, struct ringbuf_tx *tx, uint32_t nb_words)
{
  uint32_t cons_tail;
  uint32_t need_eof = 0;  // 0 never needs an EOF

  if (may_rotate(rb, nb_words) < 0) return -1;

  struct ringbuf_file *rbf = rb->rbf;

  do {
    tx->seen = rbf->prod_head;
    cons_tail = rbf->cons_tail;
    tx->record_start = tx->seen;
    // We will write the size then the data:
    tx->next = tx->record_start + 1 + nb_words;
    uint32_t alloced = 1 + nb_words;

    // Avoid wrapping inside the record
    if (tx->next > rbf->nb_words) {
      need_eof = tx->seen;
      alloced += rbf->nb_words - tx->seen;
      tx->record_start = 0;
      tx->next = 1 + nb_words;
      assert(tx->next < rbf->nb_words);
    } else if (tx->next == rbf->nb_words) {
      printf("tx->next == rbf->nb_words\n");
      tx->next = 0;
    }

    // Enough room?
    if (ringbuf_file_nb_free(rbf, cons_tail, tx->seen) <= alloced) {
      /*printf("Ringbuf is full, cannot alloc for enqueue %"PRIu32"/%"PRIu32" tot words, seen=%"PRIu32", cons_tail=%"PRIu32", nb_free=%"PRIu32"\n",
             alloced, rbf->nb_words, tx->seen, cons_tail, ringbuf_file_nb_free(rbf, cons_tail, tx->seen));*/
      return -1;
    }

  } while (!  atomic_compare_exchange_strong(&rbf->prod_head, &tx->seen, tx->next));

  if (need_eof) rbf->data[need_eof] = UINT32_MAX;
  rbf->data[tx->record_start ++] = nb_words;

  return 0;
}
