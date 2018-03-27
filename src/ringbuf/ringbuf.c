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
extern inline ssize_t ringbuf_read_first(struct ringbuf *rb, struct ringbuf_tx *tx);
extern inline ssize_t ringbuf_read_next(struct ringbuf *rb, struct ringbuf_tx *tx);

// Read the amount of bytes requested
static int really_read(int fd, void *dest, size_t sz)
{
  while (sz > 0) {
    ssize_t r = read(fd, dest, sz);
    if (r < 0) return -1;
    else if (r == 0) sched_yield();
    else {
      sz -= r;
      dest = (char *)dest + r;
    }
  }
  return 0;
}

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
    ssize_t rs = read(fd, first_seq, sizeof(*first_seq));
    if (rs < 0) {
      fprintf(stderr, "Cannot read '%s': %s\n", fname, strerror(errno));
      goto err1;
    } else if (rs == 0) {
      *first_seq = 0;
    } else if ((size_t)rs < sizeof(first_seq)) {
      assert(false);
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

  ssize_t ss = write(fd, &seqnum, sizeof(seqnum));
  if (ss < 0) {
    fprintf(stderr, "Cannot write '%s': %s\n", fname, strerror(errno));
    goto err1;
  } else if (ss != sizeof(seqnum)) {
    assert(false);
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

// Keep existing files as much as possible:
extern int ringbuf_create(bool wrap, char const *fname, uint32_t nb_words)
{
  int ret = -1;
  struct ringbuf_file rbf;

  // First try to create the file:
  int fd = open(fname, O_WRONLY|O_CREAT|O_EXCL, S_IRUSR|S_IWUSR);
  if (fd >= 0) {
    // We are the creator. Other processes are waiting for that file
    // to have a non-zero size and an nb_words greater than 0:
    //printf("Creating ringbuffer '%s'\n", fname);

    size_t file_length = sizeof(rbf) + nb_words*sizeof(uint32_t);
    if (ftruncate(fd, file_length) < 0) {
err2:
      fprintf(stderr, "Cannot ftruncate file '%s': %s\n", fname, strerror(errno));
      if (unlink(fname) < 0) {
        fprintf(stderr, "Cannot erase not-created ringbuf '%s': %s\n"
                        "Oh dear!\n", fname, strerror(errno));
      }
      goto err1;
    }

    if (0 != read_max_seqnum(fname, &rbf.first_seq)) goto err1;

    rbf.nb_words = nb_words;
    rbf.prod_head = rbf.prod_tail = 0;
    rbf.cons_head = rbf.cons_tail = 0;
    rbf.nb_allocs = 0;
    rbf.wrap = wrap;

    ssize_t w = write(fd, &rbf, sizeof(rbf));
    if (w < 0) {
      fprintf(stderr, "Cannot write ring buffer header in '%s': %s\n",
              fname, strerror(errno));
      goto err2;
    } else if ((size_t)w < sizeof(rbf)) {
      fprintf(stderr, "Cannot write the whole ring buffer header in '%s'",
              fname);
      goto err2;
    }
  } else if (fd < 0 && errno == EEXIST) {
    //printf("Opening existing ringbuffer '%s'\n", fname);

    // Wait for the file to have a size > 0 and nb_words > 0
    fd = open(fname, O_RDONLY);
    if (fd < 0) {
      fprintf(stderr, "Cannot open ring-buffer '%s': %s\n", fname, strerror(errno));
      goto err0;
    }
    if (0 != really_read(fd, &rbf, sizeof(rbf))) {
      fprintf(stderr, "Cannot read ring-buffer '%s': %s\n", fname, strerror(errno));
      goto err1;
    }
  } else {
    fprintf(stderr, "Cannot open ring-buffer '%s': %s\n", fname, strerror(errno));
    goto err0;
  }

  ret = 0;
err1:
  if (close(fd) < 0) {
    fprintf(stderr, "Cannot close ring-buffer '%s': %s\n", fname, strerror(errno));
    // so be it
  }

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
    fprintf(stderr, "Cannot close ring-buffer '%s': %s\n", rb->fname, strerror(errno));
    // so be it
  }

err0:
  return ret;
}

extern int ringbuf_load(struct ringbuf *rb, char const *fname)
{
  size_t fname_len = strlen(fname);
  if (fname_len + 1 > sizeof(rb->fname)) {
    fprintf(stderr, "Cannot load ring-buffer: Filename too long: %s\n", fname);
    return -1;
  }
  memcpy(rb->fname, fname, fname_len + 1);
  rb->rbf = NULL;
  rb->mmapped_size = 0;

  return mmap_rb(rb);
}

int ringbuf_unload(struct ringbuf *rb)
{
  assert(rb->rbf);
  if (0 != munmap(rb->rbf, rb->mmapped_size)) {
    fprintf(stderr, "Cannot munmap: %s\n", strerror(errno));
    return -1;
  }

  rb->rbf = NULL;
  rb->mmapped_size = 0;
  return 0;
}

static int lock(struct ringbuf *rb)
{
  char fname[PATH_MAX];
  if ((size_t)snprintf(fname, sizeof(fname), "%s.lock", rb->fname) >= sizeof(fname)) {
    fprintf(stderr, "Archive lockfile name truncated: '%s'\n", fname);
    return -1;
  }

  int fd = open(fname, O_CREAT|O_EXLOCK, S_IRUSR|S_IWUSR);
  if (fd < 0) {
    fprintf(stderr, "Cannot lock '%s': %s\n", fname, strerror(errno));
    return -1;
  }

  return fd;
}

static int may_rotate(struct ringbuf *rb, uint32_t nb_words)
{
  if (rb->rbf->wrap) return 0;
  // The case rb->rbf->prod_head + 1 + nb_words == rbf->nb_words
  // would *not* work because the RB would be considered full
  // (although there would be enough room for the record)
  if (rb->rbf->prod_head + 1 + nb_words < rb->rbf->nb_words)
    return 0;

  // Signal the EOF
  rb->rbf->data[rb->rbf->prod_head] = UINT32_MAX;

  printf("Rotating buffer '%s'!\n", rb->fname);

  int ret = -1;

  // We have filled the non-wrapping buffer: rotate the file!
  // We need a lock to ensure no other writers is rotating at the same time
  int lock_fd = lock(rb);
  if (lock_fd < 0) goto err0;

  uint64_t last_seq = rb->rbf->first_seq + rb->rbf->nb_allocs;
  if (0 != write_max_seqnum(rb->fname, last_seq)) goto unlock;

  // Note: let's use different subdirs per_seq/per_time etc for different
  // indexes so that directories are smaller when searching:
  char arc_fname[PATH_MAX];
  if ((size_t)snprintf(arc_fname, PATH_MAX, "%s.per_seq/%016"PRIx64"-%016"PRIx64".b",
                       rb->fname, rb->rbf->first_seq, last_seq) >= PATH_MAX) {
    fprintf(stderr, "Archive file name truncated: '%s'\n", arc_fname);
    goto unlock;
  }

  // Rename the current rb into the archival name.
  printf("Rename the current rb (%s) into the archive (%s)\n",
         rb->fname, arc_fname);
  if (0 != rename(rb->fname, arc_fname)) {
    fprintf(stderr, "Cannot rename full buffer '%s' into '%s': %s\n",
            rb->fname, arc_fname, strerror(errno));
    goto unlock;
  }

  // Also link time indexed file to the archive.
  // NOTE: that there can be several files with same time range so suffix
  // with a random string
  char rstr[8+1];
  rand_printable_chars(rstr, sizeof(rstr)-1);
  rstr[sizeof(rstr)-1] = '\0';
  char arc2_fname[PATH_MAX];
  if ((size_t)snprintf(arc2_fname, PATH_MAX, "%s.per_time/%a-%a.%s.b",
                       rb->fname, rb->rbf->tmin, rb->rbf->tmax, rstr) >= PATH_MAX) {
    fprintf(stderr, "Archive file name truncated: '%s'\n", arc2_fname);
    goto unlock;
  }
  printf("Link to time archive (%s)\n", arc2_fname);
  if (0 != link(arc_fname, arc2_fname)) {
    int ret = -1;
    if (ENOENT == errno) {
      if (0 != mkdir_for_file(arc2_fname)) goto unlock;
      ret = link(arc_fname, arc2_fname); // retry
    }
    if (ret < 0) {
      fprintf(stderr, "Cannot create '%s': %s\n", arc2_fname, strerror(errno));
      goto unlock;
    }
  }

  // Create a new buffer file under the same old name:
  printf("Create a new buffer file under the same old name\n");
  if (0 != ringbuf_create(rb->rbf->wrap, rb->fname, rb->rbf->nb_words))
    goto unlock;
  // Nmap rb
  printf("Unmap rb\n");
  if (0 != ringbuf_unload(rb)) goto unlock;
  // Mmap the new file and update rbf.
  printf("Mmap the new file and update rbr and rb\n");
  if (0 != mmap_rb(rb)) goto unlock;

  ret = 0;

unlock:
  if (0 != close(lock_fd)) {
    fprintf(stderr, "Cannot unlock fd %d: %s\n", lock_fd, strerror(errno));
    ret = -1;
  }
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
