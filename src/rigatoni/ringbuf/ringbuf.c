#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <errno.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>

#include "ringbuf.h"

extern inline uint32_t ringbuf_nb_entries(struct ringbuf const *rb, uint32_t, uint32_t);
extern inline uint32_t ringbuf_nb_free(struct ringbuf const *rb, uint32_t, uint32_t);

extern inline int ringbuf_enqueue_alloc(struct ringbuf *rb, struct ringbuf_tx *tx, uint32_t nb_words);
extern inline void ringbuf_enqueue_commit(struct ringbuf *rb, struct ringbuf_tx const *tx);
extern inline int ringbuf_enqueue(struct ringbuf *rb, uint32_t const *data, uint32_t nb_words);

extern inline ssize_t ringbuf_dequeue_alloc(struct ringbuf *rb, struct ringbuf_tx *tx);
extern inline void ringbuf_dequeue_commit(struct ringbuf *rb, struct ringbuf_tx const *tx);
extern inline ssize_t ringbuf_dequeue(struct ringbuf *rb, uint32_t *data, size_t max_size);

extern struct ringbuf *ringbuf_create(char const *fname, uint32_t tot_words)
{
  struct ringbuf *rb = NULL;

  int fd = open(fname, O_RDWR|O_CREAT|O_EXCL, S_IRUSR|S_IWUSR);
  if (fd < 0) {
    fprintf(stderr, "Cannot create ring-buffer in file '%s': %s\n", fname, strerror(errno));
    goto err0;
  }

  size_t file_length = sizeof(*rb) + tot_words*sizeof(uint32_t);

  if (ftruncate(fd, file_length) < 0) {
    fprintf(stderr, "Cannot ftruncate file '%s': %s\n", fname, strerror(errno));
    goto err1;
  }

  rb = mmap(NULL, file_length, PROT_READ|PROT_WRITE, MAP_SHARED, fd, (off_t)0);
  if (rb == MAP_FAILED) {
    fprintf(stderr, "Cannot mmap file '%s': %s\n", fname, strerror(errno));
    rb = NULL;
    goto err1;
  }

  rb->nb_words = tot_words;
  rb->prod_head = rb->prod_tail = 0;
  rb->cons_head = rb->cons_tail = 0;

err1:
  if (close(fd) < 0) {
    fprintf(stderr, "Cannot close file '%s': %s\n", fname, strerror(errno));
    // so be it
  }

err0:
  return rb;
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

extern struct ringbuf *ringbuf_load(char const *fname)
{
  struct ringbuf *rb = NULL;

  int fd = open(fname, O_RDWR, S_IRUSR|S_IWUSR);
  if (fd < 0) {
    fprintf(stderr, "Cannot load ring-buffer from file '%s': %s\n", fname, strerror(errno));
    goto err0;
  }

  off_t file_length = lseek(fd, 0, SEEK_END);
  if (file_length == (off_t)-1) {
    fprintf(stderr, "Cannot lseek into file '%s': %s\n", fname, strerror(errno));
    goto err1;
  }
  if ((size_t)file_length <= sizeof(*rb)) {
    fprintf(stderr, "Invalid ring buffer file '%s': Too small.\n", fname);
    goto err1;
  }

  rb = mmap(NULL, file_length, PROT_READ|PROT_WRITE, MAP_SHARED, fd, (off_t)0);
  if (rb == MAP_FAILED) {
    fprintf(stderr, "Cannot mmap file '%s': %s\n", fname, strerror(errno));
    rb = NULL;
    goto err1;
  }

  // Sanity checks
  if (!(
        check_header_eq(fname, "file size", rb->nb_words*sizeof(uint32_t) + sizeof(*rb), file_length) &&
        check_header_max(fname, "prod head", rb->nb_words, rb->prod_head) &&
        check_header_max(fname, "prod tail", rb->nb_words, rb->prod_tail) &&
        check_header_max(fname, "cons head", rb->nb_words, rb->cons_head) &&
        check_header_max(fname, "cons tail", rb->nb_words, rb->cons_tail)
  )) {
    munmap(rb, file_length);
    rb = NULL;
  }

err1:
  if (close(fd) < 0) {
    fprintf(stderr, "Cannot close file '%s': %s\n", fname, strerror(errno));
    // so be it
  }

err0:
  return rb;
}
