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

extern inline uint32_t ringbuf_nb_entries(struct ringbuf const *rb);
extern inline uint32_t ringbuf_free_entries(struct ringbuf const *rb);
extern inline int ringbuf_enqueue(struct ringbuf *rb, uint32_t const *data, uint32_t nb_words);
extern inline int ringbuf_dequeue(struct ringbuf *rb, uint32_t *data, uint32_t max_nb_words);

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

  rb->nb_words_mask = tot_words - 1;
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

  // Sanity check
  if (rb->nb_words_mask*sizeof(uint32_t) + sizeof(*rb) != (size_t)file_length ||
      rb->prod_head >= rb->nb_words_mask ||
      rb->prod_tail >= rb->nb_words_mask ||
      rb->cons_head >= rb->nb_words_mask ||
      rb->cons_tail >= rb->nb_words_mask) {
    fprintf(stderr, "Invalid ring buffer file '%s': Bad header.\n", fname);
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
