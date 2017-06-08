/* Ring buffer implementation for Ramen events.
 * Characteristics:
 * - possibly multiple writers but single writer most of the times;
 *
 * - possibly multiple readers but single reader most of the times; When there
 * are several readers we may want each reader to see each tuple or each tuple
 * to be read only once. For the former we will merely use several ring buffer
 * since it's much easier and avoid non-trivial inter blockages.
 *
 * - variable length messages;
 *
 * - the ring buffer is a memory mapped file used for interprocess
 * communications;
 *
 * - when there are multiple readers/writers they are in different processes.
 *
 * Inspired by DPDK ring library, same implementation and same terminology
 * whenever possible.
 */

#ifndef RINGBUF_H_20170606
#define RINGBUF_H_20170606

#include <assert.h>
#include <stdint.h>
#include <stdbool.h>
#include <errno.h>
#include <stdatomic.h>
#include <string.h>
#include <sched.h>
#include <limits.h>

/* All tuple fields must be ordered so that we do not have to specify this
 * ordering from the code of the operations.
 * We choose to order them according to field name. */
enum tuple_type_float {
  TUPLE_FLOAT,  // Standard 64bits floats
  TUPLE_STRING, // The only variable length data type. Prefixed with length
  TUPLE_U32,
  TUPLE_U64,
  TUPLE_U128,
  TUPLE_I32,
  TUPLE_I64,
  TUPLE_I128,
};

struct tuple_field {
  enum tuple_type_float type;
  char field_name[];  // varsized, nul terminated.
};

struct ringbuf {
  // Fixed length of the ring buffer. mmapped file msut be >= this.
  uint32_t nb_words_mask;
  /* Pointers to entries. We use uint32 indexes so that we do not have
   * to worry too much about modulos. */
  /* Bytes that are being added by producers lie between prod_tail and
   * prod_head. prod_tail points after the last written byte. */
  volatile uint32_t _Atomic prod_head;
  volatile uint32_t prod_tail;
  /* Bytes that are being read by consumers are between cons_tail and
   * cons_head. cons_tail points on the next bytes to read. */
  volatile uint32_t _Atomic cons_head;
  volatile uint32_t cons_tail;
  /* Now this file is made of tuples which format is declared here: first
   * the number of fields in our tuple, then for each fields its type: */
  unsigned tuple_nb_fields;
  uint8_t tuple_field_types[256]; // Undefined after tuple_nb_fields.
  /* The actual tuples start here: */
  uint32_t data[];
};

inline uint32_t ringbuf_nb_entries(struct ringbuf const *rb)
{
  return rb->prod_tail - rb->cons_head;
}

inline uint32_t ringbuf_free_entries(struct ringbuf const *rb)
{
  return rb->nb_words_mask + rb->cons_tail - rb->prod_head;
}

inline bool ringbuf_full(uint32_t head, uint32_t tail, uint32_t next)
{
    return tail < next && next < head ||
           head < tail && tail < next;
}

inline int ringbuf_enqueue(struct ringbuf *rb, uint32_t const *data, uint32_t nb_words)
{
  uint32_t prod_head, cons_tail, prod_next;
  bool cas_ok;

  do {
    prod_head = rb->prod_head;
    cons_tail = rb->cons_tail;
    // We will write the size then the data:
    prod_next = (prod_head + 1 + nb_words) & rb->nb_words_mask;

    // Enough room?
    if (ringbuf_full(prod_head, cons_tail, prod_next)) {
      return -ENOBUFS;
    }

    cas_ok = atomic_compare_exchange_strong(&rb->prod_head, &prod_head, prod_next);
  } while (! cas_ok);

  // We got the space, now copy the data:
  rb->data[prod_head] = nb_words;
  memcpy(rb->data + prod_head + 1, data, nb_words*sizeof(*data));

  // Update the prod_tail to match the new prod_head.
  while (rb->prod_tail != prod_head) sched_yield();
  rb->prod_tail = prod_next;

  return 0;
}

inline int ringbuf_dequeue(struct ringbuf *rb, uint32_t *data, uint32_t max_nb_words) {
  assert(max_nb_words < INT_MAX);

  uint32_t cons_head, prod_tail, cons_next, nb_words;
  bool cas_ok;

  do {
    cons_head = rb->cons_head;
    prod_tail = rb->prod_tail;
    nb_words = rb->data[cons_head];  // which may be wrong already
    if (nb_words > max_nb_words) {
      return -ENOMEM;
    }
    cons_next = (cons_head + 1 + nb_words) & rb->nb_words_mask;

    // Enough room?
    if (ringbuf_full(cons_head, prod_tail, cons_next)) {
      // Actually it may happen that rb->data[cons_tail] was just bogus. Not a big deal.
      // TODO: we should handle retries right here.
      return -ENOBUFS; // FIXME: find a more appropriate one
    }

    cas_ok = atomic_compare_exchange_strong(&rb->cons_head, &cons_head, cons_next);
  } while(! cas_ok);

  /* If the CAS succeeded it means nobody altered the indexes while we were
   * reading, therefore nobody wrote something silly in place of the number
   * of words present, so we are all good. */
  memcpy(data, rb->data + 1 + cons_head, nb_words*sizeof(*data));

  while (rb->cons_tail != cons_head) sched_yield();

  rb->cons_tail = cons_next;

  return (int)nb_words;
}

/* Create a new ring buffer mmaped to that file. Fails if that file exists
 * already. Returns NULL on error. */
extern struct ringbuf *ringbuf_create(char const *fname, uint32_t tot_words);

/* Mmap the ring buffer present in that file. Fails if the file does not exist
 * already. Returns NULL on error. */
extern struct ringbuf *ringbuf_load(char const *fname);

#endif
