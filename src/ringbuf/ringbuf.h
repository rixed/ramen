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
#include <inttypes.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <string.h>
#include <sched.h>
#include <limits.h>

struct ringbuf {
  // Fixed length of the ring buffer. mmapped file must be >= this.
  uint32_t nb_words;
  size_t mmapped_size;  // The size that was mmapped (for ringbuf_unload)
  /* Pointers to entries. We use uint32 indexes so that we do not have
   * to worry too much about modulos. */
  /* Bytes that are being added by producers lie between prod_tail and
   * prod_head. prod_head points to the next word to be allocated. */
  volatile uint32_t _Atomic prod_head;
  volatile uint32_t _Atomic prod_tail;
  /* Bytes that are being read by consumers are between cons_tail and
   * cons_head. cons_head points to the next word to be read.
   * The ring buffer is empty when prod_tail == cons_head and full whenever
   * prod_head == cons_tail - 1. */
  volatile uint32_t _Atomic cons_head;
  volatile uint32_t cons_tail;
  /* We count the number of tuples (actually, of allocations), and keep
   * the range of some observed "t" values: */
  volatile uint32_t _Atomic nb_allocs;
  volatile double _Atomic tmin;
  volatile double _Atomic tmax;
  /* The actual tuples start here: */
  uint32_t data[];
};

// Return the number of words currently stored in  the ring-buffer:
inline uint32_t ringbuf_nb_entries(struct ringbuf const *rb, uint32_t prod_tail, uint32_t cons_head)
{
  if (prod_tail >= cons_head) return prod_tail - cons_head;
  return (prod_tail + rb->nb_words) - cons_head;
}

// Conversely, returns the number of words free:
inline uint32_t ringbuf_nb_free(struct ringbuf const *rb, uint32_t cons_tail, uint32_t prod_head)
{
  if (cons_tail > prod_head) return cons_tail - prod_head - 1;
  return (cons_tail + rb->nb_words) - prod_head - 1;
}

struct ringbuf_tx {
    uint32_t record_start;
    uint32_t seen;
    uint32_t next;
};

inline void print_rb(struct ringbuf *rb)
{
  printf("rb@%p: cons=[%"PRIu32";%"PRIu32"] -- (%u words of data) -- prod=[%"PRIu32";%"PRIu32"]\n",
         rb,
         rb->cons_tail, rb->cons_head,
         ringbuf_nb_entries(rb, rb->prod_tail, rb->cons_head),
         rb->prod_tail, rb->prod_head);
}

/* ringbuf will have:
 *  word n: nb_words
 *  word n+1..n+nb_words: allocated.
 *  tx->record_start will point at word n+1 above. */
inline int ringbuf_enqueue_alloc(struct ringbuf *rb, struct ringbuf_tx *tx, uint32_t nb_words)
{
  uint32_t cons_tail;
  uint32_t need_eof = 0;  // 0 never needs an EOF

  do {
    tx->seen = rb->prod_head;
    cons_tail = rb->cons_tail;
    tx->record_start = tx->seen;
    // We will write the size then the data:
    tx->next = tx->record_start + 1 + nb_words;
    uint32_t alloced = 1 + nb_words;

    // Avoid wrapping inside the record
    if (tx->next > rb->nb_words) {
      need_eof = tx->seen;
      alloced += rb->nb_words - tx->seen;
      tx->record_start = 0;
      tx->next = 1 + nb_words;
      assert(tx->next < rb->nb_words);
    } else if (tx->next == rb->nb_words) {
      tx->next = 0;
    }

    // Enough room?
    if (ringbuf_nb_free(rb, cons_tail, tx->seen) <= alloced) {
      /*printf("Ringbuf is full, cannot alloc for enqueue %"PRIu32"/%"PRIu32" tot words, seen=%"PRIu32", cons_tail=%"PRIu32", nb_free=%"PRIu32"\n",
             alloced, rb->nb_words, tx->seen, cons_tail, ringbuf_nb_free(rb, cons_tail, tx->seen));*/
      return -1;
    }

  } while (!  atomic_compare_exchange_strong(&rb->prod_head, &tx->seen, tx->next));

  if (need_eof) rb->data[need_eof] = UINT32_MAX;
  rb->data[tx->record_start ++] = nb_words;

  return 0;
}

inline void ringbuf_enqueue_commit(struct ringbuf *rb, struct ringbuf_tx const *tx, double t_start, double t_stop)
{
  if (t_start > t_stop) {
    double tmp = t_start;
    t_start = t_stop;
    t_stop = tmp;
  }

  // Update the prod_tail to match the new prod_head.
  while (atomic_load_explicit(&rb->prod_tail, memory_order_acquire) != tx->seen)
    sched_yield();

  //printf("enqueue commit, set prod_tail=%"PRIu32" while cons_head=%"PRIu32"\n", tx->next, rb->cons_head);
  assert(ringbuf_nb_entries(rb, tx->next, rb->cons_head) > 0);
  // All we need is for the following prod_tail change to always
  // be visible after the changes to nb_allocs and min/max observed t:
  uint32_t prev_nb_allocs = atomic_fetch_add_explicit(&rb->nb_allocs, 1, memory_order_relaxed);
  double tmin = atomic_load_explicit(&rb->tmin, memory_order_relaxed);
  double tmax = atomic_load_explicit(&rb->tmax, memory_order_relaxed);
  if (0 == prev_nb_allocs || t_start < tmin)
      atomic_store_explicit(&rb->tmin, t_start, memory_order_relaxed);
  if (0 == prev_nb_allocs || t_stop > tmax)
      atomic_store_explicit(&rb->tmax, t_stop, memory_order_relaxed);
  atomic_store_explicit(&rb->prod_tail, tx->next, memory_order_release);
  //print_rb(rb);
}

// Combine all of the above:
inline int ringbuf_enqueue(struct ringbuf *rb, uint32_t const *data, uint32_t nb_words, double t_start, double t_stop)
{
  struct ringbuf_tx tx;
  int const err = ringbuf_enqueue_alloc(rb, &tx, nb_words);
  if (err) return err;

  memcpy(rb->data + tx.seen + 1, data, nb_words*sizeof(*data));

  ringbuf_enqueue_commit(rb, &tx, t_start, t_stop);

  return 0;
}

inline ssize_t ringbuf_dequeue_alloc(struct ringbuf *rb, struct ringbuf_tx *tx)
{
  uint32_t seen_prod_tail, nb_words;

  /* Try to "reserve" the next record after cons_head by moving rb->cons_head
   * after it */
  do {
    tx->seen = atomic_load(&rb->cons_head);
    seen_prod_tail = rb->prod_tail;
    tx->record_start = tx->seen;

    if (ringbuf_nb_entries(rb, seen_prod_tail, tx->seen) < 1) {
      //printf("Not a single word to read; prod_tail=%"PRIu32", cons_head=%"PRIu32".\n", seen_prod_tail, tx->seen);
      return -1;
    }

    nb_words = rb->data[tx->record_start ++];  // which may be wrong already
    uint32_t dequeued = 1 + nb_words;  // How many words we'd like to increment cons_head of

    if (nb_words == UINT32_MAX) { // A wrap around marker
      tx->record_start = 0;
      nb_words = rb->data[tx->record_start ++];
      dequeued = 1 + nb_words + rb->nb_words - tx->seen;
    }

    if (ringbuf_nb_entries(rb, seen_prod_tail, tx->seen) < dequeued) {
      printf("Cannot read complete record which is really strange...\n");
      return -1;
    }

    tx->next = (tx->record_start + nb_words) % rb->nb_words;

  } while (! atomic_compare_exchange_strong(&rb->cons_head, &tx->seen, tx->next));

  /* If the CAS succeeded it means nobody altered the indexes while we were
   * reading, therefore nobody wrote something silly in place of the number
   * of words present, so we are all good. */

  return nb_words*sizeof(uint32_t);
}

inline void ringbuf_dequeue_commit(struct ringbuf *rb, struct ringbuf_tx const *tx)
{
  while (rb->cons_tail != tx->seen) sched_yield();
  //printf("dequeue commit, set const_taill=%"PRIu32" while prod_head=%"PRIu32"\n", tx->next, rb->prod_head);
  rb->cons_tail = tx->next;
  //print_rb(rb);
}

inline ssize_t ringbuf_dequeue(struct ringbuf *rb, uint32_t *data, size_t max_size)
{
  struct ringbuf_tx tx;
  ssize_t const sz = ringbuf_dequeue_alloc(rb, &tx);

  if (sz < 0) return sz;
  if ((size_t)sz > max_size) {
    printf("Record too big (%zu) to fit in buffer (%zu)\n", sz, max_size);
    return -1;
  }

  memcpy(data, rb->data + tx.record_start, sz);

  ringbuf_dequeue_commit(rb, &tx);

  return sz;
}

/* Create a new ring buffer of the specified size. */
extern int ringbuf_create(char const *fname, uint32_t tot_words);

/* Mmap the ring buffer present in that file. Fails if the file does not exist
 * already. Returns NULL on error. */
extern struct ringbuf *ringbuf_load(char const *fname);

/* Unmap the ringbuffer. */
extern int ringbuf_unload(struct ringbuf *);

#endif
