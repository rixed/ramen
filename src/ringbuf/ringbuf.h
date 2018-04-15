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

struct ringbuf_file {
  uint64_t first_seq;
  // Fixed length of the ring buffer. mmapped file must be >= this.
  uint32_t nb_words;
  uint32_t wrap:1;  // Does the ring buffer act as a ring?
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

struct ringbuf {
  struct ringbuf_file *rbf;
  char fname[PATH_MAX];
  size_t mmapped_size;  // The size that was mmapped (for ringbuf_unload)
};

// Error codes
enum ringbuf_error {
  RB_OK = 0,
  RB_ERR_NO_MORE_ROOM,
  RB_ERR_FAILURE
};

// Return the number of words currently stored in  the ring-buffer:
inline uint32_t ringbuf_file_nb_entries(struct ringbuf_file const *rbf, uint32_t prod_tail, uint32_t cons_head)
{
  if (prod_tail >= cons_head) return prod_tail - cons_head;
  return (prod_tail + rbf->nb_words) - cons_head;
}

// Conversely, returns the number of words free:
inline uint32_t ringbuf_file_nb_free(struct ringbuf_file const *rbf, uint32_t cons_tail, uint32_t prod_head)
{
  if (cons_tail > prod_head) return cons_tail - prod_head - 1;
  return (cons_tail + rbf->nb_words) - prod_head - 1;
}

struct ringbuf_tx {
    // Where the record starts (point right after the record length:
    uint32_t record_start;
    // Where the record ends (points to the next record size):
    uint32_t next;
    uint32_t seen;
};

inline void print_rbf(struct ringbuf_file *rbf)
{
  printf("rbf@%p: cons=[%"PRIu32";%"PRIu32"] -- (%u words of data) -- prod=[%"PRIu32";%"PRIu32"]\n",
         rbf,
         rbf->cons_tail, rbf->cons_head,
         ringbuf_file_nb_entries(rbf, rbf->prod_tail, rbf->cons_head),
         rbf->prod_tail, rbf->prod_head);
}

extern enum ringbuf_error ringbuf_enqueue_alloc(
  struct ringbuf *rb, struct ringbuf_tx *tx, uint32_t nb_words);

inline void ringbuf_enqueue_commit(struct ringbuf *rb, struct ringbuf_tx const *tx, double t_start, double t_stop)
{
  struct ringbuf_file *rbf = rb->rbf;

  if (t_start > t_stop) {
    double tmp = t_start;
    t_start = t_stop;
    t_stop = tmp;
  }

  // Update the prod_tail to match the new prod_head.
  while (atomic_load_explicit(&rbf->prod_tail, memory_order_acquire) != tx->seen)
    sched_yield();

  //printf("enqueue commit, set prod_tail=%"PRIu32" while cons_head=%"PRIu32"\n", tx->next, rbf->cons_head);
  assert(ringbuf_file_nb_entries(rbf, tx->next, rbf->cons_head) > 0);
  // All we need is for the following prod_tail change to always
  // be visible after the changes to nb_allocs and min/max observed t:
  uint32_t prev_nb_allocs = atomic_fetch_add_explicit(&rbf->nb_allocs, 1, memory_order_relaxed);
  double tmin = atomic_load_explicit(&rbf->tmin, memory_order_relaxed);
  double tmax = atomic_load_explicit(&rbf->tmax, memory_order_relaxed);
  if (0 == prev_nb_allocs || t_start < tmin)
      atomic_store_explicit(&rbf->tmin, t_start, memory_order_relaxed);
  if (0 == prev_nb_allocs || t_stop > tmax)
      atomic_store_explicit(&rbf->tmax, t_stop, memory_order_relaxed);
  atomic_store_explicit(&rbf->prod_tail, tx->next, memory_order_release);
  //print_rbf(rbf);
}

// Combine all of the above:
inline enum ringbuf_error ringbuf_enqueue(
      struct ringbuf *rb, uint32_t const *data, uint32_t nb_words,
      double t_start, double t_stop)
{
  struct ringbuf_tx tx;
  enum ringbuf_error const err = ringbuf_enqueue_alloc(rb, &tx, nb_words);
  if (err) return err;

  struct ringbuf_file *rbf = rb->rbf;

  memcpy(rbf->data + tx.seen + 1, data, nb_words*sizeof(*data));

  ringbuf_enqueue_commit(rb, &tx, t_start, t_stop);

  return 0;
}

inline ssize_t ringbuf_dequeue_alloc(struct ringbuf *rb, struct ringbuf_tx *tx)
{
  struct ringbuf_file *rbf = rb->rbf;

  uint32_t seen_prod_tail, nb_words;

  /* Try to "reserve" the next record after cons_head by moving rbf->cons_head
   * after it */
  do {
    tx->seen = atomic_load(&rbf->cons_head);
    seen_prod_tail = rbf->prod_tail;
    tx->record_start = tx->seen;

    if (ringbuf_file_nb_entries(rbf, seen_prod_tail, tx->seen) < 1) {
      //printf("Not a single word to read; prod_tail=%"PRIu32", cons_head=%"PRIu32".\n", seen_prod_tail, tx->seen);
      return -1;
    }

    nb_words = rbf->data[tx->record_start ++];  // which may be wrong already
    uint32_t dequeued = 1 + nb_words;  // How many words we'd like to increment cons_head of

    if (nb_words == UINT32_MAX) { // A wrap around marker
      tx->record_start = 0;
      nb_words = rbf->data[tx->record_start ++];
      dequeued = 1 + nb_words + rbf->nb_words - tx->seen;
    }

    if (ringbuf_file_nb_entries(rbf, seen_prod_tail, tx->seen) < dequeued) {
      printf("Cannot read complete record which is really strange...\n");
      return -1;
    }

    tx->next = (tx->record_start + nb_words) % rbf->nb_words;

  } while (! atomic_compare_exchange_strong(&rbf->cons_head, &tx->seen, tx->next));

  /* If the CAS succeeded it means nobody altered the indexes while we were
   * reading, therefore nobody wrote something silly in place of the number
   * of words present, so we are all good. */

  return nb_words*sizeof(uint32_t);
}

inline void ringbuf_dequeue_commit(struct ringbuf *rb, struct ringbuf_tx const *tx)
{
  struct ringbuf_file *rbf = rb->rbf;

  while (rbf->cons_tail != tx->seen) sched_yield();
  //printf("dequeue commit, set const_taill=%"PRIu32" while prod_head=%"PRIu32"\n", tx->next, rbf->prod_head);
  rbf->cons_tail = tx->next;
  //print_rbf(rbf);
}

inline ssize_t ringbuf_dequeue(struct ringbuf *rb, uint32_t *data, size_t max_size)
{
  struct ringbuf_tx tx;
  ssize_t const sz = ringbuf_dequeue_alloc(rb, &tx);

  struct ringbuf_file *rbf = rb->rbf;

  if (sz < 0) return sz;
  if ((size_t)sz > max_size) {
    printf("Record too big (%zu) to fit in buffer (%zu)\n", sz, max_size);
    return -1;
  }

  memcpy(data, rbf->data + tx.record_start, sz);

  ringbuf_dequeue_commit(rb, &tx);

  return sz;
}

/* When one stops/crash with an allocated tx then the ringbuffer will remains
 * unusable (since the next process that tries to commit will wait forever
 * until the cons catch up with the observed head. So whenever it is certain
 * there are no readers and no writers the ringbuffer should be "repaired".
 * In here, it is assumed that what has not been committed was totally lost.
 * Returns true of a fix was indeed necessary. */
inline bool ringbuf_repair(struct ringbuf *rb)
{
  struct ringbuf_file *rbf = rb->rbf;
  bool needed = false;

  // Avoid writing in this mmaped page for no good reason:
  if (rbf->prod_head != rbf->prod_tail) {
    rbf->prod_head = rbf->prod_tail;
    needed = true;
  }

  if (rbf->cons_head != rbf->cons_tail) {
    rbf->cons_head = rbf->cons_tail;
    needed = true;
  }

  return needed;
}

// Initialize the given TX to point at the first record and return its size
// Returns -1 if the file is empty, -2 on error
inline ssize_t ringbuf_read_first(struct ringbuf *rb, struct ringbuf_tx *tx)
{
  struct ringbuf_file *rbf = rb->rbf;

  tx->seen = 0; // unused
  tx->record_start = 0;
  uint32_t nb_words = rbf->data[tx->record_start ++];
  if (nb_words == 0) return -1;
  tx->next = tx->record_start + nb_words;
  /*printf("read_first: nb_words=%"PRIu32", record_start=%"PRIu32", next=%"PRIu32"\n",
         nb_words, tx->record_start, tx->next);*/
  // Sanity checks:
  if (nb_words == UINT32_MAX || nb_words >= rbf->nb_words) return -2;
  return nb_words*sizeof(uint32_t);
}

// Advance the given TX to the next record and return its size,
// or -1 if we've reached the end of what's been written, and 0 on EOF
inline ssize_t ringbuf_read_next(struct ringbuf *rb, struct ringbuf_tx *tx)
{
  struct ringbuf_file *rbf = rb->rbf;

  assert(tx->record_start < tx->next); // Or we have read the whole of it already
  if (tx->next == rbf->nb_words) return 0; // Same as EOF
  uint32_t nb_words = rbf->data[tx->next];
  if (nb_words == 0) return -1;
  if (nb_words == UINT32_MAX) return 0;
  tx->record_start = tx->next + 1;
  tx->next = tx->record_start + nb_words;
  /*printf("read_next: record_start=%"PRIu32", next=%"PRIu32"\n",
         tx->record_start, tx->next);*/
  return nb_words*sizeof(uint32_t);
}

/* Create a new ring buffer of the specified size. */
extern enum ringbuf_error ringbuf_create(bool wrap, char const *fname, uint32_t tot_words);

/* Mmap the ring buffer present in that file. Fails if the file does not exist
 * already. Returns NULL on error. */
extern enum ringbuf_error ringbuf_load(struct ringbuf *rb, char const *fname);

/* Unmap the ringbuffer. */
extern enum ringbuf_error ringbuf_unload(struct ringbuf *);

#endif
