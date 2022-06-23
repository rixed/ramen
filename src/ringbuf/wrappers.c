// vim: ft=c bs=2 ts=2 sts=2 sw=2 expandtab
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <inttypes.h>
#include <string.h>
#include <sys/types.h>  // getpid
#include <time.h>
#include <unistd.h>  // getpid

#define CAML_NAME_SPACE
#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <caml/callback.h>

#include <uint64.h>
#include <uint128.h>

#include "ringbuf.h"

static bool debug = false;

static value *exn_NoMoreRoom, *exn_Empty, *exn_Damaged;
static bool exceptions_inited = false;

static void retrieve_exceptions(void)
{
  if (exceptions_inited) return;

  exn_NoMoreRoom = caml_named_value("ringbuf full exception");
  exn_Empty = caml_named_value("ringbuf empty exception");
  exn_Damaged = caml_named_value("ringbuf damaged exception");
  exceptions_inited = true;
}

/* type t for struct ringbuf */

#define Ringbuf_val(v) (*((struct ringbuf **)Data_custom_val(v)))

static struct custom_operations ringbuf_ops = {
  "org.happyleptic.ramen.ringbuf",
  custom_finalize_default,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default,
  custom_compare_ext_default
};

static value alloc_ringbuf(void)
{
  CAMLparam0();
  CAMLlocal1(res);
  retrieve_exceptions();

  struct ringbuf *rb = malloc(sizeof(*rb));
  if (! rb) caml_failwith("Cannot malloc struct ringbuf");

  res = caml_alloc_custom(&ringbuf_ops, sizeof rb, 0, 1);
  Ringbuf_val(res) = rb;
  CAMLreturn(res);
}

/* type tx for struct wrap_ringbuf_tx (we wrap in there the actual rb) */

// TODO: try to store the offset in there to minimize number of parameters
// passed from OCaml to C

struct wrap_ringbuf_tx {
  struct ringbuf *rb; // for normal TXs
  uint32_t *bytes;     // for "bytes" TXs
  struct ringbuf_tx tx;
  // Number of bytes allocated either in the RB transaction or in *bytes
  // above; just to check we do not overflow.
  size_t alloced;
};

static void wrtx_finalize(value);

static struct custom_operations tx_ops = {
  "org.happyleptic.ramen.ringbuf_tx",
  wrtx_finalize,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default,
  custom_compare_ext_default
};

#define RingbufTx_val(v) ((struct wrap_ringbuf_tx *)Data_custom_val(v))

static void wrtx_finalize(value tx)
{
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  if (wrtx->bytes) {
    assert(! wrtx->rb);
    free(wrtx->bytes);
    wrtx->bytes = NULL;
  }
}

static value alloc_tx(void)
{
  CAMLparam0();
  CAMLlocal1(res);
  res = caml_alloc_custom(&tx_ops, sizeof(struct wrap_ringbuf_tx), 0, 1);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(res);
  wrtx->rb = NULL;
  wrtx->bytes = NULL;
  CAMLreturn(res);
}

static uint64_t uint64_of_version(char const *str)
{
  uint64_t v = 0;
  char *d = (char *)&v;
  bool str_ended = false;
  unsigned i;
  for (i = 0; i < sizeof(v); i++) {
    if (str_ended) {
      d[i] = '\0';
    } else {
      d[i] = str[i];
      str_ended = str[i] == '\0';
    }
  }
  if (! str_ended && str[i] != '\0')
    caml_failwith("version string too long");

  return v;
}

CAMLprim value wrap_ringbuf_create(value version_, value wrap_, value tot_words_, value timeout_, value fname_)
{
  CAMLparam4(version_, wrap_, fname_, tot_words_);
  char *version_str = String_val(version_);
  uint64_t version = uint64_of_version(version_str);
  bool wrap = Bool_val(wrap_);
  char *fname = String_val(fname_);
  unsigned tot_words = Long_val(tot_words_);
  double timeout = Double_val(timeout_);
  enum ringbuf_error err = ringbuf_create(version, wrap, tot_words, timeout, fname);
  if (RB_OK != err) caml_failwith("Cannot create ring buffer");
  CAMLreturn(Val_unit);
}

CAMLprim value wrap_ringbuf_load(value version_, value fname_)
{
  CAMLparam2(version_, fname_);
  CAMLlocal1(res);
  res = alloc_ringbuf();
  char *version_str = String_val(version_);
  uint64_t version = uint64_of_version(version_str);
  char *fname = String_val(fname_);
  if (RB_OK != ringbuf_load(Ringbuf_val(res), version, fname))
    caml_failwith("Cannot load ring buffer");
  CAMLreturn(res);
}

CAMLprim value wrap_ringbuf_unload(value rb_)
{
  CAMLparam1(rb_);
  struct ringbuf *rb = Ringbuf_val(rb_);
  if (RB_OK != ringbuf_unload(rb))
    caml_failwith("Cannot unload ring buffer");
  free(rb);
  Ringbuf_val(rb_) = NULL;
  //printf("%d: Unmmapped @ %p\n", (int)getpid(), rb);
  CAMLreturn(Val_unit);
}

CAMLprim value wrap_ringbuf_may_archive(value rb_)
{
  CAMLparam1(rb_);
  struct ringbuf *rb = Ringbuf_val(rb_);
  (void)rotate_file(rb); // will only rotate if !wrap
  CAMLreturn(wrap_ringbuf_unload(rb_));
}

CAMLprim value wrap_ringbuf_stats(value rb_)
{
  CAMLparam1(rb_);
  CAMLlocal1(ret);
  struct ringbuf *rb = Ringbuf_val(rb_);
  struct ringbuf_file *rbf = rb->rbf;
  // See type stats in RingBuf.ml
  ret = caml_alloc_tuple(13);
  /* "Rule 6   Direct assignment to a field of a block, as in `Field(v, n) = w;`
   *  is safe only if v is a block newly allocated by caml_alloc_small; that is,
   *  if no allocation took place between the allocation of v and the assignment
   *  to the field." -- OCaml manual.
   * Here caml_copy_double does allocate. */
  Store_field(ret, 0, Val_long(rbf->num_words));
  Store_field(ret, 1, Val_bool(rbf->wrap));
  Store_field(ret, 2,
    Val_long(ringbuf_file_num_entries(rbf, rbf->prod_tail, rbf->cons_head)));
  Store_field(ret, 3, Val_long(rbf->num_allocs));
  double const tmin = atomic_load_explicit(&rbf->tmin, memory_order_relaxed);
  Store_field(ret, 4, caml_copy_double(tmin));
  double const tmax = atomic_load_explicit(&rbf->tmax, memory_order_relaxed);
  Store_field(ret, 5, caml_copy_double(tmax));
  Store_field(ret, 6, Val_long(rb->mmapped_size));
  Store_field(ret, 7, Val_long(rbf->prod_head));
  Store_field(ret, 8, Val_long(rbf->prod_tail));
  Store_field(ret, 9, Val_long(rbf->cons_head));
  Store_field(ret, 10, Val_long(rbf->cons_tail));
  Store_field(ret, 11, Val_long(rbf->first_seq));
  Store_field(ret, 12, caml_copy_double(rbf->timeout));
  CAMLreturn(ret);
}

CAMLprim value wrap_ringbuf_repair(value rb_)
{
  CAMLparam1(rb_);
  CAMLlocal1(ret);
  struct ringbuf *rb = Ringbuf_val(rb_);
  ret = Val_bool(ringbuf_repair(rb));
  CAMLreturn(ret);
}

static void check_size(int size)
{
  if (size & 3) {
    caml_invalid_argument("enqueue: size must be a multiple of 4 bytes");
  }
  if (size > (int)MAX_RINGBUF_MSG_SIZE) {
    caml_invalid_argument("enqueue: size must be less than " STR(MAX_RINGBUF_MSG_SIZE));
  }
  if (size == 0) {
    caml_invalid_argument("enqueue: there are no record of 0 words");
  }
}

static void check_error(
  enum ringbuf_error err, char const *fail, char const *bad_version)
{
  switch (err) {
    case RB_ERR_FAILURE:
      caml_failwith(fail);
      break;
    case RB_ERR_NO_MORE_ROOM:
      assert(exceptions_inited);
      caml_raise_constant(*exn_NoMoreRoom);
      break;
    case RB_ERR_BAD_VERSION:
      caml_failwith(bad_version);
      break;
    case RB_OK:
      break;
  }
}

CAMLprim value wrap_ringbuf_enqueue(value rb_, value bytes_, value size_, value tmin_, value tmax_)
{
  CAMLparam5(rb_, bytes_, size_, tmin_, tmax_);
  struct ringbuf *rb = Ringbuf_val(rb_);
  int size = Long_val(size_);
  check_size(size);
  if (size < (int)caml_string_length(bytes_)) {
    caml_invalid_argument("enqueue: size must be less than the string length");
  }
  double const tmin = Double_val(tmin_);
  double const tmax = Double_val(tmax_);
  uint32_t const num_words = size / sizeof(uint32_t);
  uint32_t const *const bytes = (uint32_t const *)String_val(bytes_);
  check_error(
    ringbuf_enqueue(rb, bytes, num_words, tmin, tmax),
    "Cannot ringbuf_enqueue",
    "Ringbuf version mismatch in ringbuf_enqueue");
  CAMLreturn(Val_unit);
}

CAMLprim value wrap_ringbuf_dequeue(value rb_)
{
  CAMLparam1(rb_);
  CAMLlocal1(bytes_);
  struct ringbuf *rb = Ringbuf_val(rb_);
  struct ringbuf_tx tx;
  ssize_t const size = ringbuf_dequeue_alloc(rb, &tx);
  if (size < 0) {
    assert(exceptions_inited);
    caml_raise_constant(*exn_Empty);
  }

  bytes_ = caml_alloc_string(size);
  if (! bytes_) caml_failwith("Cannot malloc dequeued bytes");
  memcpy(String_val(bytes_), rb->rbf->data + tx.record_start, size);

  ringbuf_dequeue_commit(rb, &tx);

  CAMLreturn(bytes_);
}

// Same as wrap_ringbuf_dequeue_alloc but does not change the reader pointer
// in ringbuffer header:
CAMLprim value wrap_ringbuf_read_first(value rb_)
{
  CAMLparam1(rb_);
  CAMLlocal1(tx);
  struct ringbuf *rb = Ringbuf_val(rb_);
  tx = alloc_tx();
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  wrtx->rb = rb;
  ssize_t const size = ringbuf_read_first(rb, &wrtx->tx);
  if (size == -2) {
    // Error:
    caml_failwith("Invalid buffer file");
  } else if (size == -1) {
    // Have to wait for content:
    assert(exceptions_inited);
    caml_raise_constant(*exn_Empty);
  } else {
    assert(size < (ssize_t)MAX_RINGBUF_MSG_SIZE);
    wrtx->alloced = (size_t)size;
    CAMLreturn(tx);
  }
}

// Same as wrap_ringbuf_dequeue_alloc but does not change the reader pointer
// in ringbuffer header:
CAMLprim value wrap_ringbuf_read_next(value tx)
{
  CAMLparam1(tx);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);

  ssize_t const size = ringbuf_read_next(wrtx->rb, &wrtx->tx);
  if (size == 0) {
    caml_raise_end_of_file();
  } else if (size == -1) {
    // Have to wait for content:
    assert(exceptions_inited);
    caml_raise_constant(*exn_Empty);
  } else {
    assert(size < (ssize_t)MAX_RINGBUF_MSG_SIZE);
    wrtx->alloced = (size_t)size;
    CAMLreturn(tx);
  }
}

// Returns a TX that writes into a buffer on the heap instead of a ringbuf:
CAMLprim value wrap_bytes_tx(value size_)
{
  CAMLparam1(size_);
  CAMLlocal1(tx);
  size_t const size = Long_val(size_);
  tx = alloc_tx();
  struct wrap_ringbuf_tx *const wrtx = RingbufTx_val(tx);
  memset(wrtx, 0, sizeof(*wrtx));
  wrtx->bytes = malloc(size);
  assert(wrtx->bytes || !size);
  assert(size < MAX_RINGBUF_MSG_SIZE);
  wrtx->alloced = size;
  CAMLreturn(tx);
}

// returns a TX that reads from a buffer on the heap instead of a ringbuf:
CAMLprim value wrap_tx_of_bytes(value bytes_)
{
  CAMLparam1(bytes_);
  CAMLlocal1(tx);
  retrieve_exceptions();
  char const *s = String_val(bytes_);
  size_t const size = caml_string_length(bytes_);
  tx = alloc_tx();
  struct wrap_ringbuf_tx *const wrtx = RingbufTx_val(tx);
  memset(wrtx, 0, sizeof(*wrtx));
  wrtx->bytes = malloc(size);
  assert(wrtx->bytes || !size);
  assert(size < MAX_RINGBUF_MSG_SIZE);
  wrtx->alloced = size;
  memcpy(wrtx->bytes, s, size);
  CAMLreturn(tx);
}

/* Lower level API */

CAMLprim value wrap_ringbuf_enqueue_alloc(value rb_, value size_)
{
  CAMLparam2(rb_, size_);
  CAMLlocal1(tx);
  struct ringbuf *rb = Ringbuf_val(rb_);
  int size = Long_val(size_);
  check_size(size);
  uint32_t num_words = size / sizeof(uint32_t);
  tx = alloc_tx();
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  wrtx->rb = rb;
  assert(size < (int)MAX_RINGBUF_MSG_SIZE);
  wrtx->alloced = size;
  check_error(
    ringbuf_enqueue_alloc(rb, &wrtx->tx, num_words),
    "Cannot ringbuf_enqueue_alloc",
    "Ringbuf version mismatch in ringbuf_enqueue_alloc");

  /*printf("Allocated %d bytes for enqueuing at offset %"PRIu32" (in words)\n",
         size, wrtx->tx.record_start);*/
  CAMLreturn(tx);
}

CAMLprim value wrap_ringbuf_tx_size(value tx)
{
  CAMLparam1(tx);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  CAMLreturn(Val_long((long)wrtx->alloced));
}

CAMLprim value wrap_ringbuf_tx_start(value tx)
{
  CAMLparam1(tx);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  CAMLreturn(Val_long((long)wrtx->tx.record_start));
}

CAMLprim value wrap_ringbuf_tx_fname(value tx)
{
  CAMLparam1(tx);
  CAMLlocal1(fname);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  fname = caml_copy_string(wrtx->rb->fname);
  CAMLreturn(fname);
}

CAMLprim value wrap_ringbuf_tx_address(value tx)
{
  CAMLparam1(tx);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  if (debug)
    fprintf(stderr, "%s: address of tx is %p\n",
            __func__, wrtx->rb->rbf->data + wrtx->tx.record_start);
  CAMLreturn(copy_uint64((uint64_t)(wrtx->rb->rbf->data + wrtx->tx.record_start)));
}

CAMLprim value wrap_ringbuf_enqueue_commit(value tx, value tmin_, value tmax_)
{
  CAMLparam3(tx, tmin_, tmax_);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  double tmin = Double_val(tmin_);
  double tmax = Double_val(tmax_);
  ringbuf_enqueue_commit(wrtx->rb, &wrtx->tx, tmin, tmax);
  CAMLreturn(Val_unit);
}

CAMLprim value wrap_ringbuf_dequeue_alloc(value rb_)
{
  CAMLparam1(rb_);
  CAMLlocal1(tx);
  struct ringbuf *rb = Ringbuf_val(rb_);
  tx = alloc_tx();
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  wrtx->rb = rb;
  ssize_t size = ringbuf_dequeue_alloc(rb, &wrtx->tx);
  if (size < 0) {
    assert(exceptions_inited);
    caml_raise_constant(*exn_Empty);
  }
  /*printf("Allocated %zd bytes for dequeuing at offset %"PRIu32" (in words)\n",
         size, wrtx->tx.record_start);*/
  assert(size < (ssize_t)MAX_RINGBUF_MSG_SIZE);
  wrtx->alloced = (size_t)size;
  CAMLreturn(tx);
}

CAMLprim value wrap_ringbuf_dequeue_commit(value tx)
{
  CAMLparam1(tx);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  ringbuf_dequeue_commit(wrtx->rb, &wrtx->tx);
  CAMLreturn(Val_unit);
}

// WRITES

static void *where_to(struct wrap_ringbuf_tx const *wrtx, size_t offs)
{
  assert(!(offs & 3));
  uint32_t *data;
  if (wrtx->rb) {
    data = (uint32_t *)wrtx->rb->rbf->data;
  } else {
    assert(wrtx->bytes);
    data = wrtx->bytes;
  }
  return data                  // Where the ringbuffer starts
       + wrtx->tx.record_start // The offset of the record within that data
       + offs/sizeof(uint32_t);
}

static time_t last_err = 0;

#define TIMED_PRINT(...) do { \
  time_t now = time(NULL); \
  if (now >= last_err + 1) { \
    last_err = now; \
    printf(__VA_ARGS__); \
  } \
} while (0)

static void write_words(struct wrap_ringbuf_tx const *wrtx, size_t offs, char const *src, size_t size)
{
  if (size + offs > wrtx->alloced) {
    TIMED_PRINT("%d: ERROR while writing %s: size (%zu) + offs (%zu) > alloced (%zu)\n", (int)getpid(), wrtx->rb->fname, size, offs, wrtx->alloced);
    DUMP_BACKTRACE();
    fflush(stdout);
    assert(exceptions_inited);
    caml_raise_constant(*exn_Damaged);
  }

  if (size > MAX_RINGBUF_MSG_SIZE) {
    TIMED_PRINT("%d: ERROR while writing %s: size (%zu) > " STR(MAX_RINGBUF_MSG_SIZE) "\n", (int)getpid(), wrtx->rb->fname, size);
    DUMP_BACKTRACE();
    fflush(stdout);
    assert(exceptions_inited);
    caml_raise_constant(*exn_Damaged);
  }
  uint32_t *addr = where_to(wrtx, offs);
/*
  printf("Write %zu bytes at offset %zu:", size, offs);
  for (unsigned s = 0 ; s < size ; s++) {
    printf(" %02" PRIx8, (uint8_t)src[s]);
  }
  printf("\n");
*/
  memcpy(addr, src, size);
}

static void read_words(struct wrap_ringbuf_tx const *wrtx, size_t offs, char *dst, size_t size)
{
  if (offs + size > wrtx->alloced) {
    TIMED_PRINT("%d: ERROR while reading %s: offs (%zu) + size (%zu) > alloced (%zu)\n", (int)getpid(), wrtx->rb->fname, offs, size, wrtx->alloced);
    fflush(stdout);
    assert(exceptions_inited);
    caml_raise_constant(*exn_Damaged);
  }
  if (size > MAX_RINGBUF_MSG_SIZE) {
    TIMED_PRINT("%d: ERROR while reading %s: size (%zu) > " STR(MAX_RINGBUF_MSG_SIZE) "\n", (int)getpid(), wrtx->rb->fname, size);
    fflush(stdout);
    assert(exceptions_inited);
    caml_raise_constant(*exn_Damaged);
  }

  uint32_t *addr = where_to(wrtx, offs);
/*
  printf("Read %zu bytes from offset %zu:", size, offs);
  for (unsigned s = 0 ; s < size ; s++) {
    printf(" %02" PRIx8, ((uint8_t *)addr)[s]);
  }
  printf("\n");
*/
  memcpy(dst, addr, size);
}

/* A function to read raw words from anywhere in the ringbuffer, used by
 * `ramen ringbuf-summary`. */
CAMLprim value wrap_ringbuf_read_raw(value rb_, value index_, value num_words_)
{
  CAMLparam3(rb_, index_, num_words_);
  CAMLlocal1(bytes_);
  struct ringbuf *rb = Ringbuf_val(rb_);
  unsigned const index = Long_val(index_);
  unsigned const num_words = Long_val(num_words_);
  ssize_t const size = num_words * sizeof(*rb->rbf->data);

  bytes_ = caml_alloc_string(size);
  if (! bytes_) caml_failwith("Cannot malloc read bytes");
  memcpy(String_val(bytes_), rb->rbf->data + index, size);

  CAMLreturn(bytes_);
}

/* A function to return a full message, used to forward messages to the
 * tunneld service. */
CAMLprim value wrap_ringbuf_read_raw_tx(value tx)
{
  CAMLparam1(tx);
  CAMLlocal1(bytes_);

  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  size_t const size = wrtx->alloced;
  assert(size < MAX_RINGBUF_MSG_SIZE);
  bytes_ = caml_alloc_string(size);
  if (! bytes_) caml_failwith("Cannot malloc tx bytes");

  if (wrtx->rb) {
    struct ringbuf_file *rbf = wrtx->rb->rbf;
    memcpy(String_val(bytes_), rbf->data + wrtx->tx.record_start, size);
  } else {
    assert(wrtx->bytes);
    memcpy(String_val(bytes_), wrtx->bytes + wrtx->tx.record_start, size);
  }

  CAMLreturn(bytes_);
}

/* The inverse of wrap_ringbuf_read_raw_tx: write a buffer into a TX. */
CAMLprim value wrap_ringbuf_write_raw_tx(value tx, value off_, value bytes_)
{
  CAMLparam3(tx, off_, bytes_);

  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  size_t offs = Long_val(off_);
  assert(Is_block(bytes_));
  assert(Tag_val(bytes_) == String_tag);
  char const *src = (char const *)Bytes_val(bytes_);
  write_words(wrtx, offs, src, caml_string_length(bytes_));
  CAMLreturn(Val_unit);
}

/* Integers are serialized in the ringbuffers as they are encoded in
 * OCaml custom values. In particular, int48s are shifted 16bits higher.
 * When we move to C workers or allow C programs to write directly in the
 * ringbuffers then we must revisit this. */

#define WRITE_BOXED(bits, custom_sz, src_offset) \
CAMLprim value write_boxed_##bits(value tx, value off_, value v_) \
{ \
  CAMLparam3(tx, off_, v_); \
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx); \
  size_t offs = Long_val(off_); \
  assert(Is_block(v_)); \
  assert(Tag_val(v_) == Custom_tag); \
  char const *src = Data_custom_val(v_); \
  /* In little endian only if src_offset>0: */ \
  write_words(wrtx, offs, src + src_offset, custom_sz - src_offset); \
  CAMLreturn(Val_unit); \
}

#define WRITE_UNBOXED_INT(bits, int_bits) \
CAMLprim value write_unboxed_##bits(value tx, value off_, value v_) \
{ \
  CAMLparam3(tx, off_, v_); \
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx); \
  size_t offs = Long_val(off_); \
  assert(Is_long(v_)); \
  uint##int_bits##_t v = (uint##int_bits##_t)Long_val(v_); \
  /* In little endian only: */ \
  write_words(wrtx, offs, (char const *)&v, bits / 8); \
  CAMLreturn(Val_unit); \
}

WRITE_BOXED(128, 16, 0);
WRITE_BOXED(64, 8, 0);
WRITE_BOXED(56, 8, 1);
WRITE_BOXED(48, 8, 2);
WRITE_BOXED(40, 8, 3);
WRITE_BOXED(32, 4, 0);
WRITE_UNBOXED_INT(24, 32);
WRITE_UNBOXED_INT(16, 16);
WRITE_UNBOXED_INT(8, 8);

/* Ips are encoded as Dessser sum types: 16 bits for the 1-bit nullmask
 * (unused for Ips), then 16 bits for the label, then the value. */
struct ip_sum_head {
  uint32_t nullmask:16;
  uint32_t tag:16;
};

CAMLprim value write_ip(value tx, value off_, value v_) \
{
  CAMLparam3(tx, off_, v_);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  size_t offs = Long_val(off_);
  assert(Is_block(v_));
  // Write the tag then the IP
  struct ip_sum_head head;
  head.nullmask = 0;
  head.tag = Tag_val(v_);
  assert(head.tag == 0 || head.tag == 1);
  write_words(wrtx, offs, (char const *)&head, sizeof head);
  if (head.tag == 0) {
    write_boxed_32(tx, Val_long(offs + sizeof head), Field(v_, 0));
  } else {
    write_boxed_128(tx, Val_long(offs + sizeof head), Field(v_, 0));
  }
  CAMLreturn(Val_unit);
}


extern struct custom_operations uint128_ops;
extern struct custom_operations uint64_ops;
extern struct custom_operations uint32_ops;
extern struct custom_operations int128_ops;
extern struct custom_operations caml_int64_ops;
extern struct custom_operations caml_int32_ops;

#define READ_BOXED(int_type, bits, ops, custom_sz, dst_offset) \
CAMLprim value read_##int_type##bits(value tx, value off_) \
{ \
  CAMLparam2(tx, off_); \
  CAMLlocal1(v); \
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx); \
  size_t offs = Long_val(off_); \
  v = caml_alloc_custom(&ops, custom_sz, 0, 1); \
  char *dst = Data_custom_val(v); \
  if (dst_offset > 0) memset(dst, 0, dst_offset); \
  read_words(wrtx, offs, dst + dst_offset, custom_sz - dst_offset); \
  CAMLreturn(v); \
}

#define READ_UNBOXED_INT(int_type, bits, int_bits) \
CAMLprim value read_##int_type##bits(value tx, value off_) \
{ \
  CAMLparam2(tx, off_); \
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx); \
  size_t offs = Long_val(off_); \
  int_type##int_bits##_t v = 0; \
  /* In little endian: */ \
  read_words(wrtx, offs, (char *)&v, bits / 8); \
  CAMLreturn(Val_long(v)); \
}

READ_BOXED(uint, 128, uint128_ops, 16, 0);
READ_BOXED(uint, 64, uint64_ops, 8, 0);
READ_BOXED(uint, 56, uint64_ops, 8, 1);
READ_BOXED(uint, 48, uint64_ops, 8, 2);
READ_BOXED(uint, 40, uint64_ops, 8, 3);
READ_BOXED(uint, 32, uint32_ops, 4, 0);
READ_UNBOXED_INT(uint, 24, 32);
READ_UNBOXED_INT(uint, 16, 16);
READ_UNBOXED_INT(uint, 8, 8);
READ_BOXED(int, 128, int128_ops, 16, 0);
READ_BOXED(int, 64, caml_int64_ops, 8, 0);
READ_BOXED(int, 56, caml_int64_ops, 8, 1);
READ_BOXED(int, 48, caml_int64_ops, 8, 2);
READ_BOXED(int, 40, caml_int64_ops, 8, 3);
READ_BOXED(int, 32, caml_int32_ops, 4, 0);
READ_UNBOXED_INT(int, 24, 32);
READ_UNBOXED_INT(int, 16, 16);
READ_UNBOXED_INT(int, 8, 8);

CAMLprim value read_ip(value tx, value off_)
{
  CAMLparam2(tx, off_);
  CAMLlocal1(v);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  size_t offs = Long_val(off_);
  struct ip_sum_head head;
  read_words(wrtx, offs, (char *)&head, sizeof head);
  assert(head.nullmask == 0);
  v = caml_alloc(1, head.tag);
  if (head.tag == 0) { // V4
    Store_field(v, 0, read_uint32(tx, Val_long(offs + sizeof head)));
  } else {
    assert(head.tag == 1);  // V6
    Store_field(v, 0, read_uint128(tx, Val_long(offs + sizeof head)));
  }
  CAMLreturn(v);
}

CAMLprim value write_str(value tx, value off_, value v_)
{
  CAMLparam3(tx, off_, v_);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  size_t offs = Long_val(off_);
  assert(Is_block(v_));
  assert(Tag_val(v_) == String_tag);
  uint32_t size = caml_string_length(v_);
  char const *src = Bp_val(v_);
  // We must start this variable size field with its length:
  write_words(wrtx, offs, (char const *)&size, sizeof size);
  write_words(wrtx, offs + sizeof size, src, size);
  CAMLreturn(Val_unit);
}

CAMLprim value write_word(value tx, value off_, value v_)
{
  CAMLparam3(tx, off_, v_);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  size_t offs = Long_val(off_);
  uint32_t *addr = where_to(wrtx, offs);

  assert(Is_long(v_));
  long v = Long_val(v_);
  //printf("Copy fixed value %ld at offset %zu\n", v, offs);
  assert(v <= UINT32_MAX);
  uint32_t src = v;

  memcpy(addr, &src, sizeof src);

  CAMLreturn(Val_unit);
}

// We need a special case thanks to the many corner cases for floats
CAMLprim value write_float(value tx, value off_, value v_)
{
  CAMLparam3(tx, off_, v_);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  size_t offs = Long_val(off_);
  assert(Tag_val(v_) == Double_tag);
  double v = Double_val(v_);
  write_words(wrtx, offs, (char const *)&v, sizeof(v));
  CAMLreturn(Val_unit);
}

CAMLprim value read_float(value tx, value off_)
{
  CAMLparam2(tx, off_);
  CAMLlocal1(v_);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  size_t offs = Long_val(off_);
  double v;
  read_words(wrtx, offs, (char *)&v, sizeof v);
  v_ = caml_alloc(Double_wosize, Double_tag);
  Store_double_val(v_, v);
  //printf("v=%f, offs=%zu\n", v, offs);
  CAMLreturn(v_);
}

CAMLprim value zero_bytes(value tx, value off_, value size_)
{
  CAMLparam3(tx, off_, size_);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  size_t offs = Long_val(off_);
  uint32_t *addr = where_to(wrtx, offs);
  int size = Long_val(size_);

  memset(addr, 0, size);

  CAMLreturn(Val_unit);
}

// Set the bit_ th bit in the tx to 1 (for the nullmask)
CAMLprim value set_bit(value tx, value offs_, value bit_)
{
  CAMLparam3(tx, offs_, bit_);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  unsigned bit = Long_val(bit_);
  unsigned offs = Long_val(offs_);
  assert(bit/8 < wrtx->alloced);
  uint8_t *addr = (uint8_t *)where_to(wrtx, offs) + bit/8;
  uint8_t mask = 1U << (bit % 8);
  *addr |= mask;
  CAMLreturn(Val_unit);
}

// Return the bit_ th bit in the tx (for the nullmask)
CAMLprim value get_bit(value tx, value offs_, value bit_)
{
  CAMLparam3(tx, offs_, bit_);
  CAMLlocal1(b);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  unsigned bit = Long_val(bit_);
  unsigned offs = Long_val(offs_);
  assert(offs + bit/8 < wrtx->alloced);
  uint8_t const *addr = (uint8_t *)where_to(wrtx, offs) + bit/8;
  uint8_t mask = 1U << (bit % 8);
  CAMLreturn(Val_bool(*addr & mask));
}

CAMLprim value read_word(value tx, value off_)
{
  CAMLparam2(tx, off_);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  size_t offs = Long_val(off_);
  uint32_t v;
  read_words(wrtx, offs, (char *)&v, sizeof v);
  CAMLreturn(Val_long(v));
}

CAMLprim value read_str(value tx, value off_)
{
  CAMLparam2(tx, off_);
  CAMLlocal1(v);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  size_t offs = Long_val(off_);
  uint32_t size;
  read_words(wrtx, offs, (char *)&size, sizeof size);
  v = caml_alloc_string(size);  // Will add the final '\0'
  read_words(wrtx, offs + sizeof size, String_val(v), size);
  CAMLreturn(v);
}

/* These four should not be here but in an additional misc lib. */

CAMLprim value wrap_strtod(value str_)
{
  CAMLparam1(str_);
  CAMLlocal1(ret);
  char const *str = String_val(str_);
  char *end;
  double d = strtod(str, &end);
  if (*end != '\0') caml_failwith("Cannot convert to double");
  ret = caml_copy_double(d);
  CAMLreturn(ret);
}

#include <signal.h>
#include <errno.h>
#define CAML_INTERNALS
#include <caml/signals.h>

CAMLprim value wrap_raise(value sig_)
{
  CAMLparam1(sig_);
  int sig = caml_convert_signal_number(Int_val(sig_));
  if (0 != raise(sig) && errno != EINTR) {
    fprintf(stderr, "Cannot raise(%d): %s\n", sig, strerror(errno));
  }
  CAMLreturn(Val_unit);
}

CAMLprim value wrap_uuid_of_u128_small(value n_)
{
  CAMLparam1(n_);
  CAMLlocal1(res);
# ifdef HAVE_UINT128
  uint128 n = get_uint128(n_);
  res = caml_alloc_string(36);
  char *s = (char *)String_val(res) + 36;
  static char nibbles[16] = "0123456789abcdef";
  for (unsigned i = 32; i-- > 0; ) {
    *(--s) = nibbles[n & 0xf];
    n >>= 4;
    if (i == 8 || i == 12 || i == 16 || i == 20) *(--s) = '-';
  }
# else
#   error "Not implemented: wrap_uuid_of_u128 when !HAVE_UINT128"
# endif
  CAMLreturn(res);
}

CAMLprim value wrap_uuid_of_u128_big(value n_)
{
  CAMLparam1(n_);
  CAMLlocal1(res);
# ifdef HAVE_UINT128
  uint128 n = get_uint128(n_);
  res = caml_alloc_string(36);
  char *s = (char *)String_val(res) + 36;
  static char bytes[2*16*16] =
    "000102030405060708090a0b0c0d0e0f"
    "101112131415161718191a1b1c1d1e1f"
    "202122232425262728292a2b2c2d2e2f"
    "303132333435363738393a3b3c3d3e3f"
    "404142434445464748494a4b4c4d4e4f"
    "505152535455565758595a5b5c5d5e5f"
    "606162636465666768696a6b6c6d6e6f"
    "707172737475767778797a7b7c7d7e7f"
    "808182838485868788898a8b8c8d8e8f"
    "909192939495969798999a9b9c9d9e9f"
    "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
    "b0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
    "c0c1c2c3c4c5c6c7c8c9cacbcccdcecf"
    "d0d1d2d3d4d5d6d7d8d9dadbdcdddedf"
    "e0e1e2e3e4e5e6e7e8e9eaebecedeeef"
    "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff";
  for (unsigned i = 16; i-- > 0; ) {
    char *byte = bytes + 2 * (n & 0xff);
    *(--s) = byte[1];
    *(--s) = byte[0];
    n >>= 8;
    if (i == 4 || i == 6 || i == 8 || i == 10) *(--s) = '-';
  }
# else
#   error "Not implemented: wrap_uuid_of_u128_bigger when !HAVE_UINT128"
# endif
  CAMLreturn(res);
}
