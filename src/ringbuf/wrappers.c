// vim: ft=c bs=2 ts=2 sts=2 sw=2 expandtab
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <inttypes.h>
#include <string.h>
#include <sys/types.h>  // getpid
#include <unistd.h>  // getpid

#define CAML_NAME_SPACE
#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <caml/callback.h>

#include "ringbuf.h"

static value exn_NoMoreRoom, exn_Empty, exn_Damaged;
static bool exceptions_inited = false;

static void retrieve_exceptions(void)
{
  if (exceptions_inited) return;

  exn_NoMoreRoom = *caml_named_value("ringbuf full exception");
  exn_Empty = *caml_named_value("ringbuf empty exception");
  exn_Damaged = *caml_named_value("ringbuf damaged exception");
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
  uint8_t *bytes;     // for "bytes" TXs
  struct ringbuf_tx tx;
  // Number of bytes alloced either in the RB transaction or in *bytes
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

CAMLprim value wrap_ringbuf_create(value version_, value wrap_, value tot_words_, value fname_)
{
  CAMLparam4(version_, wrap_, fname_, tot_words_);
  char *version_str = String_val(version_);
  uint64_t version = uint64_of_version(version_str);
  bool wrap = Bool_val(wrap_);
  char *fname = String_val(fname_);
  unsigned tot_words = Long_val(tot_words_);
  enum ringbuf_error err = ringbuf_create(version, wrap, tot_words, fname);
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

CAMLprim value wrap_ringbuf_stats(value rb_)
{
  CAMLparam1(rb_);
  struct ringbuf *rb = Ringbuf_val(rb_);
  struct ringbuf_file *rbf = rb->rbf;
  CAMLlocal1(ret);
  // See type stats in RingBuf.ml
  ret = caml_alloc_tuple(12);
  Field(ret, 0) = Val_long(rbf->num_words);
  Field(ret, 1) = Val_bool(rbf->wrap);
  Field(ret, 2) = Val_long(ringbuf_file_num_entries(rbf, rbf->prod_tail, rbf->cons_head));
  Field(ret, 3) = Val_long(rbf->num_allocs);
  Field(ret, 4) = caml_copy_double(rbf->tmin);
  Field(ret, 5) = caml_copy_double(rbf->tmax);
  Field(ret, 6) = Val_long(rb->mmapped_size);
  Field(ret, 7) = Val_long(rbf->prod_head);
  Field(ret, 8) = Val_long(rbf->prod_tail);
  Field(ret, 9) = Val_long(rbf->cons_head);
  Field(ret, 10) = Val_long(rbf->cons_tail);
  Field(ret, 11) = Val_long(rbf->first_seq);
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

#define MAX_RINGBUF_MSG_SIZE 8096

static void check_size(int size)
{
  if (size & 3) {
    caml_invalid_argument("enqueue: size must be a multiple of 4 bytes");
  }
  if (size > MAX_RINGBUF_MSG_SIZE) {
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
      caml_raise_constant(exn_NoMoreRoom);
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
  double tmin = Double_val(tmin_);
  double tmax = Double_val(tmax_);
  uint32_t num_words = size / sizeof(uint32_t);
  uint32_t *bytes = (uint32_t *)String_val(bytes_);
  check_error(
    ringbuf_enqueue(rb, bytes, num_words, tmin, tmax),
    "Cannot ringbuf_enqueue",
    "Ringbuf version mismatch in ringbuf_enqueue");
  CAMLreturn(Val_unit);
}

/* A function to read raw words from anywhere in the ringbuffer, used by
 * `ramen ringbuf-summary`. */
CAMLprim value wrap_ringbuf_read_raw(value rb_, value index_, value num_words_)
{
  CAMLparam3(rb_, index_, num_words_);
  CAMLlocal1(bytes_);
  struct ringbuf *rb = Ringbuf_val(rb_);
  unsigned index = Long_val(index_);
  unsigned num_words = Long_val(num_words_);
  ssize_t size = num_words * sizeof(*rb->rbf->data);

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

CAMLprim value wrap_ringbuf_dequeue(value rb_)
{
  CAMLparam1(rb_);
  CAMLlocal1(bytes_);
  struct ringbuf *rb = Ringbuf_val(rb_);
  struct ringbuf_tx tx;
  ssize_t size = ringbuf_dequeue_alloc(rb, &tx);
  if (size < 0) {
    assert(exceptions_inited);
    caml_raise_constant(exn_Empty);
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
  struct ringbuf *rb = Ringbuf_val(rb_);
  CAMLlocal1(tx);
  tx = alloc_tx();
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  wrtx->rb = rb;
  ssize_t size = ringbuf_read_first(rb, &wrtx->tx);
  if (size == -2) {
    // Error:
    caml_failwith("Invalid buffer file");
  } else if (size == -1) {
    // Have to wait for content:
    assert(exceptions_inited);
    caml_raise_constant(exn_Empty);
  } else {
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

  ssize_t size = ringbuf_read_next(wrtx->rb, &wrtx->tx);
  if (size == 0) {
    caml_raise_end_of_file();
  } else if (size == -1) {
    // Have to wait for content:
    assert(exceptions_inited);
    caml_raise_constant(exn_Empty);
  } else {
    wrtx->alloced = (size_t)size;
    CAMLreturn(tx);
  }
}

// Returns a TX that writes into a buffer on the heap instead of a ringbuf:
CAMLprim value wrap_bytes_tx(value size_)
{
  CAMLparam1(size_);
  CAMLlocal1(tx);
  size_t size = Long_val(size_);
  tx = alloc_tx();
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  memset(wrtx, 0, sizeof(*wrtx));
  wrtx->bytes = malloc(size);
  assert(wrtx->bytes);
  wrtx->alloced = size;
  CAMLreturn(tx);
}

/* Lower level API */

CAMLprim value wrap_ringbuf_enqueue_alloc(value rb_, value size_)
{
  CAMLparam2(rb_, size_);
  struct ringbuf *rb = Ringbuf_val(rb_);
  int size = Long_val(size_);
  check_size(size);
  uint32_t num_words = size / sizeof(uint32_t);
  CAMLlocal1(tx);
  tx = alloc_tx();
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  wrtx->rb = rb;
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
  struct ringbuf *rb = Ringbuf_val(rb_);
  CAMLlocal1(tx);
  tx = alloc_tx();
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  wrtx->rb = rb;
  ssize_t size = ringbuf_dequeue_alloc(rb, &wrtx->tx);
  if (size < 0) {
    assert(exceptions_inited);
    caml_raise_constant(exn_Empty);
  }
  /*printf("Allocated %zd bytes for dequeuing at offset %"PRIu32" (in words)\n",
         size, wrtx->tx.record_start);*/
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
  return wrtx->rb->rbf->data /* Where the mmapped data starts */
       + wrtx->tx.record_start /* The offset of the record within that data */
       + offs/sizeof(uint32_t);
}

static void write_words(struct wrap_ringbuf_tx const *wrtx, size_t offs, char const *src, size_t size)
{
  if (size + offs > wrtx->alloced) {
    printf("ERROR: size (%zu) + offs (%zu) > alloced (%zu)\n", size, offs, wrtx->alloced);
    fflush(stdout);
    assert(exceptions_inited);
    caml_raise_constant(exn_Damaged);
  }

  if (size > MAX_RINGBUF_MSG_SIZE) {
    printf("ERROR: size (%zu) > " STR(MAX_RINGBUF_MSG_SIZE) "\n", size);
    fflush(stdout);
    assert(exceptions_inited);
    caml_raise_constant(exn_Damaged);
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
    printf("ERROR: offs (%zu) + size (%zu) > alloced (%zu)\n", offs, size, wrtx->alloced);
    fflush(stdout);
    assert(exceptions_inited);
    caml_raise_constant(exn_Damaged);
  }
  if (size > MAX_RINGBUF_MSG_SIZE) {
    printf("ERROR: size (%zu) > " STR(MAX_RINGBUF_MSG_SIZE) "\n", size);
    fflush(stdout);
    assert(exceptions_inited);
    caml_raise_constant(exn_Damaged);
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

/* Integers are serialized in the ringbuffers as they are encoded in
 * OCaml custom values. In particular, int48s are shifted 16bits higher.
 * When we move to C workers or allow C programs to write directly in the
 * ringbuffers then we must revisit this. */

#define WRITE_BOXED(bits, custom_sz) \
CAMLprim value write_boxed_##bits(value tx, value off_, value v_) \
{ \
  CAMLparam3(tx, off_, v_); \
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx); \
  size_t offs = Long_val(off_); \
  assert(Is_block(v_)); \
  assert(Tag_val(v_) == Custom_tag); \
  char const *src = Data_custom_val(v_); \
  write_words(wrtx, offs, src, custom_sz); \
  CAMLreturn(Val_unit); \
}

#define WRITE_UNBOXED_INT(bits) \
CAMLprim value write_unboxed_##bits(value tx, value off_, value v_) \
{ \
  CAMLparam3(tx, off_, v_); \
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx); \
  size_t offs = Long_val(off_); \
  assert(Is_long(v_)); \
  uint##bits##_t v = (uint##bits##_t)Long_val(v_); \
  /* In little endian only: */ \
  write_words(wrtx, offs, (char const *)&v, bits / 8); \
  CAMLreturn(Val_unit); \
}

WRITE_BOXED(128, 16);
WRITE_BOXED(64, 8);
WRITE_BOXED(48, 8);
WRITE_BOXED(32, 4);
WRITE_UNBOXED_INT(16);
WRITE_UNBOXED_INT(8);

CAMLprim value write_ip(value tx, value off_, value v_) \
{
  CAMLparam3(tx, off_, v_);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  size_t offs = Long_val(off_);
  assert(Is_block(v_));
  // Write the tag then the IP
  uint32_t const tag = Tag_val(v_);
  assert(tag == 0 || tag == 1);
  write_words(wrtx, offs, (char const *)&tag, sizeof tag);
  if (tag == 0) {
    write_boxed_32(tx, Val_long(offs + sizeof tag), Field(v_, 0));
  } else {
    write_boxed_128(tx, Val_long(offs + sizeof tag), Field(v_, 0));
  }
  CAMLreturn(Val_unit);
}


extern struct custom_operations uint128_ops;
extern struct custom_operations uint64_ops;
extern struct custom_operations uint32_ops;
extern struct custom_operations int128_ops;
extern struct custom_operations caml_int64_ops;
extern struct custom_operations caml_int32_ops;

#define READ_BOXED(int_type, bits, ops, custom_sz) \
CAMLprim value read_##int_type##bits(value tx, value off_) \
{ \
  CAMLparam2(tx, off_); \
  CAMLlocal1(v); \
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx); \
  size_t offs = Long_val(off_); \
  v = caml_alloc_custom(&ops, custom_sz, 0, 1); \
  char *dst = Data_custom_val(v); \
  read_words(wrtx, offs, dst, custom_sz); \
  CAMLreturn(v); \
}

#define READ_UNBOXED_INT(int_type, bits) \
CAMLprim value read_##int_type##bits(value tx, value off_) \
{ \
  CAMLparam2(tx, off_); \
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx); \
  size_t offs = Long_val(off_); \
  int_type##bits##_t v = 0; \
  read_words(wrtx, offs, (char *)&v, bits / 8); \
  CAMLreturn(Val_long(v)); \
}

READ_BOXED(uint, 128, uint128_ops, 16);
READ_BOXED(uint, 64, uint64_ops, 8);
READ_BOXED(uint, 48, uint64_ops, 8);
READ_BOXED(uint, 32, uint32_ops, 4);
READ_UNBOXED_INT(uint, 16);
READ_UNBOXED_INT(uint, 8);
READ_BOXED(int, 128, int128_ops, 16);
READ_BOXED(int, 64, caml_int64_ops, 8);
READ_BOXED(int, 32, caml_int32_ops, 4);
READ_UNBOXED_INT(int, 16);
READ_UNBOXED_INT(int, 8);

CAMLprim value read_ip(value tx, value off_)
{
  CAMLparam2(tx, off_);
  CAMLlocal1(v);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  size_t offs = Long_val(off_);
  uint32_t tag;
  read_words(wrtx, offs, (char *)&tag, sizeof tag);
  v = caml_alloc(1, tag);
  if (tag == 0) { // V4
    Store_field(v, 0, read_uint32(tx, Val_long(offs + sizeof tag)));
  } else {
    assert(tag == 1);  // V6
    Store_field(v, 0, read_uint128(tx, Val_long(offs + sizeof tag)));
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

/* Those two should not be here but in an additional misc lib. */

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
