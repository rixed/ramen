#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>

#define CAML_NAME_SPACE
#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/custom.h>
#include <caml/fail.h>

#include "ringbuf/ringbuf.h"

#define STR_(s) STR(s)
#define STR(s) #s

/* type t for struct ringbuf */

static struct custom_operations ringbuf_ops = {
  "org.happyleptic.ramen.ringbuf",
  custom_finalize_default,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default,
  custom_compare_ext_default
};

#define Ringbuf_val(v) (*((struct ringbuf **)Data_custom_val(v)))

static value alloc_ringbuf(struct ringbuf *rb)
{
  CAMLparam0();
  CAMLlocal1(res);
  res = caml_alloc_custom(&ringbuf_ops, sizeof rb, 0, 1);
  Ringbuf_val(res) = rb;
  CAMLreturn(res);
}

/* type tx for struct wrap_ringbuf_tx (we wrap in there the actual rb) */

struct wrap_ringbuf_tx {
  struct ringbuf *rb;
  struct ringbuf_tx tx;
  size_t alloced;  // number of bytes alloced, just to check we do not overflow
};

static struct custom_operations tx_ops = {
  "org.happyleptic.ramen.ringbuf_tx",
  custom_finalize_default,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default,
  custom_compare_ext_default
};

#define RingbufTx_val(v) ((struct wrap_ringbuf_tx *)Data_custom_val(v))

static value alloc_tx(void)
{
  CAMLparam0();
  CAMLlocal1(res);
  res = caml_alloc_custom(&tx_ops, sizeof(struct wrap_ringbuf_tx), 0, 1);
  CAMLreturn(res);
}

CAMLprim value wrap_ringbuf_create(value fname_, value tot_words_)
{
  CAMLparam2(fname_, tot_words_);
  char *fname = String_val(fname_);
  unsigned tot_words = Long_val(tot_words_);
  int res = ringbuf_create(fname, tot_words);
  if (res < 0) caml_failwith("Cannot create ring buffer");
  CAMLreturn(Val_unit);
}

CAMLprim value wrap_ringbuf_load(value fname_)
{
  CAMLparam1(fname_);
  CAMLlocal1(res);
  char *fname = String_val(fname_);
  struct ringbuf *rb = ringbuf_load(fname);
  if (! rb) caml_failwith("Cannot load ring buffer");
  printf("MMapped %s @ %p\n", fname, rb);
  res = alloc_ringbuf(rb);
  CAMLreturn(res);
}

CAMLprim value wrap_ringbuf_unload(value rb_)
{
  CAMLparam1(rb_);
  struct ringbuf *rb = Ringbuf_val(rb_);
  if (0 != ringbuf_unload(rb)) caml_failwith("Cannot unload ring buffer");
  CAMLreturn(Val_unit);
}

CAMLprim value wrap_capacity(value rb_)
{
  CAMLparam1(rb_);
  struct ringbuf *rb = Ringbuf_val(rb_);
  CAMLlocal1(ret);
  ret = Val_long(rb->nb_words);
  CAMLreturn(ret);
}

CAMLprim value wrap_nb_entries(value rb_)
{
  CAMLparam1(rb_);
  struct ringbuf *rb = Ringbuf_val(rb_);
  CAMLlocal1(ret);
  ret = Val_long(ringbuf_nb_entries(rb, rb->prod_tail, rb->cons_head));
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
}

CAMLprim value wrap_ringbuf_enqueue(value rb_, value bytes_, value size_)
{
  CAMLparam3(rb_, bytes_, size_);
  struct ringbuf *rb = Ringbuf_val(rb_);
  int size = Long_val(size_);
  check_size(size);
  if (size < (int)caml_string_length(bytes_)) {
    caml_invalid_argument("enqueue: size must be less than the string length");
  }
  uint32_t nb_words = size / sizeof(uint32_t);
  uint32_t *bytes = (uint32_t *)String_val(bytes_);
  if (0 != ringbuf_enqueue(rb, bytes, nb_words)) {
    caml_failwith("Cannot enqueue bytes");
  }
  CAMLreturn(Val_unit);
}

CAMLprim value wrap_ringbuf_dequeue(value rb_)
{
  CAMLparam1(rb_);
  CAMLlocal1(bytes_);
  struct ringbuf *rb = Ringbuf_val(rb_);
  struct ringbuf_tx tx;
  ssize_t size = ringbuf_dequeue_alloc(rb, &tx);
  if (size < 0) caml_failwith("Cannot dequeue alloc");

  bytes_ = caml_alloc_string(size);
  if (! bytes_) caml_failwith("Cannot malloc dequeued bytes");
  memcpy(String_val(bytes_), rb->data + tx.record_start, size);

  ringbuf_dequeue_commit(rb, &tx);

  CAMLreturn(bytes_);
}

/* Lower level API */

CAMLprim value wrap_ringbuf_enqueue_alloc(value rb_, value size_)
{
  CAMLparam2(rb_, size_);
  struct ringbuf *rb = Ringbuf_val(rb_);
  int size = Long_val(size_);
  check_size(size);
  uint32_t nb_words = size / sizeof(uint32_t);
  CAMLlocal1(tx);
  tx = alloc_tx();
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  wrtx->rb = rb;
  wrtx->alloced = size;
  if (0 != ringbuf_enqueue_alloc(rb, &wrtx->tx, nb_words)) {
    caml_failwith("Cannot alloc for enqueue");
  }
  /*printf("Allocated %d bytes for enqueuing at offset %"PRIu32" (in words)\n",
         size, wrtx->tx.record_start);*/
  CAMLreturn(tx);
}

CAMLprim value wrap_ringbuf_enqueue_commit(value tx)
{
  CAMLparam1(tx);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  ringbuf_enqueue_commit(wrtx->rb, &wrtx->tx);
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
    caml_failwith("Cannot alloc for dequeue");
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
  return wrtx->rb->data /* Where the mmapped data starts */
       + wrtx->tx.record_start /* The offset of the record within that data */
       + offs/sizeof(uint32_t);
}

static void write_words(struct wrap_ringbuf_tx const *wrtx, size_t offs, char const *src, size_t size)
{
  assert(!(offs & 3));
  assert(size + offs <= wrtx->alloced);
  assert(size <= MAX_RINGBUF_MSG_SIZE);
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
  assert(!(offs & 3));
  if (offs + size > wrtx->alloced) {
    printf("BAD OFFS: offs=%zu, size=%zu but tx->alloced only %zu\n", offs, size, wrtx->alloced);
  }
  assert(size + offs <= wrtx->alloced);
  assert(size <= MAX_RINGBUF_MSG_SIZE);
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

#define WRITE_BOXED(bits) \
CAMLprim value write_boxed_##bits(value tx, value off_, value v_) \
{ \
  CAMLparam3(tx, off_, v_); \
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx); \
  size_t offs = Long_val(off_); \
  assert(Is_block(v_)); \
  assert(Tag_val(v_) == Custom_tag); \
  char const *src = Data_custom_val(v_); \
  write_words(wrtx, offs, src, bits / 8); \
  CAMLreturn(Val_unit); \
}

#define WRITE_UNBOXED_INT(bits) \
CAMLprim value write_boxed_##bits(value tx, value off_, value v_) \
{ \
  CAMLparam3(tx, off_, v_); \
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx); \
  size_t offs = Long_val(off_); \
  assert(Is_long(v_)); \
  uint##bits##_t v = (uint##bits##_t)Long_val(v_); \
  write_words(wrtx, offs, (char const *)&v, bits / 8); \
  CAMLreturn(Val_unit); \
}

WRITE_BOXED(128);
WRITE_BOXED(64);
WRITE_BOXED(48);
WRITE_BOXED(32);
WRITE_UNBOXED_INT(16);
WRITE_UNBOXED_INT(8);

extern struct custom_operations uint128_ops;
extern struct custom_operations uint64_ops;
extern struct custom_operations uint32_ops;
extern struct custom_operations int128_ops;
extern struct custom_operations caml_int64_ops;
extern struct custom_operations caml_int32_ops;

#define READ_BOXED(int_type, bits, ops) \
CAMLprim value read_##int_type##bits(value tx, value off_) \
{ \
  CAMLparam2(tx, off_); \
  CAMLlocal1(v); \
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx); \
  size_t offs = Long_val(off_); \
  v = caml_alloc_custom(&ops, bits / 8, 0, 1); \
  char *dst = Data_custom_val(v); \
  read_words(wrtx, offs, dst, bits / 8); \
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

READ_BOXED(uint, 128, uint128_ops);
READ_BOXED(uint, 64, uint64_ops);
READ_BOXED(uint, 48, uint32_ops);
READ_BOXED(uint, 32, uint32_ops);
READ_UNBOXED_INT(uint, 16);
READ_UNBOXED_INT(uint, 8);
READ_BOXED(int, 128, int128_ops);
READ_BOXED(int, 64, caml_int64_ops);
READ_BOXED(int, 32, caml_int32_ops);
READ_UNBOXED_INT(int, 16);
READ_UNBOXED_INT(int, 8);

CAMLprim value write_boxed_str(value tx, value off_, value v_)
{
  CAMLparam3(tx, off_, v_);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  size_t offs = Long_val(off_);
  assert(Is_block(v_));
  assert(Tag_val(v_) >= String_tag);
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
  assert(!(offs & 3));
  uint32_t *addr = where_to(wrtx, offs);

  assert(Is_long(v_));
  long v = Long_val(v_);
  //printf("Copy fixed value %ld at offset %zu\n", v, offs);
  assert(v <= UINT32_MAX);
  uint32_t src = v;

  memcpy(addr, &src, sizeof src);

  CAMLreturn(Val_unit);
}

// We need a special case for floats thanks to the many corner cases for floats
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
  read_words(wrtx, offs, (char *)&v, sizeof(v));
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
  assert(!(offs & 3));
  uint32_t *addr = where_to(wrtx, offs);
  int size = Long_val(size_);

  memset(addr, 0, size);

  CAMLreturn(Val_unit);
}

// Set the bit_ th bit in the tx to 1 (for the nullmask)
CAMLprim value set_bit(value tx, value bit_)
{
  CAMLparam2(tx, bit_);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  unsigned bit = Long_val(bit_);
  assert(bit/8 < wrtx->alloced);
  uint8_t *addr = (uint8_t *)where_to(wrtx, 0) + bit/8;
  uint8_t mask = 1U << (bit % 8);
  *addr |= mask;
  CAMLreturn(Val_unit);
}

// Return the bit_ th bit in the tx (for the nullmask)
CAMLprim value get_bit(value tx, value bit_)
{
  CAMLparam2(tx, bit_);
  CAMLlocal1(b);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  unsigned bit = Long_val(bit_);
  assert(bit/8 < wrtx->alloced);
  uint8_t const *addr = (uint8_t *)where_to(wrtx, 0) + bit/8;
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
  v = caml_alloc_string(size);
  read_words(wrtx, offs + sizeof size, String_val(v), size);
  CAMLreturn(v);
}
