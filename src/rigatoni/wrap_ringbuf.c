#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>

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
  res = caml_alloc_custom(&ringbuf_ops, sizeof(rb), 0, 1);
  Ringbuf_val(res) = rb;
  CAMLreturn(res);
}

/* type tx for struct wrap_ringbuf_tx (we wrap in there the actual rb) */

struct wrap_ringbuf_tx {
  struct ringbuf *rb;
  struct ringbuf_tx tx;
  size_t alloced;  // just to check we do not overflow
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
  CAMLlocal1(res);
  char *fname = String_val(fname_);
  unsigned tot_words = Int_val(tot_words_);
  struct ringbuf *rb = ringbuf_create(fname, tot_words);
  if (! rb) caml_failwith("Cannot create ring buffer");
  printf("MMapped %s @ %p\n", fname, rb);
  res = alloc_ringbuf(rb);
  CAMLreturn(res);
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
  int size = Int_val(size_);
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
  int size = Int_val(size_);
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
  printf("Allocated %d bytes for enqueuing at offset %"PRIu32" (in words)\n",
         size, wrtx->tx.record_start);
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
  printf("Allocated %zd bytes for dequeuing at offset %"PRIu32" (in words)\n",
         size, wrtx->tx.record_start);
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

static uint32_t *where_to_write(struct wrap_ringbuf_tx const *wrtx, size_t offs)
{
  return wrtx->rb->data /* Where the mmapped data starts */
       + wrtx->tx.record_start /* The offset of the record within that data */
       + offs/4;
}

static void write_boxed(struct wrap_ringbuf_tx const *wrtx, size_t offs, char const *src, size_t size)
{
  assert(!(offs & 3));
  assert(size + offs <= wrtx->alloced);
  assert(size <= MAX_RINGBUF_MSG_SIZE);
  uint32_t *addr = where_to_write(wrtx, offs);

  printf("Copy %zu bytes at offset %zu:", size, offs);
  for (unsigned s = 0 ; s < size ; s++) {
    printf(" %02" PRIx8, (uint8_t)src[s]);
  }
  printf("\n");

  memcpy(addr, src, size);
}

#define WRITE_BOXED(bits) \
CAMLprim value write_boxed_##bits(value tx, value off_, value v_) \
{ \
  CAMLparam3(tx, off_, v_); \
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx); \
  size_t offs = Int_val(off_); \
  assert(Is_block(v_)); \
  assert(Tag_val(v_) == Custom_tag); \
  char const *src = Data_custom_val(v_); \
  write_boxed(wrtx, offs, src, bits / 8); \
  CAMLreturn(Val_unit); \
}

WRITE_BOXED(128);
WRITE_BOXED(64);
WRITE_BOXED(32);
WRITE_BOXED(16);
WRITE_BOXED(8);

CAMLprim value write_boxed_str(value tx, value off_, value v_)
{
  CAMLparam3(tx, off_, v_);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  size_t offs = Int_val(off_);
  assert(Is_block(v_));
  assert(Tag_val(v_) >= String_tag);
  uint32_t size = caml_string_length(v_);
  char const *src = Bp_val(v_);
  // We must start this variable size field with its length:
  write_boxed(wrtx, offs, (char const *)&size, sizeof(size));
  write_boxed(wrtx, offs + sizeof(size), src, size);
  CAMLreturn(Val_unit);
}


CAMLprim value write_int(value tx, value off_, value v_)
{
  CAMLparam3(tx, off_, v_);
  struct wrap_ringbuf_tx *wrtx = RingbufTx_val(tx);
  size_t offs = Int_val(off_);
  assert(!(offs & 3));
  uint32_t *addr = where_to_write(wrtx, offs);

  assert(Is_long(v_));
  long v = Long_val(v_);
  printf("Copy fixed value %ld at offset %zu\n", v, offs);
  assert(v <= UINT32_MAX);
  uint32_t src = v;

  memcpy(addr, &src, sizeof(src));

  CAMLreturn(Val_unit);
}
