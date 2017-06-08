#define CAML_NAME_SPACE
#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/custom.h>
#include <caml/fail.h>

#include "ringbuf.h"

#define STR_(s) STR(s)
#define STR(s) #s

/* type t for ring buffers */

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

CAMLprim value wrap_ringbuf_create(value fname_, value tot_words_)
{
  CAMLparam2(fname_, tot_words_);
  CAMLlocal1(res);
  char *fname = String_val(fname_);
  unsigned tot_words = Int_val(tot_words_);
  struct ringbuf *rb = ringbuf_create(fname, tot_words);
  if (! rb) caml_failwith("Cannot create ring buffer");
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
  res = alloc_ringbuf(rb);
  CAMLreturn(res);
}

#define MAX_RINGBUF_MSG_SIZE 8096

CAMLprim value wrap_ringbuf_enqueue(value rb_, value bytes_, value size_)
{
  CAMLparam3(rb_, bytes_, size_);
  struct ringbuf *rb = Ringbuf_val(rb_);
  int size = Int_val(size_);
  if (size & 3) {
    caml_invalid_argument("enqueue: size must be a multiple of 4 bytes");
  }
  if (size < (int)caml_string_length(bytes_)) {
    caml_invalid_argument("enqueue: size must be less than the string length");
  }
  if (size > MAX_RINGBUF_MSG_SIZE) {
    caml_invalid_argument("enqueue: size must be less than " STR(MAX_RINGBUF_MSG_SIZE));
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
  bytes_ = caml_alloc_string(MAX_RINGBUF_MSG_SIZE);
  if (! bytes_) caml_failwith("Cannot malloc " STR(MAX_RINGBUF_MSG_SIZE) " bytes");
  uint32_t *bytes = (uint32_t *)String_val(bytes_);
  assert((intptr_t)bytes & 3 == 0);
  if (0 != ringbuf_dequeue(rb, bytes, MAX_RINGBUF_MSG_SIZE/sizeof(uint32_t))) {
    caml_failwith("Cannot dequeue bytes");
  }
}

