// vim: ft=c bs=2 ts=2 sts=2 sw=2 expandtab
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <inttypes.h>
#include <string.h>

#define CAML_NAME_SPACE
#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <caml/callback.h>

#include "collectd.h"

static void set_nullable_string(value block, unsigned idx, char const *str)
{
  CAMLparam1(block);
  CAMLlocal1(tmp);
  if (!str || str[0] == '\0')
    Store_field(block, idx, Val_int(0));
  else {
    tmp = caml_alloc(1, 0);
    Store_field(tmp, 0, caml_copy_string(str));
    Store_field(block, idx, tmp);
  }
  CAMLreturn0;
}

CAMLprim value wrap_collectd_decode(value buffer_, value start_, value stop_)
{
  CAMLparam3(buffer_, start_, stop_);
  CAMLlocal4(res, arr, m_tup, tmp);
  unsigned start = Long_val(start_);
  unsigned stop = Long_val(stop_);
  assert(start <= stop);
  assert(start <= caml_string_length(buffer_));
  assert(stop <= caml_string_length(buffer_));

  unsigned num_metrics;
  struct collectd_metric *metrics; // Will point into mem
  unsigned consumed;
  char mem[4096];
  // Must not call caml_alloc from there until we are done with buffer
  char *buffer = String_val(buffer_) + start;
  enum collectd_decode_status status =
    collectd_decode(stop - start, buffer, sizeof(mem), mem, &num_metrics, &metrics, &consumed);

  /* Return an array of collectd_metric and number of consumed bytes */
  arr = caml_alloc(num_metrics, 0);

  //printf("collectd_decode: collected %u metrics\n", num_metrics);
  for (unsigned i = 0; i < num_metrics; i++) {
    struct collectd_metric *m = metrics + i;
    assert(m->num_values > 0);
    m_tup = caml_alloc(6 + COLLECTD_NB_VALUES, 0);
    Store_field(m_tup, 0, caml_copy_string(m->host));
    set_nullable_string(m_tup, 1, m->plugin_instance);
    set_nullable_string(m_tup, 2, m->plugin_name);
    Store_field(m_tup, 3, caml_copy_double(m->time));
    set_nullable_string(m_tup, 4, m->type_instance);
    set_nullable_string(m_tup, 5, m->type_name);
    Store_field(m_tup, 6+0, caml_copy_double(m->values[0]));
    unsigned v;
    for (v = 1; v < m->num_values; v++) {
      tmp = caml_alloc(1, 0);
      Store_field(tmp, 0, caml_copy_double(m->values[v]));
      Store_field(m_tup, 6+v, tmp);
    }
    for (; v < COLLECTD_NB_VALUES; v++) {
      Store_field(m_tup, 6+v, Val_int(0)); // None
    }
    Store_field(arr, i, m_tup);
  }

  switch (status) {
    case COLLECTD_OK:
      break;
    case COLLECTD_NOT_ENOUGH_RAM:
      fprintf(stderr, "collectd_decode: not enough RAM!\n");
      break;
    case COLLECTD_PARSE_ERROR:
      fprintf(stderr, "collectd_decode: parse error!\n");
      break;
  }

  /* Now add the number of consumed bytes: */
  res = caml_alloc_tuple(2);
  Store_field(res, 0, arr);
  Store_field(res, 1, Val_long(consumed));
  CAMLreturn(res);
}
