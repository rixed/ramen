// vim: ft=c bs=2 ts=2 sts=2 sw=2 expandtab
#include <assert.h>
#define CAML_NAME_SPACE
#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <caml/callback.h>

#include "country_of_ip.h"

CAMLprim value wrap_country_of_ipv4(value ip_)
{
  CAMLparam1(ip_);
  CAMLlocal1(ret_);
  assert(Is_block(ip_));
  assert(Tag_val(ip_) == Custom_tag);
  uint32_t ip = *(uint32_t *)Data_custom_val(ip_);
  char const *cc = country_of_ipv4(ip);
  if (! cc) caml_raise_not_found();

  ret_ = caml_copy_string(cc);
  CAMLreturn(ret_);
}

CAMLprim value wrap_country_of_ipv6(value ip_)
{
  CAMLparam1(ip_);
  CAMLlocal1(ret_);
  assert(Is_block(ip_));
  assert(Tag_val(ip_) == Custom_tag);
  uint128_t ip = *(uint128_t *)Data_custom_val(ip_);
  char const *cc = country_of_ipv6(ip);
  if (! cc) caml_raise_not_found();

  ret_ = caml_copy_string(cc);
  CAMLreturn(ret_);
}
