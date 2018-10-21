// vim: ft=c bs=2 ts=2 sts=2 sw=2 expandtab
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <inttypes.h>
#include <string.h>
#include <arpa/inet.h>

#define CAML_NAME_SPACE
#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <caml/callback.h>

#include <uint8.h>
#include <uint16.h>
#include <uint32.h>

struct nf_msg {
  uint16_t version;
  uint16_t num_flows;
  uint32_t sys_uptime;
  uint32_t ts_sec;
  uint32_t ts_nsec;
  uint32_t seqnum;
  uint8_t engine_type;
  uint8_t engine_id;
  uint16_t sampling;
} __attribute__((__packed__));

struct nf_flow {
  uint32_t addr[2];
  uint32_t next_hop;
  uint16_t in_iface, out_iface;
  uint32_t packets;
  uint32_t bytes;
  uint32_t first; // sysuptime at the first packet
  uint32_t last;  // sysuptime at the last packet
  uint16_t port[2];
  uint8_t padding;
  uint8_t tcp_flags;
  uint8_t ip_proto;
  uint8_t ip_tos;
  uint16_t as[2];
  uint8_t mask[2];
  uint16_t padding2;
} __attribute__((__packed__));

CAMLprim value wrap_netflow_v5_decode(
    value buffer_, value num_bytes_, value source_)
{
  CAMLparam3(buffer_, num_bytes_, source_);
  CAMLlocal2(res, tup);
  unsigned num_bytes = Long_val(num_bytes_);
  assert(caml_string_length(buffer_) >= num_bytes);
  if (num_bytes < sizeof(struct nf_msg)) {
    caml_invalid_argument("message smaller than netflow header");
  }

  // Assuming buffer will be suitably aligned:
  struct nf_msg const *msg = (struct nf_msg *)String_val(buffer_);

  unsigned const version = ntohs(msg->version);
  if (version != 5) {
    caml_invalid_argument("not netflow v5");
  }
  unsigned const num_flows = ntohs(msg->num_flows);
  double const boot_time =
    ntohl(msg->ts_sec) +
    ntohl(msg->ts_nsec) / 1e9 -
    ntohl(msg->sys_uptime) / 1e3;

  size_t const tot_size = sizeof(*msg) + num_flows*sizeof(struct nf_flow);
  if (num_bytes < tot_size) {
    caml_invalid_argument("truncated message or not netflow");
  }

  struct nf_flow const *f = (struct nf_flow *)(msg+1);
  // The array of tuples:
  res = caml_alloc(num_flows, 0);

# define NB_FLOW_FIELDS 24
  for (unsigned i = 0; i < num_flows; i++, f++) {
    // Alloc a new tuple:
    tup = caml_alloc(NB_FLOW_FIELDS, 0);
    unsigned j = 0;
    // Source
    Store_field(tup, j++, source_);
    // Time
    double const first = boot_time + ntohl(f->first) / 1e3;
    Store_field(tup, j++, caml_copy_double(first));
    double const last = boot_time + ntohl(f->last) / 1e3;
    Store_field(tup, j++, caml_copy_double(last));
    // Header
    Store_field(tup, j++, copy_uint32(ntohl(msg->seqnum)));
    Store_field(tup, j++, Val_uint8(msg->engine_type));
    Store_field(tup, j++, Val_uint8(msg->engine_id));
    unsigned const sampling = ntohs(msg->sampling);
    Store_field(tup, j++, Val_uint8(sampling >> 14U));
    Store_field(tup, j++, Val_uint16(sampling & 0x2FFF));
    // Flow
    Store_field(tup, j++, copy_uint32(ntohl(f->addr[0])));
    Store_field(tup, j++, copy_uint32(ntohl(f->addr[1])));
    Store_field(tup, j++, copy_uint32(ntohl(f->next_hop)));
    Store_field(tup, j++, Val_uint16(ntohs(f->port[0])));
    Store_field(tup, j++, Val_uint16(ntohs(f->port[1])));
    Store_field(tup, j++, Val_uint16(ntohs(f->in_iface)));
    Store_field(tup, j++, Val_uint16(ntohs(f->out_iface)));
    Store_field(tup, j++, copy_uint32(ntohl(f->packets)));
    Store_field(tup, j++, copy_uint32(ntohl(f->bytes)));
    Store_field(tup, j++, Val_uint8(f->tcp_flags));
    Store_field(tup, j++, Val_uint8(f->ip_proto));
    Store_field(tup, j++, Val_uint8(f->ip_tos));
    Store_field(tup, j++, Val_uint16(ntohs(f->as[0])));
    Store_field(tup, j++, Val_uint16(ntohs(f->as[1])));
    Store_field(tup, j++, Val_uint8(f->mask[0]));
    Store_field(tup, j++, Val_uint8(f->mask[1]));

    assert(j == NB_FLOW_FIELDS);
    Store_field(res, i, tup);
  }

  CAMLreturn(res);
}
