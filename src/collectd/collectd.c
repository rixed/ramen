#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <stdio.h>

#include "collectd.h"

/* We have two things to allocate: strings and the array of metrics,
 * that will grow to some unknown size. We allocate the metrics from the
 * beginning of the given memory block, and the strings from the end.
 * For simplicity, we store a few info at the beginning of the block itself
 * and make it a struct alloc_block:
 */
struct arena {
  size_t size;
  size_t metrics_size;
  size_t strings_size;
  struct collectd_metric bytes[];
};

static void construct_arena(struct arena *arena, size_t mem_size)
{
  assert(mem_size > sizeof(struct arena));
  arena->size = mem_size - sizeof(struct arena);
  arena->metrics_size = arena->strings_size = 0;
}

static struct collectd_metric *alloc_metric(struct arena *arena)
{
  if (arena->metrics_size + arena->strings_size + sizeof(struct collectd_metric) >
      arena->size) return NULL;

  struct collectd_metric *ret =
    (struct collectd_metric *)((char *)arena->bytes + arena->metrics_size);
  arena->metrics_size += sizeof(struct collectd_metric);
  return ret;
}

// len must include the terminal NUL byte
static char *alloc_string(struct arena *arena, size_t len)
{
  if (arena->metrics_size + arena->strings_size + len >
      arena->size) return NULL;

  arena->strings_size += len;
  return (char *)arena->bytes + arena->size - arena->strings_size;
}

enum collectd_decode_status collectd_decode(
  size_t msg_size, char const *msg_, size_t mem_size, void *mem,
  unsigned *nb_metrics, struct collectd_metric **metrics)
{
  unsigned char const *msg = (unsigned char const *)msg_;
  struct arena *arena = (struct arena *)mem;
  construct_arena(arena, mem_size);

  *nb_metrics = 0;
  *metrics = arena->bytes;

  /* Context. We keep those values until replaced.
   * Experience shows that the data type (type=6) comes last so we commit the
   * metric when we got that one.
   * We cannot keep pointers toward the buffer, which is still an OCaml
   * byte string, which can therefore be moved in memory next time an alloc
   * is done (ie when wrap_collectd_decode build the resulting array. *).
   * So we will move all interesting strings in the arena as well. */
  char *host = "";  // Not nullable so better start from a valid string
  double time = 0.;
  char *plugin_name = NULL;
  char *plugin_instance = NULL;
  char *type_name = NULL;
  char *type_instance = NULL;

  // Each iteration decodes a part
  for (size_t p = 0; p < msg_size; ) {
#   define CHECK(SZ) do { \
      if (p + SZ > msg_size) return COLLECTD_SHORT_DATA; \
    } while (0)
    // decode part header
    CHECK(4);
    unsigned part_type = (msg[p] << 8U) + msg[p+1];
    unsigned part_length = (msg[p+2] << 8U) + msg[p+3];
    if (part_length < 4) return COLLECTD_PARSE_ERROR;
    p += 4;
    part_length -= 4;

    CHECK(part_length);
    // TODO: use part type 9 to get an idea of when the next collection should
    // take place?

    switch (part_type) {
#     define DECODE_STRING(VAR) do { \
        if (part_length < 1) return COLLECTD_PARSE_ERROR; \
        char const *src = (char const *)(msg + p); \
        p += part_length; \
        if (msg[p-1] != '\0') return COLLECTD_PARSE_ERROR; \
        VAR = alloc_string(arena, part_length); \
        if (! (VAR)) return COLLECTD_NOT_ENOUGH_RAM; \
        memcpy(VAR, src, part_length); \
      } while (0)

#     define DECODE_NUM(VAR) do { \
        uint64_t u = \
          ((uint64_t)msg[p+0] << 56U) | ((uint64_t)msg[p+1] << 48U) | \
          ((uint64_t)msg[p+2] << 40U) | ((uint64_t)msg[p+3] << 32U) | \
          ((uint64_t)msg[p+4] << 24U) | ((uint64_t)msg[p+5] << 16U) | \
          ((uint64_t)msg[p+6] <<  8U) | ((uint64_t)msg[p+7]); \
        VAR = u; \
        p += 8; \
      } while (0)

#     define DECODE_SNUM(VAR) do { \
        int64_t u = \
          ((uint64_t)msg[p+0] << 56U) | ((uint64_t)msg[p+1] << 48U) | \
          ((uint64_t)msg[p+2] << 40U) | ((uint64_t)msg[p+3] << 32U) | \
          ((uint64_t)msg[p+4] << 24U) | ((uint64_t)msg[p+5] << 16U) | \
          ((uint64_t)msg[p+6] <<  8U) | ((uint64_t)msg[p+7]); \
        VAR = u; \
        p += 8; \
      } while (0)

      case 0: // hostname
        DECODE_STRING(host);
        break;

      case 1: // low-res time
        if (part_length != 8) return COLLECTD_PARSE_ERROR;
        DECODE_NUM(time);
        break;

      case 8: // high-res time
        if (part_length != 8) return COLLECTD_PARSE_ERROR;
        DECODE_NUM(time);
        time /= 1073741824.;
        break;

      case 2: // plugin name
        DECODE_STRING(plugin_name);
        break;

      case 3: // plugin instance
        DECODE_STRING(plugin_instance);
        break;

      case 4: // type name
        DECODE_STRING(type_name);
        break;

      case 5: // type instance
        DECODE_STRING(type_instance);
        break;

      case 6: // finally the value
        {
          if (part_length < 2) {
            fprintf(stderr, "part_length = %u for a value part?!\n", part_length);
            return COLLECTD_PARSE_ERROR;
          }
          unsigned nb_vals = (msg[p] << 8U) + msg[p+1];
          p += 2;
          part_length -= 2;
          if (part_length != nb_vals * 9) {
            fprintf(stderr, "collectd_decode: %u bytes left in value part but only %u values!\n",
                    part_length, nb_vals);
            return COLLECTD_PARSE_ERROR;
          }
          uint8_t const *types = msg + p;
          p += nb_vals;

          struct collectd_metric *metric = alloc_metric(arena);
          if (! metric) return COLLECTD_NOT_ENOUGH_RAM;
          metric->host = host;
          metric->time = time;
          metric->plugin_name = plugin_name;
          metric->plugin_instance = plugin_instance;
          metric->type_name = type_name;
          metric->type_instance = type_instance;
          metric->nb_values = nb_vals;
          // Fill in the values:
          unsigned v;
          for (v = 0; v < nb_vals && v < COLLECTD_NB_VALUES; v++) {
            double *val = metric->values + v;
            switch (types[v]) {
              case 1: // gauge, little endian double because why not
                assert(sizeof(*val) <= 8);
                memcpy(val, msg+p, sizeof(*val));
                p += 8;
                break;
              case 0:
              case 3: // unsigned int
                DECODE_NUM(*val);
                break;
              case 2: // signed int
                DECODE_SNUM(*val);
                break;
              default:
                fprintf(stderr, "collectd_decode: unknown value type %"PRIu8"\n", types[v]);
                return COLLECTD_PARSE_ERROR;
            }
          }

          (*nb_metrics) ++;
        }
        break;
      default: // skip anything else
        p += part_length;
        break;
    }
  }
  return COLLECTD_OK;
}
