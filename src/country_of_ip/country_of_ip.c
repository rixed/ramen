// vim: ft=c bs=2 ts=2 sts=2 sw=2 expandtab
#include <stdbool.h>
#include <stdlib.h>
#include "country_of_ip.h"
#include "db_v4.h"
#include "db_v6.h"

#define SEARCH(db) do { \
  size_t first = 0, last = SIZEOF_ARRAY(db) - 1; \
 \
  if (ip < db[first].from || ip > db[last].to) return NULL; \
 \
  while (last > first) { \
    size_t mid = first + (last - first + 1)/2; \
    if (ip < db[mid].from) { \
      last = mid - 1; \
    } else { \
      first = mid; \
    } \
  } \
 \
  if (ip < db[first].from || ip > db[first].to) return NULL; \
 \
  return db[first].cc; \
} while (false)

char const *country_of_ipv4(uint32_t ip)
{
  SEARCH(db_v4);
}

char const *country_of_ipv6(uint128_t ip)
{
  SEARCH(db_v6);
}
