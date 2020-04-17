// vim: ft=c bs=2 ts=2 sts=2 sw=2 expandtab
#ifndef COUNTRY_OF_IP_DB_H_200417
#define COUNTRY_OF_IP_DB_H_200417
#include <stdint.h>

typedef __uint128_t uint128_t;

struct db_entry_v4 {
  uint32_t from;
  uint32_t to;
  char cc[3];
};

struct db_entry_v6 {
  uint128_t from;
  uint128_t to;
  char cc[3];
};

#define SIZEOF_ARRAY(x) (sizeof(x) / sizeof(*(x)))

#endif
