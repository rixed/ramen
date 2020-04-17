// vim: ft=c bs=2 ts=2 sts=2 sw=2 expandtab
#ifndef COUTRY_OF_IP_H_200417
#define COUTRY_OF_IP_H_200417
#include <stdint.h>

typedef __uint128_t uint128_t;

char const *country_of_ipv4(uint32_t ip);
char const *country_of_ipv6(uint128_t ip);

#endif
