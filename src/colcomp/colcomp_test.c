#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>
#include <sys/time.h> // gettimeofday
#include "colcomp.h"

void check_ok(enum colcomp_result res)
{
  switch (res) {
    case COLCOMP_OK: break;
    case COLCOMP_ERR_INVALID_PARAMETER:
      printf("Got COLCOMP_ERR_INVALID_PARAMETER\n");
      assert(false);
      break;
    case COLCOMP_ERR_CANNOT_MALLOC:
      printf("Got COLCOMP_ERR_CANNOT_MALLOC\n");
      assert(false);
      break;
  }
}

#define COLCOMP_TEST_ARR(TNAME, TYPE) \
static void colcomp_test_##TNAME(void) \
{ \
  unsigned nb_items = rand() % 5; \
  TYPE arr[nb_items]; \
  for (unsigned i = 0; i < nb_items ; i++) { \
    /* Init with some slowly varying values */ \
    arr[i] = rand() % 100 - 50; \
  } \
  void *comp; size_t comp_sz; char *op; \
  check_ok(colcomp_compress_##TNAME(arr, nb_items, &comp, &comp_sz, &op)); \
  printf("Compressed %u " #TYPE " (%zu bytes) into %zu bytes using %s\n", \
         nb_items, sizeof(arr), comp_sz, op); \
  TYPE *out; unsigned nb_items_out; \
  check_ok(colcomp_decompress_##TNAME(comp, comp_sz, op, &out, &nb_items_out)); \
  assert(nb_items_out == nb_items); \
  assert(0 == memcmp(out, arr, sizeof(arr))); \
  free(comp); free(out); \
}

COLCOMP_TEST_ARR(int8, int8_t);
COLCOMP_TEST_ARR(int16, int16_t);
COLCOMP_TEST_ARR(int32, int32_t);
COLCOMP_TEST_ARR(int64, int64_t);
#ifdef HAVE_INT128
COLCOMP_TEST_ARR(int128, int128_t);
#endif

COLCOMP_TEST_ARR(uint8, uint8_t);
COLCOMP_TEST_ARR(uint16, uint16_t);
COLCOMP_TEST_ARR(uint32, uint32_t);
COLCOMP_TEST_ARR(uint64, uint64_t);
#ifdef HAVE_INT128
COLCOMP_TEST_ARR(uint128, uint128_t);
#endif

COLCOMP_TEST_ARR(float, float);
COLCOMP_TEST_ARR(double, double);

static void colcomp_test_bool(void)
{
  unsigned nb_bits = rand() % 100;
  char s[(nb_bits+7)/8];
  for (unsigned i = 0; i < sizeof(s); i++) s[i] = rand();

  void *comp; size_t comp_sz; char *op;
  check_ok(colcomp_compress_bool(s, nb_bits, &comp, &comp_sz, &op));
  printf("Compressed %u bits (%zu bytes) into %zu bytes using %s\n",
         nb_bits, sizeof(s), comp_sz, op);

  char *out; unsigned nb_bits_out;
  check_ok(colcomp_decompress_bool(comp, comp_sz, op, &out, &nb_bits_out));
  assert(nb_bits == nb_bits_out);
  assert(0 == memcmp(out, s, nb_bits/8));
  unsigned nb_bits_last = nb_bits % 8;
  if (nb_bits_last) {
    unsigned mask = (1 << nb_bits_last) - 1;
    assert((s[sizeof(s)-1] & mask) == (out[sizeof(s)-1] & mask));
  }
}

static void colcomp_test_string(void)
{
  unsigned nb_strings = rand() % 100;
  char *arr[nb_strings];
  size_t tot_in_size = sizeof(arr);

  for (unsigned si = 0; si < nb_strings; si++) {
    unsigned s_len = 1 + rand() % 10;
    arr[si] = malloc(s_len);
    tot_in_size += s_len;
    assert(arr[si]);
    for (unsigned ci = 0; ci < s_len-1; ci++) {
      arr[si][ci] = 'a' + rand() % 10;
    }
    arr[si][s_len-1] = '\0';
  }

  void *comp; size_t comp_sz; char *op;
  check_ok(colcomp_compress_string((char const *const *)arr, nb_strings, &comp, &comp_sz, &op));
  printf("Compressed %u strings (%zu total bytes) into %zu bytes using %s\n",
         nb_strings, tot_in_size, comp_sz, op);

  char **out; unsigned nb_strings_out;
  check_ok(colcomp_decompress_string(comp, comp_sz, op, &out, &nb_strings_out));
  assert(nb_strings_out == nb_strings);
  for (unsigned si = 0; si < nb_strings; si++) {
    assert(0 == strcmp(arr[si], out[si]));
  }
}

int main(int nb_args, char **args)
{
  struct timeval tv;
  (void)gettimeofday(&tv, NULL);
  unsigned seed = tv.tv_usec;
  if (nb_args >= 2) seed = strtoul(args[1], NULL, 0);
  printf("random seed: %u\n", seed);
  srand(seed);

  colcomp_test_int8();
  colcomp_test_int16();
  colcomp_test_int32();
  colcomp_test_int64();
# ifdef HAVE_INT128
  colcomp_test_int128();
# endif

  colcomp_test_uint8();
  colcomp_test_uint16();
  colcomp_test_uint32();
  colcomp_test_uint64();
# ifdef HAVE_INT128
  colcomp_test_uint128();
# endif

  colcomp_test_float();
  colcomp_test_double();

  colcomp_test_bool();
  colcomp_test_string();

  printf("colcomp_test Ok\n");
}
