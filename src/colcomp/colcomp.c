#include <stdlib.h>
#include <string.h>

#include "colcomp.h"
#include "growblock.h"

#define NOP_COPY(IN_SZ) do { \
  void *out = malloc(IN_SZ); \
  if (! out) return COLCOMP_ERR_CANNOT_MALLOC; \
  memcpy(out, in, IN_SZ); \
  *out_ = out; \
  return COLCOMP_OK; \
} while (0)

#define COMPRESS_NOP_ARR(TYPE) do { \
  size_t in_sz = sizeof(TYPE) * nb_items; \
  NOP_COPY(in_sz); \
  *out_sz_ = in_sz; \
  *op_ = strdup("Nop"); \
} while (0)

#define DECOMPRESS_NOP_ARR(TYPE) do { \
  if (0 != strcmp("Nop", op)) return COLCOMP_ERR_INVALID_PARAMETER; \
  NOP_COPY(in_sz); \
  *nb_items_ = in_sz / sizeof(TYPE); \
} while (0)


enum colcomp_result colcomp_compress_int8(int8_t const *in, unsigned nb_items, void **out_, size_t *out_sz_, char **op_)
{
  COMPRESS_NOP_ARR(int8_t);
}

enum colcomp_result colcomp_compress_int16(int16_t const *in, unsigned nb_items, void **out_, size_t *out_sz_, char **op_)
{
	COMPRESS_NOP_ARR(int16_t);
}

enum colcomp_result colcomp_compress_int32(int32_t const *in, unsigned nb_items, void **out_, size_t *out_sz_, char **op_)
{
	COMPRESS_NOP_ARR(int32_t);
}

enum colcomp_result colcomp_compress_int64(int64_t const *in, unsigned nb_items, void **out_, size_t *out_sz_, char **op_)
{
	COMPRESS_NOP_ARR(int64_t);
}

#ifdef HAVE_INT128
enum colcomp_result colcomp_compress_int128(int128_t const *in, unsigned nb_items, void **out_, size_t *out_sz_, char **op_)
{
	COMPRESS_NOP_ARR(int128_t);
}
#endif

enum colcomp_result colcomp_compress_uint8(uint8_t const *in, unsigned nb_items, void **out_, size_t *out_sz_, char **op_)
{
	COMPRESS_NOP_ARR(uint8_t);
}

enum colcomp_result colcomp_compress_uint16(uint16_t const *in, unsigned nb_items, void **out_, size_t *out_sz_, char **op_)
{
	COMPRESS_NOP_ARR(uint16_t);
}

enum colcomp_result colcomp_compress_uint32(uint32_t const *in, unsigned nb_items, void **out_, size_t *out_sz_, char **op_)
{
	COMPRESS_NOP_ARR(uint32_t);
}

enum colcomp_result colcomp_compress_uint64(uint64_t const *in, unsigned nb_items, void **out_, size_t *out_sz_, char **op_)
{
	COMPRESS_NOP_ARR(uint64_t);
}

#ifdef HAVE_UINT128
enum colcomp_result colcomp_compress_uint128(uint128_t const *in, unsigned nb_items, void **, size_t *out_sz_, char **op_)
{
	COMPRESS_NOP_ARR(uint128_t);
}
#endif

enum colcomp_result colcomp_compress_bool(char const *in, unsigned nb_items, void **out_, size_t *out_sz_, char **op_)
{
  size_t in_sz = (nb_items + 7) / 8;
	NOP_COPY(in_sz);
  *out_sz_ = in_sz;
  *op_ = strdup("NopX");
  (*op_)[3] = '0' + (nb_items & 7);
  return COLCOMP_OK;
}

enum colcomp_result colcomp_compress_float(float const *in, unsigned nb_items, void **out_, size_t *out_sz_, char **op_)
{
	COMPRESS_NOP_ARR(float);
}

enum colcomp_result colcomp_compress_double(double const *in, unsigned nb_items, void **out_, size_t *out_sz_, char **op_)
{
	COMPRESS_NOP_ARR(double);
}

enum colcomp_result colcomp_compress_string(char const **in, unsigned nb_items, void **out_, size_t *out_sz_, char **op_)
{
  /* Even if we do no compression we still have to compact all the strings in
   * a single block. */

  if (nb_items == 0) {
    *out_ = NULL;
    *out_sz_ = 0;
    *op_ = "Nop";
    return COLCOMP_OK;
  };

  struct growblock gb;
  size_t ptr_array_sz = nb_items * sizeof(char *);
  if (0 != growblock_ctor(&gb, ptr_array_sz + nb_items * strlen(in[0]))) {
    return COLCOMP_ERR_CANNOT_MALLOC;
  }
  char **ptr_array = (char **)gb.start;
  gb.sz = ptr_array_sz;

  for (unsigned i = 0; i < nb_items; i ++) {
    ptr_array[i] = (char *)gb.start+gb.sz;
    if (0 != growblock_append(&gb, in[i], strlen(in[i]) + 1)) {
      free(gb.start);
      return COLCOMP_ERR_CANNOT_MALLOC;
    }
  }

  growblock_chop(&gb);
  *out_ = gb.start;
  *out_sz_ = gb.sz;
  *op_ = "Nop";
  return COLCOMP_OK;
}

/* Decompression.
 *
 * All functions takes 3 input and 2 output parameters:
 *
 * 1. the address of a block as input,
 * 2. the size of the data in this block,
 * 3. a string describing the compression that has been performed (the one
 *    returned by the compressor),
 * 4. the decompressed data,
 * 5. the number of items that have been uncompressed.
 *
 * Uncompressed data is allocated with malloc and it is the responsibility of
 * the caller to free it whenever it is not needed anymore.
 *
 * The string decompresser returns an array of strings; this array as well as each
 * individual strings have been allocated using malloc.
 */

enum colcomp_result colcomp_decompress_int8(void const *in, size_t in_sz, char const *op, int8_t **out_, unsigned *nb_items_)
{
	DECOMPRESS_NOP_ARR(int8_t);
}

enum colcomp_result colcomp_decompress_int16(void const *in, size_t in_sz, char const *op, int16_t **out_, unsigned *nb_items_)
{
	DECOMPRESS_NOP_ARR(int16_t);
}

enum colcomp_result colcomp_decompress_int32(void const *in, size_t in_sz, char const *op, int32_t **out_, unsigned *nb_items_)
{
	DECOMPRESS_NOP_ARR(int32_t);
}

enum colcomp_result colcomp_decompress_int64(void const *in, size_t in_sz, char const *op, int64_t **out_, unsigned *nb_items_)
{
	DECOMPRESS_NOP_ARR(int64_t);
}

#ifdef HAVE_INT128
enum colcomp_result colcomp_decompress_int128(void const *in, size_t in_sz, char const *op, int128_t **out_, unsigned *nb_items_)
{
	DECOMPRESS_NOP_ARR(int128_t);
}

#endif

enum colcomp_result colcomp_decompress_uint8(void const *in, size_t in_sz, char const *op, uint8_t **out_, unsigned *nb_items_)
{
	DECOMPRESS_NOP_ARR(uint8_t);
}

enum colcomp_result colcomp_decompress_uint16(void const *in, size_t in_sz, char const *op, uint16_t **out_, unsigned *nb_items_)
{
	DECOMPRESS_NOP_ARR(uint16_t);
}

enum colcomp_result colcomp_decompress_uint32(void const *in, size_t in_sz, char const *op, uint32_t **out_, unsigned *nb_items_)
{
	DECOMPRESS_NOP_ARR(uint32_t);
}

enum colcomp_result colcomp_decompress_uint64(void const *in, size_t in_sz, char const *op, uint64_t **out_, unsigned *nb_items_)
{
	DECOMPRESS_NOP_ARR(uint64_t);
}

#ifdef HAVE_UINT128
enum colcomp_result colcomp_decompress_uint128(void const *in, size_t in_sz, char const *op, uint128_t **out_, unsigned *nb_items_)
{
	DECOMPRESS_NOP_ARR(uint128_t);
}
#endif

enum colcomp_result colcomp_decompress_bool(void const *in, size_t in_sz, char const *op, char **out_, unsigned *nb_items_)
{
  if ((in_sz & 7) || !in_sz) return COLCOMP_ERR_INVALID_PARAMETER;

  if (op[0] != 'N' || op[1] != 'o' || op[2] != 'p' || op[4] != '\0')
    return COLCOMP_ERR_INVALID_PARAMETER;

  unsigned nb_bits_last = op[3] - '0';
  if (nb_bits_last > 7) return COLCOMP_ERR_INVALID_PARAMETER;

  NOP_COPY(in_sz);

  *nb_items_ =
    nb_bits_last == 0 ? in_sz * 8 : (in_sz-1) * 8 + nb_bits_last;
}

enum colcomp_result colcomp_decompress_float(void const *in, size_t in_sz, char const *op, float **out_, unsigned *nb_items_)
{
	DECOMPRESS_NOP_ARR(float);
}

enum colcomp_result colcomp_decompress_double(void const *in, size_t in_sz, char const *op, double **out_, unsigned *nb_items_)
{
	DECOMPRESS_NOP_ARR(double);
}

enum colcomp_result colcomp_decompress_string(void const *in, size_t in_sz, char const *op, char ***out_, unsigned *nb_items_)
{
  if (0 != strcmp("Nop", op)) return COLCOMP_ERR_INVALID_PARAMETER;

  if (in_sz == 0) {
    *out_ = NULL;
    *nb_items_ = 0;
    return COLCOMP_OK;
  }

  if (in_sz < sizeof(char *)) return COLCOMP_ERR_INVALID_PARAMETER;

  char const **arr = (char const **)in;
  char const **arr_end = (char const **)arr[0];
  if (arr_end < arr || (char *)arr_end > (char *)in+in_sz) return COLCOMP_ERR_INVALID_PARAMETER;
  if ((arr_end - arr) % sizeof(char **)) return COLCOMP_ERR_INVALID_PARAMETER;

  unsigned nb_items = arr_end - arr;
  char **out = malloc(sizeof(char **) * nb_items);
  if (! out) return COLCOMP_ERR_CANNOT_MALLOC;

  enum colcomp_result err = COLCOMP_ERR_INVALID_PARAMETER;

  for (unsigned i = 0; i < nb_items; i++) {
    char const *str = arr[i];
    if (str < (char *)arr_end || str > (char *)in+in_sz) {
err:
      while (i-- > 0) free(out[i]);
      free(out);
      return err;
    }
    // TODO: maybe we should check that the NUL is within the in block
    out[i] = strdup(str);
    if (! out[i]) {
      err = COLCOMP_ERR_CANNOT_MALLOC;
      goto err;
    }
  }

  *out_ = out;
  *nb_items_ = nb_items;

  return COLCOMP_OK;
}
