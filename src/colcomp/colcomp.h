#ifndef COLCOMP_H_170911
#define COLCOMP_H_170911

/* Quick (De)Compression of a block of integers, floats, booleans or strings.
 */

#include <inttypes.h>
#include <stdint.h>

/* Compression.
 *
 * All functions take 2 input and 3 output parameters:
 *
 * 1. a pointer to the data to be compressed,
 * 2. the number of such values,
 * 3. pointer to the compressed data,
 * 4. the size in bytes of the compressed data,
 * 5. a short string describing how the data was encoded.
 *
 * This last string must be passed to the uncompress function.  The compress
 * functions will return a status code indicating the success or error of the
 * operation.
 *
 * The compressed data will be allocated on the heap using malloc and it is the
 * responsibility of the caller to free it whenever it is not needed any more.
 * In case of an error is returned, output parameters are junk ; no RAM is still
 * allocated.
 *
 * The bool compressor takes as input a bitfield and the size is expected to be
 * the number of bits in the input.
 *
 * The string compressor takes an array of nul terminated strings as input and
 * the size is expected to be the number of such strings.
 */

enum colcomp_result {
  COLCOMP_OK,
  COLCOMP_ERR_INVALID_PARAMETER,
  COLCOMP_ERR_CANNOT_MALLOC,
};

enum colcomp_result colcomp_compress_int8(int8_t const *, unsigned, void **, size_t *, char **);
enum colcomp_result colcomp_compress_int16(int16_t const *, unsigned, void **, size_t *, char **);
enum colcomp_result colcomp_compress_int32(int32_t const *, unsigned, void **, size_t *, char **);
enum colcomp_result colcomp_compress_int64(int64_t const *, unsigned, void **, size_t *, char **);
#ifdef HAVE_INT128
enum colcomp_result colcomp_compress_int128(int128_t const *, unsigned, void **, size_t *, char **);
#endif

enum colcomp_result colcomp_compress_uint8(uint8_t const *, unsigned, void **, size_t *, char **);
enum colcomp_result colcomp_compress_uint16(uint16_t const *, unsigned, void **, size_t *, char **);
enum colcomp_result colcomp_compress_uint32(uint32_t const *, unsigned, void **, size_t *, char **);
enum colcomp_result colcomp_compress_uint64(uint64_t const *, unsigned, void **, size_t *, char **);
#ifdef HAVE_UINT128
enum colcomp_result colcomp_compress_uint128(uint128_t const *, unsigned, void **, size_t *, char **);
#endif

enum colcomp_result colcomp_compress_bool(char const *, unsigned, void **, size_t *, char **);
enum colcomp_result colcomp_compress_float(float const *, unsigned, void **, size_t *, char **);
enum colcomp_result colcomp_compress_double(double const *, unsigned, void **, size_t *, char **);
enum colcomp_result colcomp_compress_string(char const **, unsigned, void **, size_t *, char **);

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

enum colcomp_result colcomp_decompress_int8(void const *, size_t, char const *, int8_t **, unsigned *);
enum colcomp_result colcomp_decompress_int16(void const *, size_t, char const *, int16_t **, unsigned *);
enum colcomp_result colcomp_decompress_int32(void const *, size_t, char const *, int32_t **, unsigned *);
enum colcomp_result colcomp_decompress_int64(void const *, size_t, char const *, int64_t **, unsigned *);
#ifdef HAVE_INT128
enum colcomp_result colcomp_decompress_int128(void const *, size_t, char const *, int128_t **, unsigned *);
#endif

enum colcomp_result colcomp_decompress_uint8(void const *, size_t, char const *, uint8_t **, unsigned *);
enum colcomp_result colcomp_decompress_uint16(void const *, size_t, char const *, uint16_t **, unsigned *);
enum colcomp_result colcomp_decompress_uint32(void const *, size_t, char const *, uint32_t **, unsigned *);
enum colcomp_result colcomp_decompress_uint64(void const *, size_t, char const *, uint64_t **, unsigned *);
#ifdef HAVE_UINT128
enum colcomp_result colcomp_decompress_uint128(void const *, size_t, char const *, uint128_t **, unsigned *);
#endif

enum colcomp_result colcomp_decompress_bool(void const *, size_t, char const *, char **, unsigned *);
enum colcomp_result colcomp_decompress_float(void const *, size_t, char const *, float **, unsigned *);
enum colcomp_result colcomp_decompress_double(void const *, size_t, char const *, double **, unsigned *);
enum colcomp_result colcomp_decompress_string(void const *, size_t, char const *, char ***, unsigned *);

#endif
