#include <stdio.h>

#include <stdint.h>

#define CAML_NAME_SPACE
#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/alloc.h>
#include <caml/custom.h>

#include <../stdint/uint64.h>
#include <../stdint/uint32.h>
#include <../stdint/uint16.h>
#include <../stdint/uint8.h>
#include <../stdint/int16.h>
#include <../stdint/int8.h>


int chr_to_int(char c) {
  switch(c) {
    case '0': return 0; break;
    case '1': return 1; break;
    case '2': return 2; break;
    case '3': return 3; break;
    case '4': return 4; break;
    case '5': return 5; break;
    case '6': return 6; break;
    case '7': return 7; break;
    case '8': return 8; break;
    case '9': return 9; break;
    case 'F': return 15; break;
    case 'f': return 15; break;
    default: return 0; break;
  }
}

uint64_t uint64_of_string(int off, int len, value str) {
  CAMLparam1 (str);
  char * s = String_val(str);
  int base = 10;

  uint64_t res = 0;

  if (*s && s[0] == '0') {
    if (s[1] == 'x' || s[1] == 'X') {
      base = 16;
      s+=2;
    }
  }

  while (*s) {
    res = res*base + chr_to_int(*s++);
  }

  CAMLreturn (copy_uint64(res));
}

uint32_t uint32_of_string(int off, int len, value str) {
  CAMLparam1 (str);
  char * s = String_val(str);
  int base = 10;

  uint32_t res = 0;

  if (*s && s[0] == '0') {
    if (s[1] == 'x' || s[1] == 'X') {
      base = 16;
      s+=2;
    }
  }

  while (*s) {
    res = res*base + chr_to_int(*s++);
  }

  CAMLreturn (copy_uint32(res));
}

uint16_t uint16_of_string(int off, int len, value str) {
  CAMLparam1 (str);
  char * s = String_val(str);
  int base = 10;

  uint16_t res = 0;

  if (*s && s[0] == '0') {
    if (s[1] == 'x' || s[1] == 'X') {
      base = 16;
      s+=2;
    }
  }

  while (*s) {
    res = res*base + chr_to_int(*s++);
  }

  CAMLreturn (Val_uint16(res));
}

uint8_t uint8_of_string(int off, int len, value str) {
  CAMLparam1 (str);
  char * s = String_val(str);
  int base = 10;

  uint8_t res = 0;

  if (*s && s[0] == '0') {
    if (s[1] == 'x' || s[1] == 'X') {
      base = 16;
      s+=2;
    }
  }

  while (*s) {
    res = res*base + chr_to_int(*s++);
  }

  CAMLreturn (Val_uint8(res));
}

int64_t int64_of_string(int off, int len, value str) {
  CAMLparam1 (str);
  char * s = String_val(str);
  int base = 10;

  int64_t res = 0;

  if (*s && s[0] == '0') {
    if (s[1] == 'x' || s[1] == 'X') {
      base = 16;
      s+=2;
    }
  }

  while (*s) {
    res = res*base + chr_to_int(*s++);
  }

  CAMLreturn (caml_copy_int64(res));
}

int32_t int32_of_string(int off, int len, value str) {
  CAMLparam1 (str);
  char * s = String_val(str);
  int base = 10;

  int32_t res = 0;

  if (*s && s[0] == '0') {
    if (s[1] == 'x' || s[1] == 'X') {
      base = 16;
      s+=2;
    }
  }

  while (*s) {
    res = res*base + chr_to_int(*s++);
  }

  CAMLreturn (caml_copy_int32(res));
}

int16_t int16_of_string(int off, int len, value str) {
  CAMLparam1 (str);
  char * s = String_val(str);
  int base = 10;

  int16_t res = 0;

  if (*s && s[0] == '0') {
    if (s[1] == 'x' || s[1] == 'X') {
      base = 16;
      s+=2;
    }
  }

  while (*s) {
    res = res*base + chr_to_int(*s++);
  }

  CAMLreturn (Val_int16(res));
}

int8_t int8_of_string(int off, int len, value str) {
  CAMLparam1 (str);
  char * s = String_val(str);
  int base = 10;

  int8_t res = 0;

  if (*s && s[0] == '0') {
    if (s[1] == 'x' || s[1] == 'X') {
      base = 16;
      s+=2;
    }
  }

  while (*s) {
    res = res*base + chr_to_int(*s++);
  }

  CAMLreturn (Val_int8(res));
}
