#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "growblock.h"

int growblock_ctor(struct growblock *gb, size_t init_size)
{
  gb->start = malloc(init_size);
  if (! gb->start) return -1;
  gb->sz = 0;
  gb->capa = init_size;
  return 0;
}

int growblock_append(struct growblock *gb, void const *p, size_t sz)
{
  if (gb->sz + sz > gb->capa) {
    // grow that buffer
    size_t new_sz = gb->capa * 2 + sz;
    void *start = realloc(gb->start, new_sz);
    if (! start) return -1;
    gb->start = start;
    gb->capa = new_sz;
  }

  memcpy(gb->start + gb->sz, p, sz);
  gb->sz += sz;
  return 0;
}

void growblock_chop(struct growblock *gb)
{
  void *res = realloc(gb->start, gb->sz);
  assert(res == 0 || res == gb->start);
  gb->capa = gb->sz;
}
