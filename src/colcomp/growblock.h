#ifndef GROWBLOCK_H_170911
#define GROWBLOCK_H_170911

struct growblock {
  void *start;
  size_t sz;
  size_t capa;
};

int growblock_ctor(struct growblock *, size_t init_size);
int growblock_append(struct growblock *, void const *, size_t);
void growblock_chop(struct growblock *);

#endif
