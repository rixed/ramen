#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <time.h>
#include <limits.h>
#include "ringbuf.h"

#define RB_WORDS 50
#define RB_VERSION 42
#define RB_MSG_SZ_MAX 20 // in words
#define NUM_LOOPS 100000

char fname[PATH_MAX];

static void load(struct ringbuf *rb)
{
  if (RB_OK != ringbuf_load(rb, RB_VERSION, fname)) {
    fprintf(stderr, "Cannot load ringbuffer %s\n", fname);
    exit(EXIT_FAILURE);
  }
}

static struct timespec const quick = { .tv_sec = 0, .tv_nsec = 666 };

static void read_once(struct ringbuf *rb)
{
  uint32_t data[RB_MSG_SZ_MAX];
  ssize_t const sz = ringbuf_dequeue(rb, data, sizeof(data));
  if (sz < 0) {
    nanosleep(&quick, NULL);
  } else {
    assert(sz >= 4);
    assert(sz <= 4 * RB_MSG_SZ_MAX);

    // Every message is supposed to end with newline:
    size_t const str_len = data[0];
    assert(str_len <= (size_t)sz - 4);
    char *str = (char *)(data + 1);
    assert(str[str_len - 1] == '\n');

    printf("%d: Reader: str_len=%zd, %.*s",
           getpid(), str_len, (int)str_len, str);
  }
}

static void reader(int c)
{
  printf("Starting reader %d\n", c);
  struct ringbuf rb;
  load(&rb);

  for (unsigned loop = 0; loop < NUM_LOOPS; loop++)
    read_once(&rb);

  ringbuf_unload(&rb);
}

static char const *random_str(void)
{
  static char const *strs[] = {
    "La pluie nous a débués et lavés,\n",
    "Et le soleil desséchés et noircis.\n",
    "Pies, corbeaux nous ont les yeux cavés,\n",
    "Et arraché la barbe et les sourcils.\n",
    "Jamais nul temps nous ne sommes assis\n",
    "Puis çà, puis là, comme le vent varie,\n",
    "A son plaisir sans cesser nous charrie,\n",
    "Plus becquetés d'oiseaux que dés à coudre.\n",
    "Ne soyez donc de notre confrérie;\n",
    "Mais priez Dieu que tous nous veuille absoudre!\n"
  };
  return strs[random() % (sizeof(strs)/sizeof(*strs))];
}

static void write_once(struct ringbuf *rb)
{
  char const *str = random_str();
  size_t const str_len = strlen(str);

  uint32_t data[RB_MSG_SZ_MAX];
  data[0] = str_len;
  memcpy(data + 1, str, str_len);

  size_t const num_words = 1 + (str_len + 3) / 4;

  switch (ringbuf_enqueue(rb, data, num_words, 0., 0.)) {
    case RB_OK:
      printf("%d: Writer: str_len=%zu, num_words=%zd, %.*s",
             getpid(), str_len, num_words, (int)str_len, str);
      break;
    case RB_ERR_NO_MORE_ROOM:
      nanosleep(&quick, NULL);
      break;
    default:
      assert(false);
  }
}

static void writer(int c)
{
  printf("Starting writer %d\n", c);
  struct ringbuf rb;
  load(&rb);
  srandom(time(NULL));

  for (unsigned loop = 0; loop < NUM_LOOPS; loop++)
    write_once(&rb);

  ringbuf_unload(&rb);
}

int main(int num_args, char const **args)
{
  if (num_args != 4) {
syntax:
    printf("%s [mono|multi] [num_writers] [num_readers]\n", args[0]);
    return EXIT_FAILURE;
  }

  bool mono = strcmp("mono", args[1]) == 0;
  if (! mono && strcmp("multi", args[1]) != 0) goto syntax;
  int num_writers = strtol(args[2], NULL, 10);
  int num_readers = strtol(args[3], NULL, 10);

  if (num_writers <= 0 || num_readers <= 0) goto syntax;

  snprintf(fname, sizeof(fname), "/tmp/ringbuf_test.%d.rb", (int)getpid());
  if (RB_OK != ringbuf_create(RB_VERSION, true, RB_WORDS, fname)) {
    fprintf(stderr, "Cannot create ringbuffer in %s\n", fname);
    return EXIT_FAILURE;
  }

  if (mono) {
    struct ringbuf rb;
    load(&rb);

    srandom(time(NULL));

    while (true) {
      if (random() % (num_writers + num_readers) < num_writers) {
        write_once(&rb);
      } else {
        read_once(&rb);
      }
    }
  } else {
    for (int c = 0; c < num_readers; c++) {
      pid_t p = fork();
      if (! p) {
        reader(c);
        return EXIT_SUCCESS;
      }
    }

    for (int c = 0; c < num_writers; c++) {
      pid_t p = fork();
      if (! p) {
        writer(c);
        return EXIT_SUCCESS;
      }
    }

    for (int c = 0; c < num_readers + num_writers; c++) wait(NULL);
  }

  return EXIT_SUCCESS;
}
