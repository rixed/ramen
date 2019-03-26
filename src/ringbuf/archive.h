// vim: ft=c bs=2 ts=2 sts=2 sw=2 expandtab
/* Move a completed archive file (either a full non-wrapping ringbuffer or an
 * ORC file) into a subdirectory, named after the time range that it contains
 */
#ifndef ARCHIVE_H_20190228
#define ARCHIVE_H_20190228

int mkdir_for_file(char *fname);
int ramen_archive(char const *fname, double start, double stop);
void dirname_of_fname(char *dirname, size_t sz, char const *fname);
char const *extension_of_fname(char const *fname);
int lock(char const *fname, int op /* LOCK_SH|LOCK_EX */, bool only_if_exist);
int unlock(int);

#endif
