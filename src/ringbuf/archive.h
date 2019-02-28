// vim: ft=c bs=2 ts=2 sts=2 sw=2 expandtab
/* Move a completed archive file (either a full non-wrapping ringbuffer or an
 * ORC file) into a subdirectory, named after the time range that it contains
 */
#ifndef ARCHIVE_H_20190228
#define ARCHIVE_H_20190228

int ramen_archive(char const *fname, double start, double stop);

#endif
