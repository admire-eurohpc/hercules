#define FUSE_USE_VERSION 26
#include "map.hpp"
#include "imss.h"
#include "hercules.h"
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
// #include <unistd.h>
#include <inttypes.h>
#include <stdlib.h>
#include <time.h>
#include <limits.h>

#ifndef H_IMSS_POSIX_API
#define H_IMSS_POSIX_API

// #ifndef __USE_FILE_OFFSET64
// #define __USE_FILE_OFFSET64
// #endif

#undef _FILE_OFFSET_BITS
#undef __USE_LARGEFILE64
#undef __USE_FILE_OFFSET64

// #define __USE_FILE_OFFSET64
// #undef __REDIRECT

#include <dirent.h>

struct dirent64
{
	__ino64_t d_ino;
	__off64_t d_off;
	unsigned short int d_reclen;
	unsigned char d_type;
	char d_name[256]; /* We must not include limits.h! */
};

typedef int (*posix_fill_dir_t)(void *buf, const char *name,
								const struct stat *stbuf, off_t off);

void fd_lookup(const char *path, int *fd, struct stat *s, char **aux);
void get_iuri(const char *path, /*output*/ char *uri);
int imss_truncate(const char *path, off_t offset);
int imss_access(const char *path, int permission);
int imss_refresh(const char *path);
int imss_getattr(const char *path, struct stat *stbuf);

int imss_readdir(const char *path, void *buf, posix_fill_dir_t filler, off_t offset);
int imss_open(char *path, uint64_t *fh);
int imss_read(const char *path, void *buf, size_t size, off_t offset);
int imss_sread(const char *path, void *buf, size_t size, off_t offset);
int imss_split_readv(const char *path, char *buf, size_t size, off_t offset);
int imss_vread_prefetch(const char *path, char *buf, size_t size, off_t offset);
int imss_vread_no_prefetch(const char *path, char *buf, size_t size, off_t offset);
int imss_vread_2x(const char *path, char *buf, size_t size, off_t offset);
ssize_t imss_write(const char *path, const void *buf, size_t size, off_t off);
int imss_split_writev(const char *path, const char *buf, size_t size, off_t off);
int imss_release(const char *path);
int imss_create(const char *path, mode_t mode, uint64_t *fh);
int imss_opendir(const char *path);
int imss_releasedir(const char *path);
int imss_flush(const char *path);

int imss_symlinkat(char *new_path_1, char *new_path_2, int _case);

int imss_rmdir(const char *path);
int imss_unlink(const char *path);
int imss_utimens(const char *path, const struct timespec tv[2]);
int imss_mkdir(const char *path, mode_t mode);
int imss_getxattr(const char *path, const char *attr, char *value, size_t s);
int imss_chmod(const char *path, mode_t mode);
int imss_chown(const char *path, uid_t uid, gid_t gid);
int imss_rename(const char *old_path, const char *new_path);

int imss_close(const char *path);

#endif
