#define FUSE_USE_VERSION 26
#include "map.hpp"
#include "imss.h"
#include "hercules.h"
#include <fuse.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdlib.h>
#include <time.h>
#include <limits.h>


#ifndef H_IMSS_POSIX_API
#define H_IMSS_POSIX_API

long fd_lookup(const char * path);
void get_iuri(const char * path, /*output*/ char * uri);
int imss_truncate(const char * path, off_t offset);
int imss_access(const char *path, int permission);
int imss_getattr(const char *path, struct stat *stbuf);
int imss_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi);
int imss_open(const char *path, struct fuse_file_info *fi);
int imss_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
int imss_write(const char *path, const char *buf, size_t size, off_t off, struct fuse_file_info *fi);
int imss_release(const char * path, struct fuse_file_info *fi);
int imss_create(const char * path, mode_t mode, struct fuse_file_info * fi);
int imss_opendir(const char * path, struct fuse_file_info * fi);
int imss_releasedir(const char * path, struct fuse_file_info * fi);
int imss_rmdir(const char * path);
int imss_unlink(const char * path);
int imss_utimens(const char * path, const struct timespec tv[2]);
int imss_mkdir(const char * path, mode_t mode);
int imss_flush(const char * path, struct fuse_file_info * fi);
int imss_getxattr(const char * path, const char *attr, char *value, size_t s);
int imss_chmod(const char *path, mode_t mode);
int imss_chown(const char *path, uid_t uid, gid_t gid);
int imss_rename(const char *old_path, const char *new_path);

#endif
