#define FUSE_USE_VERSION 26
#include <fuse.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <inttypes.h>
#include <stdlib.h>
#include <time.h>
#include <limits.h>

#ifndef H_IMSS_FUSE_API
#define H_IMSS_FUSE_API

int imss_fuse_truncate(const char * path, off_t offset);
int imss_fuse_access(const char *path, int permission);
int imss_fuse_getattr(const char *path, struct stat *stbuf);

int imss_fuse_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi);
int imss_fuse_open(const char *path, struct fuse_file_info *fi);
int imss_fuse_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
int imss_fuse_write(const char *path, const char *buf, size_t size, off_t off, struct fuse_file_info *fi);
int imss_fuse_release(const char * path, struct fuse_file_info *fi);
int imss_fuse_create(const char * path, mode_t mode, struct fuse_file_info * fi);
int imss_fuse_opendir(const char * path, struct fuse_file_info * fi);
int imss_fuse_releasedir(const char * path, struct fuse_file_info * fi);
int imss_fuse_flush(const char * path, struct fuse_file_info * fi);

int imss_fuse_rmdir(const char * path);
int imss_fuse_unlink(const char * path);
int imss_fuse_utimens(const char * path, const struct timespec tv[2]);
int imss_fuse_mkdir(const char * path, mode_t mode);
int imss_fuse_getxattr(const char * path, const char *attr, char *value, size_t s);
int imss_fuse_chmod(const char *path, mode_t mode);
int imss_fuse_chown(const char *path, uid_t uid, gid_t gid);
int imss_fuse_rename(const char *old_path, const char *new_path);

#endif
