/*
FUSE: Filesystem in Userspace
Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

This program can be distributed under the terms of the GNU GPL.
See the file COPYING.

gcc -Wall imss.c `pkg-config fuse --cflags --libs` -o imss
*/

#define FUSE_USE_VERSION 26
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
#include <math.h>

#include "imss_fuse_api.h"
#include "imss_posix_api.h"

int imss_fuse_truncate(const char * path, off_t offset){
	return imss_truncate(path, offset);
}

int imss_fuse_access(const char *path, int permission){
	return imss_access(path, permission);
}

int imss_fuse_getattr(const char *path, struct stat *stbuf){
	return imss_getattr(path, stbuf);
}

int imss_fuse_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi){
	return imss_readdir(path, buf, filler, offset); // TODO
}

int imss_fuse_open(const char *path, struct fuse_file_info *fi){
	return imss_open(path, &(fi->fh));
}

int imss_fuse_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
	return imss_read(path, buf, size, offset);
}

int imss_fuse_write(const char *path, const char *buf, size_t size, off_t off, struct fuse_file_info *fi){
	return imss_write(path, buf, size, off);
}

int imss_fuse_release(const char * path, struct fuse_file_info *fi){
	return imss_release(path);
}

int imss_fuse_create(const char * path, mode_t mode, struct fuse_file_info * fi){
	return imss_create(path, mode, &(fi->fh));
}

int imss_fuse_opendir(const char * path, struct fuse_file_info * fi){
	return imss_opendir(path);
}

int imss_fuse_releasedir(const char * path, struct fuse_file_info * fi){
	return imss_releasedir(path);
}

int imss_fuse_flush(const char * path, struct fuse_file_info * fi){
	return imss_flush(path);
}

int imss_fuse_rmdir(const char * path){
	return imss_rmdir(path);
}

int imss_fuse_unlink(const char * path){
	return imss_unlink(path);
}

int imss_fuse_utimens(const char * path, const struct timespec tv[2]){
	return imss_utimens(path, tv);    
}

int imss_fuse_mkdir(const char * path, mode_t mode){
	return imss_mkdir(path, mode);
}

int imss_fuse_getxattr(const char * path, const char *attr, char *value, size_t s){
	return imss_getxattr(path, attr, value, s);
}

int imss_fuse_chmod(const char *path, mode_t mode){
	return imss_chmod(path, mode);
}

int imss_fuse_chown(const char *path, uid_t uid, gid_t gid){
	return imss_chown(path, uid, gid);
}

int imss_fuse_rename(const char *old_path, const char *new_path){
	return imss_rename(old_path, new_path);
}
