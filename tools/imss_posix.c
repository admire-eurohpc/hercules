#define _GNU_SOURCE
#include "map.hpp"
#include "mapfd.hpp"
#include <stdio.h>
#include <dlfcn.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/xattr.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <string.h>
#include "imss.h"
#include <imss_posix_api.h>
#include <stdarg.h>
#include <dirent.h>
#include "mapprefetch.hpp"
#include <math.h>
#include <sys/utsname.h>

#undef _FILE_OFFSET_BITS


#ifndef O_CREAT
# define O_CREAT           0100        /* Not fcntl.  */
#endif

#define KB 1024 
#define GB 1073741824
uint32_t deployment = 2;//Default 1=ATACHED, 0=DETACHED ONLY METADATA SERVER 2=DETACHED METADATA AND DATA SERVERS
//char * POLICY = "RR"; //Default RR
char * POLICY = "HASH";
uint16_t IMSS_SRV_PORT = 1; //Not default, 1 will fail
uint16_t METADATA_PORT = 1; //Not default, 1 will fail
int32_t  N_SERVERS = 1; //Default
int32_t N_BLKS = 1; //Default 1
int32_t  N_META_SERVERS = 1;
char     METADATA_FILE[512]; //Not default
char     IMSS_HOSTFILE[512]; //Not default
char     IMSS_ROOT[32];
char     META_HOSTFILE[512];
uint64_t STORAGE_SIZE = 16; //In GB
uint64_t META_BUFFSIZE = 16; //In GB
uint64_t IMSS_BLKSIZE = 1024;//In KB 
uint64_t IMSS_BUFFSIZE = 2; //In GB
int32_t REPL_FACTOR = 1; //Default none
int32_t  IMSS_DEBUG = 0;

uint16_t PREFETCH = 6;

uint16_t threshold_read_servers = 4;
uint16_t BEST_PERFORMANCE_READ = 0;//if 1    then n_servers < threshold => SREAD, else if n_servers > threshold => SPLIT_READV 
//if 0 only one method of read applied specified in MULTIPLE_READ

uint16_t MULTIPLE_READ = 4;//1=vread with prefetch, 2=vread without prefetch, 
//3=vread_2x 4=imss_split_readv(distributed) else sread
uint16_t MULTIPLE_WRITE = 0;//1=writev(only 1 server), 2=imss_split_writev(distributed) else swrite
char prefetch_path[256];
int32_t prefetch_first_block = -1; 
int32_t prefetch_last_block = -1;
int32_t prefetch_pos = 0;
pthread_t prefetch_t;
int16_t prefetch_ds = 0;
int32_t prefetch_offset = 0;

pthread_cond_t      cond_prefetch;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

uint64_t IMSS_DATA_BSIZE;

int LD_PRELOAD = 0;
void * map;
void * map_prefetch;


char * MOUNT_POINT;
void * map_fd;

int init = 0;


static off_t (*real_lseek)(int fd, off_t offset, int whence) =  NULL;
static int (*real__lxstat)(int fd, const char *pathname, struct stat *buf) = NULL;
static int (*real_xstat)(int fd, const char *path, struct stat *buf) = NULL;
static int (*real_stat)(const char *pathname, struct stat *buf) = NULL;
static int (*real_close)(int fd) = NULL;
static int (*real_puts)(const char* str) = NULL;
static int (*real__open_2)(const char *pathname, int flags, ...) = NULL;
static int (*real_open64)(const char *pathname, int flags, ...) = NULL;
static int (*real_open)(const char *pathname, int flags, ...) = NULL;
static FILE* (*real_fopen)(const char *restrict pathname, const char *restrict mode) = NULL;
static FILE* (*real_fopen64)(const char *restrict pathname, const char *restrict mode) = NULL;
static int (*real_access)(const char *pathname, int mode) = NULL;
static int (*real_mkdir)(const char *path, mode_t mode) = NULL;
static ssize_t (*real_write)(int fd, const void *buf, size_t size) = NULL;
static ssize_t (*real_read)(int fd, const void *buf, size_t size) = NULL;
static int (*real_remove) (const char *name) = NULL;
static int (*real_unlink) (const char *name) = NULL;
static int (*real_rmdir) (const char *path)  = NULL;
static int (*real_unlinkat) (int fd, const char *name, int flag) = NULL;
static int (*real_rename) (const char *old, const char *new) = NULL;
static int (*real_fchmodat) (int dirfd, const char *pathname, mode_t mode, int flags) = NULL;
static int (*real_fchownat)(int dirfd, const char *pathname, uid_t owner, gid_t group, int flags) = NULL;
static DIR *(*real_opendir)(const char *name) = NULL;
static struct dirent *(*real_readdir)(DIR *dirp) = NULL;
static int (*real_closedir)(DIR *dirp) = NULL;
static int (*real_statvfs)(const char *restrict path, struct statvfs *restrict buf) = NULL;



void *
prefetch_function (void * th_argv)
{
	for (;;) {

		pthread_mutex_lock(&lock);
		while( prefetch_ds  < 0 ){
			pthread_cond_wait(&cond_prefetch, &lock);
		}


		if(prefetch_first_block<prefetch_last_block && prefetch_first_block != -1){
			//printf("Se activo Prefetch path:%s$%d-$%d\n",prefetch_path, prefetch_first_block, prefetch_last_block);
			int exist_first_block, exist_last_block, position;
			char * buf = map_get_buffer_prefetch(map_prefetch, prefetch_path, &exist_first_block, &exist_last_block);
			int err = readv_multiple(prefetch_ds, prefetch_first_block, prefetch_last_block, buf, IMSS_BLKSIZE, prefetch_offset, IMSS_DATA_BSIZE * (prefetch_last_block - prefetch_first_block));
			if(err==-1){
				pthread_mutex_unlock(&lock);
				continue;
			}
			map_update_prefetch(map_prefetch, prefetch_path, prefetch_first_block, prefetch_last_block);

		}


		prefetch_ds = -1;
		pthread_mutex_unlock(&lock);
	}

	pthread_exit(NULL);
}


char * convert_path(const char * name, char * replace){
	char * path = calloc(256, sizeof(char));	
	strcpy(path,name);
	//printf("path=%s\n",path);
	size_t len = strlen(MOUNT_POINT);
	if (len > 0) {
		char *p = path;
		while ((p = strstr(p, MOUNT_POINT)) != NULL) {
			memmove(p, p + len, strlen(p + len) + 1);
		}
	}

	char * new_path = calloc(256, sizeof(char));	

	if(! strncmp(path, "/", strlen("/")) ) {
		strcat(new_path, "imss:/");
	}else{
		strcat(new_path, "imss://");
	}

	strcat(new_path, path);
	return new_path;
}


__attribute__((constructor))
void
imss_posix_init(void)
{
	if (IMSS_DEBUG) fprintf(stderr,"IMSS2 client starting\n");
	map_fd = map_fd_create();
	if (getenv("IMSS_MOUNT_POINT") != NULL) {
		MOUNT_POINT = getenv("IMSS_MOUNT_POINT");
	}


	strcpy(IMSS_ROOT, "imss://");

	if (getenv("IMSS_HOSTFILE") != NULL) {
		strcpy(IMSS_HOSTFILE, getenv("IMSS_HOSTFILE"));
	}

	if (getenv("IMSS_N_SERVERS") != NULL) {
		N_SERVERS = atoi(getenv("IMSS_N_SERVERS"));
	}

	if (getenv("IMSS_SRV_PORT") != NULL) {
		IMSS_SRV_PORT = atoi(getenv("IMSS_SRV_PORT"));
	}

	if (getenv("IMSS_BUFFSIZE") != NULL) {
		IMSS_BUFFSIZE = atol(getenv("IMSS_BUFFSIZE"));
	}

	if (getenv("IMSS_META_HOSTFILE") != NULL) {
		strcpy(META_HOSTFILE, getenv("IMSS_META_HOSTFILE"));
	}

	if (getenv("IMSS_META_PORT") != NULL) {
		METADATA_PORT = atoi(getenv("IMSS_META_PORT"));
	}

	if (getenv("IMSS_META_SERVERS") != NULL) {
		N_META_SERVERS = atoi(getenv("IMSS_META_SERVERS"));
	}

	if (getenv("IMSS_BLKSIZE") != NULL) {
		IMSS_BLKSIZE = atoi(getenv("IMSS_BLKSIZE"));
	}

	if (getenv("IMSS_STORAGE_SIZE") != NULL) {
		STORAGE_SIZE = atol(getenv("IMSS_STORAGE_SIZE"));
	}

	if (getenv("IMSS_METADATA_FILE") != NULL) {
		strcpy(METADATA_FILE, getenv("IMSS_METADATA_FILE"));
	}

	if (getenv("IMSS_DEBUG") != NULL) {
		IMSS_DEBUG = 1;
	}

	if (getenv("IMSS_DEPLOYMENT") != NULL) {
		deployment = atoi(getenv("IMSS_DEPLOYMENT"));
	}

    if (IMSS_DEBUG) {
		fprintf(stderr," -- IMSS_MOUNT_POINT: %s\n", MOUNT_POINT);
		fprintf(stderr," -- IMSS_HOSTFILE: %s\n", IMSS_HOSTFILE);
		fprintf(stderr," -- IMSS_N_SERVERS: %d\n", N_SERVERS );
		fprintf(stderr," -- IMSS_SRV_PORT: %d\n", IMSS_SRV_PORT );
		fprintf(stderr," -- IMSS_BUFFSIZE: %ld\n", IMSS_BUFFSIZE );
		fprintf(stderr," -- META_HOSTFILE: %s\n", META_HOSTFILE);
		fprintf(stderr," -- IMSS_META_PORT: %d\n",  METADATA_PORT);
		fprintf(stderr," -- IMSS_META_SERVERS: %d\n",  N_META_SERVERS);
		fprintf(stderr," -- IMSS_BLKSIZE: %ld\n",  IMSS_BLKSIZE);
		fprintf(stderr," -- IMSS_STORAGE_SIZE: %ld\n",  STORAGE_SIZE);
		fprintf(stderr," -- IMSS_METADATA_FILE: %s\n",  METADATA_FILE);
		fprintf(stderr," -- IMSS_DEPLOYMENT: %d\n",  deployment);
    }

	IMSS_DATA_BSIZE = IMSS_BLKSIZE*KB;
	//Hercules init -- Attached deploy
	if(deployment==1){
		//Hercules init -- Attached deploy
		if (hercules_init(0, STORAGE_SIZE, IMSS_SRV_PORT, 1, METADATA_PORT, META_BUFFSIZE, METADATA_FILE) == -1){
			//In case of error notify and exit
			fprintf(stderr, "[IMSS-FUSE]	Hercules init failed, cannot deploy IMSS.\n");
		}
	}

	//Metadata server
	if (stat_init(META_HOSTFILE, METADATA_PORT, N_META_SERVERS,1) == -1){
		//In case of error notify and exit
		fprintf(stderr, "Stat init failed, cannot connect to Metadata server.\n");
	}

	if(deployment==2){
		open_imss(IMSS_ROOT);
	}

	if(deployment!=2){
		//Initialize the IMSS servers
		if(init_imss(IMSS_ROOT, IMSS_HOSTFILE, META_HOSTFILE, N_SERVERS, IMSS_SRV_PORT, IMSS_BUFFSIZE, deployment, "/home/hcristobal/imss/build/server", METADATA_PORT) < 0) {
			fprintf(stderr, "[IMSS-FUSE]	IMSS init failed, cannot create servers.\n");
		}
	}

	map_prefetch = map_create_prefetch();
	map = map_create();
	if(MULTIPLE_READ==1){
		int ret;

		pthread_attr_t tattr;
		ret = pthread_attr_init(&tattr);
		ret = pthread_attr_setdetachstate(&tattr,PTHREAD_CREATE_DETACHED);

		if (pthread_create(&prefetch_t, &tattr, prefetch_function, NULL) == -1)
		{
			perror("ERRIMSS_PREFETCH_DEPLOY");
			pthread_exit(NULL);
		}
	}

    init = 1;
}

void __attribute__((destructor)) run_me_last() {
	if (init) {
		release_imss("imss://", CLOSE_DETACHED);
		stat_release();
	}
}

void check_ld_preload(void){

	if(LD_PRELOAD==0){
		printf("\nActivating... ld_preload=%d\n\n",LD_PRELOAD);
		LD_PRELOAD=1;
		imss_posix_init();
	}
}

int
close(int fd)
{
	int ret = 0;   
	real_close = dlsym(RTLD_NEXT,"close");
	if (!init) { 
      if (IMSS_DEBUG)  fprintf(stderr, "WARNING: IMSS not initialized yet. Running regular 'close'.\n");
      return real_close(fd);
	}
	map_fd_search_by_val_close(map_fd, fd);  // MIRAR
	ret = real_close(fd);  
	return ret;


}

int __lxstat(int fd, const char *pathname, struct stat *buf)
{
	int ret;
	unsigned long p = 0;
	char * workdir = getenv("PWD");
	real__lxstat = dlsym(RTLD_NEXT, "__lxstat");

	if (!init) {
        if (IMSS_DEBUG)  fprintf(stderr, "WARNING: IMSS not initialized yet. Running regular '__lxstat'.\n");
		return real__lxstat(fd,pathname, buf);
	}
	if(! strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT)) || ! strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT))) {
		//printf("__lxstat WORKED!\n");
		char * new_path; 
		new_path = convert_path(pathname, MOUNT_POINT);
		//int exist = map_fd_search(map_fd, new_path, &ret, &p);
		ret = imss_getattr(new_path, buf);
	}else{
		ret = real__lxstat(fd,pathname, buf);
	}

	return ret;
}

int __xstat(int fd, const char *pathname, struct stat *buf)
{
	int ret;
	unsigned long p = 0;
	char * workdir = getenv("PWD");
	real_xstat = dlsym(RTLD_NEXT, "__xstat");
	
	if (!init) {
        if (IMSS_DEBUG)  fprintf(stderr, "WARNING: IMSS not initialized yet. Running regular '__xstat'.\n");
        return real_xstat(fd,pathname, buf);
    }

	if(! strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT)) || ! strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT))) {
		//printf("__xstat WORKED!\n");
		char * new_path; 
		new_path = convert_path(pathname, MOUNT_POINT);
		// int exist = map_fd_search(map_fd, new_path, &ret, &p);
		ret = imss_getattr(new_path, buf);
	}else{
		ret = real_xstat(fd,pathname, buf);
	}
	return ret;
}

int stat(const char *pathname, struct stat *buf){
	int ret;
	unsigned long p = 0;
	char * workdir = getenv("PWD");
	real_stat = dlsym(RTLD_NEXT, "stat");

    if (!init) {
        if (IMSS_DEBUG)  fprintf(stderr, "WARNING: IMSS not initialized yet. Running regular 'stat'.\n");
        return real_stat(pathname, buf);
    }

	if(! strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT)) || ! strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT))) {
		//printf("stat WORKED!\n");
		char * new_path; 
		new_path = convert_path(pathname, MOUNT_POINT);
		//int exist = map_fd_search(map_fd, new_path, &ret, &p);
		ret = imss_getattr(new_path, buf);
	}else{
		ret = real_stat(pathname, buf);
	}

	return ret;
}

int statvfs(const char *restrict path, struct statvfs *restrict buf){
    real_statvfs = dlsym(RTLD_NEXT,"statvfs");
    char * workdir = getenv("PWD");
    if(! strncmp(path, MOUNT_POINT, strlen(MOUNT_POINT)) || ! strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT))) {
        buf->f_bsize = IMSS_BLKSIZE * KB;
        buf->f_namemax = URI_;
		return 0;
    }else{
        return real_statvfs(path, buf);
    }
}

int __open_2(const char *pathname, int flags, ...){
	real__open_2 = dlsym(RTLD_NEXT,"__open_2");
	int ret;
	uint64_t ret_ds;
	unsigned long p = 0;
	va_list valist;
	va_start(valist, flags);

    if (!init) {
        if (IMSS_DEBUG)  fprintf(stderr, "WARNING: IMSS not initialized yet. Running regular '__open_2'.\n");
        return real__open_2(pathname, flags);
    }
  
	mode_t mode;
	for (int i = 0; i < 1; i++) {
		mode= va_arg(valist, mode_t);
	}

	va_end(valist);
	char * workdir = getenv("PWD");

	if(! strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT)) || ! strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT))) {

		char * new_path; 
		new_path = convert_path(pathname, MOUNT_POINT);

		int exist = map_fd_search(map_fd, new_path, &ret, &p);
		if (exist==-1){
			ret = real__open_2("/dev/null", flags);//Get a file descriptor
			map_fd_put(map_fd, new_path, ret, p);
			int create_flag = (flags & O_CREAT);
			if (create_flag){
				imss_create(new_path, mode, &ret_ds);
			}else{
				imss_open(new_path, &ret_ds);
			}
			//map_fd_search(map_fd, new_path, &ret, &p);
		}


	} else {
		ret = real__open_2(pathname, flags);
	}
	return ret;    
}


int open64(const char *pathname, int flags, ...)
{
	real_open64 = dlsym(RTLD_NEXT,"open64");
	int ret;
	uint64_t ret_ds;
	unsigned long p = 0;
	va_list valist;
	va_start(valist, flags);

    if (!init) {
        if (IMSS_DEBUG)  fprintf(stderr, "WARNING: IMSS not initialized yet. Running regular 'open64'.\n");
        return real_open64(pathname, flags);
    }

	mode_t mode;
	for (int i = 0; i < 1; i++) {
		mode = va_arg(valist, mode_t);
	}

	va_end(valist);
	char * workdir = getenv("PWD");

	if(! strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT)) || ! strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT))) {

		char * new_path; 
		new_path = convert_path(pathname, MOUNT_POINT);

		int exist = map_fd_search(map_fd, new_path, &ret, &p);
		if (exist==-1){
			ret = real_open64("/dev/null", flags);//Get a file descriptor
			map_fd_put(map_fd, new_path, ret, p);
			int create_flag = (flags & O_CREAT);
			if (create_flag){
				imss_create(new_path, mode, &ret_ds);
			}else{
				imss_open(new_path, &ret_ds);
			}
		}
	} else {
		fprintf(stderr, "REAL OPEN worked!\n"); 
		ret = real_open64(pathname, flags);
	}
	return ret;  
}

int open(const char *pathname, int flags, ...)
{
	real_open = dlsym(RTLD_NEXT,"open");
	int ret;
	uint64_t ret_ds;
	unsigned long p = 0;
	va_list valist;
	va_start(valist, flags);

    if (!init) {
        if (IMSS_DEBUG)  fprintf(stderr, "WARNING: IMSS not initialized yet. Running regular 'open'.\n");
        return real_open(pathname, flags);
    }

	mode_t mode;
	for (int i = 0; i < 1; i++) {
		mode= va_arg(valist, mode_t);
	}

	va_end(valist);
	char * workdir = getenv("PWD");

	if(! strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT)) || ! strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT))) {
		char * new_path; 
		new_path = convert_path(pathname, MOUNT_POINT);

		int exist = map_fd_search(map_fd, new_path, &ret, &p);
		if (exist==-1){
			ret = real_open("/dev/null", flags);//Get a file descriptor
			map_fd_put(map_fd, new_path, ret, p);
			int create_flag = (flags & O_CREAT);
			if (create_flag){
				imss_create(new_path, mode, &ret_ds);
			}else{
				imss_open(new_path, &ret_ds);
			}
		}

	} else {
		ret = real_open(pathname, flags);
	}
	return ret;   
}

int mkdir(const char *path, mode_t mode){
	real_mkdir = dlsym(RTLD_NEXT,"mkdir");
	size_t ret;

    if (!init) {
        if (IMSS_DEBUG)  fprintf(stderr, "WARNING: IMSS not initialized yet. Running regular 'mkdir'.\n");
        return real_mkdir(path, mode);
    }

	char * workdir = getenv("PWD");
	if(!strncmp(path, MOUNT_POINT, strlen(MOUNT_POINT)) || ! strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT))) {
		char * new_path; 
		new_path = convert_path(path, MOUNT_POINT);
		ret = imss_mkdir(new_path, mode);
	} else {
		ret = real_mkdir(path, mode);
	}
	return ret;
}


off_t lseek(int fd, off_t offset, int whence){
	real_lseek = dlsym(RTLD_NEXT,"lseek");
	long ret;
	unsigned long p = 0;
	
    if (!init) {
        if (IMSS_DEBUG) fprintf(stderr, "WARNING: IMSS not initialized yet. Running regular 'lseek'.\n");
        return real_lseek(fd, offset, whence);
    }

	char * path = (char *) calloc(256, sizeof(char));

	if(map_fd_search_by_val(map_fd, path, fd) == 1) {

		if(whence == SEEK_SET){
			//printf("SEEK_SET=%ld\n",offset);
			ret = offset;
			map_fd_update_value(map_fd, path, fd, ret);
		}else if(whence == SEEK_CUR){
			//printf("SEEK_CUR=%ld\n",offset);
			map_fd_search(map_fd, path, &fd, &p);
			ret = p+offset;
			map_fd_update_value(map_fd, path, fd, ret);
		}else if(whence == SEEK_END){
			//printf("SEEK_END=%ld\n",offset);
			struct stat ds_stat_n;
			imss_getattr(path, &ds_stat_n);
			ret = offset + ds_stat_n.st_size;
			map_fd_update_value(map_fd, path, fd, ret);
		}    

	}else{     
		ret = real_lseek(fd, offset, whence);
	}
	free(path);
	return ret;
}

ssize_t write(int fd, const void *buf, size_t size){
	real_write = dlsym(RTLD_NEXT,"write");
	size_t ret;
	unsigned long p = 0;

    if (!init) {
        if (IMSS_DEBUG) fprintf(stderr, "WARNING: IMSS not initialized yet. Running regular 'write'.\n");
        return real_write(fd, buf, size);
    }

	char * path = (char *) calloc(256, sizeof(char));

	if(map_fd_search_by_val(map_fd, path, fd) == 1) {
		struct stat ds_stat_n;
		imss_getattr(path, &ds_stat_n);
		map_fd_search(map_fd, path, &fd, &p);
		//printf("CUSTOM write worked! path=%s fd=%d, size=%ld offset=%ld, buffer=%s\n", path, fd, size, p, buf); 
		ret=imss_write(path,buf,size,p);
		imss_release(path);
	}else{
		//printf("REAL write worked! fd=%d, size=%ld\n", fd, size); 
		ret = real_write(fd, buf, size);
	}
	free(path);
	return ret;
}

ssize_t read(int fd, void *buf, size_t size){
	real_read = dlsym(RTLD_NEXT,"read");
	//printf("read=%d\n",fd);
	size_t ret;
	unsigned long p = 0;

    if (!init) {
        if (IMSS_DEBUG) fprintf(stderr, "WARNING: IMSS not initialized yet. Running regular 'read'.\n");
        return real_read(fd, buf, size);
    }

	char * path = (char *) calloc(256, sizeof(char));

	if(map_fd_search_by_val(map_fd, path, fd) == 1) {
		//printf("CUSTOM read worked! path=%s fd=%d, size=%ld\n",path, fd, size);
		map_fd_search(map_fd, path, &fd, &p);
		ret = imss_read(path,buf,size,p);
	
	}else{
		ret = real_read(fd, buf, size);
	}
	free(path);
	return ret;
}

int unlink (const char *name){//unlink
	real_unlink = dlsym(RTLD_NEXT,"unlink");
	int ret;
	char * workdir = getenv("PWD");

    if (!init) {
        if (IMSS_DEBUG) fprintf(stderr, "WARNING: IMSS not initialized yet. Running regular 'unlink'.\n");
        return real_unlink(name);
    }

	if(!strncmp(name, MOUNT_POINT, strlen(MOUNT_POINT)) || ! strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT))) {
		char * new_path;

		new_path = convert_path(name, MOUNT_POINT);      
		int32_t type = get_type(new_path);
		if(type == 0){
			strcat(new_path,"/");
			type = get_type(new_path);

			if(type == 2){
				ret = imss_rmdir(new_path);
			}
		}else{
			ret = imss_unlink(new_path);
		}
	} else if(! strncmp(name, "imss://", strlen("imss://"))){
		char * path = (char *) calloc(256, sizeof(char));
		strcpy(path,name);
		int32_t type = get_type(path);
		if(type == 0){
			strcat(path,"/");
			type = get_type(path);

			if(type == 2){
				ret = imss_rmdir(path);
			}
		}else{
			ret = imss_unlink(path);
		}
		free(path);
	} else {
		ret = real_unlink(name);
	}
	return ret;
}


int rmdir (const char *path) {

	real_rmdir = dlsym(RTLD_NEXT,"rmdir");
	int ret;
	char * workdir = getenv("PWD");

    if (!init) {
        if (IMSS_DEBUG) fprintf(stderr, "WARNING: IMSS not initialized yet. Running regular 'rmdir'.\n");
        return real_rmdir(path);
    }

	if(! strncmp(path, MOUNT_POINT, strlen(MOUNT_POINT)) || ! strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT))) {
		char * new_path; 
		new_path = convert_path(path, MOUNT_POINT);
		ret = imss_rmdir(new_path);
		if(ret==-1){//special case io500
			ret=unlinkat(0,path,0);
		}

	} else if(! strncmp(path, "imss://", strlen("imss://"))){
		ret = imss_rmdir(path);

	} else{
		ret = real_rmdir(path);
	}
	return ret;
}

int unlinkat (int fd, const char *name, int flag){//rm & rm -r
	//printf("unlinkat WORKED!\n");
	real_unlinkat = dlsym(RTLD_NEXT,"unlinkat");
	int ret = 0;
	char * workdir = getenv("PWD");

    if (!init) {
        if (IMSS_DEBUG) fprintf(stderr, "WARNING: IMSS not initialized yet. Running regular 'unlinkat'.\n");
        return real_unlinkat(fd,name,flag);
    }

	if(! strncmp(name, MOUNT_POINT, strlen(MOUNT_POINT)) || ! strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT))) {
		char * new_path; 
		new_path = convert_path(name, MOUNT_POINT);
		int n_ent = 0;
		char *buffer;
		char **refs;

		if((n_ent = get_dir((char*)new_path, &buffer, &refs)) < 0){
			strcat(new_path,"/");
			if((n_ent = get_dir((char*)new_path, &buffer, &refs)) < 0){	
				return -ENOENT;
			}
		}

		for(int i = n_ent-1; i>-1 ; --i) {
			char *last = refs[i] + strlen(refs[i]) - 1;

			if(refs[i][strlen(refs[i])-1]=='/'){
				rmdir(refs[i]);
			}else{
				unlink(refs[i]);
			}        
		}

	} else {
		ret = real_unlinkat(fd,name,flag);
	}

	return ret;
}

int rename (const char *old, const char *new){
	
	real_rename = dlsym(RTLD_NEXT,"rename");
	int ret;
	char * workdir = getenv("PWD");

    if (!init) {
        if (IMSS_DEBUG) fprintf(stderr, "WARNING: IMSS not initialized yet. Running regular 'rename'.\n");
        return real_rename(old, new);
    }

	if((!strncmp(old, MOUNT_POINT, strlen(MOUNT_POINT)) && ! strncmp(new, MOUNT_POINT, strlen(MOUNT_POINT)) ) || ! strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT))) {
		char * old_path; 
		old_path = convert_path(old, MOUNT_POINT);
		char * new_path; 
		new_path = convert_path(new, MOUNT_POINT);
		imss_rename(old_path,new_path);

	} else {
		ret = real_rename(old, new);
	} 
	return ret;
}


int fchmodat(int dirfd, const char *pathname, mode_t mode, int flags){
	real_fchmodat = dlsym(RTLD_NEXT,"chmod");
	int ret;
	char * workdir = getenv("PWD");

    if (!init) {
        if (IMSS_DEBUG) fprintf(stderr, "WARNING: IMSS not initialized yet. Running regular 'fchmodat'.\n");
        return real_fchmodat(dirfd, pathname, mode, flags);
    }
   
	if(!strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT)) || ! strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT))) {
		char * new_path; 
		new_path = convert_path(pathname, MOUNT_POINT);
		ret = imss_chmod(new_path, mode);

	} else {
		ret = real_fchmodat(dirfd, pathname, mode, flags);
	}

	return ret;
}

int fchownat(int dirfd, const char *pathname, uid_t owner, gid_t group, int flags){
	real_fchownat = dlsym(RTLD_NEXT,"chown");
	int ret;
	char * workdir = getenv("PWD");

    if (!init) {
        if (IMSS_DEBUG) fprintf(stderr, "WARNING: IMSS not initialized yet. Running regular 'fchownat'.\n");
        return real_fchownat(dirfd, pathname, owner, group, flags);
    }

	if(!strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT)) || ! strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT))) {
		char * new_path; 
		new_path = convert_path(pathname, MOUNT_POINT);
		ret = imss_chown(new_path, owner, group);

	} else {
		ret = real_fchownat(dirfd, pathname, owner, group, flags);
	}

	return ret;
}


DIR *opendir(const char *name)
{

	real_opendir = dlsym(RTLD_NEXT, "opendir");

	DIR *dirp; 

	char * workdir = getenv("PWD");

    if (!init) {
        if (IMSS_DEBUG) fprintf(stderr, "WARNING: IMSS not initialized yet. Running regular 'opendir'.\n");
        return real_opendir(name);
    }

	if(! strncmp(name, MOUNT_POINT, strlen(MOUNT_POINT)) || ! strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT))) {
		char * new_path; 
		new_path = convert_path(name, MOUNT_POINT);
		int a=1;
		int ret = 0; 
		dirp = real_opendir("/tmp");
		seekdir(dirp, 0);
		unsigned long p = 0;
		int fd = 0;

		if(map_fd_search(map_fd, new_path, &fd, &p) == 1) {
			map_fd_update_value(map_fd, new_path, dirfd(dirp), p);

		} else {
			map_fd_put(map_fd, new_path, dirfd(dirp), p);
		}

	} else {
		dirp = real_opendir(name);
	}
	return dirp;
}


int myfiller(void *buf, const char *name, const struct stat *stbuf, off_t off) {
	strcat(buf, name);
	strcat(buf,"$");
	return 1;
}

struct dirent *readdir(DIR *dirp)
{
	real_readdir = dlsym(RTLD_NEXT, "readdir");
	size_t ret;

	if (!init) {
        if (IMSS_DEBUG) fprintf(stderr, "WARNING: IMSS not initialized yet. Running regular 'readdir'.\n");
        return real_readdir(dirp);
    }

	struct dirent *entry = (struct dirent *) malloc(sizeof(struct dirent));
	char *path = calloc(256, sizeof(char));	
	if(map_fd_search_by_val(map_fd, path, dirfd(dirp)) == 1) {
		char buf[KB*KB]={0};
		char *token;
		//fprintf(stderr,"CUSTOM IMSS_READDIR\n");
		imss_readdir(path, buf, myfiller, 0);
		unsigned long pos = telldir(dirp);

		token = strtok(buf,"$");
		//printf("readddir token=%s\n",token);
		int i = 0;

		while( token != NULL ) {
			if (i == pos) {
				entry->d_ino = 0;
				entry->d_off = pos;

				//name of file
				strcpy(entry->d_name, token);

				char path_search[256]={0};
				sprintf(path_search,"imss://%s",token);
				//type of file;
				int32_t type = get_type(path_search);

				if(!strncmp(token,".",strlen(token))){
					entry->d_type=DT_DIR;
				}else  if(!strncmp(token,"..",strlen(token))){
					entry->d_type=DT_DIR;

				}else if(type == 0){
					strcat(path_search,"/");
					type = get_type(path_search);
					if(type == 2){
						entry->d_type=DT_DIR;
					}else{
						entry->d_type=DT_REG;
					}
				}else{
					entry->d_type=DT_REG;
				}

				//length of this record
				if(strlen(token)<5){
					entry->d_reclen = 24;
				}else{
					entry->d_reclen = ceil((double)(strlen(token)-4) / 8)*8 + 24;
				}
				break;
			}
			token = strtok(NULL, "$");
			i++;
		}
		seekdir(dirp, pos + 1);
		if (token == NULL) {
			entry = NULL;
		}

	}else{
		entry = real_readdir(dirp);
	}
	/* if(entry!=NULL){
	   printf("entry->d_ino=%ld\n",entry->d_ino);
	   printf("entry->d_off=%ld\n",entry->d_off);
	   printf("entry->d_reclen=%d\n",entry->d_reclen);
	   printf("entry->d_type=%d\n",entry->d_type);
	   printf("entry->d_name:%s\n",entry->d_name);
	   }*/

	free(path);
	return entry;

}

int closedir(DIR *dirp){
	real_closedir = dlsym(RTLD_NEXT, "closedir");

    if (!init) {
        if (IMSS_DEBUG) fprintf(stderr, "WARNING: IMSS not initialized yet. Running regular 'closedir'.\n");
        return real_closedir(dirp);
    }

	map_fd_search_by_val_close(map_fd, dirfd(dirp)); 
	//printf("closedir worked! fd=%d\n",dirfd(dirp));
	int ret = real_closedir(dirp);

	return ret;
}
