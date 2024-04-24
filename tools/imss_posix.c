#define _GNU_SOURCE

#include "map.hpp"
#include "mapfd.hpp"
#include "cfg_parse.h"
#include "flags.h"
#include "resolvepath.h"
#include "tempname.h"
#include <stdio.h>
#include <stdint.h>
#include <sys/types.h>
#include <dlfcn.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/xattr.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/statvfs.h>
#include <sys/vfs.h> // statfs
#include "imss.h"
#include <imss_posix_api.h>
#include <stdarg.h>
#include "mapprefetch.hpp"
#include <math.h>
#include <sys/utsname.h>
#include <sys/epoll.h>
#include <sys/time.h>
// #include <fcntl.h>
// Those are used by reports functions in stat.
#include <pwd.h>
#include <sys/sysmacros.h>
#undef __USE_GNU
// #include <poll.h>

#include <sys/ptrace.h>

#ifndef FCNTL_ADJUST_CMD
#define FCNTL_ADJUST_CMD(__cmd) __cmd
#endif

#define KB 1024
#define GB 1073741824
uint32_t DEPLOYMENT = 2; // Default 1=ATACHED, 0=DETACHED ONLY METADATA SERVER 2=DETACHED METADATA AND DATA SERVERS
char *POLICY = "RR";	 // Default RR
// char *POLICY = "LOCAL";
// char *POLICY = "HASH";
uint64_t IMSS_SRV_PORT = 1; // Not default, 1 will fail
uint64_t METADATA_PORT = 1; // Not default, 1 will fail
int32_t N_SERVERS = 1;		// Default
int32_t N_BLKS = 1;			// Default 1
int32_t N_META_SERVERS = 1;
char METADATA_FILE[512]; // Not default
char IMSS_HOSTFILE[512]; // Not default
char IMSS_ROOT[32];
char META_HOSTFILE[512];
uint64_t STORAGE_SIZE = 16;	  // In GB
uint64_t META_BUFFSIZE = 16;  // In GB
uint64_t IMSS_BLKSIZE = 1024; // In KB
uint64_t IMSS_BUFFSIZE = 2;	  // In GB
uint64_t IMSS_DATA_BSIZE;	  // In Bytes.
int32_t REPL_FACTOR = 1;	  // Default none
int32_t IMSS_DEBUG_FILE = 0;
int32_t IMSS_DEBUG_SCREEN = 0;
int IMSS_DEBUG_LEVEL = SLOG_FATAL;

extern int32_t MALLEABILITY;
extern int32_t UPPER_BOUND_SERVERS;
extern int32_t LOWER_BOUND_SERVERS;

uint16_t PREFETCH = 6;

uint16_t threshold_read_servers = 5;
uint16_t BEST_PERFORMANCE_READ = 0; // if 1    then n_servers < threshold => SREAD, else if n_servers > threshold => SPLIT_READV
// if 0 only one method of read applied specified in MULTIPLE_READ

uint16_t MULTIPLE_READ = 0;	 // 1=vread with prefetch, 2=vread without prefetch, 3=vread_2x 4=imss_split_readv(distributed) else sread
uint16_t MULTIPLE_WRITE = 0; // 1=writev(only 1 server), 2=imss_split_writev(distributed) else swrite
char prefetch_path[256];
int32_t prefetch_first_block = -1;
int32_t prefetch_last_block = -1;
int32_t prefetch_pos = 0;
pthread_t prefetch_t;
int16_t prefetch_ds = 0;
int32_t prefetch_offset = 0;

pthread_cond_t cond_prefetch;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t lock2 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t system_lock = PTHREAD_MUTEX_INITIALIZER;

#define MAX_PATH 1024
// char *aux_refresh;
// char *imss_path_refresh;

int LD_PRELOAD = 0;
void *map;
void *map_prefetch;

char MOUNT_POINT[512];
char HERCULES_PATH[512];
void *map_fd;

uint32_t rank = -1;
static int init = 0;
int release = 1;

// log path.
char log_path[1000] = {'\0'};
pid_t g_pid = -1;

int getConfiguration();
char *checkHerculesPath(const char *pathname);
char *convert_path(const char *name);
int generalOpen(char *new_path, int flags, mode_t mode);
ssize_t generalWrite(const char *pathname, int fd, const void *buf, size_t size, size_t offset);
int IsAbsolutePath(const char *pathname);
int ResolvePath(const char *path_, char *resolved);

static off_t (*real_lseek)(int fd, off_t offset, int whence) = NULL;
static off64_t (*real_lseek64)(int fd, off64_t offset, int whence) = NULL;
static int (*real_fseek)(FILE *stream, long int offset, int whence) = NULL;
static void (*real_seekdir)(DIR *dirp, long loc) = NULL;
static int (*real__lxstat)(int fd, const char *pathname, struct stat *buf) = NULL;
static int (*real__lxstat64)(int ver, const char *pathname, struct stat64 *buf) = NULL;
static int (*real_lstat)(const char *file_name, struct stat *buf) = NULL;
static int (*real_xstat)(int fd, const char *pathname, struct stat *buf) = NULL;
static int (*real_stat)(const char *pathname, struct stat *buf) = NULL;
static int (*real_stat64)(const char *__restrict__ pathname, struct stat64 *__restrict__ buf) = NULL;
static int (*real___xstat)(int ver, const char *pathname, struct stat *stat_buf) = NULL;
static int (*real__xstat64)(int ver, const char *path, struct stat64 *stat_buf) = NULL;
static int (*real_fstat)(int fd, struct stat *buf) = NULL;
static int (*real_fstatat)(int __fd, const char *__restrict __file, struct stat *__restrict __buf, int __flag) = NULL;
static int (*real_fstatat64)(int __fd, const char *__restrict __file, struct stat64 *__restrict __buf, int __flag) = NULL;
static int (*real_newfstatat)(int __fd, const char *__restrict __file, struct stat *__restrict __buf, int __flag) = NULL;
static int (*real__fxstat64)(int ver, int fd, struct stat64 *buf) = NULL;
static int (*real__fxstat)(int ver, int fd, struct stat *buf) = NULL;
static int (*real_close)(int fd) = NULL;
static int (*real_puts)(const char *str) = NULL;
static int (*real__open_2)(const char *pathname, int flags, ...) = NULL;
static int (*real_open64)(const char *pathname, int flags, ...) = NULL;
static int (*real_open)(const char *pathname, int flags, ...) = NULL;
static int (*real_creat)(const char *pathname, mode_t mode) = NULL;
static FILE *(*real_fopen)(const char *restrict pathname, const char *restrict mode) = NULL;
static FILE *(*real_fdopen)(int fildes, const char *mode) = NULL;
// static FILE *(*real_fopen64)(const char *restrict pathname, const char *restrict mode) = NULL;
static int (*real_access)(const char *pathname, int mode) = NULL;
static int (*real_mkdir)(const char *path, mode_t mode) = NULL;
static ssize_t (*real_write)(int fd, const void *buf, size_t size) = NULL;
static ssize_t (*real_read)(int fd, const void *buf, size_t size) = NULL;
static int (*real_remove)(const char *name) = NULL;
static int (*real_unlink)(const char *name) = NULL;
static int (*real_rmdir)(const char *path) = NULL;
static int (*real_rename)(const char *old, const char *new) = NULL;
static int (*real_fchmodat)(int dir_fd, const char *pathname, mode_t mode, int flags) = NULL;
static int (*real_fchownat)(int dir_fd, const char *pathname, uid_t owner, gid_t group, int flags) = NULL;
static DIR *(*real_opendir)(const char *name) = NULL;
static struct dirent *(*real_readdir)(DIR *dirp) = NULL;
static struct dirent64 *(*real_readdir64)(DIR *dirp) = NULL;
// static int (*real_readdir)(unsigned int fd, struct old_linux_dirent *dirp, unsigned int count);
static int (*real_closedir)(DIR *dirp) = NULL;
static int (*real_statvfs)(const char *restrict path, struct statvfs *restrict buf) = NULL;
static int (*real_fstatvfs)(int fd, struct statvfs *buf) = NULL;
static int (*real_statvfs64)(const char *restrict path, struct statvfs64 *restrict buf) = NULL;
static int (*real_statfs)(const char *path, struct statfs *buf) = NULL;
static char *(*real_realpath)(const char *restrict path, char *restrict resolved_path) = NULL;
// static int (*real__openat)(int dir_fd, const char *pathname, int flags, ...) = NULL;
static int (*real_openat)(int dir_fd, const char *pathname, int flags, ...) = NULL;
// static int (*real__openat64)(int fd, const char *file, int oflag, ...) = NULL;
// static int (*real__openat64_2)(int fd, const char *file, int oflag) = NULL;
// static int (*real__libc_openat)(int fd, const char *file, int oflag, ...) = NULL;
static int (*real__libc_open64)(const char *file, int oflag, ...) = NULL;
// static int (*real_openat)(int dir_fd, const char *pathname, int flags) = NULL;
static int (*real_fclose)(FILE *fp) = NULL;
static size_t (*real_fread)(void *buf, size_t size, size_t count, FILE *fp) = NULL;
static size_t (*real_fwrite)(const void *buf, size_t size, size_t count, FILE *fp) = NULL;
static void (*real_clearerr)(FILE *fp) = NULL;
static int (*real_ferror)(FILE *fp) = NULL;
static int (*real_feof)(FILE *fp) = NULL;
static long int (*real_ftell)(FILE *fp) = NULL;
static void (*real_rewind)(FILE *stream) = NULL;
static void *(*real_mmap)(void *addr, size_t length, int prot, int flags, int fd, off_t offset) = NULL;
static int (*real_symlink)(const char *name1, const char *name2) = NULL;
static int (*real_symlinkat)(const char *name1, int fd, const char *name2) = NULL;
static int (*real_chdir)(const char *pathname) = NULL;
static int (*real_fchdir)(int fd) = NULL;
static int (*real__chdir)(const char *path) = NULL;
static int (*real___chdir)(const char *path) = NULL;
static int (*real_sys_chdir)(const char *filename) = NULL;
static int (*real_wchdir)(const wchar_t *dirname) = NULL;
static int (*real_chmod)(const char *pathname, mode_t mode) = NULL;
static int (*real_fchmod)(int fd, mode_t mode) = NULL;
static int (*real_execve)(const char *pathname, char *const argv[], char *const envp[]) = NULL;
// static int (*real_execv)(const char *pathname, char *const argv[]) = NULL;
static char *(*real_getcwd)(char *buf, size_t size) = NULL;
static int (*real_change_to_directory)(char *, int, int) = NULL;
static int (*real_bindpwd)(int) = NULL;
static int (*real_epoll_ctl)(int epfd, int op, int fd, struct epoll_event *event) = NULL;
static pid_t (*real_fork)(void) = NULL;
static pid_t (*real_vfork)(void) = NULL;
static pid_t (*real_wait)(int *wstatus) = NULL;
static int (*real___fwprintf_chk)(FILE *stream, int flag, const wchar_t *format) = NULL;
static ssize_t (*real_pread)(int fd, void *buf, size_t count, off_t offset) = NULL;
static ssize_t (*real_pwrite)(int fd, const void *buf, size_t count, off_t offset) = NULL;
static int (*real_truncate)(const char *path, off_t length) = NULL;
static int (*real_ftruncate)(int fd, off_t length) = NULL;
static int (*real_flock)(int fd, int operation) = NULL;
static int (*real_dup2)(int oldfd, int newfd) = NULL;
static int (*real_dup)(int oldfd) = NULL;
static int (*real_mkstemp)(char *template) = NULL;
static ssize_t (*real_readv)(int fd, const struct iovec *iov, int iovcnt) = NULL;
static ssize_t (*real_writev)(int fd, const struct iovec *iov, int iovcnt) = NULL;
// ssize_t preadv(int fd, const struct iovec *iov, int iovcnt, off_t offset);
static ssize_t (*real_pwritev)(int fd, const struct iovec *iov, int iovcnt, off_t offset) = NULL;
static ssize_t (*real_pwrite64)(int fd, const void *buf, size_t count, off64_t offset) = NULL;
// static int (*real_poll)(struct pollfd *fds, nfds_t nfds, int timeout) = NULL;
// static int (*real_ppoll)(struct pollfd *fds, nfds_t nfds, const struct timespec tmo_p, const sigset_t sigmask) = NULL;
// static int (*real_fcntl)(int fd, int cmd, ... /* arg */) = NULL;
static int (*real_syncfs)(int fd) = NULL;
static int (*real_posix_fadvise)(int fd, off_t offset, off_t len, int advice) = NULL;
static int (*real_posix_fadvise64)(int fd, off64_t offset, off64_t len, int advice) = NULL;
static int (*real___fxstatat)(int __ver, int __fildes, const char *__filename, struct stat *__stat_buf, int __flag) = NULL;
static int (*real___fxstatat64)(int __ver, int __fildes, const char *__filename, struct stat64 *__stat_buf, int __flag) = NULL;
static int (*real_faccessat)(int dir_fd, const char *pathname, int mode, int flags) = NULL;
static int (*real_unlinkat)(int fd, const char *name, int flag) = NULL;
static int (*real_renameat2)(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, unsigned int flags) = NULL;
// static int (*real_fstatat)(int dir_fd, const char *pathname, struct stat *buf, int flags) = NULL;
// static int (*real_getdents)(unsigned int fd, struct linux_dirent *dirp, unsigned int count) = NULL;
static int (*real_fsync)(int fd) = NULL;
static int (*real_pthread_create)(pthread_t *restrict thread,
								  const pthread_attr_t *restrict attr,
								  void *(*start_routine)(void *),
								  void *restrict arg) = NULL;

void checkOpenFlags(const char *pathname, int flags)
{
	slog_debug("Checking flags");
	if (flags & O_CREAT)
	{
		slog_debug("[POSIX]. O_CREAT flag, pathname=%s, flags=%x, O_CREAT=%x", pathname, flags, O_CREAT);
		// fprintf(stderr, "[POSIX]. O_CREAT flag, pathname=%s, flags=%x, O_CREAT=%x\n", pathname, flags, O_CREAT);
	}
	if (flags & O_TRUNC)
	{
		slog_debug("[POSIX]. O_TRUNC flag, pathname=%s, flags=%x, O_TRUNC=%x", pathname, flags, O_TRUNC);
		// fprintf(stderr, "[POSIX]. O_TRUNC flag, pathname=%s, flags=%x, O_TRUNC=%x\n", pathname, flags, O_TRUNC);
	}
	if (flags & O_EXCL)
	{

		slog_debug("[POSIX]. O_EXCL flag, pathname=%s, flags=%x, O_EXCL=%x", pathname, flags, O_EXCL);
		// fprintf(stderr, "[POSIX]. O_EXCL flag, pathname=%s, flags=%x, O_EXCL=%x\n", pathname, flags, O_EXCL);
	}
	if (flags & O_RDONLY)
	{

		slog_debug("[POSIX]. O_RDONLY flag, pathname=%s\n", pathname);
		// fprintf(stderr, "[POSIX]. O_RDONLY flag, pathname=%s\n", pathname);
	}
	if (flags & O_WRONLY)
	{

		slog_debug("[POSIX]. O_WRONLY flag, pathname=%s, flags=%x, O_WRONLY=%x", pathname, flags, O_WRONLY);
		// fprintf(stderr, "[POSIX]. O_WRONLY flag, pathname=%s, flags=%x, O_WRONLY=%x\n", pathname, flags, O_WRONLY);
	}
	if (flags & O_RDWR)
	{

		slog_debug("[POSIX]. O_RDWR flag, pathname=%s, flags=%x, O_RDWR=%x", pathname, flags, O_RDWR);
		// fprintf(stderr, "[POSIX]. O_RDWR flag, pathname=%s, flags=%x, O_RDWR=%x\n", pathname, flags, O_RDWR);
	}
	if (flags & O_APPEND)
	{

		slog_debug("[POSIX]. O_APPEND flag, pathname=%s, flags=%x, O_APPEND=%x", pathname, flags, O_APPEND);
		// fprintf(stderr, "[POSIX]. O_RDWR flag, pathname=%s, flags=%x, O_RDWR=%x\n", pathname, flags, O_RDWR);
	}
	if (flags & O_NONBLOCK)
	{

		slog_debug("[POSIX]. O_NONBLOCK flag, pathname=%s, flags=%x, O_NONBLOCK=%x", pathname, flags, O_NONBLOCK);
		// fprintf(stderr, "[POSIX]. O_RDWR flag, pathname=%s, flags=%x, O_RDWR=%x\n", pathname, flags, O_RDWR);
	}
	if (flags & O_SYNC)
	{

		slog_debug("[POSIX]. O_SYNC flag, pathname=%s, flags=%x, O_SYNC=%x", pathname, flags, O_SYNC);
		// fprintf(stderr, "[POSIX]. O_RDWR flag, pathname=%s, flags=%x, O_RDWR=%x\n", pathname, flags, O_RDWR);
	}
	if (flags & O_PATH)
	{

		slog_debug("[POSIX]. O_PATH flag, pathname=%s, flags=%x, O_PATH=%x", pathname, flags, O_PATH);
		// fprintf(stderr, "[POSIX]. O_RDWR flag, pathname=%s, flags=%x, O_RDWR=%x\n", pathname, flags, O_RDWR);
	}
}

char *checkHerculesPath(const char *pathname)
{
	char *new_path = NULL;
	char *workdir = getenv("PWD");
	char absolute_pathname[MAX_PATH] = {'\0'};
	int ret = 0;

	// fprintf(stderr, "pathname=%s\n", pathname);

	// if (!strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT) - 1)) // error when  pathname=/mnt/hercules/data/unet3d and MOUNT_POINT=/mnt/hercules,
	// if (!strncmp(pathname, MOUNT_POINT, strlen(pathname) - 1))
	if (!strncmp(pathname, MOUNT_POINT, MAX(strlen(pathname), strlen(MOUNT_POINT)) - 1))
	{
		// slog_debug("[HERCULES][checkHerculesPath] pathname=%s, MOUNT_POINT=%s, Success", pathname, MOUNT_POINT);
		// new_path = calloc(strlen("Success"), sizeof(char));
		new_path = calloc(strlen("imss://"), sizeof(char));
		// strcpy(new_path, "Success");
		strcat(new_path, "imss://");
	}
	else
	{
		//
		if (!strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT) - 1) || (pathname[0] != '/' && !strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT) - 1)))
		{
			// if (pathname[0] == '.')
			if (!strncmp(pathname, ".", strlen(pathname)))
			{
				slog_debug("[IMSS][checkHerculesPath] pathname=%s, workdir=%s", pathname, workdir);
				new_path = convert_path(workdir);
			}
			else if (!strncmp(pathname, "./", strlen("./")))
			{
				slog_debug("[IMSS][checkHerculesPath] ./ case=%s", pathname);
				new_path = convert_path(pathname + strlen("./"));
			}
			else
			{
				// slog_debug("[HERCULES][checkHerculesPath] after resolve path, pathname=%s, real_pathname=%s", pathname, real_pathname);
				// new_path = convert_path(pathname);
				ret = ResolvePath(pathname, absolute_pathname);
				// slog_debug("[IMSS][checkHerculesPath] last option, pathname=%s, absolute_pathname_len=%d, workdir=%s", pathname, ret, workdir);
				if (ret > 0)
				{
					// slog_debug("[IMSS][checkHerculesPath] absolute_pathname=%s", absolute_pathname);
					new_path = convert_path(absolute_pathname);
				}
				else
				{
					new_path = convert_path(pathname);
				}

				// slog_debug("[HERCULES][checkHerculesPath] pathname=%s, realpath=%s, new_path=%s", pathname, real_pathname, new_path);
				// free(real_pathname);
			}
		}
	}
	// slog_debug("[HERCULES][checkHerculesPath] pathname=%s, new_path=%s", pathname, new_path);
	return new_path;
}

uint32_t MurmurOAAT32(const char *key)
{
	uint32_t h = 335ul;
	for (; *key; ++key)
	{
		h ^= *key;
		h *= 0x5bd1e995;
		h ^= h >> 15;
	}
	return abs(h);
}

void *
prefetch_function(void *th_argv)
{
	for (;;)
	{

		pthread_mutex_lock(&lock);
		while (prefetch_ds < 0)
		{
			pthread_cond_wait(&cond_prefetch, &lock);
		}

		if (prefetch_first_block < prefetch_last_block && prefetch_first_block != -1)
		{
			// printf("Se activo Prefetch path:%s$%d-$%d\n",prefetch_path, prefetch_first_block, prefetch_last_block);
			int exist_first_block, exist_last_block, position;
			char *buf = map_get_buffer_prefetch(map_prefetch, prefetch_path, &exist_first_block, &exist_last_block);
			int err = readv_multiple(prefetch_ds, prefetch_first_block, prefetch_last_block, buf, IMSS_BLKSIZE, prefetch_offset, IMSS_DATA_BSIZE * (prefetch_last_block - prefetch_first_block));
			if (err == -1)
			{
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

char *convert_path(const char *name)
{
	char *path = calloc(1024, sizeof(char));
	strcpy(path, name);
	size_t len = strlen(MOUNT_POINT);
	// remove MOUNT_POINT prefix from the path.
	if (len > 0)
	{
		char *p = path;
		while ((p = strstr(p, MOUNT_POINT)) != NULL)
		{
			memmove(p, p + len, strlen(p + len) + 1);
		}
	}

	char *new_path = calloc(1024, sizeof(char));

	// seeks initial slashes "/" in the path.
	len = strlen(path);
	size_t desplacements = 0;
	// fprintf(stderr, "path=%s, len=%ld, strncmp=%d\n", path, len, !strncmp(path, "/", strlen("/")));
	for (size_t i = 0; i < len; i++)
	{
		if (!strncmp(path + i, "/", strlen("/")))
		{
			// fprintf(stderr,"Increasing desplacements\n");
			desplacements++;
		}
		else
		{
			// path += desplacements;
			// strcat(new_path, "imss://");
			break;
		}
	}
	// fprintf(stderr, "path=%s, desplacements=%ld\n", path, desplacements);
	// deletes initial slashes "/" from the path.
	if (desplacements > 0)
	{
		path += desplacements;
	}
	// add the URL to the new path.
	strcat(new_path, "imss://");

	// fprintf(stderr, "updated path=%s, desplacements=%ld\n", path, desplacements);
	// if (!strncmp(path, "/", strlen("/")))
	// {
	// 	strcat(new_path, "imss:/");
	// }
	// else
	// {
	// 	strcat(new_path, "imss://");
	// }
	// add the path to the new_path, which has the URL prefix.
	if (desplacements < len)
	{
		strcat(new_path, path);
	}
	// fprintf(stderr, "updated path=%s, desplacements=%ld, new_path=%s\n", path, desplacements, new_path);

	return new_path;
}

__attribute__((constructor)) void imss_posix_init(void)
{
	errno = 0;
	// double init_time = 0.0, finish_time = 0.0;
	// double time_taken = 0.0;
	// init_time = clock();
	// time(&init_time);

	// struct timeval tv;
	// gettimeofday(&tv, NULL);

	// double begin = (tv.tv_sec) * 1000 + (tv.tv_usec) / 1000;

	map_fd = map_fd_create();

	// Getting a mostly unique id for the distributed deployment.
	char hostname_[512], hostname[1024];
	int ret = gethostname(&hostname_[0], 512);
	if (ret == -1)
	{
		perror("gethostname");
		exit(EXIT_FAILURE);
	}
	sprintf(hostname, "%s:%d", hostname_, getpid());
	g_pid = getpid();

	rank = MurmurOAAT32(hostname);

	// fill global variables with the enviroment variables value.
	ret = getConfiguration();
	if (ret == -1)
	{
		exit(EXIT_FAILURE);
	}

	// fprintf(stderr, "IMSS_DEBUG_LEVEL=%d\n", IMSS_DEBUG_LEVEL);

	IMSS_DATA_BSIZE = IMSS_BLKSIZE * KB; // block size in bytes.
	// aux_refresh = (char *)malloc(IMSS_DATA_BSIZE); // global buffer to refresh metadata.
	// imss_path_refresh = calloc(MAX_PATH, sizeof(char));
	// Hercules init -- Attached deploy
	if (DEPLOYMENT == 1)
	{
		// Hercules init -- Attached deploy
		if (hercules_init(0, STORAGE_SIZE, IMSS_SRV_PORT, 1, METADATA_PORT, META_BUFFSIZE, METADATA_FILE) == -1)
		{
			// In case of error notify and exit
			slog_fatal("[IMSS-FUSE]	Hercules init failed, cannot deploy IMSS.\n");
		}
	}

	// log init.
	time_t t = time(NULL);
	struct tm tm = *localtime(&t);

	char *workdir = getenv("PWD");

	// strcpy(pwd_init, workdir);

	// strcpy(pwd_init, "/tmp");

	// fprintf(stderr, "[%d] ************ Calling constructor, HERCULES_PATH=%s, pid=%d, init=%d ************\n", rank, HERCULES_PATH, getpid(), init);

	// fprintf(stderr, "LOG PATH= %s, pid=%d\n", log_path, getpid());
	// if (!strncmp(log_path, "", 1))
	{
		// sprintf(log_path, "%s/client.%02d-%02d.%d", HERCULES_PATH, tm.tm_hour, tm.tm_min, rank); // originial.
		sprintf(log_path, "%s/client-thread-%ld.%02d-%02d.%d", HERCULES_PATH, pthread_self(), tm.tm_hour, tm.tm_min, getpid());
	}
	// if (IMSS_DEBUG_FILE)
	// {
	// 	fprintf(stderr, "LOG PATH= %s\n", log_path); // this line raise an exception running a python app with threads.
	// }
	// sprintf(log_path, "./client.%02d-%02d-%02d.%d", tm.tm_hour, tm.tm_min, tm.tm_sec, rank);
	slog_init(log_path, IMSS_DEBUG_LEVEL, IMSS_DEBUG_FILE, IMSS_DEBUG_SCREEN, 1, 1, 1, rank);
	slog_info(",Time(msec), Comment, RetCode");

	slog_debug(" -- HERCULES_MOUNT_POINT: %s", MOUNT_POINT);
	slog_debug(" -- HERCULES_ROOT: %s", IMSS_ROOT);
	slog_debug(" -- HERCULES_HOSTFILE: %s", IMSS_HOSTFILE);
	slog_debug(" -- HERCULES_N_SERVERS: %d", N_SERVERS);
	slog_debug(" -- HERCULES_SRV_PORT: %d", IMSS_SRV_PORT);
	slog_debug(" -- HERCULES_BUFFSIZE: %ld", IMSS_BUFFSIZE);
	slog_debug(" -- META_HOSTFILE: %s", META_HOSTFILE);
	slog_debug(" -- HERCULES_META_PORT: %d", METADATA_PORT);
	slog_debug(" -- HERCULES_META_SERVERS: %d", N_META_SERVERS);
	slog_debug(" -- HERCULES_BLKSIZE: %ld kB", IMSS_BLKSIZE);
	slog_debug(" -- HERCULES_STORAGE_SIZE: %ld GB", STORAGE_SIZE);
	slog_debug(" -- HERCULES_METADATA_FILE: %s", METADATA_FILE);
	slog_debug(" -- HERCULES_DEPLOYMENT: %d", DEPLOYMENT);
	slog_debug(" -- HERCULES_MALLEABILITY: %d", MALLEABILITY);
	slog_debug(" -- UPPER_BOUND_SERVERS: %d", UPPER_BOUND_SERVERS);
	slog_debug(" -- LOWER_BOUND_SERVERS: %d", LOWER_BOUND_SERVERS);
	slog_debug(" -- REPL_FACTOR: %d", REPL_FACTOR);
	slog_debug(" -- POLICY: %s", POLICY);
	slog_debug(" -- RELEASE: %d", release);

	// Metadata server
	if (stat_init(META_HOSTFILE, METADATA_PORT, N_META_SERVERS, rank) == -1)
	{
		// In case of error notify and exit
		slog_error("Stat init failed, cannot connect to Metadata server.");
		return;
	}

	if (DEPLOYMENT == 2)
	{
		ret = open_imss(IMSS_ROOT);
		if (ret < 0)
		{
			release = 0;
			slog_fatal("Error creating HERCULES's resources, the process cannot be started");
			// printf("Error creating HERCULES's resources, the process cannot be started\n");
			return;
		}
	}

	if (DEPLOYMENT != 2)
	{
		// Initialize the IMSS servers
		if (init_imss(IMSS_ROOT, IMSS_HOSTFILE, META_HOSTFILE, N_SERVERS, IMSS_SRV_PORT, IMSS_BUFFSIZE, DEPLOYMENT, "hercules_server", METADATA_PORT) < 0)
		{
			slog_fatal("[IMSS-FUSE]	IMSS init failed, cannot create servers.\n");
		}
	}

	map_prefetch = map_create_prefetch();
	map = map_create();
	if (MULTIPLE_READ == 1)
	{
		int ret;

		pthread_attr_t tattr;
		ret = pthread_attr_init(&tattr);
		ret = pthread_attr_setdetachstate(&tattr, PTHREAD_CREATE_DETACHED);

		if (pthread_create(&prefetch_t, &tattr, prefetch_function, NULL) == -1)
		{
			perror("ERRIMSS_PREFETCH_DEPLOY");
			pthread_exit(NULL);
		}
	}

	slog_debug("[CLIENT %d] ready!\n", rank);

	slog_debug("IMSS EXIST=%d\n", is_alive(IMSS_ROOT));

	// fprintf(stderr, "[CLIENT %d] ready!\n", rank);

	// sleep(10);

	// finish_time = clock();
	// time(&finish_time);
	// time_taken = ((double)(finish_time - init_time)) / (CLOCKS_PER_SEC);

	// gettimeofday(&tv, NULL);
	// double end = (tv.tv_sec) * 1000 + (tv.tv_usec) / 1000;

	// fprintf(stderr, "CLIENT_CONSTRUCTOR_TIME %f, finish %f, init %f\n", end - begin, end, begin);
	// fprintf(stderr, "Client started\n");
	init = 1;
}

int getConfiguration()
{
	struct cfg_struct *cfg;

	/***************************************************************/
	/******************* PARSE FILE ARGUMENTS **********************/
	/***************************************************************/
	int ret = 0;

	char *conf_path;
	char abs_exe_path[1024];
	char *aux;

	cfg = cfg_init();
	conf_path = getenv("HERCULES_CONF");
	if (conf_path != NULL)
	{
		// slog_debug("Loading %s", conf_path);
		// fprintf(stderr, "Loading %s\n", conf_path);
		ret = cfg_load(cfg, conf_path);
		if (ret)
		{
			fprintf(stderr, "%s has not been loaded\n", conf_path);
		}
	}
	else
	{
		ret = 1;
	}

	if (ret)
	{
		char default_paths[3][PATH_MAX] = {
			"/etc/hercules.conf",
			"./hercules.conf",
			"hercules.conf"};

		for (size_t i = 0; i < 3; i++)
		{
			// slog_debug("Loading %s\n", default_paths[i]);
			// fprintf(stderr, "Loading %s\n", default_paths[i]);
			if (cfg_load(cfg, default_paths[i]) == 0)
			{
				ret = 0;
				break;
			}
		}
		if (ret)
		{
			if (getcwd(abs_exe_path, sizeof(abs_exe_path)) != NULL)
			{
				conf_path = (char *)malloc(sizeof(char) * PATH_MAX);
				sprintf(conf_path, "%s/%s", abs_exe_path, "../conf/hercules.conf");
				if (cfg_load(cfg, conf_path) == 0)
				{
					ret = 0;
				}
			}
		}

		// if (!ret)
		// {
		// 	// slog_debug("[CLIENT] Configuration file loaded: %s\n", conf_path);
		// 	// fprintf(stderr, "[CLIENT %d] Configuration file loaded: %s\n", rank, conf_path);
		// }
		// else
		if (ret)
		{
			fprintf(stderr, "[HERCULES CLIENT %d] Configuration file not found\n", rank);
			perror("ERRIMSS_CONF_NOT_FOUND");
			return -1;
		}
		free(conf_path);
	}
	// else
	// {
	// 	slog_debug("[CLIENT] Configuration file loaded: %s\n", conf_path);
	// 	// fprintf(stderr, "[CLIENT %d] Configuration file loaded: %s\n", rank, conf_path);
	// }

	if (cfg_get(cfg, "URI"))
	{
		aux = cfg_get(cfg, "URI");
		strcpy(IMSS_ROOT, aux);
	}

	if (cfg_get(cfg, "BLOCK_SIZE"))
		IMSS_BLKSIZE = atoi(cfg_get(cfg, "BLOCK_SIZE"));

	if (cfg_get(cfg, "MOUNT_POINT"))
	{
		aux = cfg_get(cfg, "MOUNT_POINT");
		strcpy(MOUNT_POINT, aux);
	}

	if (cfg_get(cfg, "HERCULES_PATH"))
	{
		aux = cfg_get(cfg, "HERCULES_PATH");
		strcpy(HERCULES_PATH, aux);
	}

	if (cfg_get(cfg, "METADATA_PORT"))
		METADATA_PORT = atol(cfg_get(cfg, "METADATA_PORT"));

	if (cfg_get(cfg, "DATA_PORT"))
		IMSS_SRV_PORT = atol(cfg_get(cfg, "DATA_PORT"));

	if (cfg_get(cfg, "NUM_DATA_SERVERS"))
		N_SERVERS = atoi(cfg_get(cfg, "NUM_DATA_SERVERS"));

	if (cfg_get(cfg, "NUM_META_SERVERS"))
		N_META_SERVERS = atoi(cfg_get(cfg, "NUM_META_SERVERS"));

	if (cfg_get(cfg, "MALLEABILITY"))
		MALLEABILITY = atoi(cfg_get(cfg, "MALLEABILITY"));

	if (cfg_get(cfg, "UPPER_BOUND_MALLEABILITY"))
		UPPER_BOUND_SERVERS = atoi(cfg_get(cfg, "UPPER_BOUND_MALLEABILITY"));

	if (cfg_get(cfg, "LOWER_BOUND_MALLEABILITY"))
		LOWER_BOUND_SERVERS = atoi(cfg_get(cfg, "LOWER_BOUND_MALLEABILITY"));

	if (cfg_get(cfg, "REPL_FACTOR"))
		REPL_FACTOR = atoi(cfg_get(cfg, "REPL_FACTOR"));

	if (cfg_get(cfg, "POLICY"))
		POLICY = cfg_get(cfg, "POLICY");

	if (cfg_get(cfg, "METADATA_HOSTFILE"))
	{
		aux = cfg_get(cfg, "METADATA_HOSTFILE");
		strcpy(META_HOSTFILE, aux);
	}

	if (cfg_get(cfg, "DATA_HOSTFILE"))
	{
		aux = cfg_get(cfg, "DATA_HOSTFILE");
		strcpy(IMSS_HOSTFILE, aux);
	}

	if (cfg_get(cfg, "METADA_PERSISTENCE_FILE"))
	{
		aux = cfg_get(cfg, "METADA_PERSISTENCE_FILE");
		strcpy(METADATA_FILE, aux);
	}

	if (cfg_get(cfg, "DEBUG_LEVEL"))
	{
		aux = cfg_get(cfg, "DEBUG_LEVEL");
	}
	else if (getenv("IMSS_DEBUG") != NULL)
	{
		aux = getenv("IMSS_DEBUG");
	}
	else
	{
		aux = NULL;
	}

	if (aux != NULL)
	{
		if (strstr(aux, "file"))
		{
			IMSS_DEBUG_FILE = 1;
			IMSS_DEBUG_SCREEN = 0;
			IMSS_DEBUG_LEVEL = SLOG_LIVE;
		}
		else if (strstr(aux, "stdout"))
			IMSS_DEBUG_SCREEN = 1;
		else if (strstr(aux, "debug"))
			IMSS_DEBUG_LEVEL = SLOG_DEBUG;
		else if (strstr(aux, "live"))
			IMSS_DEBUG_LEVEL = SLOG_LIVE;
		else if (strstr(aux, "all"))
		{
			IMSS_DEBUG_FILE = 1;
			IMSS_DEBUG_SCREEN = 1;
			IMSS_DEBUG_LEVEL = SLOG_PANIC;
		}
		else if (strstr(aux, "none"))
		{
			IMSS_DEBUG_FILE = 0;
			IMSS_DEBUG_SCREEN = 0;
			IMSS_DEBUG_LEVEL = SLOG_NONE;
			unsetenv("IMSS_DEBUG");
		}
		else
		{
			IMSS_DEBUG_FILE = 1;
			IMSS_DEBUG_LEVEL = getLevel(aux);
		}
	}

	/*************************************************************************/

	if (getenv("IMSS_MOUNT_POINT") != NULL)
	{
		strcpy(MOUNT_POINT, getenv("IMSS_MOUNT_POINT"));
	}

	// strcpy(IMSS_ROOT, "imss://");

	if (getenv("IMSS_HOSTFILE") != NULL)
	{
		strcpy(IMSS_HOSTFILE, getenv("IMSS_HOSTFILE"));
	}

	if (getenv("IMSS_N_SERVERS") != NULL)
	{
		N_SERVERS = atoi(getenv("IMSS_N_SERVERS"));
	}

	if (getenv("IMSS_SRV_PORT") != NULL)
	{
		IMSS_SRV_PORT = atol(getenv("IMSS_SRV_PORT"));
	}

	if (getenv("IMSS_BUFFSIZE") != NULL)
	{
		IMSS_BUFFSIZE = atol(getenv("IMSS_BUFFSIZE"));
	}

	if (getenv("IMSS_META_HOSTFILE") != NULL)
	{
		strcpy(META_HOSTFILE, getenv("IMSS_META_HOSTFILE"));
	}

	if (getenv("IMSS_META_PORT") != NULL)
	{
		METADATA_PORT = atol(getenv("IMSS_META_PORT"));
	}

	if (getenv("IMSS_META_SERVERS") != NULL)
	{
		N_META_SERVERS = atoi(getenv("IMSS_META_SERVERS"));
	}

	if (getenv("IMSS_BLKSIZE") != NULL)
	{
		IMSS_BLKSIZE = atoi(getenv("IMSS_BLKSIZE"));
	}

	if (getenv("IMSS_STORAGE_SIZE") != NULL)
	{
		STORAGE_SIZE = atol(getenv("IMSS_STORAGE_SIZE"));
	}

	if (getenv("IMSS_METADATA_FILE") != NULL)
	{
		strcpy(METADATA_FILE, getenv("IMSS_METADATA_FILE"));
	}

	if (getenv("IMSS_DEPLOYMENT") != NULL)
	{
		DEPLOYMENT = atoi(getenv("IMSS_DEPLOYMENT"));
	}

	if (getenv("IMSS_MALLEABILITY") != NULL)
	{
		MALLEABILITY = atoi(getenv("IMSS_MALLEABILITY"));
	}

	if (getenv("IMSS_UPPER_BOUND_MALLEABILITY") != NULL)
	{
		UPPER_BOUND_SERVERS = atoi(getenv("IMSS_UPPER_BOUND_MALLEABILITY"));
	}

	if (getenv("IMSS_LOWER_BOUND_MALLEABILITY") != NULL)
	{
		LOWER_BOUND_SERVERS = atoi(getenv("IMSS_LOWER_BOUND_MALLEABILITY"));
	}

	return 1;
}

void __attribute__((destructor)) run_me_last()
{
	errno = 0;
	slog_live("Calling 'run_me_last', pid=%d, rank=%d, release=%d", g_pid, rank, release);
	// sleep(60);
	if (release == 1)
	// if (false)
	{
		// clock_t t_s;
		// double time_taken;
		// t_s = clock();
		release = -1;
		slog_live("[POSIX] release_imss()");
		release_imss("imss://", CLOSE_DETACHED);
		slog_live("[POSIX] stat_release()");
		stat_release();

		// imss_comm_cleanup();
		// t_s = clock() - t_s;
		// time_taken = ((double)t_s) / (CLOCKS_PER_SEC);
	}
	slog_live("End 'run_me_last', pid=%d, release=%d", g_pid, release);
}

void check_ld_preload(void)
{

	if (LD_PRELOAD == 0)
	{
		DPRINT("\nActivating... ld_preload=%d\n\n", LD_PRELOAD);
		fprintf(stderr, "\nActivating... ld_preload\n");
		LD_PRELOAD = 1;
		imss_posix_init();
	}
}

int close(int fd)
{

	if (!real_close)
		real_close = dlsym(RTLD_NEXT, "close");

	if (!init)
	{
		return real_close(fd);
	}

	errno = 0;
	int ret = 0;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		// if (release)
		{
			// pthread_mutex_lock(&system_lock);
			slog_debug("[POSIX]. Calling Hercules 'close', pathname=%s, fd=%d", pathname, fd);
			ret = imss_close(pathname, fd);
			slog_debug("[POSIX]. Ending Hercules 'close', pathname=%s, ret=%d\n", pathname, ret);
			// fprintf(stderr,"[POSIX]. Ending Hercules 'close', pathname=%s, ret=%d\n", pathname, ret);

			map_fd_update_value(map_fd, pathname, fd, 0);
		}
		// pthread_mutex_unlock(&system_lock);
	}
	else
	{
		slog_full("[POSIX]. Calling Real 'close', fd=%d", fd);
		ret = real_close(fd);
		slog_full("[POSIX]. Ending Real 'close', ret=%d", ret);
	}
	return ret;
}

int __lxstat(int ver, const char *pathname, struct stat *buf)
{
	if (!real__lxstat)
		real__lxstat = dlsym(RTLD_NEXT, "__lxstat");

	if (!init)
	{
		return real__lxstat(ver, pathname, buf);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)

	// errno = 0;
	// int ret = 0;
	// char *pathname_dir, *new_path = NULL;
	// if (dir_fd == AT_FDCWD)
	// {
	// 	// pathname_dir = getenv("PWD");
	// 	// new_path = checkHerculesPath(pathname_dir);
	// 	// pathname_dir = NULL;
	// 	new_path = checkHerculesPath(pathname);
	// 	pathname_dir = NULL;
	// }
	// else
	// {
	// 	pathname_dir = map_fd_search_by_val(map_fd, dir_fd);
	// }

	// if (pathname_dir != NULL || new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules '__lxstat', pathname=%s, new_path=%s, ver=%d", pathname, new_path, ver);
		// imss_refresh(new_path);
		// if (!strncmp(new_path, "Success", strlen("Success")))
		// {
		// 	ret = 0;
		// }
		// else
		{
			ret = imss_refresh(new_path);
			// if (ret < 0)
			// {
			// 	errno = -ret;
			// 	ret = -1;
			// 	// perror("ERRIMSS_ACCESS_IMSSREFRESH");
			// }
			// else
			// {
			ret = imss_getattr(new_path, buf);
			if (ret < 0)
			{
				errno = -ret;
				ret = -1;
			}
			// }
		}
		slog_debug("[POSIX]. End Hercules '__lxstat', pathname=%s, new_path=%s, ver=%d, ret=%d, file_size=%lu\n", pathname, new_path, ver, ret, buf->st_size);
		free(new_path);
	}
	else
	{
		slog_debug("[POSIX]. Calling real '__lxstat', pathname=%s", pathname);
		ret = real__lxstat(ver, pathname, buf);
		slog_debug("[POSIX]. End real '__lxstat', pathname=%s", pathname);
	}

	return ret;
}

int __lxstat64(int fd, const char *pathname, struct stat64 *buf)
{
	if (!real__lxstat64)
		real__lxstat64 = dlsym(RTLD_NEXT, "__lxstat64");

	if (!init)
	{
		return real__lxstat64(fd, pathname, buf);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules '__lxstat64', pathname=%s", pathname);

		imss_refresh(new_path);
		ret = imss_getattr(new_path, (struct stat *)buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
		}
		slog_debug("[POSIX]. End Hercules '__lxstat64', ret=%d, errno=%d", ret, errno);
		free(new_path);
	}
	else
	{
		slog_full("[POSIX]. Calling Real '__lxstat64', pathname=%s", pathname);
		ret = real__lxstat64(fd, pathname, buf);
		slog_full("[POSIX]. End Real '__lxstat64', pathname=%s", pathname);
	}

	return ret;
}

int __xstat(int ver, const char *pathname, struct stat *stat_buf)
{
	if (!real___xstat)
		real___xstat = dlsym(RTLD_NEXT, "__xstat");

	if (!init)
	{
		return real___xstat(ver, pathname, stat_buf);
	}

	errno = 0;
	int ret = -1;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_debug("[POSIX] Calling Hercules '__xstat', pathname=%s, ver=%d, new_path=%s", pathname, ver, new_path);
		imss_refresh(new_path);
		ret = imss_getattr(new_path, stat_buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_error("[POSIX] Error Hercules '__xstat', %s, %d:%s", pathname, errno, strerror(errno));
		}

		slog_debug("[POSIX] End Hercules '__xstat', pathname=%s, ver=%d, new_path=%s, ret=%d, filesize=%ld\n", pathname, ver, new_path, ret, stat_buf->st_size);
		// fprintf(stderr,"[POSIX] End Hercules '__xstat', pathname=%s, ver=%d, new_path=%s, ret=%d, filesize=%ld\n", pathname, ver, new_path, ret, stat_buf->st_size);
		free(new_path);
	}
	else
	{
		slog_full("[POSIX]. Calling Real '__xstat', pathname=%s, ver=%d", pathname, ver);
		ret = real___xstat(ver, pathname, stat_buf);
		slog_full("[POSIX]. End Real '__xstat', pathname=%s, ver=%d, errno=%d:%s, ret=%d", pathname, ver, errno, strerror(errno), ret);
	}
	return ret;
}

pid_t wait(int *wstatus)
{
	if (!real_wait)
		real_wait = dlsym(RTLD_NEXT, "wait");

	if (!init)
	{
		return real_wait(wstatus);
	}

	errno = 0;
	slog_debug("[POSIX] Calling wait");

	return real_wait(wstatus);
}

pid_t fork(void)
{
	if (!real_fork)
		real_fork = dlsym(RTLD_NEXT, "fork");

	if (!init)
	{
		return real_fork();
	}

	errno = 0;
	slog_debug("[POSIX] Calling fork");
	pid_t pid = real_fork();

	if (pid == -1)
	{

		perror("Fork error");
		slog_error("[POSIX] Error 'real fork', errno=%d:%s", errno, strerror(errno));
		exit(EXIT_FAILURE);
	}

	// char old_log_path[1000];
	// strncpy(old_log_path, log_path, strlen(log_path));
	// strcpy(old_log_path, log_path);

	if (pid == 0) // child process.
	{
		// pid = getpid();
		// g_pid = pid;
		// fprintf(stderr, "[POSIX]. Calling fork\n");
		// release is set to 0 to prevent clossing the communication twice (only the parent process must do it).
		release = 0;

		char hostname_[512], hostname[1024];
		int ret = gethostname(&hostname_[0], 512);
		if (ret == -1)
		{
			perror("gethostname");
			exit(EXIT_FAILURE);
		}
		sprintf(hostname, "%s:%d", hostname_, pid);

		int new_rank = MurmurOAAT32(hostname);

		// // fill global variables with the enviroment variables value.
		// getConfiguration();
		time_t t = time(NULL);
		struct tm tm = *localtime(&t);
		// sprintf(log_path, "%s/client-child.%02d-%02d.%d", HERCULES_PATH, tm.tm_hour, tm.tm_min, new_rank); // original.
		// sprintf(log_path, "%s/client-child.%02d-%02d.%d", HERCULES_PATH, tm.tm_hour, tm.tm_min, pid); // original.

		// fprintf(stderr, "[POSIX]. Fork child created, hostname=%s, pid=%d, new rank = %d, log_path=%s, old_log_path=%s\n", hostname, pid, new_rank, log_path, old_log_path);
		// slog_init(log_path, IMSS_DEBUG_LEVEL, IMSS_DEBUG_FILE, IMSS_DEBUG_SCREEN, 1, 1, 1, new_rank);
		// slog_info("[POSIX]. Fork child created, hostname=%s, new rank=%d, log_path=%s, old_log_path=%s, init=%d", hostname, new_rank, log_path, old_log_path, init);
		// slog_info("[POSIX]. Fork child created, hostname=%s, pid=%d, log_path=%s, old_log_path=%s, init=%d", hostname, pid, log_path, old_log_path, init);
	}
	// else // parent process.
	// {
	// fprintf(stderr, "[POSIX]. Fork parent status, pid=%d, rank=%d, log_path=%s, old_log_path=%s\n", pid, rank, log_path, old_log_path);
	// slog_info("[POSIX]. Calling fork, rank=%d, log_path=%s, old_log_path=%s", rank, log_path, old_log_path);
	// slog_info("[POSIX]. Calling fork, child pid=%d, log_path=%s, old_log_path=%s", pid, log_path, old_log_path);
	// }

	return pid;
}

pid_t vfork(void)
{
	if (!real_vfork)
		real_vfork = dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_vfork();
	}
	// pid_t pid = real_vfork();
	// slog_debug("[POSIX] Ending real '%s', pid=%d", __func__, pid);

	errno = 0;
	slog_debug("[POSIX] Calling vfork");
	// pid_t pid = real_vfork();
	int status = -1;
	pid_t pid = real_vfork(); // vfork(); // real_fork();

	if (pid == -1)
	{

		slog_error("[POSIX] Error 'real %s', errno=%d:%s", __func__, errno, strerror(errno));
		perror("Vfork error");
		exit(EXIT_FAILURE);
	}

	if (pid == 0) // child process.
	{
		// release is set to 0 to prevent clossing the communication twice (only the parent process must do it).
		release = 0;
		slog_debug("[POSIX] Child process");
	}
	else
	{
		// errno = 0;
		slog_debug("[POSIX] Parent process, pid=%d", pid);
		// while (wait(&status) != pid)
		// 	;
		// if (status == 0)
		// {
		// 	slog_debug("[POSIX] Child process has ended, pid=%d", pid);
		// }
		// else
		// {
		// 	slog_error("[POSIX] Child process has failed, pid=%d", pid);
		// }

		// sleep(120);
		// slog_debug("[POSIX] Ending '%s'", __func__);
	}

	return pid;
}

int lstat(const char *pathname, struct stat *buf)
{
	if (!real_lstat)
		real_lstat = dlsym(RTLD_NEXT, "lstat");

	if (!init)
	{
		return real_lstat(pathname, buf);
	}

	errno = 0;
	int ret;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_debug("[POSIX] Calling Hercules 'lstat', new_path=%s", new_path);
		imss_refresh(new_path);
		ret = imss_getattr(new_path, buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
		}
		slog_debug("[POSIX] Ending Hercules 'lstat', new_path=%s, errno=%d:%s", new_path, errno, strerror(errno));
		free(new_path);
	}
	else
	{
		slog_debug("[POSIX] Calling real 'lstat', pathname=%s", pathname);
		ret = real_lstat(pathname, buf);
		slog_debug("[POSIX] End real 'lstat', pathname=%s, ret=%d", pathname, ret);
	}

	return ret;
}

int stat(const char *pathname, struct stat *buf)
{
	// fprintf(stderr, "[POSIX]. Calling 'stat', new_path=%s\n", pathname);
	if (!real_stat)
		real_stat = dlsym(RTLD_NEXT, "stat");

	if (!init)
	{
		return real_stat(pathname, buf);
	}

	errno = 0;
	int ret;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'stat', new_path=%s.", new_path);
		imss_refresh(new_path);
		ret = imss_getattr(new_path, buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
		}
		slog_debug("[POSIX]. Ending Hercules 'stat', new_path=%s, ret=%d\n", new_path, ret);
		free(new_path);
	}
	else
	{
		slog_debug("[POSIX]. Calling Real 'stat', pathname=%s.", pathname);
		ret = real_stat(pathname, buf);
		slog_debug("[POSIX]. Ending Real 'stat', pathname=%s.", pathname);
	}

	return ret;
}

extern int statvfs(const char *__restrict __file, struct statvfs *__restrict __buf)
{
	// fprintf(stderr, "Calling statvfs, path=%s\n", __file);
	if (!real_statvfs)
		real_statvfs = dlsym(RTLD_NEXT, "statvfs");

	if (!init)
	{
		return real_statvfs(__file, __buf);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(__file);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'statvfs', path=%s", new_path);
		__buf->f_bsize = IMSS_BLKSIZE * KB;
		__buf->f_namemax = URI_;
		slog_debug("[POSIX]. End Hercules 'statvfs', path=%s", new_path);
		free(new_path);
	}
	else
	{
		slog_full("[POSIX]. Calling Real 'statvfs', path=%s", __file);
		ret = real_statvfs(__file, __buf);
		slog_full("[POSIX]. Ending Real 'statvfs', path=%s", __file);
	}

	return ret;
}

int fstatvfs(int fd, struct statvfs *buf)
{
	if (!real_fstatvfs)
		real_fstatvfs = dlsym(RTLD_NEXT, "fstatvfs");

	if (!init)
	{
		return real_fstatvfs(fd, buf);
	}

	printf("[POSIX][TODO] Calling 'fstatvfs'\n");

	// errno = 0;
	int ret = 0;
	// char *new_path = checkHerculesPath(path);
	// if (new_path != NULL)
	// {
	// 	slog_debug("[POSIX]. Calling Hercules 'fstatvfs', path=%s", new_path);
	// 	buf->f_bsize = IMSS_BLKSIZE * KB;
	// 	buf->f_namemax = URI_;
	// 	slog_debug("[POSIX]. End Hercules 'fstatvfs', path=%s", new_path);
	// 	free(new_path);
	// }
	// else
	{
		// slog_debug("[POSIX]. Calling Real 'fstatvfs', path=%s", path);
		ret = real_fstatvfs(fd, buf);
		// slog_debug("[POSIX]. Ending Real 'fstatvfs', path=%s", path);
	}

	return ret;
}

int statvfs64(const char *restrict path, struct statvfs64 *restrict buf)
{
	if (!real_statvfs64)
		real_statvfs64 = dlsym(RTLD_NEXT, "statvfs64");

	if (!init)
	{
		return real_statvfs64(path, buf);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(path);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'statvfs64', path=%s", new_path);
		buf->f_bsize = IMSS_BLKSIZE * KB;
		buf->f_namemax = URI_;
		slog_debug("[POSIX]. End Hercules 'statvfs64', path=%s", new_path);
		free(new_path);
	}
	else
	{
		slog_debug("[POSIX]. Calling Real 'statvfs64', path=%s", path);
		ret = real_statvfs64(path, buf);
		slog_debug("[POSIX]. Ending Real 'statvfs64', path=%s", path);
	}

	return ret;
}

int statfs(const char *restrict path, struct statfs *restrict buf)
{
	if (!real_statfs)
		real_statfs = dlsym(RTLD_NEXT, "statfs");

	if (!init)
	{
		return real_statfs(path, buf);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(path);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'statfs', path=%s, new_path=%s", path, new_path);
		buf->f_bsize = IMSS_BLKSIZE * KB;
		buf->f_namelen = URI_;
		slog_debug("[POSIX]. Ending Hercules 'statfs', path=%s", path);
		free(new_path);
	}
	else
	{
		slog_debug("[POSIX]. Calling Real 'statfs', path=%s.", path);
		ret = real_statfs(path, buf);
		slog_debug("[POSIX]. Ending Real 'statfs', path=%s.", path);
	}
	return ret;
}

int __xstat64(int ver, const char *pathname, struct stat64 *stat_buf)
{
	if (!real__xstat64)
		real__xstat64 = dlsym(RTLD_NEXT, "__xstat64");

	if (!init)
	{
		return real__xstat64(ver, pathname, stat_buf);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules '__xstat64', pathname=%s, new_path=%s", pathname, new_path);
		// if (!strncmp(new_path, "Success", strlen("Success")))
		// {
		// 	ret = 0;
		// }
		// else
		{
			imss_refresh(new_path);
			ret = imss_getattr(new_path, (struct stat *)stat_buf);
			if (ret < 0)
			{
				errno = -ret;
				ret = -1;
			}
			free(new_path);
		}
		slog_debug("[POSIX]. Ending Hercules '__xstat64', pathname=%s, ret=%d\n", pathname, ret);
	}
	else
	{
		slog_full("[POSIX]. Calling real '__xstat64', pathname=%s.", pathname);
		ret = real__xstat64(ver, pathname, stat_buf);
		slog_full("[POSIX]. Ending real '__xstat64', pathname=%s, ret=%d", pathname, ret);
	}

	return ret;
}

char *realpath(const char *pathname, char *resolved_path)
{
	if (!real_realpath)
		real_realpath = dlsym(RTLD_NEXT, "realpath");

	if (!init)
	{
		return real_realpath(pathname, resolved_path);
		// slog_debug("[POSIX]. Calling Real 'realpath', pathname=%s.", pathname);
	}

	errno = 0;
	int ret = 0;
	char *p;
	// char *new_path = checkHerculesPath(pathname);
	// if (new_path != NULL)
	// {
	// 	slog_debug("[POSIX]. Calling Hercules 'realpath', pathname=%s, new_path=%s", pathname, new_path);
	// 	p = real_realpath(pathname, resolved_path);
	// 	slog_debug("[POSIX]. Ending Hercules 'realpath', pathname=%s, resolved_path=%s, ret=%d, errno=%d:%s\n", pathname, resolved_path, ret, errno, strerror(errno));
	// }
	// else
	{
		slog_debug("[POSIX]. Calling real 'realpath', pathname=%s", pathname);
		p = real_realpath(pathname, resolved_path);
		slog_debug("[POSIX]. Ending real 'realpath', pathname=%s, resolved_path=%s, ret=%d\n", pathname, resolved_path, ret);
	}
	return p;
}

int __open_2(const char *pathname, int flags, ...)
{
	if (!real__open_2)
	{
		real__open_2 = dlsym(RTLD_NEXT, "__open_2");
	}

	// Access additional arguments when O_CREAT flag is set.
	mode_t mode = 0;
	if (flags & O_CREAT)
	{
		va_list ap;
		va_start(ap, flags);
		mode = va_arg(ap, mode_t);
		va_end(ap);
	}

	if (!init)
	{
		if (!mode)
			return real__open_2(pathname, flags);
		else
			return real__open_2(pathname, flags, mode);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules '__open_2', new_path=%s.", new_path);

		// checkOpenFlags(pathname, flags);

		ret = generalOpen(new_path, flags, mode);

		slog_debug("[POSIX]. Ending Hercules '__open_2', new_path=%s, ret=%d, errno=%d:%s\n", new_path, ret, errno, strerror(errno));
		free(new_path);
	}
	else
	{
		slog_full("Calling real '__open_2', pathname=%s\n", pathname);
		if (!mode)
			ret = real__open_2(pathname, flags);
		else
			ret = real__open_2(pathname, flags, mode);
	}
	return ret;
}

int open64(const char *pathname, int flags, ...)
{
	if (!real_open64)
		real_open64 = dlsym(RTLD_NEXT, "open64");

	// Access additional arguments when O_CREAT flag is set.
	mode_t mode = 0;
	if (flags & O_CREAT)
	{
		va_list ap;
		va_start(ap, flags);
		mode = va_arg(ap, mode_t);
		va_end(ap);
	}

	if (!init)
	{
		if (!mode)
			return real_open64(pathname, flags);
		else
			return real_open64(pathname, flags, mode);
	}

	errno = 0;
	int ret;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'open64', pathname=%s, new_path=%s", pathname, new_path);
		// checkOpenFlags(pathname, flags);

		ret = generalOpen(new_path, flags, mode);

		slog_debug("[POSIX]. Ending Hercules 'open64', pathname=%s, fd=%d\n", pathname, ret);
		free(new_path);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'open64', pathname=%s", pathname);
		if (!mode)
			ret = real_open64(pathname, flags);
		else
			ret = real_open64(pathname, flags, mode);
		slog_full("[POSIX]. Ending real 'open64', pathname=%s, ret=%d, errno=%d:%s", pathname, ret, errno, strerror(errno));
	}
	return ret;
}

int mkstemp(char *template)
{
	if (!real_mkstemp)
		real_mkstemp = dlsym(RTLD_NEXT, "mkstemp");

	if (!init)
	{
		return real_mkstemp(template);
	}

	errno = 0;
	int ret;
	char *new_path = checkHerculesPath(template);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'mkstemp', new_path=%s, template=%s", new_path, new_path, template);
		char *new_path_by_template;
		mode_t mode = 0;
		int flags = 0;

		// Get a unique pathname.
		try_tempname_len(template);
		// Get HERCULES uri by using the unique pathname.
		new_path_by_template = checkHerculesPath(template);

		// Set mode and flags.
		mode |= S_IRUSR | S_IWUSR;
		// flags = O_EXCL | O_CREAT;
		flags = O_RDWR | O_CREAT | O_EXCL;

		// Creates the file and returns the file descriptor.
		ret = generalOpen(new_path_by_template, flags, mode);

		slog_debug("[POSIX]. Ending Hercules 'mkstemp', new_path=%s, template=%s, fd=%d, new_path_by_template=%s\n", new_path, template, ret, new_path_by_template);
		free(new_path_by_template);
		free(new_path);
	}
	else
	{
		slog_full("[POSIX]. Calling Real 'mkstemp', pathname=%s", template);
		ret = real_mkstemp(template);
		slog_full("[POSIX]. Ending Real 'mkstemp', pathname=%s", template);
	}
	return ret;
}

int flock(int fd, int operation)
{
	if (!real_flock)
		real_flock = dlsym(RTLD_NEXT, "flock");

	if (!init)
	{
		return real_flock(fd, operation);
	}

	errno = 0;
	int ret = 0;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		// TODO
		slog_debug("[POSIX][TODO] Calling Hercules 'flock', pathname=%s\n", pathname);
		// fprintf(stderr, "[POSIX]. Calling Hercules 'flock', pathname=%s\n", pathname);
		// fprintf(stderr,"[POSIX]. Ending Hercules 'flock', pathname=%s\n", pathname);
	}
	else
	{
		slog_debug("[POSIX] Calling real 'flock', fd=%d\n", fd);
		ret = real_flock(fd, operation);
	}
	return ret;
}

int fclose(FILE *fp)
{
	if (!real_fclose)
		real_fclose = dlsym(RTLD_NEXT, "fclose");

	if (!init)
	{
		return real_fclose(fp);
	}

	errno = 0;
	int ret = 0;
	int fd = fp->_fileno;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'fclose', pathname=%s, fd=%d", pathname, fd);
		ret = imss_close(pathname, fd);
		slog_debug("[POSIX]. Ending Hercules 'fclose' pathname=%s, fd=%d\n", pathname, fd);
		// fprintf(stderr, "Calling Hercules 'fclose', pathname=%s, fd=%d, ret=%d\n", pathname, fd, ret);
		map_fd_update_value(map_fd, pathname, fd, 0);
		// free(fp);
	}
	else
	{ // don't call slog here!
		// slog_debug("[POSIX]. Calling real 'fclose', fd=%d", fd); // don't call slog here!
		// fprintf(stderr, "Calling Real 'fclose', fd=%d, ret=%d\n", fd, ret);
		ret = real_fclose(fp);
	}
	return ret;
}

size_t fwrite(const void *buf, size_t size, size_t count, FILE *fp)
{
	if (!real_fwrite)
		real_fwrite = dlsym(RTLD_NEXT, "fwrite");

	if (!init)
	{
		return real_fwrite(buf, size, count, fp);
	}

	errno = 0;
	size_t ret = -1;
	char *pathname;
	int fd = fp->_fileno;
	if (pathname = map_fd_search_by_val(map_fd, fd))
	{
		size_t to_write = size * count;
		// size_t written = 0;

		// while (to_write > 0)
		// {
		// 	size_t n = to_write;
		// 	written += n;
		// 	to_write -= n;

		// }

		unsigned long offset = -1;
		slog_debug("[POSIX]. Calling Hercules 'fwrite', pathname=%s, to_write=%ld, size=%ld, count=%ld", pathname, to_write, size, count);

		// struct stat ds_stat_n;
		// imss_getattr(pathname, &ds_stat_n);
		// if (ret < 0)
		// {
		// 	errno = -ret;
		// 	ret = -1;
		// 	slog_error("[POSIX] Error Hercules 'write'	: %d:%s", errno, strerror(errno));
		// 	return ret;
		// }
		// map_fd_search(map_fd, pathname, fp->_fileno, &offset);

		// slog_debug("[POSIX]. pathname=%s, size=%lu, current_file_size=%lu, offset=%d", pathname, to_write, ds_stat_n.st_size, offset);

		// ret = imss_write(pathname, buf, to_write, offset);

		// if (ds_stat_n.st_size + to_write > ds_stat_n.st_size)
		// {
		// 	map_fd_update_value(map_fd, pathname, fp->_fileno, ds_stat_n.st_size + to_write);
		// }

		ret = generalWrite(pathname, fd, buf, to_write, offset);

		slog_debug("[POSIX]. Ending Hercules 'fwrite', pathname=%s, ret=%ld\n", pathname, ret);
	}
	else
	{
		// slog_full("[POSIX]. Calling real 'fwrite', fd=%d", fd);
		ret = real_fwrite(buf, size, count, fp);
		// slog_full("[POSIX]. Ending real 'fwrite', fd=%d, ret=%d\n", fd, ret);
	}

	return ret;
}

ssize_t readv(int fd, const struct iovec *iov, int iovcnt)
{
	if (!real_readv)
		real_readv = dlsym(RTLD_NEXT, "readv");

	if (!init)
	{
		return real_readv(fd, iov, iovcnt);
	}

	errno = 0;
	ssize_t ret = -1;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'readv', pathname=%s, ret=%ld\n", pathname, ret);
		unsigned long offset = 0;
		/* Find the total number of bytes to be written.  */
		size_t bytes = 0;
		for (int i = 0; i < iovcnt; ++i)
		{
			/* Check for ssize_t overflow.  */
			if (SSIZE_MAX - bytes < iov[i].iov_len)
			{
				errno = EINVAL;
				return -1;
			}
			bytes += iov[i].iov_len;
		}

		/* Allocate a temporary buffer to hold the data.  We should normally
		 use alloca since it's faster and does not require synchronization
		 with other threads.  But we cannot if the amount of memory
		 required is too large.  */
		char *buffer;
		// char *malloced_buffer = NULL;

		// malloced_buffer =
		buffer = (char *)malloc(bytes);
		if (buffer == NULL)
			/* XXX I don't know whether it is acceptable to try writing
			   the data in chunks.  Probably not so we just fail here.  */
			return -1;

		/* Read the data */
		ssize_t bytes_read = read(fd, buffer, bytes);
		if (bytes_read < 0)
			return -1;

		/* Copy the data from BUFFER into the memory specified by VECTOR.  */
		bytes = bytes_read;
		for (int i = 0; i < iovcnt; ++i)
		{
			size_t copy = MIN(iov[i].iov_len, bytes);
			(void)memcpy((void *)iov[i].iov_base, (void *)buffer, copy);
			buffer += copy;
			bytes -= copy;
			if (bytes == 0)
				break;
		}

		ret = bytes_read;

		slog_debug("[POSIX]. Ending Hercules 'readv', pathname=%s, ret=%ld\n", pathname, ret);
	}
	else
	{
		ret = real_readv(fd, iov, iovcnt);
		slog_full("[POSIX]. Ending real 'readv', fd=%d", fd);
	}

	return ret;
}

ssize_t writev(int fd, const struct iovec *iov, int iovcnt)
{
	if (!real_writev)
		real_writev = dlsym(RTLD_NEXT, "writev");

	if (!init)
	{
		return real_writev(fd, iov, iovcnt);
	}

	errno = 0;
	size_t ret = -1;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		unsigned long offset = -1;
		/* Find the total number of bytes to be written.  */
		size_t bytes = 0;
		for (int i = 0; i < iovcnt; ++i)
		{
			/* Check for ssize_t overflow.  */
			if (SSIZE_MAX - bytes < iov[i].iov_len)
			{
				errno = EINVAL;
				return -1;
			}
			bytes += iov[i].iov_len;
		}

		/* Allocate a temporary buffer to hold the data.  We should normally
		 use alloca since it's faster and does not require synchronization
		 with other threads.  But we cannot if the amount of memory
		 required is too large.  */
		char *buffer;
		// char *malloced_buffer = NULL;

		// malloced_buffer =
		buffer = (char *)malloc(bytes);
		if (buffer == NULL)
			/* XXX I don't know whether it is acceptable to try writing
			   the data in chunks.  Probably not so we just fail here.  */
			return -1;

		/* Copy the data into BUFFER.  */
		size_t to_copy = bytes;
		char *bp = buffer;
		for (int i = 0; i < iovcnt; ++i)
		{
			size_t copy = MIN(iov[i].iov_len, to_copy);
			bp = __mempcpy((void *)bp, (void *)iov[i].iov_base, copy);
			to_copy -= copy;
			if (to_copy == 0)
				break;
		}

		slog_debug("[POSIX]. Calling Hercules 'writev', pathname=%s", pathname);
		// map_fd_search(map_fd, pathname, fd, &p);
		// ret = imss_write(pathname, buffer, bytes, p);
		ret = generalWrite(pathname, fd, buffer, bytes, offset);
		slog_debug("[POSIX]. Ending Hercules 'writev', pathname=%s, ret=%ld, errno=%d:%s\n", pathname, ret, errno, strerror(errno));
	}
	else
	{
		ret = real_writev(fd, iov, iovcnt);
		slog_full("[POSIX]. Ending real 'writev', fd=%d", fd);
	}

	return ret;
}

ssize_t pwritev(int fd, const struct iovec *iov, int iovcnt, off_t offset)
{
	if (!real_pwritev)
		real_pwritev = dlsym(RTLD_NEXT, "pwritev");

	if (!init)
	{
		return real_pwritev(fd, iov, iovcnt, offset);
	}

	errno = 0;
	size_t ret = -1;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'pwritev', pathname=%s, fd=%d, offset=%d", pathname, fd, offset);
		// unsigned long p = 0;
		/* Find the total number of bytes to be written.  */
		size_t bytes = 0;
		for (int i = 0; i < iovcnt; ++i)
		{
			/* Check for ssize_t overflow.  */
			if (SSIZE_MAX - bytes < iov[i].iov_len)
			{
				errno = EINVAL;
				return -1;
			}
			bytes += iov[i].iov_len;
		}

		/* Allocate a temporary buffer to hold the data.  We should normally
		 use alloca since it's faster and does not require synchronization
		 with other threads.  But we cannot if the amount of memory
		 required is too large.  */
		char *buffer;
		// char *malloced_buffer = NULL;

		// malloced_buffer =
		buffer = (char *)malloc(bytes);
		if (buffer == NULL)
			/* XXX I don't know whether it is acceptable to try writing
			   the data in chunks.  Probably not so we just fail here.  */
			return -1;

		/* Copy the data into BUFFER.  */
		size_t to_copy = bytes;
		char *bp = buffer;
		for (int i = 0; i < iovcnt; ++i)
		{
			size_t copy = MIN(iov[i].iov_len, to_copy);
			bp = __mempcpy((void *)bp, (void *)iov[i].iov_base, copy);
			to_copy -= copy;
			if (to_copy == 0)
				break;
		}

		// map_fd_search(map_fd, pathname, fd, &p);
		// ret = imss_write(pathname, buffer, bytes, offset);
		ret = generalWrite(pathname, fd, buffer, bytes, offset);
		slog_debug("[POSIX]. Ending Hercules 'pwritev', pathname=%s, fd=%d, ret=%ld,  errno=%d:%s\n", pathname, fd, ret, errno, strerror(errno));
	}
	else
	{
		ret = real_pwritev(fd, iov, iovcnt, offset);
		slog_debug("[POSIX]. Ending real 'pwritev', fd=%d, errno=%d:%s", fd, errno, strerror(errno))
	}
	return ret;
}

ssize_t pwrite64(int fd, const void *buf, size_t count, off64_t offset)
{

	if (!real_pwrite64)
		real_pwrite64 = dlsym(RTLD_NEXT, "pwrite64");

	if (!init)
	{
		return real_pwrite64(fd, buf, count, offset);
	}

	errno = 0;
	size_t ret = -1;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'pwrite64', pathname=%s, fd=%d, offset=%d", pathname, fd, offset);
		// ret = -1;
		// errno = -2;
		ret = generalWrite(pathname, fd, buf, count, offset);

		slog_debug("[POSIX]. Ending Hercules 'pwrite64', pathname=%s, fd=%d, ret=%ld,  errno=%d:%s\n", pathname, fd, ret, errno, strerror(errno));
	}
	else
	{
		ret = real_pwrite64(fd, buf, count, offset);
		slog_debug("[POSIX]. Ending real ' pwrite64', fd=%d, errno=%d:%s", fd, errno, strerror(errno))
	}
	return ret;
}

// int poll(struct pollfd *fds, nfds_t nfds, int timeout)
// {
// 	if (!real_poll)
// 		real_poll = dlsym(RTLD_NEXT, "poll");

// 	if (!init)
// 	{
// 		return real_poll(fds, nfds, timeout);
// 	}

// 	errno = 0;
// 	size_t ret = -1;
// 	int fd = fds->fd;
// 	char *pathname = map_fd_search_by_val(map_fd, fd);
// 	if (pathname != NULL)
// 	{
// 		slog_debug("[POSIX]. Calling Hercules 'poll', pathname=%s, fd=%d", pathname, fds->fd);
// 		ret = 0;
// 		// errno = ETIME;
// 		slog_debug("[POSIX]. Ending Hercules 'poll', pathname=%s, fd=%d, ret=%ld\n", pathname, fds->fd, ret);
// 	}
// 	else
// 	{
// 		// slog_debug("[POSIX]. Calling real 'poll', fd=%d, errno=%d:%s", fds->fd, errno, strerror(errno))
// 		ret = real_poll(fds, nfds, timeout);
// 		slog_debug("[POSIX]. Ending real 'poll', fd=%d", fds->fd)
// 	}
// 	return ret;
// }

// int ppoll(struct pollfd *fds, nfds_t nfds, const struct timespec tmo_p, const sigset_t sigmask)
// {
// 	if (!real_ppoll)
// 		real_ppoll = dlsym(RTLD_NEXT, "ppoll");

// 	if (!init)
// 	{
// 		return real_ppoll(fds, nfds, tmo_p, sigmask);
// 	}

// 	errno = 0;
// 	size_t ret = -1;
// 	char *pathname = map_fd_search_by_val(map_fd, fds->fd);
// 	if (pathname != NULL)
// 	{
// 		slog_debug("[POSIX]. Calling Hercules 'ppoll', pathname=%s, fd=%d", pathname, fds->fd);
// 		ret = 0;
// 		// errno = ETIME;
// 		slog_debug("[POSIX]. Ending Hercules 'ppoll', pathname=%s, fd=%d, ret=%ld,  errno=%d:%s\n", pathname, fds->fd, ret, errno, strerror(errno));
// 	}
// 	else
// 	{
// 		// slog_debug("[POSIX]. Calling real 'ppoll', fd=%d, errno=%d:%s", fds->fd, errno, strerror(errno));
// 		ret = real_ppoll(fds, nfds, tmo_p, sigmask);
// 		slog_debug("[POSIX]. Ending real 'ppoll', fd=%d", fds->fd);
// 	}
// 	return ret;
// }


void clearerr(FILE *fp)
{
	if (!real_clearerr)
		real_clearerr = dlsym(RTLD_NEXT, "clearerr");

	if (!init)
	{
		return real_clearerr(fp);
	}

	errno = 0;
	int fd = fp->_fileno;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'clearerr', pathname=%s", pathname);
		fp->_flags &= ~(_IO_ERR_SEEN|_IO_EOF_SEEN);
		slog_debug("[POSIX]. End Hercules 'clearerr', pathname=%s\n", pathname);
	}
	else
	{
		real_clearerr(fp);
	}
}


int ferror(FILE *fp)
{
	if (!real_ferror)
		real_ferror = dlsym(RTLD_NEXT, "ferror");

	if (!init)
	{
		return real_ferror(fp);
	}

	errno = 0;
	int ret = 0;
	int fd = fp->_fileno;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'ferror', pathname=%s", pathname);
		// fprintf(stderr, "[POSIX][TODO]. Calling Hercules 'ferror', pathname=%s\n", pathname);
		ret = ((fp->_flags & _IO_ERR_SEEN) != 0);
		// fprintf(stderr, "[POSIX][TODO]. Ending Hercules 'ferror', pathname=%s, ret=%d\n", pathname, ret);
		slog_debug("[POSIX]. Ending Hercules 'ferror', pathname=%s, ret=%d", pathname, ret);
	}
	else
	{
		ret = real_ferror(fp);
	}

	return ret;
}

int feof(FILE *fp)
{
	if (!real_feof)
		real_feof = dlsym(RTLD_NEXT, "feof");

	if (!init)
	{
		return real_feof(fp);
	}

	errno = 0;
	int ret = 0;
	int fd = fp->_fileno;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'feof', pathname=%s", pathname);
		// if ((fp->_flags & _IO_EOF_SEEN) != 0)
		// ret = _IO_feof_unlocked(fp);
		if((fp->_flags & _IO_EOF_SEEN) != 0){
			ret = 1;
		} else {
			ret = 0;
		}

		slog_debug("[POSIX]. End Hercules 'feof', pathname=%s, ret=%d\n", pathname, ret);
	}
	else
	{
		ret = real_feof(fp);
	}

	return ret;
}

long int ftell(FILE *fp)
{
	if (!real_ftell)
		real_ftell = dlsym(RTLD_NEXT, "ftell");

	if (!init)
	{
		return real_ftell(fp);
	}

	errno = 0;
	long int ret = -1;
	int fd = fp->_fileno;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		unsigned long offset = 0;
		slog_debug("[POSIX]. Calling Hercules 'ftell', pathname=%s, fd=%d, errno=%d:%s", pathname, fd, errno, strerror(errno));

		ret = map_fd_search(map_fd, pathname, fd, &offset);
		slog_debug("[POSIX]. ret=%ld, offset=%ld", ret, offset);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_debug("[POSIX]. Error in 'ftell', ret=%ld, errno=%d:%s", ret, errno, strerror(errno));
			return ret;
		}
		else
		{
			ret = offset;
		}
		// map_fd_update_value(map_fd, pathname, fd, ret);
		slog_debug("[POSIX]. Ending Hercules 'ftell', ret=%ld\n", ret);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'ftell', fd=%d, errno=%d:%s", fd, errno, strerror(errno));
		ret = real_ftell(fp);
	}

	return ret;
}

void rewind(FILE *stream)
{
	if (!real_rewind)
		real_rewind = dlsym(RTLD_NEXT, "rewind");

	if (!init)
	{
		return real_rewind(stream);
	}

	errno = 0;
	int fd = stream->_fileno;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'rewind', pathname=%s, fd=%d, errno=%d:%s", pathname, fd, errno, strerror(errno));

		fseek(stream, 0L, SEEK_SET);

		slog_debug("[POSIX]. Ending Hercules 'rewind'");
	}
	else
	{
		return real_rewind(stream);
	}
}

FILE *fopen(const char *restrict pathname, const char *restrict mode)
{
	if (!real_fopen)
		real_fopen = dlsym(RTLD_NEXT, "fopen");

	if (!init)
	{
		return real_fopen(pathname, mode);
	}

	errno = 0;
	FILE *file = NULL;
	int ret = 0;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		uint64_t ret_ds;
		unsigned long offset = 0;
		// mode_t new_mode = 0;
		int flags = 0, oflags = 0;

		if ((flags = __sflags(mode, &oflags)) == 0)
			return (NULL);

		// To interpret the mode recived:
		// Opening a file in append mode (a as the first character of mode)
		// causes all subsequent write operations to this stream to occur at
		// end-of-file, as if preceded by the call:
		//   fseek(stream, 0, SEEK_END);

		// if (strstr(mode, "w"))
		// {
		// 	new_mode |= S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH;
		// 	flags = O_WRONLY | O_CREAT | O_TRUNC;
		// }

		// slog_debug("[POSIX] Calling Hercules 'fopen', pathname=%s, mode=%s", new_path, mode);

		// if (strstr(mode, "w"))
		// 	flags = O_CREAT;

		ret = generalOpen(new_path, oflags, DEFFILEMODE);
		// ret = generalOpen(new_path, flags, new_mode);

		// slog_debug("[POSIX][fopen] File descriptor=%d", ret);

		if (ret < 0)
		{
			return NULL;
		}

		file = malloc(sizeof(struct _IO_FILE));

		if (file == NULL)
		{
			// slog_error("File struct %s was not created\n", pathname);
			fprintf(stderr, "Error: File struct %s was not created\n", pathname);
			return NULL;
		}

		file->_fileno = ret; // file descriptor.
		// file->_flags2 = IMSS_BLKSIZE * KB;
		file->_offset = offset;
		file->_flags = flags;
		// file->_flags2 = flags;
		// file->_mode = mode;

		if (oflags & O_APPEND)
		{
			ret = fseek(file, 0, SEEK_END);
			if (ret < 0)
			{
				// slog_warn("Error calling fseek inside fopen");
			}
		}

		// fprintf(stderr, "[POSIX] file->_fileno=%d, file->_offset=%ld\n", file->_fileno, file->_offset);
		// if (file != NULL)
		// 	fprintf(stderr, "[POSIX] Calling Hercules 'fopen', pathname=%s, mode=%s, file->_fileno=%d, file->_offset=%ld\n", new_path, mode, file->_fileno, file->_offset);
		// else
		// fprintf(stderr, "Calling Hercules 'fopen', file NULL\n");

		// slog_debug("[POSIX] Ending Hercules 'fopen', new_path=%s, ret=%d, errno=%d:%s\n", new_path, ret, errno, strerror(errno));
		free(new_path);
	}
	else /* Do not try to use slog_ here! This function uses 'fopen' internally. */
	{
		// if (strncmp(pathname + strlen(pathname) - 3, "log", strlen("log")))
		// {
		// 	fprintf(stderr, "Calling Real 'fopen', pathname=%s\n", pathname);
		// }

		file = real_fopen(pathname, mode);

		// if (strncmp(pathname + strlen(pathname) - 3, "log", strlen("log")))
		// {
		// 	if (file != NULL)
		// 		fprintf(stderr, "Calling Real 'fopen', pathname=%s, file->_fileno=%d, file->_offset=%ld\n", pathname, file->_fileno, file->_offset);
		// 	else
		// 		fprintf(stderr, "Calling Real 'fopen', pathname=%s, file NULL\n", pathname);
		// }
	}

	return file;
}

FILE *fdopen(int fildes, const char *mode)
{
	if (!real_fdopen)
		real_fdopen = dlsym(RTLD_NEXT, "fdopen");

	if (!init)
	{
		return real_fdopen(fildes, mode);
	}

	errno = 0;
	FILE *file = NULL;
	int ret = 0;
	// char *new_path = checkHerculesPath(pathname);
	// if (new_path != NULL)
	char *pathname = map_fd_search_by_val(map_fd, fildes);
	if (pathname != NULL) // recibe un fd que debe estar el mapa si el archivo pertenece a Hercules, se hace el generalOpen y se rellena la estructura.
	{
		uint64_t ret_ds;
		unsigned long offset = 0;
		mode_t new_mode = 0;
		int flags = 0;

		// To interpret the mode recived:
		// Opening a file in append mode (a as the first character of mode)
		// causes all subsequent write operations to this stream to occur at
		// end-of-file, as if preceded by the call:
		//   fseek(stream, 0, SEEK_END);
		if (strstr(mode, "w"))
		{
			new_mode |= S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH;
			flags = O_WRONLY | O_CREAT | O_TRUNC;
		}

		slog_debug("[POSIX] Calling Hercules 'fdopen', pathname=%s, mode=%s, new_mode=%o, fildes=%d", pathname, mode, new_mode, fildes);

		// if (strstr(mode, "w"))
		// 	flags = O_CREAT;

		ret = generalOpen(pathname, flags, new_mode);

		slog_debug("[POSIX][fdopen] File descriptor=%d", ret);

		if (ret < 0)
		{
			return NULL;
		}

		file = malloc(sizeof(struct _IO_FILE));

		file->_fileno = ret;
		file->_flags2 = IMSS_BLKSIZE * KB;
		file->_offset = offset;
		// file->_mode = mode;

		if (file == NULL)
		{
			slog_debug("File %s was not found\n", pathname);
		}

		slog_debug("[POSIX] Ending Hercules 'fdopen', pathname=%s, fildes=%d\n", pathname, fildes);
	}
	else
	{
		slog_full("[POSIX] Calling real 'fdopen', fd=%d", fildes);
		file = real_fdopen(fildes, mode);
	}

	return file;
}

ssize_t generalWrite(const char *pathname, int fd, const void *buf, size_t size, size_t offset)
{
	// pthread_mutex_lock(&system_lock);
	ssize_t ret = 0;
	struct stat ds_stat_n;
	int update_offset = 0;
	if (offset == -1)
	{
		update_offset = 1;
		map_fd_search(map_fd, pathname, fd, &offset);

		imss_getattr(pathname, &ds_stat_n);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_error("[POSIX] Error Hercules 'write'	: %d:%s", errno, strerror(errno));
			// pthread_mutex_unlock(&system_lock);
			return ret;
		}
	}

	slog_debug("[POSIX]. pathname=%s, size to write=%lu, offset=%lu", pathname, size, offset);

	ret = imss_write(pathname, buf, size, offset);

	if (update_offset)
	{
		if (ds_stat_n.st_size + size > ds_stat_n.st_size)
		{
			slog_debug("pathname=%s, updating , file size=%ld, size=%ld", pathname, ds_stat_n.st_size, size);
			map_fd_update_value(map_fd, pathname, fd, ds_stat_n.st_size + size);
		}
	}

	return ret;
}

int generalOpen(char *new_path, int flags, mode_t mode)
{

	// pthread_mutex_lock(&system_lock);

	int ret = 0;
	uint64_t ret_ds = 0;
	unsigned long p = 0;
	// Search for the path "new_path" on the map "map_fd".
	slog_debug("[POSIX] Searching for the %s on the map", new_path);
	int exist = map_fd_search_by_pathname(map_fd, new_path, &ret, &p);
	if (exist == -1) // if the "new_path" was not find on the local map:
	{
		int create_flag = (flags & O_CREAT);
		slog_debug("[POSIX] new_path:%s, exist: %d, create_flag: %d", new_path, exist, create_flag);
		if (create_flag == O_CREAT) // if the file does not exist, then we create it.
		{
			slog_debug("[POSIX] New file %s, ret=%d", new_path, ret);
			int err_create = imss_create(new_path, mode, &ret_ds, 1);
			slog_debug("[POSIX] imss_create(%s, %d, %ld), err_create: %d", new_path, mode, ret_ds, err_create);
			if (err_create == -EEXIST)
			{
				slog_debug("[POSIX] 1 - Dataset already exists, imss_open(%s, %ld)", new_path, ret_ds);
				ret = imss_open(new_path, &ret_ds);
				// errno = EEXIST;
				errno = -ret;
				slog_debug("[POSIX] 1 - imss_open(%s, %ld), ret=%d", new_path, ret_ds, ret);
				ret = 0;
			}
		}
		else // if O_CREAT flag was not set, the file must exists.
		{
			slog_debug("[POSIX] File must exists - imss_open(%s, %ld)", new_path, ret_ds);
			ret = imss_open(new_path, &ret_ds);
			slog_debug("[POSIX] 2 - ret_ds=%d, ret=%d, new_path=%s", ret_ds, ret, new_path);

			if (ret < 0)
			{
				errno = -ret;
				ret = -1;
			}

			// If we get a "ret_ds" equal to "-2", we are in the case of symbolic link pointing to a file stored in the system.
			if (ret_ds == -2)
			{
				slog_debug("[POSIX] Calling real_open(%s)", new_path);
				// Calling the real open.
				if (!mode)
					ret = real_open(new_path, flags);
				else
					ret = real_open(new_path, flags, mode);
				// stores the file descriptor "ret" into the map "map_fd".
				map_fd_put(map_fd, new_path, ret, p); // TO CHECK!
			}
		}
		// if (ret == 0)
		// {
		// 	slog_debug("[POSIX] Puting fd %d into map", ret);
		// 	map_fd_put(map_fd, new_path, ret, p);
		// }
		// else
		if (ret > -1)
		{
			// errno = 0;
			ret = real_open("/dev/null", 0); // Get a file descriptor
			// stores the file descriptor "ret" into the map "map_fd".
			slog_debug("[POSIX] Puting fd %d into map", ret);
			map_fd_put(map_fd, new_path, ret, p);
		}
	}
	else
	{
		slog_debug("[POSIX]. exist=%d, O_TRUNC=%d, fd=%d", exist, flags & O_TRUNC, ret);
		if (flags & O_TRUNC)
		{
			map_fd_update_value(map_fd, new_path, ret, 0);
			int ret_aux = 0;
			struct stat stats;
			ret_aux = imss_getattr(new_path, &stats);
			if (ret_aux < 0)
			{
				errno = -ret_aux;
				ret = -1;
				slog_error("[POSIX] Error Hercules 'open', errno=%d:%s", errno, strerror(errno));
				// free(new_path);
				// pthread_mutex_unlock(&system_lock);
				return ret;
			}
			stats.st_size = 0;
			// stats.st_blocks = 0;
			map_update(map, new_path, ret, stats);
		}
	}
	// pthread_mutex_unlock(&system_lock);
	return ret;
}

int open(const char *pathname, int flags, ...)
{
	if (!real_open)
		real_open = dlsym(RTLD_NEXT, "open");

	// Access additional arguments when O_CREAT flag is set.
	mode_t mode = 0;
	if (flags & O_CREAT)
	{
		va_list ap;
		va_start(ap, flags);
		mode = va_arg(ap, mode_t);
		va_end(ap);
	}

	if (!init)
	{
		if (!mode)
			return real_open(pathname, flags);
		else
			return real_open(pathname, flags, mode);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_debug("[POSIX] Calling Hercules 'open' flags=%d, mode=%o, pathname=%s, new_path=%s", flags, mode, pathname, new_path);
		checkOpenFlags(pathname, flags);

		ret = generalOpen(new_path, flags, mode);

		slog_debug("[POSIX] Ending Hercules 'open', mode=%o, ret=%d\n", mode, ret);
		free(new_path);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'open', flags=%d, mode=%o, pathname=%s", flags, mode, pathname);
		// checkOpenFlags(pathname, flags);

		if (!mode)
			ret = real_open(pathname, flags);
		else
			ret = real_open(pathname, flags, mode);
		slog_full("[POSIX]. Ending real 'open', mode=%o, pathname=%s, ret=%d, errno=%d:%s", mode, pathname, ret, errno, strerror(errno));
	}

	return ret;
}

/**
 * Verify if @pathname is absolute or relative.
 * @return
 * 1 = path is absolute.
 * 0 = path is relative.
 * -1 = error.
 */
int IsAbsolutePath(const char *pathname)
{
	int mount_point_len = 0;
	const char *file_name_without_prefix;
	int ret = -1;

	mount_point_len = strlen(MOUNT_POINT);

	file_name_without_prefix = pathname + mount_point_len;

	if (file_name_without_prefix[0] == '/') // path is absolute.
	{
		ret = 1;
	}
	else
	{ // path is relative.
		ret = 0;
	}
	return ret;
}

int openat(int dir_fd, const char *pathname, int flags, ...)
{

	if (!real_openat)
		real_openat = dlsym(RTLD_NEXT, "openat");

	int mode = 0;
	if (flags & O_CREAT)
	{
		va_list ap;
		va_start(ap, flags);
		mode = va_arg(ap, unsigned);
		va_end(ap);
	}

	if (!init)
	{
		if (!mode)
			return real_openat(dir_fd, pathname, flags);
		else
			return real_openat(dir_fd, pathname, flags, mode);
	}

	errno = 0;
	int ret = 0;
	char *pathname_dir, *new_path = NULL;
	if (dir_fd == AT_FDCWD)
	{
		// pathname_dir = getenv("PWD");
		// new_path = checkHerculesPath(pathname_dir);
		// pathname_dir = NULL;
		new_path = checkHerculesPath(pathname);
		pathname_dir = NULL;
	}
	else
	{
		pathname_dir = map_fd_search_by_val(map_fd, dir_fd);
	}

	if (pathname_dir != NULL || new_path != NULL)
	{
		slog_debug("[POSIX] Calling Hercules 'openat' flags=%d, mode=%o, dir_fd=%d, pathname_dir=%s, pathname=%s, errno=%d:%s", flags, mode, dir_fd, pathname_dir, pathname, errno, strerror(errno));
		// checkOpenFlags(pathname, flags);

		int is_absolute_path = IsAbsolutePath(pathname);

		// If pathname is absolute, then dir_fd is ignored.
		if (is_absolute_path == 1)
		{
			char *new_path = checkHerculesPath(pathname);
			slog_debug("[POSIX] is absolute, 'openat', new_path=%s", new_path);
			if (flags & O_DIRECTORY)
			{
				DIR *dirp = opendir(pathname);
				if (dirp != NULL)
				{
					ret = dirfd(dirp);
				}
				else
				{
					ret = -1;
				}
			}
			else
			{
				ret = generalOpen(new_path, flags, mode);
			}
			free(new_path);
		}
		else if (is_absolute_path == 0) // pathname is relative.
		{
			if (dir_fd == AT_FDCWD) // dir_fd is the special value AT_FDCWD.
			{						// TO CHECK!
				char *new_path = checkHerculesPath(pathname);
				slog_debug("[POSIX] is relative, current directory, 'openat', new_path=%s", new_path);
				// pathname is interpreted relative to the current working directory of the calling process (like real_open).
				ret = generalOpen(new_path, flags, mode);
				free(new_path);
			}
			else
			{
				// // get the pathname of the directory pointed by dir_fd if it is storage in the local map "map_fd".
				// char *pathname_dir = map_fd_search_by_val(map_fd, dir_fd);
				// if (pathname_dir == NULL)
				// { // if dir_fd is not storage locally
				// 	// search dir_fd on the metadata server.
				// 	// fprintf(stderr,"");
				// 	slog_error("[POSIX] dir_fd=%d could not be resolved.");
				// 	return -1;
				// }

				char absolute_pathname[MAX_PATH];
				char *dirr = pathname_dir + strlen("imss://");
				sprintf(absolute_pathname, "%s/%s/%s", MOUNT_POINT, dirr, pathname);

				char *new_path = checkHerculesPath(absolute_pathname);
				slog_debug("[POSIX] is relative, 'openat', new_path=%s", new_path);
				ret = generalOpen(new_path, flags, mode);
				free(new_path);
			}
		}

		slog_debug("[POSIX] Ending Hercules 'openat', mode=%o, ret=%d, errno=%d:%s\n", mode, ret, errno, strerror(errno));
		// free(new_path);
	}
	else
	{
		slog_full("[POSIX] Calling real 'openat', flags=%d, mode=%o, dir_fd=%d, pathname=%s", flags, mode, dir_fd, pathname);
		if (!mode)
		{
			ret = real_openat(dir_fd, pathname, flags);
		}
		else
		{
			ret = real_openat(dir_fd, pathname, flags, mode);
		}
	}

	return ret;
}

int mkdir(const char *path, mode_t mode)
{
	if (!real_mkdir)
		real_mkdir = dlsym(RTLD_NEXT, "mkdir");

	if (!init)
	{
		return real_mkdir(path, mode);
	}

	errno = 0;
	int ret;
	char *new_path = checkHerculesPath(path);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'mkdir', path=%s, new_path=%s", path, new_path);

		// char *new_path;
		// new_path = convert_path(path, MOUNT_POINT);
		ret = imss_mkdir(new_path, mode);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_debug("[POSIX]. Error in 'mkdir', ret=%d, errno=%d:%s", ret, errno, strerror(errno));
		}
		slog_debug("[POSIX]. Ending hercules 'mkdir', path=%s, new_path=%s, ret=%d\n", path, new_path, ret);
		free(new_path);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'mkdir', path=%s.", path);
		ret = real_mkdir(path, mode);
		slog_full("[POSIX]. Ending real 'mkdir', path=%s.", path);
	}
	return ret;
}

int symlink(const char *name1, const char *name2)
{
	// TODO.
	if (!real_symlink)
		real_symlink = dlsym(RTLD_NEXT, "symlink");

	fprintf(stderr, "Calling symlink \n ******");

	return real_symlink(name1, name2);
}

int symlinkat(const char *name1, int fd, const char *name2)
{

	if (!real_symlinkat)
		real_symlinkat = dlsym(RTLD_NEXT, "symlinkat");

	if (!init)
	{
		return real_symlinkat(name1, fd, name2);
	}

	// Fix: to check fd.

	errno = 0;
	int ret;
	char *new_path_1 = checkHerculesPath(name1);
	char *new_path_2 = checkHerculesPath(name2);
	if (new_path_1 != NULL || new_path_2 != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'symlinkat', name1=%s, name2=%s.", name1, name2);

		if (new_path_1 != NULL && new_path_2 != NULL)
		{
			slog_debug("[POSIX]. Both new_path_1=%s, new_path_2=%s", new_path_1, new_path_2);
			// new_path_1 = convert_path(name1, MOUNT_POINT);
			// new_path_2 = convert_path(name2, MOUNT_POINT);
			ret = imss_symlinkat(new_path_1, new_path_2, 0);
			free(new_path_1);
			free(new_path_2);
		}

		// if (!strncmp(name2, MOUNT_POINT, strlen(MOUNT_POINT)))
		if (new_path_1 == NULL && new_path_2 != NULL)
		{
			slog_debug("[POSIX]. Only second new_path_2=%s", new_path_2);
			// new_path_1 = name1;
			strcpy(new_path_1, name1);
			// new_path_2 = convert_path(name2, MOUNT_POINT);
			ret = imss_symlinkat(new_path_1, new_path_2, 1);
			// free(new_path_1) ?
			free(new_path_2);
		}
		slog_debug("[POSIX]. Ending Hercules 'symlinkat', name1=%s, name2=%s.", name1, name2);
	}
	else
	{
		slog_debug("[POSIX]. Calling Real 'symlinkat', name1=%s, name2=%s.", name1, name2);
		ret = real_symlinkat(name1, fd, name2);
		slog_debug("[POSIX]. Ending Real 'symlinkat', name1=%s, name2=%s.", name1, name2);
	}
	return ret;
}

off_t lseek(int fd, off_t offset, int whence)
{
	if (!real_lseek)
		real_lseek = dlsym(RTLD_NEXT, "lseek");

	if (!init)
	{
		return real_lseek(fd, offset, whence);
	}

	errno = 0;
	off_t ret = -1;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		unsigned long p = 0;
		slog_debug("[POSIX]. Calling Hercules 'lseek', pathname=%s, fd=%d, whence=%d, offset=%ld, errno=%d:%s", pathname, fd, whence, offset, errno, strerror(errno));
		// fprintf(stderr,"[POSIX]. Calling Hercules 'lseek', pathname=%s, fd=%d, whence=%d, offset=%ld, errno=%d:%s\n", pathname, fd, whence, offset, errno, strerror(errno));

		// slog_info("[POSIX]. whence=%d, offset=%ld", whence, offset);
		if (whence == SEEK_SET)
		{
			slog_debug("[POSIX]. SEEK_SET, offset=%ld", offset);
			ret = offset;
			map_fd_update_value(map_fd, pathname, fd, ret);
		}
		else if (whence == SEEK_CUR)
		{
			ret = map_fd_search(map_fd, pathname, fd, &p);
			slog_debug("[POSIX]. SEEK_CUR=%ld, ret=%ld, p=%ld", offset, ret, p);
			if (ret < 0)
			{
				errno = -ret;
				ret = -1;
				slog_debug("[POSIX]. Error in 'lseek', ret=%ld, errno=%d:%s", ret, errno, strerror(errno));
				return ret;
			}
			ret = p + offset;
			slog_debug("[POSIX]. SEEK_CUR=%ld, p+offset=%ld", offset, ret);
			map_fd_update_value(map_fd, pathname, fd, ret);
		}
		else if (whence == SEEK_END)
		{
			slog_debug("SEEK_END, offset=%ld", offset);
			struct stat ds_stat_n;
			ret = imss_getattr(pathname, &ds_stat_n);
			if (ret < 0)
			{
				errno = -ret;
				ret = -1;
				slog_debug("[POSIX]. Error in 'lseek', %s, ret=%ld, errno=%d:%s", pathname, ret, errno, strerror(errno));
				return ret;
			}
			ret = offset + ds_stat_n.st_size;
			map_fd_update_value(map_fd, pathname, fd, ret);
		}

		slog_debug("[POSIX]. Ending Hercules 'lseek', ret=%ld, errno=%d:%s\n", ret, errno, strerror(errno));
	}
	else
	{
		slog_full("[POSIX]. Calling Real 'lseek', fd=%d, whence=%d, offset=%ld", fd, whence, offset);
		// fprintf(stderr,"[POSIX]. Calling real 'lseek', fd=%d, whence=%d, offset=%ld, errno=%d:%s\n", fd, whence, offset, errno, strerror(errno));
		ret = real_lseek(fd, offset, whence);
		slog_full("[POSIX]. Ending Real 'lseek', fd=%d, whence=%d, offset=%ld", fd, whence, offset);
		// fprintf(stderr,"[POSIX]. Ending real 'lseek', fd=%d, whence=%d, offset=%ld, errno=%d:%s\n", fd, whence, offset, errno, strerror(errno));
	}
	return ret;
}

off64_t lseek64(int fd, off64_t offset, int whence)
{
	if (!real_lseek64)
		real_lseek64 = dlsym(RTLD_NEXT, "lseek64");

	if (!init)
	{
		return real_lseek64(fd, offset, whence);
	}

	errno = 0;
	off64_t ret = -1;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		unsigned long p = 0;
		slog_debug("[POSIX]. Calling Hercules 'lseek64', pathname=%s, fd=%d", pathname, fd);
		slog_info("[POSIX]. whence=%d, offset=%ld", whence, offset);
		if (whence == SEEK_SET)
		{
			slog_debug("[POSIX]. SEEK_SET=%ld", offset);
			ret = offset;
			map_fd_update_value(map_fd, pathname, fd, ret);
		}
		else if (whence == SEEK_CUR)
		{
			ret = map_fd_search(map_fd, pathname, fd, &p);
			slog_debug("[POSIX]. SEEK_CUR=%ld, ret=%ld, p=%ld", offset, ret, p);
			if (ret < 0)
			{
				errno = -ret;
				ret = -1;
				slog_debug("[POSIX]. Error in 'lseek64', ret=%ld, errno=%d:%s", ret, errno, strerror(errno));
				return ret;
			}
			ret = p + offset;
			slog_debug("[POSIX]. SEEK_CUR=%ld, p+offset=%ld", offset, ret);
			map_fd_update_value(map_fd, pathname, fd, ret);
		}
		else if (whence == SEEK_END)
		{
			slog_debug("SEEK_END=%ld", offset);
			struct stat ds_stat_n;
			ret = imss_getattr(pathname, &ds_stat_n);
			if (ret < 0)
			{
				errno = -ret;
				ret = -1;
				slog_debug("[POSIX]. Error in 'lseek64', ret=%ld, errno=%d:%s", ret, errno, strerror(errno));
				return ret;
			}
			ret = offset + ds_stat_n.st_size;
			map_fd_update_value(map_fd, pathname, fd, ret);
		}

		slog_debug("[POSIX]. Ending Hercules 'lseek64', ret=%ld\n", ret);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'lseek64', fd=%d", fd);
		ret = real_lseek64(fd, offset, whence);
	}
	return ret;
}

int fseek(FILE *stream, long int offset, int whence)
{
	if (!real_fseek)
		real_fseek = dlsym(RTLD_NEXT, "fseek");

	if (!init)
	{
		return real_fseek(stream, offset, whence);
	}

	errno = 0;
	off_t ret = -1;
	int fd = stream->_fileno;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		unsigned long p = 0;
		slog_debug("[POSIX]. Calling Hercules 'fseek', pathname=%s, fd=%d", pathname, fd);
		if (whence == SEEK_SET)
		{
			slog_debug("[POSIX]. SEEK_SET=%ld", offset);
			ret = offset;
			map_fd_update_value(map_fd, pathname, fd, ret);
		}
		else if (whence == SEEK_CUR)
		{
			ret = map_fd_search(map_fd, pathname, fd, &p);
			slog_debug("[POSIX]. SEEK_CUR=%ld, ret=%ld, p=%ld", offset, ret, p);
			if (ret < 0)
			{
				errno = -ret;
				ret = -1;
				slog_debug("[POSIX]. Error in 'fseek', ret=%ld, errno=%d:%s", ret, errno, strerror(errno));
				return ret;
			}
			ret = p + offset;
			slog_debug("[POSIX]. SEEK_CUR=%ld, p+offset=%ld", offset, ret);
			map_fd_update_value(map_fd, pathname, fd, ret);
		}
		else if (whence == SEEK_END)
		{
			slog_debug("SEEK_END=%ld", offset);
			struct stat ds_stat_n;
			ret = imss_getattr(pathname, &ds_stat_n);
			if (ret < 0)
			{
				errno = -ret;
				ret = -1;
				slog_error("[POSIX]. Error in 'lseek', ret=%ld, errno=%d:%s", ret, errno, strerror(errno));
			}
			else
			{
				// ret = offset + ds_stat_n.st_size;
				ret = ds_stat_n.st_size;
				slog_debug("Updating offset to %ld", ret);
				map_fd_update_value(map_fd, pathname, fd, ret);
			}
		}
		slog_debug("[POSIX]. Ending Hercules 'fseek', ret=%ld, errno=%d:%s\n", ret, errno, strerror(errno));
		if (ret >= 0)
		{
			ret = 0;
		}
	}
	else
	{
		slog_debug("[POSIX]. Calling Real 'fseek', fd=%d, errno=%d:%s", fd, errno, strerror(errno));
		ret = real_fseek(stream, offset, whence);
	}
	// The fseek()and fseeko()functions shall return 0 if they succeed.
	// Otherwise, they shall return -1 and set errno to indicate the error.
	return ret;
}

void seekdir(DIR *dirp, long loc)
{
	if (!real_seekdir)
		real_seekdir = dlsym(RTLD_NEXT, "seekdir");

	if (!init)
	{
		return real_seekdir(dirp, loc);
	}

	errno = 0;
	off_t ret = -1;
	int fd = dirfd(dirp);
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		slog_debug("[POSIX] Calling Hercules 'seekdir', fd=%d, loc=%ld", fd, loc);
		// lseek(fd, loc, SEEK_SET);
		// dirp->size = 0;
		// dirp->offset = 0;
		// dirp->filepos = loc;
		real_seekdir(dirp, loc);
		slog_debug("[POSIX] Ending Hercules 'seekdir', fd=%d, loc=%ld", fd, loc);
	}
	else
	{
		slog_full("[POSIX] Calling real 'seekdir', fd=%d, loc=%ld", fd, loc);
		real_seekdir(dirp, loc);
		slog_full("[POSIX] Ending real 'seekdir', fd=%d, loc=%ld", fd, loc);
	}
}

int truncate(const char *path, off_t length)
{
	if (!real_truncate)
		real_truncate = dlsym(RTLD_NEXT, "truncate");

	if (init)
	{
		// TODO.
		fprintf(stderr, "[POSIX][TODO]. Calling truncate, path=%s, length=%ld", path, length);
	}

	return real_truncate(path, length);
}

int ftruncate(int fd, off_t length)
{
	if (!real_ftruncate)
		real_ftruncate = dlsym(RTLD_NEXT, "ftruncate");

	if (!init)
	{
		return real_ftruncate(fd, length);
	}

	errno = 0;
	int ret;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		// TODO.
		slog_debug("[POSIX][TODO]. Calling Hercules 'ftruncate', fd=%d, length=%ld, errno=%d:%s\n", fd, length, errno, strerror(errno));
		ret = 1;
		slog_debug("[POSIX][TODO]. Ending Hercules 'ftruncate', ret=%d, fd=%d, length=%ld, errno=%d:%s\n", ret, fd, length, errno, strerror(errno));
	}
	else
	{
		slog_debug("[POSIX] Calling real 'ftruncate', fd=%d", fd);
		ret = real_ftruncate(fd, length);
	}

	return ret;
}

ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset)
{
	if (!real_pwrite)
		real_pwrite = dlsym(RTLD_NEXT, "pwrite");

	if (!init)
	{
		return real_pwrite(fd, buf, count, offset);
	}

	errno = 0;
	ssize_t ret;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		slog_debug("[POSIX] Calling Hercules 'pwrite', pathname=%s, fd=%d, count=%ld, offset=%ld, errno=%d:%s", pathname, fd, count, offset, errno, strerror(errno));
		// ret = imss_write(pathname, buf, count, offset);
		ret = generalWrite(pathname, fd, buf, count, offset);
		slog_debug("[POSIX] Ending Hercules 'pwrite', pathname=%s, fd=%d, ret=%ld, count=%ld, offset=%ld, errno=%d:%s", pathname, fd, ret, count, offset, errno, strerror(errno));
	}
	else
	{
		slog_debug("[POSIX] Calling Real 'pwrite', fd=%d, count=%ld, offset=%ld, errno=%d:%s", fd, count, offset, errno, strerror(errno));
		ret = real_pwrite(fd, buf, count, offset);
	}
	return ret;
}

ssize_t write(int fd, const void *buf, size_t size)
{
	if (!real_write)
		real_write = dlsym(RTLD_NEXT, "write");

	if (!init)
	{
		return real_write(fd, buf, size);
	}

	errno = 0;
	ssize_t ret = -1;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		unsigned long offset = -1;
		slog_debug("[POSIX]. Calling Hercules 'write', pathname=%s, size=%lu, fd=%d", pathname, size, fd);

		// struct stat ds_stat_n;
		// imss_getattr(pathname, &ds_stat_n);
		// if (ret < 0)
		// {
		// 	errno = -ret;
		// 	ret = -1;
		// 	slog_error("[POSIX] Error Hercules 'write'	: %d:%s", errno, strerror(errno));
		// 	return ret;
		// }
		// map_fd_search(map_fd, pathname, fd, &offset);
		// slog_debug("[POSIX]. pathname=%s, size=%lu, current_file_size=%lu, offset=%d", pathname, size, ds_stat_n.st_size, offset);

		// ret = TIMING(imss_write(pathname, buf, size, offset), "imss_write", int);

		// if (ds_stat_n.st_size + size > ds_stat_n.st_size)
		// {
		// 	map_fd_update_value(map_fd, pathname, fd, ds_stat_n.st_size + size);
		// }
		ret = generalWrite(pathname, fd, buf, size, offset);

		slog_debug("[POSIX]. Ending Hercules 'write', pathname=%s, size=%lu, ret=%ld, fd=%d\n", pathname, size, ret, fd);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'write', fd=%d, size=%lu", fd, size);
		ret = real_write(fd, buf, size);
		slog_full("[POSIX]. Ending real 'write', fd=%d, ret=%ld", fd, ret);
	}

	return ret;
}

void *mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset)
{
	fprintf(stderr, "[POSIX] Calling 'mmap', addr=%p\n", &addr);
	if (!real_mmap)
		real_mmap = dlsym(RTLD_NEXT, "mmap");

	if (init)
	{
		slog_debug("[POSIX %d] Calling Real 'mmap'", rank);
	}

	return mmap(addr, length, prot, flags, fd, offset);
}

ssize_t read(int fd, void *buf, size_t size)
{
	if (!real_read)
		real_read = dlsym(RTLD_NEXT, "read");

	if (!init)
	{
		return real_read(fd, buf, size);
	}

	errno = 0;
	ssize_t ret;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		// pthread_mutex_lock(&system_lock);
		if (size <= 0)
		{
			// memset(buf, 0, 1);
			buf = '\0';
			// pthread_mutex_unlock(&system_lock);
			return 0;
			// return size;
		}

		unsigned long offset = 0;
		slog_debug("[POSIX]. Calling Hercules 'read', pathname=%s, size=%ld, fd=%d.", pathname, size, fd);
		// fprintf(stderr, "[POSIX]. Calling Hercules 'read', pathname=%s, size=%ld, fd=%d\n", pathname, size, fd);

		if (fd < 0)
		{
			errno = EBADF;
			slog_error("[POSIX] Error in Hercules while reading '%s', %d:%s", pathname, errno, strerror(errno));
			// pthread_mutex_unlock(&system_lock);
			return -1;
		}

		// if (buf == NULL)
		// {
		// 	errno = EINVAL;
		// 	slog_error("[POSIX] Error in Hercules while reading '%s', %d:%s", pathname, errno, strerror(errno));
		// 	return -1;
		// }

		map_fd_search(map_fd, pathname, fd, &offset);
		struct stat ds_stat_n;
		ret = imss_getattr(pathname, &ds_stat_n);
		slog_debug("[POSIX]. pathname=%s, stat.size=%ld, offset=%lu, ret=%ld", pathname, ds_stat_n.st_size, offset, ret);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_error("[POSIX] Error in Hercules while reading '%s'", pathname);
		}
		else if (offset > ds_stat_n.st_size)
		{
			slog_warn("[POSIX] Trying to read %ld bytes in the gap, offset=%ld >= data_size=%ld", size, offset, ds_stat_n.st_size);
			fprintf(stderr, "[POSIX] Trying to read %ld bytes in the gap, offset=%ld >= data_size=%ld\n", size, offset, ds_stat_n.st_size);

			// memcpy(buf, '0', size);
			// memset(buf, '\0', size);
			// memset(buf, 0, size);
			buf = '\0';
			ret = 0;
			// ret = size;
			//  if (ret >= 0)
			//  {
			//  	offset += ret;
			//  	slog_debug("[POSIX] Updating map_fd, offset=%d", offset);
			//  	map_fd_update_value(map_fd, pathname, fd, offset);
			//  }
		}
		else
		{
			// fprintf(stderr, "[POSIX] Read Hercules size=%ld, offset=%lu\n", size, offset);
			ret = imss_read(pathname, buf, size, offset);
			if (ret > 0)
			{
				offset += ret;
				// fprintf(stderr, "[POSIX] Updating map_fd, offset=%lu, data_size=%ld\n", offset, ds_stat_n.st_size);

				slog_debug("[POSIX] Updating map_fd, offset=%lu, data_size=%ld", offset, ds_stat_n.st_size);
				map_fd_update_value(map_fd, pathname, fd, offset);
			}
			// fprintf(stderr, "[POSIX] Hercules read, pathname=%s, ret=%ld\n", pathname, ret);
			// fprintf(stderr, "[POSIX ] READ HERCULES ret=%ld\n", ret);
		}
		slog_debug("[POSIX]. End Hercules 'read', pathname=%s, ret=%zd, size=%ld, fd=%d\n", pathname, ret, size, fd);
		// fprintf(stderr, "[POSIX]. Hercules 'read', size=%ld, fd=%d, ret=%lu, offset=%lu, errno=%d:%s\n", size, fd, ret, offset, errno, strerror(errno));
	}
	else
	{
		slog_full("[POSIX]. Calling real 'read', size=%ld, fd=%ld.", size, fd);
		// unsigned long old_offset, new_offset;
		// old_offset = lseek(fd, 0L, SEEK_CUR);
		ret = real_read(fd, buf, size);
		// new_offset = lseek(fd, 0L, SEEK_CUR);
		// unsigned long offset = lseek(fd, 0L, SEEK_CUR);
		// slog_full("[POSIX]. Ending real 'read', size=%ld, fd=%ld, ret=%d, old_offset=%d, new_offset=%d, errno=%d:%s.", size, fd, ret, old_offset, new_offset, errno, strerror(errno));
		// fprintf(stderr, "[POSIX]. Real 'read', size=%ld, fd=%d, ret=%lu, offset=%lu, errno=%d:%s\n", size, fd, ret, offset, errno, strerror(errno));

		slog_full("[POSIX]. Ending real 'read', size=%ld, fd=%ld, ret=%d, errno=%d:%s.", size, fd, ret, errno, strerror(errno));
	}
	return ret;
}

ssize_t pread(int fd, void *buf, size_t count, off_t offset)
{
	if (!real_pread)
		real_pread = dlsym(RTLD_NEXT, "pread");

	if (!init)
	{
		return real_pread(fd, buf, count, offset);
	}

	errno = 0;
	ssize_t ret = -1;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'pread', pathname=%s, size=%ld, offset=%ld, fd=%ld.", pathname, count, offset, fd);

		struct stat ds_stat_n;
		ret = imss_getattr(pathname, &ds_stat_n);
		slog_debug("[POSIX]. pathname=%s, offset=%ld, stat.size=%ld, remaining=%ld", pathname, offset, ds_stat_n.st_size, ds_stat_n.st_size - offset);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_error("[POSIX] Error Hercules 'pread'");
		}
		else if (offset >= ds_stat_n.st_size)
		{
			ret = 0;
		}
		else
		{
			ret = imss_read(pathname, buf, count, offset);
			// The file offset is not changed.
		}

		slog_debug("[POSIX]. End Hercules 'pread', pathname=%s, ret=%ld, size=%ld, offset=%d, fd=%d", pathname, ret, count, offset, fd);
	}
	else
	{
		slog_debug("[POSIX]. Calling real 'pread', size=%ld, fd=%d.", count, fd);
		ret = real_pread(fd, buf, count, offset);
		slog_debug("[POSIX]. Ending real 'pread', size=%ld, fd=%d, ret=%d", count, fd, ret);
	}
	return ret;
}

size_t fread(void *buf, size_t size, size_t count, FILE *fp)
{
	if (!real_fread)
		real_fread = dlsym(RTLD_NEXT, "fread");

	if (!init)
	{
		return real_fread(buf, size, count, fp);
	}

	errno = 0;
	size_t ret;
	int fd = fp->_fileno;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		unsigned long offset = 0;
		slog_debug("[POSIX]. Calling Hercules 'fread', pathname=%s", pathname);
		map_fd_search(map_fd, pathname, fd, &offset);

		struct stat ds_stat_n;
		ret = imss_getattr(pathname, &ds_stat_n);
		slog_debug("[POSIX]. pathname=%s, stat.size=%ld, ret=%d.", pathname, ds_stat_n.st_size, ret);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			fp->_flags |= _IO_ERR_SEEN;
			slog_error("[POSIX] Error Hercules 'fread'	: %s", strerror(errno));
		}
		else if (offset >= ds_stat_n.st_size)
		{
			fp->_flags |= _IO_EOF_SEEN;
			ret = 0;
		}
		else
		{
			ret = imss_read(pathname, buf, count, offset);
			offset += ret;
			slog_debug("[POSIX] Updating map_fd, offset=%d", offset);
			map_fd_update_value(map_fd, pathname, fd, offset);
		}
		slog_debug("[POSIX]. End Hercules 'fread', ret=%ld\n", ret);
	}
	else
	{
		slog_full("[POSIX] Calling real 'fread', fd=%d", fd);
		ret = real_fread(buf, size, count, fp);
	}

	return ret;
}

int unlink(const char *name)
{
	// fprintf(stderr, "Starting unlink, name=%s\n", name);

	if (!real_unlink)
		real_unlink = dlsym(RTLD_NEXT, "unlink");

	if (!init)
	{
		return real_unlink(name);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(name);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'unlink', new_path=%s", new_path);
		// fprintf(stderr, "[POSIX]. Calling Hercules 'unlink', new_path=%s\n", new_path);
		int32_t type = get_type(new_path);
		// slog_debug("[POSIX][unlink] type=%d, new_path=%s", type, new_path);
		if (type == 0)
		{
			strcat(new_path, "/");
			type = get_type(new_path);
			slog_debug("[POSIX][unlink] type=%d, new_path=%s", type, new_path);
			if (type == 2)
			{
				ret = imss_rmdir(new_path);
			}

			if (type != 0)
			{
				ret = imss_unlink(new_path);
			}
		}
		else
		{
			slog_debug("[POSIX][unlink] type=%d, new_path=%s", type, new_path);
			ret = imss_unlink(new_path);
		}

		// unlink error.
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_error("[POSIX]. Error Hercules 'unlink', errno=%d:%s", errno, strerror(errno));
		}
		// remove the file descriptor from the local map.
		if (ret == 1)
		{
			if (map_fd_erase_by_pathname(map_fd, new_path) == -1)
			{
				slog_warn("[POSIX]. Hercules Warning, no file descriptor found for the pathname=%s", new_path);
				ret = -1;
				// errno =
			}
			else
			{
				ret = 0;
			}
		}

		slog_debug("[POSIX]. Ending Hercules 'unlink', type %d, new_path=%s, ret=%d\n", type, new_path, ret);
		free(new_path);
	}
	// else if (!strncmp(name, "imss://", strlen("imss://"))) // TO REVIEW!
	// {
	// 	slog_debug("[POSIX]. Calling 'unlink' op 2, name=%s.", name);
	// 	// fprintf(stderr, "[POSIX]. Calling 'unlink' op 2, name=%s\n", name);
	// 	char *pathname = (char *)calloc(256, sizeof(char));
	// 	strcpy(pathname, name);
	// 	int32_t type = get_type(pathname);
	// 	if (type == 0)
	// 	{
	// 		strcat(pathname, "/");
	// 		type = get_type(pathname);

	// 		if (type == 2)
	// 		{
	// 			ret = imss_rmdir(pathname);
	// 		}
	// 	}
	// 	else
	// 	{
	// 		ret = imss_unlink(pathname);
	// 	}
	// 	free(pathname);
	// }
	else
	{
		slog_full("[POSIX]. Calling Real 'unlink', name=%s", name);
		ret = real_unlink(name);
		slog_full("[POSIX]. Ending Real 'unlink', name=%s, ret=%d", name, ret);
	}
	// fprintf(stderr, "Ending unlink, name=%s\n", name);

	return ret;
}

int rmdir(const char *path)
{

	if (!real_rmdir)
		real_rmdir = dlsym(RTLD_NEXT, "rmdir");

	if (!init)
	{
		return real_rmdir(path);
	}

	errno = 0;
	int ret;
	char *new_path = checkHerculesPath(path);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'rmdir', new_path=%s", new_path);
		ret = imss_rmdir(new_path);
		if (ret == -1)
		{ // special case io500
			ret = real_unlinkat(0, path, 0);
		}
		free(new_path);
	}
	else if (!strncmp(path, "imss://", strlen("imss://"))) // TO REVIEW!
	{
		ret = imss_rmdir(path);
	}
	else
	{
		ret = real_rmdir(path);
	}
	return ret;
}

// int unlinkat(int fd, const char *name, int flag)
// { // rm & rm -r
// 	// fprintf(stderr, "Starting unlinkat, name=%s\n", name);
// 	if (!real_unlinkat)
// 		real_unlinkat = dlsym(RTLD_NEXT, "unlinkat");

// 	if (!init)
// 	{
// 		return real_unlinkat(fd, name, flag);
// 	}

// 	errno = 0;
// 	int ret = 0;
// 	char *new_path = checkHerculesPath(name);
// 	if (new_path != NULL)
// 	{
// 		slog_debug("[POSIX]. Calling Hercules 'unlinkat', new_path=%s", new_path);
// 		// char *new_path;
// 		// new_path = convert_path(name, MOUNT_POINT);
// 		int n_ent = 0;
// 		char *buffer;
// 		char **refs;

// 		if ((n_ent = get_dir((char *)new_path, &buffer, &refs)) < 0)
// 		{
// 			strcat(new_path, "/");
// 			if ((n_ent = get_dir((char *)new_path, &buffer, &refs)) < 0)
// 			{
// 				return -ENOENT;
// 			}
// 		}

// 		for (int i = n_ent - 1; i > -1; --i)
// 		{
// 			char *last = refs[i] + strlen(refs[i]) - 1;

// 			if (refs[i][strlen(refs[i]) - 1] == '/')
// 			{
// 				rmdir(refs[i]);
// 			}
// 			else
// 			{
// 				unlink(refs[i]);
// 			}
// 		}
// 		slog_debug("[POSIX]. Calling Hercules 'unlinkat', new_path=%s", new_path);
// 		free(new_path);
// 	}
// 	else
// 	{
// 		slog_debug("[POSIX]. Calling real 'unlinkat', pathname=%s", name);
// 		ret = real_unlinkat(fd, name, flag);
// 	}
// 	// fprintf(stderr, "Ending unlinkat, name=%s\n", name);
// 	return ret;
// }

int remove(const char *name)
{
	if (!real_remove)
		real_remove = dlsym(RTLD_NEXT, "remove");

	if (!init)
	{
		return real_remove(name);
	}

	errno = 0;
	int ret, ret_map = 0;
	char *new_path = checkHerculesPath(name);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'remove', new_path=%s", new_path);
		int32_t type = get_type(new_path);
		if (type == 0)
		{
			strcat(new_path, "/");
			type = get_type(new_path);
			slog_debug("[POSIX][remove] type=%d, new_path=%s", type, new_path);
			if (type == 2)
			{
				ret = imss_rmdir(new_path);
			}

			if (type != 0)
			{
				ret = imss_unlink(new_path);
			}
		}
		else
		{
			slog_debug("[POSIX][remove] type=%d, new_path=%s", type, new_path);
			ret = imss_unlink(new_path);
		}

		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_error("[POSIX]. Error Hercules 'remove', errno=%d:%s", errno, strerror(errno));
		}
		else if (ret == 0)
		{
			slog_debug("[POSIX]. Removing %s from the map", new_path);
			// remove the file descriptor from the local map.
			ret_map = map_fd_erase_by_pathname(map_fd, new_path);
			if (ret_map == -1)
			{
				slog_error("[POSIX]. Error Hercules no file descriptor found for the pathname=%s", new_path);
			}
		}
		else
		{
			ret = 0;
		}

		slog_debug("[POSIX]. Ending Hercules 'remove', type %d, new_path=%s, ret=%d, ret_map=%d\n", type, new_path, ret, ret_map);
		free(new_path);
	}
	else
	{
		slog_debug("[POSIX] Calling real 'remove', pathname=%s", name);
		ret = real_remove(name);
		slog_debug("[POSIX] Ending real 'remove', pathname=%s, errno=%d:%s", name, errno, strerror(errno));
	}

	return ret;
}

int rename(const char *old, const char *new)
{

	if (!real_rename)
		real_rename = dlsym(RTLD_NEXT, "rename");

	if (!init)
	{
		return real_rename(old, new);
	}

	errno = 0;
	int ret;
	char *old_path = checkHerculesPath(old);
	char *new_path = checkHerculesPath(new);
	if (old_path != NULL && new_path != NULL)
	{ // move from Hercules to Hercules.
		slog_debug("[POSIX]. Calling Hercules 'rename', old=%s, new=%s, old path=%s, new path=%s", old, new, old_path, new_path);
		ret = imss_rename(old_path, new_path);
		slog_debug("[POSIX]. End Hercules 'rename', old path=%s, new path=%s, ret=%d\n", old_path, new_path, ret);
		free(old_path);
		free(new_path);
	}
	else if (old_path == NULL && new_path != NULL)
	{ // move from file system to Hercules.
		slog_debug("[POSIX]. Calling Hercules 'rename', old=%s, new=%s, new path=%s", old, new, new_path);

		// open both files.
		int fd_old = open(old, O_RDONLY);
		int fd_new = open(new, O_WRONLY | O_APPEND | O_CREAT, 0644);

		// get old file stat.
		struct stat *old_file_stat;
		old_file_stat = malloc(sizeof(struct stat));
		ret = __fxstat(1, fd_old, old_file_stat);
		// old file size.
		off_t old_file_size = old_file_stat->st_size;

		// read old file.
		char *old_file_buffer = NULL;
		old_file_buffer = (char *)malloc(old_file_size * sizeof(char));
		ssize_t ret = read(fd_old, old_file_buffer, old_file_size);
		slog_info("[POSIX]. bytes read from %s = %ld/%ld", old, ret, old_file_size);

		ret = write(fd_new, old_file_buffer, ret);

		slog_info("[POSIX]. bytes write to %s = %ld/%ld", new, ret, old_file_size);

		close(fd_new);
		close(fd_old);
	}
	else
	{
		slog_debug("[POSIX]. Calling Real 'rename', old path=%s, new path=%s", old, new);
		ret = real_rename(old, new);
		slog_debug("[POSIX]. End Real 'rename', old path=%s, new path=%s", old, new);
	}
	return ret;
}

int fchmodat(int dir_fd, const char *pathname, mode_t mode, int flags)
{
	if (!real_fchmodat)
		real_fchmodat = dlsym(RTLD_NEXT, "fchmodat");

	if (!init)
	{
		return real_fchmodat(dir_fd, pathname, mode, flags);
	}

	// Fix: to check dir_fd.

	errno = 0;
	int ret;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'fchmodat', pathname=%s", pathname);
		ret = imss_chmod(new_path, mode);
		slog_debug("[POSIX]. End Hercules 'fchmodat', pathname=%s, ret=%d", pathname, ret);
		free(new_path);
	}
	else
	{
		ret = real_fchmodat(dir_fd, pathname, mode, flags);
	}

	return ret;
}

int chmod(const char *pathname, mode_t mode)
{
	if (!real_chmod)
		real_chmod = dlsym(RTLD_NEXT, "chmod");

	if (!init)
	{
		return real_chmod(pathname, mode);
	}

	errno = 0;
	int ret;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'chmod', pathname=%s", pathname);
		ret = imss_chmod(new_path, mode);
		slog_debug("[POSIX]. End Hercules 'chmod', pathname=%s, ret=%d\n", pathname, ret);
		free(new_path);
	}
	else
	{
		ret = real_chmod(pathname, mode);
	}
	return ret;
}

// int execl(const char *path, const char *arg0, ... /*, (char *)0 */)
// {
// 	fprintf(stderr, "*********** Running execl\n");
// 	return dlsym(RTLD_NEXT, "execl");
// }

// int execlp(const char *file, const char *arg, ... /*, (char *) NULL */)
// {
// 	fprintf(stderr, "*********** Running execlp\n");
// 	return dlsym(RTLD_NEXT, "execlp");
// }

// int execle(const char *pathname, const char *arg, ... /*, (char *) NULL, char *const envp[] */)
// {
// 	fprintf(stderr, "*********** Running execle\n");
// 	return dlsym(RTLD_NEXT, "execle");
// }

// int execv(const char *pathname, char *const argv[])
// {
// 	real_execv = dlsym(RTLD_NEXT, "execv");

// 	if (init)
// 	{
// 		fprintf(stderr, "[POSIX] Running execv, pathname=%s\n", pathname);
// 	}

// 	return real_execv(pathname, argv);
// }

// int execvp(const char *file, char *const argv[])
// {
// 	fprintf(stderr, "*********** Running execvp\n");
// 	return dlsym(RTLD_NEXT, "execvp");
// }

// int execvpe(const char *file, char *const argv[], char *const envp[])
// {
// 	fprintf(stderr, "*********** Running execvpe\n");
// 	return dlsym(RTLD_NEXT, "execvpe");
// }

int execve(const char *pathname, char *const argv[], char *const envp[])
{

	real_execve = dlsym(RTLD_NEXT, "execve");

	// fprintf(stderr, "*********** Running execve, pathname=%s\n", pathname);

	return real_execve(pathname, argv, envp);
}

int dup(int oldfd)
{
	if (!real_dup)
		real_dup = dlsym(RTLD_NEXT, "dup");

	if (!init)
	{
		return real_dup(oldfd);
	}

	errno = 0;
	int ret;
	char *pathname = map_fd_search_by_val(map_fd, oldfd);
	if (pathname != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'dup', pathname=%s, oldfd=%d.", pathname, oldfd);

		int lowest_fd;

		// Attempt to duplicate the lowest available file descriptor (>= 0).
		lowest_fd = real_open("/dev/null", 0); // fcntl(0, F_DUPFD, 0);

		if (lowest_fd != -1)
		{
			slog_debug("[POSIX]. Lowest available file descriptor: %d", lowest_fd);
			// ret = close(lowest_fd); // Close the duplicated file descriptor
			// slog_debug("[POSIX]. File descriptor %d closed, ret=%d", lowest_fd, ret);
			ret = map_fd_put(map_fd, pathname, lowest_fd, 0);
			slog_debug("[POSIX]. Putting %d in the map, ret=%d", lowest_fd, ret);
			if (ret == -1)
			{
				errno = 9;
				slog_error("[POSIX] Error Hercules in 'dup', lowest_fd=%d already exist, errno=%d:%s.", lowest_fd, errno, strerror(errno)); // -1 when error, and errno is set.
			}
			else
			{
				ret = lowest_fd;
			}
		}
		else
		{
			perror("Failed to get the lowest available file descriptor");
			ret = -1;
		}

		slog_debug("[POSIX]. End Hercules 'dup', pathname=%s, ret=%d.", pathname, ret);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'dup', oldfd=%d.", oldfd);
		ret = real_dup(oldfd);
		slog_full("[POSIX]. End real 'dup', oldfd=%d, newfd=%d.", oldfd, ret);
	}

	return ret;
}

int dup2(int oldfd, int newfd)
{
	if (!real_dup2)
		real_dup2 = dlsym(RTLD_NEXT, "dup2");

	if (!init)
	{
		return real_dup2(oldfd, newfd);
	}

	errno = 0;
	int ret;
	char *pathname = map_fd_search_by_val(map_fd, oldfd);
	if (pathname != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'dup2', pathname=%s, oldfd=%d, newfd=%d.", pathname, oldfd, newfd);
		if (oldfd == newfd)
		{
			ret = newfd;
		}
		else
		{
			ret = map_fd_put(map_fd, pathname, newfd, 0);
			if (ret == -1)
			{
				slog_error("[POSIX] Error Hercules in 'dup2', newfd=%d already exist.", newfd); // -1 when error, and errno is set.
			}
			else
			{
				ret = newfd;
			}
		}
		slog_debug("[POSIX]. End Hercules 'dup2', pathname=%s, ret=%d.", pathname, ret);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'dup2', oldfd=%d, newfd=%d.", oldfd, newfd);
		ret = real_dup2(oldfd, newfd);
	}

	return ret;
}

int fchmod(int fd, mode_t mode)
{
	if (!real_fchmod)
		real_fchmod = dlsym(RTLD_NEXT, "fchmod");

	if (!init)
	{
		return real_fchmod(fd, mode);
	}

	errno = 0;
	int ret;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'fchmod', pathname=%s.", pathname);
		ret = imss_chmod(pathname, mode);
		slog_debug("[POSIX]. End Hercules 'fchmod', pathname=%s, ret=%d.", pathname, ret);
	}
	else
	{
		ret = real_fchmod(fd, mode);
	}

	return ret;
}

int fchownat(int dir_fd, const char *pathname, uid_t owner, gid_t group, int flags)
{
	if (!real_fchownat)
		real_fchownat = dlsym(RTLD_NEXT, "chown");

	if (!init)
	{
		return real_fchownat(dir_fd, pathname, owner, group, flags);
	}

	// Fix: to check dir_fd.

	errno = 0;
	int ret;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_debug("[POSIX %d]. Calling Hercules 'fchownat'.", rank);
		ret = imss_chown(new_path, owner, group);
	}
	else
	{
		ret = real_fchownat(dir_fd, pathname, owner, group, flags);
	}

	return ret;
}

DIR *opendir(const char *name)
{
	if (!real_opendir)
		real_opendir = dlsym(RTLD_NEXT, "opendir");

	if (!init)
	{
		return real_opendir(name);
	}

	errno = 0;
	DIR *dirp;
	char *new_path = checkHerculesPath(name);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'opendir', pathname=%s, new_path=%s", name, new_path);
		int a = 1;
		int ret = 0;
		dirp = real_opendir("/tmp");
		seekdir(dirp, 0);
		unsigned long p = 0;
		int fd = -1;
		// Search for the path "new_path" on the map "map_fd",
		// if it exists then a file descriptor "fd" is going to point it.
		ret = map_fd_search_by_pathname(map_fd, new_path, &fd, &p);
		if (ret != -1)
		{
			slog_debug("[POSIX] map_fd_update_value, new_path=%s, fd=%d, ret=%d", new_path, fd, ret);
			// map_fd_update_value(map_fd, new_path, fd, dirfd(dirp), p);
			map_fd_update_fd(map_fd, new_path, fd, dirfd(dirp), p);
		}
		else
		{
			slog_debug("[POSIX] map_fd_put, new_path=%s, fd=%d", new_path, dirfd(dirp));
			ret = map_fd_put(map_fd, new_path, dirfd(dirp), p);
		}
		slog_debug("[POSIX]. End Hercules 'opendir', pathname=%s, new_path=%s, ret=%d\n", name, new_path, ret);
		free(new_path);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'opendir', pathname=%s", name);
		dirp = real_opendir(name);
		// slog_debug("[POSIX]. Ending real 'opendir', pathname=%s, fd=%d", name, dirfd(dirp));
	}
	return dirp;
}

int myfiller(void *buf, const char *name, const struct stat *stbuf, off_t off)
{
	strcat(buf, name);
	strcat(buf, "$");
	return 1;
}

struct dirent *readdir(DIR *dirp)
{
	if (!real_readdir)
		real_readdir = dlsym(RTLD_NEXT, "readdir");

	if (!init)
	{
		return real_readdir(dirp);
	}

	errno = 0;
	size_t ret;
	char *pathname = map_fd_search_by_val(map_fd, dirfd(dirp));
	struct dirent *entry = NULL;
	if (pathname != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'readdir', pathname=%s", pathname);
		entry = (struct dirent *)malloc(sizeof(struct dirent));
		char buf[KB * KB] = {0};
		char *token;
		imss_readdir(pathname, buf, myfiller, 0);
		unsigned long pos = telldir(dirp);

		token = strtok(buf, "$");
		int i = 0;
		slog_debug("Init while, first token=%s, pos=%lu", token, pos);
		while (token != NULL)
		{
			// slog_debug("[POSIX] current token=%s, i=%d, pos=%d", token, i, pos);
			if (i == pos)
			{
				entry->d_ino = 0;
				entry->d_off = pos;

				// name of file
				strcpy(entry->d_name, token);

				char path_search[256] = {0};
				// sprintf(path_search, "imss://%s", token); // original
				// sprintf(path_search, "%s", token);

				if (!strncmp(token, ".", strlen(token)))
				{
					sprintf(path_search, "imss://%s", token);
					entry->d_type = DT_DIR;
				}
				else if (!strncmp(token, "..", strlen(token)))
				{
					sprintf(path_search, "imss://%s", token);
					entry->d_type = DT_DIR;
				}
				else
				{
					sprintf(path_search, "%s/%s", pathname, token);
					// type of file;
					int32_t type = get_type(path_search);

					if (type == 0)
					{
						strcat(path_search, "/");
						type = get_type(path_search);
						if (type == 2)
						{
							entry->d_type = DT_DIR;
						}
						else
						{
							entry->d_type = DT_REG;
						}
					}
					else
					{
						entry->d_type = DT_REG;
					}
				}

				// length of this record
				if (strlen(token) < 5)
				{
					entry->d_reclen = 24;
				}
				else
				{
					entry->d_reclen = ceil((double)(strlen(token) - 4) / 8) * 8 + 24;
				}
				slog_debug("[imss_posix][readdir] path_searched = %s", path_search);
				break;
			}
			token = strtok(NULL, "$");
			i++;
		}
		seekdir(dirp, pos + 1);
		if (token == NULL)
		{
			entry = NULL;
		}
		slog_debug("[POSIX]. Ending Hercules 'readdir',  pathname=%s\n", pathname);
		// fprintf(stderr, "Hercules readdir\n");
	}
	else
	{
		slog_full("[POSIX]. Calling real 'readdir', fd=%d.", dirfd(dirp));
		entry = real_readdir(dirp);
		slog_full("[POSIX]. Ending real 'readdir'");
	}

	return entry;
}

struct dirent64 *readdir64(DIR *dirp)
{
	if (!real_readdir64)
		real_readdir64 = dlsym(RTLD_NEXT, "readdir64");

	if (!init)
	{
		return real_readdir64(dirp);
	}

	errno = 0;
	char *pathname = map_fd_search_by_val(map_fd, dirfd(dirp));
	if (pathname != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'readdir64', pathname=%s", pathname);
		// struct dirent64 *entry = (struct dirent64 *)malloc(sizeof(struct dirent64));
		struct dirent64 *entry = (struct dirent64 *)malloc(sizeof(struct dirent64));
		char buf[KB * KB] = {0};
		char *token;
		imss_readdir(pathname, buf, myfiller, 0);
		unsigned long pos = telldir(dirp);

		token = strtok(buf, "$");
		int i = 0;
		slog_debug("Init while, first token=%s, pos=%lu", token, pos);
		while (token != NULL)
		{
			// slog_debug("[POSIX] current token=%s, i=%d, pos=%d", token, i, pos);
			if (i == pos)
			{
				entry->d_ino = 0;
				entry->d_off = pos;

				// name of file
				strcpy(entry->d_name, token);

				char path_search[256] = {0};
				// sprintf(path_search, "imss://%s", token); // original
				// sprintf(path_search, "%s", token);

				if (!strncmp(token, ".", strlen(token)))
				{
					sprintf(path_search, "imss://%s", token);
					entry->d_type = DT_DIR;
				}
				else if (!strncmp(token, "..", strlen(token)))
				{
					sprintf(path_search, "imss://%s", token);
					entry->d_type = DT_DIR;
				}
				else
				{
					sprintf(path_search, "%s/%s", pathname, token);
					// type of file;
					int32_t type = get_type(path_search);

					if (type == 0)
					{
						strcat(path_search, "/");
						type = get_type(path_search);
						if (type == 2)
						{
							entry->d_type = DT_DIR;
						}
						else
						{
							entry->d_type = DT_REG;
						}
					}
					else
					{
						entry->d_type = DT_REG;
					}
				}

				// length of this record
				if (strlen(token) < 5)
				{
					entry->d_reclen = 24;
				}
				else
				{
					entry->d_reclen = ceil((double)(strlen(token) - 4) / 8) * 8 + 24;
				}
				slog_debug("[imss_posix][readdir64] path_searched = %s", path_search);
				break;
			}
			token = strtok(NULL, "$");
			i++;
		}
		seekdir(dirp, pos + 1);
		if (token == NULL)
		{
			entry = NULL;
		}
		slog_debug("[POSIX]. Ending Hercules 'readdir64', pathname=%s\n", pathname);
		return (struct dirent64 *)entry;
	}
	else
	{
		slog_full("[POSIX]. Calling real 'readdir64', fd=%d", dirfd(dirp));
		return real_readdir64(dirp);
	}
}

int closedir(DIR *dirp)
{
	if (!real_closedir)
		real_closedir = dlsym(RTLD_NEXT, "closedir");

	if (!init)
	{
		return real_closedir(dirp);
	}

	// map_fd_search_by_val_close(map_fd, dirfd(dirp));

	errno = 0;
	int ret = -1;
	int fd = dirfd(dirp);
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		// fprintf(stderr, "Hercules closedir, %s\n", pathname);
		slog_debug("[POSIX] Calling Hercules 'closedir', pathname=%s, fd=%d.", pathname, fd);
		// The closedir() function returns 0 on success.  On error, -1 is
		// returned, and errno is set to indicate the error.
		ret = map_fd_search_by_val_close(map_fd, fd);

		if (ret != 0)
		{
			errno = 2;
			slog_error("[POSIX] Error while Hercules closed the directory %s, errno=%d:%s", pathname, errno, strerror(errno));
		}

		slog_debug("[POSIX] End Hercules 'closedir', fd=%d, ret=%d\n", fd, ret);
		// fprintf(stderr, "Hercules closedir, %d\n", ret);
	}
	else
	{
		slog_full("[POSIX] Calling Real 'closedir', fd=%d", fd);
		ret = real_closedir(dirp);
		slog_full("[POSIX] End Real 'closedir', fd=%d, ret=%d", fd, ret);
	}

	// int ret = real_closedir(dirp);

	return ret;
}

// int _openat(int dirfd, const char *pathname, int flags)
// {
// 	fprintf(stderr, "Calling _openat %s\n", pathname);
// 	return 1;
// }

// int __openat64(int fd, const char *file, int oflag)
// {
// 	fprintf(stderr, "Calling __openat64 %s\n", file);
// 	return 1;
// }

// int __openat64_2(int fd, const char *file, int oflag)
// {
// 	fprintf(stderr, "Calling __openat64_2 %s\n", file);
// 	return 1;
// }

// int __libc_openat(int fd, const char *file, int oflag, ...)
// {
// 	fprintf(stderr, "Calling __libc_openat %s\n", file);
// 	return 1;
// }

// int __libc_open64(const char *file, int oflag, ...)
// {
// 	fprintf(stderr, "Calling __libc_open64 %s\n", file);
// 	return 1;
// }

// int __open64(const char *file, int oflag, ...)
// {
// 	fprintf(stderr, "Calling __open64 %s\n", file);
// 	return 1;
// }

// int __open(const char *file, int oflag, ...)
// {
// 	fprintf(stderr, "Calling __open %s\n", file);
// 	return 1;
// }

// int _open(const char *pathname, int flags, ...)
// {
// 	fprintf(stderr, "Calling _open %s\n", pathname);
// 	return 1;
// }

/*****************************/

// 	slog_debug("[POSIX %d]. End '__xstat64'  %d %d.", rank, ret, errno);

// 	return ret;
// }

int stat64(const char *pathname, struct stat64 *buf)
{
	if (!real_stat64)
		real_stat64 = dlsym(RTLD_NEXT, "stat64");

	if (!init)
	{
		return real_stat64(pathname, buf);
	}

	errno = 0;
	int ret;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'stat', new_path=%s.", new_path);
		imss_refresh(new_path);
		ret = imss_getattr(new_path, (struct stat *)buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
		}
		slog_debug("[POSIX]. Ending Hercules 'stat', new_path=%s, ret=%d, errno=%d:%s", new_path, ret, errno, strerror(errno));
		free(new_path);
	}
	else
	{
		slog_debug("[POSIX]. Calling Real 'stat', pathname=%s.", pathname);
		ret = real_stat64(pathname, buf);
		slog_debug("[POSIX]. Ending Real 'stat', pathname=%s.", pathname);
	}

	return ret;
}

int fstat(int fd, struct stat *buf)
{
	if (!real_fstat)
		real_fstat = dlsym(RTLD_NEXT, "fstat");

	if (!init)
	{
		return real_fstat(fd, buf);
	}

	errno = 0;
	int ret;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		slog_debug("[POSIX] Calling Hercules 'fstat', pathname=%s, fd=%d.", pathname, fd);
		imss_refresh(pathname);
		ret = imss_getattr(pathname, buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_error("[POSIX] Error Hercules 'fstat', errno=%d:%s", errno, strerror(errno));
		}

		slog_debug("[POSIX] End Hercules 'fstat', pathname=%s, fd=%d, errno=%d:%s, ret=%d, st_size=%ld, st_blocks=%ld, st_blksize=%ld\n", pathname, fd, errno, strerror(errno), ret, buf->st_size, buf->st_blocks, buf->st_blksize);
	}
	else
	{
		slog_debug("[POSIX] Calling Real 'fstat', fd=%d", fd);
		ret = real_fstat(fd, buf);
		slog_debug("[POSIX] End Real 'fstat', fd=%d, errno=%d:%s, ret=%d, st_size=%ld, st_blocks=%ld, st_blksize=%ld", fd, errno, strerror(errno), ret, buf->st_size, buf->st_blocks, buf->st_blksize);
	}
	return ret;
}

int __fxstatat(int ver, int dir_fd, const char *pathname, struct stat *stat_buf, int flags)
{
	// fprintf(stderr, "[POSIX]. Calling Real '__fxstatat', pathname=%s, dir_fd=%d\n", pathname, dir_fd);
	if (!real___fxstatat)
	{
		real___fxstatat = dlsym(RTLD_NEXT, __func__);
	}

	if (!init)
	{
		return real___fxstatat(ver, dir_fd, pathname, stat_buf, flags);
	}

	errno = 0;
	int ret = 0;
	char *pathname_dir = NULL, *new_path = NULL;
	if (dir_fd == AT_FDCWD)
	{
		// pathname_dir = getenv("PWD");
		// new_path = checkHerculesPath(pathname_dir);
		// pathname_dir = NULL;
		new_path = checkHerculesPath(pathname);
		pathname_dir = NULL;
	}
	else
	{
		pathname_dir = map_fd_search_by_val(map_fd, dir_fd);
	}

	if (pathname_dir != NULL || new_path != NULL)
	{
		// fprintf(stderr, "[POSIX] Calling Hercules '__fxstatat' flags=%d, dir_fd=%d, pathname_dir=%s, new_path_dir=%s, pathname=%s\n", flags, dir_fd, pathname_dir, new_path, pathname);
		slog_debug("[POSIX] Calling Hercules '__fxstatat', flags=%d, dir_fd=%d, pathname_dir=%s, pathname=%s", flags, dir_fd, pathname_dir, pathname);
		int is_absolute_path = IsAbsolutePath(pathname);

		// If pathname is absolute, then dir_fd is ignored.
		if (is_absolute_path == 1)
		{
			// char *new_path = checkHerculesPath(pathname);
			slog_debug("[POSIX] is absolute, '__fxstatat', pathname=%s", pathname);
			ret = stat(pathname, stat_buf);

			// slog_debug("[POSIX] stat_buf->st_size=%ld", stat_buf->st_size);

			// ret = generalOpen(new_path, flags, mode);
			// free(new_path);
		}
		else if (is_absolute_path == 0) // pathname is relative.
		{
			if (dir_fd == AT_FDCWD) // dir_fd is the special value AT_FDCWD.
			{						// TO CHECK!
				// char *new_path = checkHerculesPath(pathname);
				// slog_debug("[POSIX] is relative, current directory, '__fxstatat', new_path=%s", new_path);
				slog_debug("[POSIX] is relative, current directory, '__fxstatat', pathname=%s", pathname);
				// pathname is interpreted relative to the current working directory of the calling process (like real_open).
				// ret = generalOpen(new_path, flags, mode);
				ret = stat(pathname, stat_buf);
				// free(new_path);
			}
			else
			{
				// // get the pathname of the directory pointed by dir_fd if it is storage in the local map "map_fd".
				// char *pathname_dir = map_fd_search_by_val(map_fd, dir_fd);
				// if (pathname_dir == NULL)
				// { // if dir_fd is not storage locally
				// 	// search dir_fd on the metadata server.
				// 	// fprintf(stderr,"");
				// 	slog_error("[POSIX] dir_fd=%d could not be resolved.");
				// 	return -1;
				// }

				char absolute_pathname[MAX_PATH];
				char *dirr = pathname_dir + strlen("imss://");
				sprintf(absolute_pathname, "%s/%s/%s", MOUNT_POINT, dirr, pathname);

				// char *new_path = checkHerculesPath(absolute_pathname);
				//  slog_debug("[POSIX] is relative, '__fxstatat', new_path=%s", new_path);
				slog_debug("[POSIX] is relative, '__fxstatat', absolute_pathname=%s", absolute_pathname);
				ret = stat(absolute_pathname, stat_buf);

				// ret = generalOpen(new_path, flags, mode);
				// free(new_path);
			}
		}

		slog_debug("[POSIX] Ending Hercules '__fxstatat', ret=%d\n", ret);
		if (new_path != NULL)
			free(new_path);
	}
	else
	{
		slog_full("[POSIX] Calling real '__fxstatat', flags=%d, dir_fd=%d, pathname=%s", flags, dir_fd, pathname);
		// fprintf(stderr, "[POSIX] Calling real '__fxstatat' flags=%d, dir_fd=%d, pathname=%s\n", flags, dir_fd, pathname);
		ret = real___fxstatat(ver, dir_fd, pathname, stat_buf, flags);
		slog_full("[POSIX] Ending real '__fxstatat', flags=%d, dir_fd=%d, pathname=%s", flags, dir_fd, pathname);
	}

	return ret;
}

int __fxstatat64(int ver, int dir_fd, const char *pathname, struct stat64 *stat_buf, int flags)
{
	// fprintf(stderr, "[POSIX][TODO]. Calling Real '__fxstatat64', pathname=%s\n", __filename);
	if (!real___fxstatat64)
	{
		real___fxstatat64 = dlsym(RTLD_NEXT, __func__);
	}

	if (!init)
	{
		return real___fxstatat64(ver, dir_fd, pathname, stat_buf, flags);
	}

	errno = 0;
	int ret = 0;
	char *pathname_dir, *new_path = NULL;
	if (dir_fd == AT_FDCWD)
	{
		// pathname_dir = getenv("PWD");
		// new_path = checkHerculesPath(pathname_dir);
		// pathname_dir = NULL;
		new_path = checkHerculesPath(pathname);
		pathname_dir = NULL;
	}
	else
	{
		pathname_dir = map_fd_search_by_val(map_fd, dir_fd);
	}

	if (pathname_dir != NULL || new_path != NULL)
	{
		// fprintf(stderr, "[POSIX] Calling Hercules '__fxstatat' flags=%d, dir_fd=%d, pathname_dir=%s, new_path_dir=%s, pathname=%s\n", flags, dir_fd, pathname_dir, new_path, pathname);
		slog_debug("[POSIX] Calling Hercules '__fxstatat64' flags=%d, dir_fd=%d, pathname_dir=%s, pathname=%s", flags, dir_fd, pathname_dir, pathname);
		int is_absolute_path = IsAbsolutePath(pathname);

		// If pathname is absolute, then dir_fd is ignored.
		if (is_absolute_path == 1)
		{
			// char *new_path = checkHerculesPath(pathname);
			slog_debug("[POSIX] is absolute, '__fxstatat', pathname=%s", pathname);
			ret = stat64(pathname, stat_buf);

			// ret = generalOpen(new_path, flags, mode);
			// free(new_path);
		}
		else if (is_absolute_path == 0) // pathname is relative.
		{
			if (dir_fd == AT_FDCWD) // dir_fd is the special value AT_FDCWD.
			{						// TO CHECK!
				// char *new_path = checkHerculesPath(pathname);
				// slog_debug("[POSIX] is relative, current directory, '__fxstatat', new_path=%s", new_path);
				slog_debug("[POSIX] is relative, current directory, '__fxstatat', pathname=%s", pathname);
				// pathname is interpreted relative to the current working directory of the calling process (like real_open).
				// ret = generalOpen(new_path, flags, mode);
				ret = stat64(pathname, stat_buf);
				// free(new_path);
			}
			else
			{
				// // get the pathname of the directory pointed by dir_fd if it is storage in the local map "map_fd".
				// char *pathname_dir = map_fd_search_by_val(map_fd, dir_fd);
				// if (pathname_dir == NULL)
				// { // if dir_fd is not storage locally
				// 	// search dir_fd on the metadata server.
				// 	// fprintf(stderr,"");
				// 	slog_error("[POSIX] dir_fd=%d could not be resolved.");
				// 	return -1;
				// }

				char absolute_pathname[MAX_PATH];
				char *dirr = pathname_dir + strlen("imss://");
				sprintf(absolute_pathname, "%s/%s/%s", MOUNT_POINT, dirr, pathname);

				// char *new_path = checkHerculesPath(absolute_pathname);
				//  slog_debug("[POSIX] is relative, '__fxstatat', new_path=%s", new_path);
				slog_debug("[POSIX] is relative, '__fxstatat', absolute_pathname=%s", absolute_pathname);
				ret = stat64(absolute_pathname, stat_buf);

				// ret = generalOpen(new_path, flags, mode);
				// free(new_path);
			}
		}

		slog_debug("[POSIX] Ending Hercules '__fxstatat64', ret=%d, errno=%d:%s\n", ret, errno, strerror(errno));
		if (new_path != NULL)
			free(new_path);
	}
	else
	{
		slog_full("[POSIX] Calling real '__fxstatat64' flags=%d, dir_fd=%d, pathname=%s", flags, dir_fd, pathname);
		// fprintf(stderr, "[POSIX] Calling real '__fxstatat' flags=%d, dir_fd=%d, pathname=%s\n", flags, dir_fd, pathname);
		ret = real___fxstatat64(ver, dir_fd, pathname, stat_buf, flags);
		// slog_debug("[POSIX] Ending real '__fxstatat64' flags=%d, dir_fd=%d, pathname=%s", flags, dir_fd, pathname);
	}

	return ret;
}

// int (*real_faccessat)(int dir_fd, const char *pathname, int mode, int flags) = NULL;
int faccessat(int dir_fd, const char *pathname, int mode, int flags)
{
	if (!real_faccessat)
	{
		real_faccessat = dlsym(RTLD_NEXT, __func__);
	}

	// fprintf(stderr, "Calling real 'faccessat', pathname=%s\n", pathname);

	if (!init)
	{
		return real_faccessat(dir_fd, pathname, mode, flags);
	}

	errno = 0;
	int saved_errno;
	int ret = 0;
	char *pathname_dir, *new_path = NULL;
	if (dir_fd == AT_FDCWD)
	{
		// pathname_dir = getenv("PWD");
		// new_path = checkHerculesPath(pathname_dir);
		// pathname_dir = NULL;
		new_path = checkHerculesPath(pathname);
		pathname_dir = NULL;
	}
	else
	{
		pathname_dir = map_fd_search_by_val(map_fd, dir_fd);
	}

	if (pathname_dir != NULL || new_path != NULL)
	{
		// fprintf(stderr, "[POSIX] Calling Hercules 'faccessat' flags=%d, dir_fd=%d, pathname_dir=%s, new_path_dir=%s, pathname=%s\n", flags, dir_fd, pathname_dir, new_path, pathname);
		slog_debug("[POSIX] Calling Hercules 'faccessat' flags=%d, dir_fd=%d, pathname_dir=%s, pathname=%s", flags, dir_fd, pathname_dir, pathname);
		int is_absolute_path = IsAbsolutePath(pathname);

		// If pathname is absolute, then dir_fd is ignored.
		if (is_absolute_path == 1)
		{
			// char *new_path = checkHerculesPath(pathname);
			slog_debug("[POSIX] is absolute, 'faccessat', pathname=%s", pathname);
			ret = access(pathname, mode);

			// ret = generalOpen(new_path, flags, mode);
			// free(new_path);
		}
		else if (is_absolute_path == 0) // pathname is relative.
		{
			if (dir_fd == AT_FDCWD) // dir_fd is the special value AT_FDCWD.
			{						// TO CHECK!
				// char *new_path = checkHerculesPath(pathname);
				// slog_debug("[POSIX] is relative, current directory, 'faccessat', new_path=%s", new_path);
				slog_debug("[POSIX] is relative, current directory, 'faccessat', pathname=%s", pathname);
				// pathname is interpreted relative to the current working directory of the calling process (like real_open).
				// ret = generalOpen(new_path, flags, mode);
				ret = access(pathname, mode);
				// free(new_path);
			}
			else
			{
				// // get the pathname of the directory pointed by dir_fd if it is storage in the local map "map_fd".
				// char *pathname_dir = map_fd_search_by_val(map_fd, dir_fd);
				// if (pathname_dir == NULL)
				// { // if dir_fd is not storage locally
				// 	// search dir_fd on the metadata server.
				// 	// fprintf(stderr,"");
				// 	slog_error("[POSIX] dir_fd=%d could not be resolved.");
				// 	return -1;
				// }

				char absolute_pathname[MAX_PATH];
				char *dirr = pathname_dir + strlen("imss://");
				sprintf(absolute_pathname, "%s/%s/%s", MOUNT_POINT, dirr, pathname);

				// char *new_path = checkHerculesPath(absolute_pathname);
				//  slog_debug("[POSIX] is relative, 'faccessat', new_path=%s", new_path);
				slog_debug("[POSIX] is relative, 'faccessat', absolute_pathname=%s", absolute_pathname);
				ret = access(absolute_pathname, mode);

				// ret = generalOpen(new_path, flags, mode);
				// free(new_path);
			}
		}

		slog_debug("[POSIX] Ending Hercules 'faccessat', ret=%d, errno=%d:%s\n", ret, errno, strerror(errno));
		if (new_path != NULL)
			free(new_path);
	}
	else
	{
		slog_full("[POSIX] Calling real 'faccessat' flags=%d, dir_fd=%d, pathname=%s", flags, dir_fd, pathname);
		// fprintf(stderr, "[POSIX] Calling real 'faccessat' flags=%d, dir_fd=%d, pathname=%s\n", flags, dir_fd, pathname);
		ret = real_faccessat(dir_fd, pathname, mode, flags);
		slog_full("[POSIX] Ending real 'faccessat' flags=%d, dir_fd=%d, pathname=%s, errno=%d:%s", flags, dir_fd, pathname, errno, strerror(errno));
	}

	return ret;
}

int unlinkat(int dir_fd, const char *pathname, int flags)
{
	if (!real_unlinkat)
	{
		real_unlinkat = dlsym(RTLD_NEXT, __func__);
	}

	// fprintf(stderr, "Calling real 'unlinkat', pathname=%s\n", pathname);
	if (!init)
	{
		return real_unlinkat(dir_fd, pathname, flags);
	}

	errno = 0;
	int ret = 0;
	char *pathname_dir, *new_path = NULL;
	if (dir_fd == AT_FDCWD)
	{
		// pathname_dir = getenv("PWD");
		// new_path = checkHerculesPath(pathname_dir);
		// pathname_dir = NULL;
		new_path = checkHerculesPath(pathname);
		pathname_dir = NULL;
	}
	else
	{
		pathname_dir = map_fd_search_by_val(map_fd, dir_fd);
	}

	if (pathname_dir != NULL || new_path != NULL)
	{
		slog_debug("[POSIX] Calling Hercules 'unlinkat' flags=%d, dir_fd=%d, pathname_dir=%s, pathname=%s", flags, dir_fd, pathname_dir, pathname);
		int is_absolute_path = IsAbsolutePath(pathname);

		// If pathname is absolute, then dir_fd is ignored.
		if (is_absolute_path == 1)
		{
			// char *new_path = checkHerculesPath(pathname);
			slog_debug("[POSIX] is absolute, 'unlinkat', pathname=%s", pathname);
			ret = unlink(pathname);
			// ret = generalOpen(new_path, flags, mode);
			// free(new_path);
		}
		else if (is_absolute_path == 0) // pathname is relative.
		{
			if (dir_fd == AT_FDCWD) // dir_fd is the special value AT_FDCWD.
			{						// TO CHECK!
				// char *new_path = checkHerculesPath(pathname);
				// slog_debug("[POSIX] is relative, current directory, 'unlinkat', new_path=%s", new_path);
				slog_debug("[POSIX] is relative, current directory, 'unlinkat', pathname=%s", pathname);
				// pathname is interpreted relative to the current working directory of the calling process (like real_open).
				// ret = generalOpen(new_path, flags, mode);
				ret = unlink(pathname);
				// free(new_path);
			}
			else
			{
				// // get the pathname of the directory pointed by dir_fd if it is storage in the local map "map_fd".
				// char *pathname_dir = map_fd_search_by_val(map_fd, dir_fd);
				// if (pathname_dir == NULL)
				// { // if dir_fd is not storage locally
				// 	// search dir_fd on the metadata server.
				// 	// fprintf(stderr,"");
				// 	slog_error("[POSIX] dir_fd=%d could not be resolved.");
				// 	return -1;
				// }

				char absolute_pathname[MAX_PATH];
				char *dirr = pathname_dir + strlen("imss://");
				sprintf(absolute_pathname, "%s/%s/%s", MOUNT_POINT, dirr, pathname);

				// char *new_path = checkHerculesPath(absolute_pathname);
				//  slog_debug("[POSIX] is relative, 'unlinkat', new_path=%s", new_path);
				slog_debug("[POSIX] is relative, 'unlinkat', absolute_pathname=%s", absolute_pathname);
				ret = unlink(absolute_pathname);

				// ret = generalOpen(new_path, flags, mode);
				// free(new_path);
			}
		}

		slog_debug("[POSIX] Ending Hercules 'unlinkat', ret=%d, errno=%d:%s\n", ret, errno, strerror(errno));
		if (new_path != NULL)
			free(new_path);
	}
	else
	{
		slog_debug("[POSIX] Calling real 'unlinkat' flags=%d, dir_fd=%d, pathname=%s", flags, dir_fd, pathname);
		fprintf(stderr, "[POSIX] Calling real 'unlinkat' flags=%d, dir_fd=%d, pathname=%s\n", flags, dir_fd, pathname);
		ret = real_unlinkat(dir_fd, pathname, flags);
		slog_debug("[POSIX] Ending real 'unlinkat' flags=%d, dir_fd=%d, pathname=%s, errno=%d:%s", flags, dir_fd, pathname, errno, strerror(errno));
	}

	return ret;
}

int renameat2(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, unsigned int flags)
{
	if (!real_renameat2)
	{
		real_renameat2 = dlsym(RTLD_NEXT, __func__);
	}

	if (!init)
	{
		return real_renameat2(olddirfd, oldpath, newdirfd, newpath, flags);
	}

	errno = 0;
	int ret = 0;
	char *pathname_dir, *new_path = NULL;
	if (olddirfd == AT_FDCWD)
	{
		// pathname_dir = getenv("PWD");
		// new_path = checkHerculesPath(pathname_dir);
		// pathname_dir = NULL;
		new_path = checkHerculesPath(oldpath);
		pathname_dir = NULL;
	}
	else
	{
		pathname_dir = map_fd_search_by_val(map_fd, olddirfd);
	}

	if (pathname_dir != NULL || new_path != NULL)
	{
		slog_debug("[POSIX] Calling Hercules 'renameat2', flags=%d, olddirfd=%d, oldpath=%s, newdirfd=%d, newpath=%s", flags, olddirfd, oldpath, newdirfd, newpath);
		int is_absolute_path = IsAbsolutePath(oldpath);

		// If pathname is absolute, then dir_fd is ignored.
		if (is_absolute_path == 1)
		{
			slog_debug("[POSIX] is absolute, 'renameat2', oldpath=%s", oldpath);
			ret = rename(oldpath, newpath);
		}
		else if (is_absolute_path == 0) // pathname is relative.
		{
			if (olddirfd == AT_FDCWD) // dir_fd is the special value AT_FDCWD.
			{						  // TO CHECK!
				slog_debug("[POSIX] is relative, current directory, 'renameat2', oldpath=%s", oldpath);
				// pathname is interpreted relative to the current working directory of the calling process (like real_open).
				ret = rename(oldpath, newpath);
			}
			else
			{
				// // get the pathname of the directory pointed by dir_fd if it is storage in the local map "map_fd".
				char absolute_pathname[MAX_PATH];
				char *dirr = pathname_dir + strlen("imss://");
				sprintf(absolute_pathname, "%s/%s/%s", MOUNT_POINT, dirr, oldpath);

				// char *new_path = checkHerculesPath(absolute_pathname);
				//  slog_debug("[POSIX] is relative, 'renameat2', new_path=%s", new_path);
				slog_debug("[POSIX] is relative, 'renameat2', absolute_pathname=%s", absolute_pathname);
				ret = rename(oldpath, newpath);
			}
		}

		slog_debug("[POSIX] Ending Hercules 'renameat2', ret=%d, errno=%d:%s\n", ret, errno, strerror(errno));
		if (new_path != NULL)
			free(new_path);
	}
	else
	{
		slog_debug("[POSIX] Calling real 'renameat2', flags=%d, olddirfd=%d, oldpath=%s, newdirfd=%d, newpath=%s", flags, olddirfd, oldpath, newdirfd, newpath);
		ret = real_renameat2(olddirfd, oldpath, newdirfd, newpath, flags);
		slog_debug("[POSIX] Calling real 'renameat2', flags=%d, olddirfd=%d, oldpath=%s, newdirfd=%d, newpath=%s, ret=%d", flags, olddirfd, oldpath, newdirfd, newpath, ret);
	}

	return ret;
}

int __fxstat64(int ver, int fd, struct stat64 *buf)
{
	if (!real__fxstat64)
	{
		real__fxstat64 = dlsym(RTLD_NEXT, "__fxstat64");
	}

	if (!init)
	{
		// slog_debug("[POSIX %d] Calling Real '__fxstat64'", rank);
		return real__fxstat64(ver, fd, buf);
	}

	errno = 0;
	int ret;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		slog_debug("[POSIX] Calling Hercules '__fxstat64', pathname=%s, fd=%d.", pathname, fd);
		imss_refresh(pathname);
		ret = imss_getattr(pathname, (struct stat *)buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_error("[POSIX] Error Hercules '__fxstat64'	: %s", strerror(errno));
		}

		slog_debug("[POSIX] End Hercules '__fxstat64', pathname=%s, fd=%d, errno=%d:%s, ret=%d, st_size=%ld, st_blocks=%ld, st_blksize=%ld\n", pathname, fd, errno, strerror(errno), ret, buf->st_size, buf->st_blocks, buf->st_blksize);
	}
	else
	{
		slog_full("[POSIX] Calling real '__fxstat64', ver=%d, fd=%d", ver, fd);
		ret = real__fxstat64(ver, fd, buf);
		slog_full("[POSIX] End real '__fxstat64', fd=%d, ret=%d, st_size=%ld, st_blocks=%ld, st_blksize=%ld", fd, ret, buf->st_size, buf->st_blocks, buf->st_blksize);
	}
	// report(pathname, buf);
	// StatReport(fd, *buf);
	return ret;
}

// int fstatat(int __fd, const char *__restrict __file, struct stat *__restrict __buf, int __flag)
// {
// 	if (!real_fstatat)
// 	{
// 		real_fstatat = dlsym(RTLD_NEXT, __func__);
// 	}

// 	if (!init)
// 	{
// 		// slog_debug("[POSIX %d] Calling Real '__fxstat64'", rank);
// 		return real_fstatat(__fd, __file, __buf, __flag);
// 	}

// 	slog_debug("[POSIX] Calling fstatat, pathname=%s", __file);
// 	fprintf(stderr, "[POSIX] Calling fstatat, pathname=%s", __file);

// 	return real_fstatat(__fd, __file, __buf, __flag);
// }

// int fstatat64(int __fd, const char *__restrict __file, struct stat64 *__restrict __buf, int __flag)
// {
// 	if (!real_fstatat64)
// 	{
// 		real_fstatat64 = dlsym(RTLD_NEXT, __func__);
// 	}

// 	if (!init)
// 	{
// 		// slog_debug("[POSIX %d] Calling Real '__fxstat64'", rank);
// 		return real_fstatat64(__fd, __file, __buf, __flag);
// 	}

// 	slog_debug("[POSIX] Calling fstatat64, pathname=%s", __file);
// 	fprintf(stderr, "[POSIX] Calling fstatat64, pathname=%s", __file);

// 	return real_fstatat64(__fd, __file, __buf, __flag);
// }

// /*
// int fstatat(int dir_fd, const char *pathname, struct stat *buf, int flags) {
// */
// int fstatat(int __fd, const char *__restrict __file, struct stat *__restrict __buf, int __flag)
// {
// 	fprintf(stderr, "[POSIX][TODO]. Calling Real 'fstatat', pathname=%s\n", __file);

// 	if (!real_fstatat)
// 	{
// 		// void *handle;
// 		// /* open the needed object */
// 		// handle = dlopen("/lib/x86_64-linux-gnu/libc.so.6", RTLD_LOCAL | RTLD_LAZY);
// 		// /* find the address of function and data objects */
// 		// real_fstatat = dlsym(handle, "fstatat");
// 		real_fstatat = dlsym(RTLD_NEXT, "fstatat");
// 	}

// 	// fprintf(stderr, "[POSIX][TODO]. Calling Real 'fstatat', real_fstatat=%p\n", &real_fstatat);

// 	// if (init)
// 	// {
// 	// 	slog_warn("[POSIX][TODO]. Calling Real 'fstatat', pathname=%s", __file);
// 	// }
// 	if (real_fstatat == NULL)
// 		fprintf(stderr, "error: real_fstatat is NULL\n");

// 	// // TODO.

// 	return real_fstatat(__fd, __file, __buf, __flag);
// }

// // int fstatat64(int dir_fd, const char *pathname, struct stat *buf, int flags)
// int fstatat64(int __fd, const char *__restrict __file, struct stat64 *__restrict __buf, int __flag)
// {
// 	fprintf(stderr, "[POSIX][TODO]. Calling Real 'fstatat64', pathname=%s\n", __file);

// 	if (!real_fstatat64)
// 		real_fstatat64 = dlsym(RTLD_NEXT, "fstatat64");

// 	if (init)
// 	{
// 		// slog_warn("[POSIX][TODO]. Calling Real 'fstatat64', pathname=%s", __file);
// 	}

// 	// fprintf(stderr, "[POSIX][TODO]. 'fstatat64', real_fstatat=%p\n", &real_fstatat);

// 	// TODO.

// 	return real_fstatat64(__fd, __file, __buf, __flag);
// }

// int newfstatat(int __fd, const char *__restrict __file, struct stat *__restrict __buf, int __flag)
// {
// 	if (!real_newfstatat)
// 		real_newfstatat = dlsym(RTLD_NEXT, "newfstatat");

// 	if (!init)
// 	{
// 		return real_newfstatat(__fd, __file, __buf, __flag);
// 		// slog_warn("[POSIX][TODO]. Calling Real 'newfstatat', pathname=%s", __file);
// 	}

// 	fprintf(stderr, "[POSIX][TODO]. Calling Real 'newfstatat', pathname=%s\n", __file);
// 	// TODO.

// 	return real_newfstatat(__fd, __file, __buf, __flag);
// }

void StatReport(int fd, struct stat sb)
{
	slog_info("File descriptor: %d", fd);
	slog_info("ID of containing device:  [%x,%x]",
			  major(sb.st_dev),
			  minor(sb.st_dev));

	slog_info("File type:                ");

	switch (sb.st_mode & S_IFMT)
	{
	case S_IFBLK:
		slog_info("\tblock device");
		break;
	case S_IFCHR:
		slog_info("\tcharacter device");
		break;
	case S_IFDIR:
		slog_info("\tdirectory");
		break;
	case S_IFIFO:
		slog_info("\tFIFO/pipe");
		break;
	case S_IFLNK:
		slog_info("\tsymlink");
		break;
	case S_IFREG:
		slog_info("\tregular file");
		break;
	case S_IFSOCK:
		slog_info("\tsocket");
		break;
	default:
		slog_info("\tunknown?");
		break;
	}

	slog_info("I-node number:            %ju", (uintmax_t)sb.st_ino);

	slog_info("Mode:                     %jo (octal)",
			  (uintmax_t)sb.st_mode);

	slog_info("Link count:               %ju", (uintmax_t)sb.st_nlink);
	slog_info("Ownership:                UID=%ju   GID=%ju",
			  (uintmax_t)sb.st_uid, (uintmax_t)sb.st_gid);

	slog_info("Preferred I/O block size: %jd bytes",
			  (intmax_t)sb.st_blksize);
	slog_info("File size:                %jd bytes",
			  (intmax_t)sb.st_size);
	slog_info("Blocks allocated:         %jd",
			  (intmax_t)sb.st_blocks);

	slog_info("Last status change:       %s", ctime(&sb.st_ctime));
	slog_info("Last file access:         %s", ctime(&sb.st_atime));
	slog_info("Last file modification:   %s", ctime(&sb.st_mtime));
}

int __fxstat(int ver, int fd, struct stat *buf)
{
	if (!real__fxstat)
		real__fxstat = dlsym(RTLD_NEXT, "__fxstat");

	if (!init)
	{
		return real__fxstat(ver, fd, buf);
	}

	errno = 0;
	int ret;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		slog_debug("[POSIX] Calling Hercules '__fxstat', pathname=%s, fd=%d.", pathname, fd);
		imss_refresh(pathname);
		ret = imss_getattr(pathname, buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_error("[POSIX] Error Hercules '__fxstat'	: %s", strerror(errno));
		}

		slog_debug("[POSIX] End Hercules '__fxstat', pathname=%s, fd=%d, ret=%d\n", pathname, fd, ret);
	}
	else
	{
		slog_full("[POSIX] Calling real '__fxstat', ver=%d, fd=%d", ver, fd);
		ret = real__fxstat(ver, fd, buf);
		// slog_full("[POSIX] End real '__fxstat', fd=%d, ret=%d, st_size=%ld, st_blocks=%ld, st_blksize=%ld", fd, ret, buf->st_size, buf->st_blocks, buf->st_blksize);
	}
	// report(pathname, buf);
	// StatReport(fd, *buf);
	return ret;
}

int __fwprintf_chk(FILE *stream, int flag, const wchar_t *format)
{
	if (!real___fwprintf_chk)
		real___fwprintf_chk = dlsym(RTLD_NEXT, "__fwprintf_chk");
	fprintf(stderr, "Calling __fwprintf_chk\n");
	// TODO.
	return real___fwprintf_chk(stream, flag, format);
}

int access(const char *path, int mode)
{
	if (!real_access)
		real_access = dlsym(RTLD_NEXT, "access");

	if (!init)
	{
		return real_access(path, mode);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(path);
	if (new_path != NULL)
	{
		pthread_mutex_lock(&system_lock);
		struct stat stat_buf;
		int permissions = 0;
		slog_debug("[POSIX]. Calling Hercules 'access', new_path=%s", new_path);
		// fprintf(stderr, "[POSIX]. Calling Hercules 'access', new_path=%s\n", new_path);
		ret = imss_refresh(new_path);
		// if (ret < 0)
		// {
		// 	// errno = -ret;
		// 	ret = -1;
		// 	// perror("ERRIMSS_ACCESS_IMSSREFRESH");
		// }
		ret = imss_getattr(new_path, &stat_buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			// perror("ERRIMSS_ACCESS_IMSSGETATTR");
		}
		else
		{
			errno = 0;
			// ret = imss_getattr(new_path, &stat_buf);
			// if (ret < 0)
			// {
			// 	errno = -ret;
			// 	ret = -1;
			// 	// perror("ERRIMSS_ACCESS_IMSSGETATTR");
			// }
			// else
			{
				/* check permissions */
				if ((mode & F_OK) == F_OK)
					permissions |= F_OK; /* file exists */
				if ((mode & R_OK) == R_OK && (stat_buf.st_mode & S_IRUSR))
					permissions |= R_OK; /* read permissions granted */
				if ((mode & W_OK) == W_OK && (stat_buf.st_mode & S_IWUSR))
					permissions |= W_OK; /* write permissions granted */
				if ((mode & X_OK) == X_OK && (stat_buf.st_mode & S_IXUSR))
					permissions |= X_OK; /* execute permissions granted */

				/* check if all the tested permissions are granted */
				if (mode == permissions)
					ret = 0;
				else
					ret = -1;
			}
		}
		pthread_mutex_unlock(&system_lock);
		slog_debug("[POSIX]. End Hercules 'access', new_path=%s ret=%d\n", new_path, ret);
		// fprintf(stderr, "[POSIX]. End Hercules 'access', new_path=%s ret=%d, errno=%d:%s.\n", new_path, ret, errno, strerror(errno));
		free(new_path);
	}
	else
	{
		slog_full("[POSIX]. Calling Real 'access', path=%s", path);
		ret = real_access(path, mode);
		slog_full("[POSIX]. End Real 'access', path=%s, ret=%d", path, ret);
	}

	return ret;
}

// int fsync(int fd)
// {

// 	real_fsync = dlsym(RTLD_NEXT, "fsync");
// 	if (!init)
// 	{
// 		return real_fsync(fd);
// 	}
// 	slog_info("********* [POSIX] Calling fsync ********");

// 	return real_fsync(fd);
// }

// int epoll_ctl(int epfd, int op, int fd, struct epoll_event *event)
// {
// 	if (!real_epoll_ctl)
// 		real_epoll_ctl = dlsym(RTLD_NEXT, "epoll_ctl");
// 	// fprintf(stderr, "Calling 'epoll_ctl'\n");
// 	return real_epoll_ctl(epfd, op, fd, event);
// }

// int change_to_directory(char *newdir, int nolinks, int xattr)
// {
// 	if (!real_change_to_directory)
// 		real_change_to_directory = dlsym(RTLD_NEXT, "change_to_directory");
// 	fprintf(stderr, "Calling change_to_directory\n");
// 	return real_change_to_directory(newdir, nolinks, xattr);
// }

// int bindpwd(int no_symlinks)
// {
// 	if (!real_bindpwd)
// 		real_bindpwd = dlsym(RTLD_NEXT, "bindpwd");
// 	fprintf(stderr, "Calling bindpwd\n");
// 	return real_bindpwd(no_symlinks);
// }

// int sys_chdir(const char *filename)
// {
// 	if (!real_sys_chdir)
// 		real_sys_chdir = dlsym(RTLD_NEXT, "sys_chdir");
// 	fprintf(stderr, "Calling sys_chdir\n");
// 	return real_sys_chdir(filename);
// }

// int _wchdir(const wchar_t *dirname)
// {
// 	if (!real_wchdir)
// 		real_wchdir = dlsym(RTLD_NEXT, "wchdir");
// 	fprintf(stderr, "Calling _wchdir\n");
// 	return real_wchdir(dirname);
// }

char *getcwd(char *buf, size_t size)
{
	if (!real_getcwd)
		real_getcwd = dlsym(RTLD_NEXT, "getcwd");
	// fprintf(stderr, "Calling getcwd, size=%ld\n", size);
	// buf = real_getcwd(buf, size);
	// ***
	if (!strncmp(getenv("PWD"), MOUNT_POINT, strlen(MOUNT_POINT)))
	{
		slog_debug("[POSIX] Calling Hercules 'getcwd'");
		// buf = getenv("PWD");
		char *curr_dir = getenv("PWD");
		strncpy(buf, curr_dir, strlen(curr_dir));
		slog_debug("[POSIX] Ending Hercules 'getcwd', buf=%s", buf);
		// return buf;
	}
	else
	{
		slog_full("[POSIX] Calling real 'getcwd'");
		real_getcwd(buf, size);
		slog_full("[POSIX] Ending real 'getcwd', buf=%s", buf);
	}
	// fprintf(stderr, "End getcwd, buf=%s\n", buf);
	// ***return buf;
	return buf;
}

int chdir(const char *pathname)
{
	if (!real_chdir)
		real_chdir = dlsym(RTLD_NEXT, "chdir");

	if (!init)
	{
		return real_chdir(pathname);
	}

	errno = 0;
	int ret = 0;
	// if (!strncmp(path, MOUNT_POINT, strlen(MOUNT_POINT)))
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		// fprintf(stderr, "[%d] Calling Hercules 'chdir', pathname=%s\n", rank, path);
		slog_debug("Calling Hercules 'chdir', pathname=%s", pathname);
		setenv("PWD", pathname, 1);
		slog_debug("End Hercules 'chdir', pathname=%s, ret=%d", pathname, ret);
		// fprintf(stderr, "[%d] End Hercules 'chdir', pathname=%s\n", rank, path);
	}
	else
	{
		// fprintf(stderr, "Calling Real 'chdir', pathname=%s\n", path);
		slog_full("[POSIX] Calling real 'chdir', pathname=%s", pathname);
		ret = real_chdir(pathname);
		slog_full("[POSIX] Ending real 'chdir', pathname=%s, ret=%d\n", pathname, ret);
		// fprintf(stderr, "End Real 'chdir', pathname=%s, ret=%d\n", path, ret);
	}

	return ret;
}

int fchdir(int fd)
{
	if (!real_fchdir)
		real_fchdir = dlsym(RTLD_NEXT, "fchdir");

	if (init)
	{
		slog_debug("[POSIX][TODO] Calling real 'fchdir'");
	}

	return real_fchdir(fd);
}

// int __chdir(const char *path)
// {
// 	if (!real___chdir)
// 		real___chdir = dlsym(RTLD_NEXT, "__chdir");

// 	if (init)
// 	{
// 		slog_debug("Calling __chdir, pathname=%s", path);
// 	}

// 	return real___chdir(path);
// }

// int _chdir(const char *path)
// {
// 	if (!real__chdir)
// 		real__chdir = dlsym(RTLD_NEXT, "_chdir");
// 	fprintf(stderr, "Calling _chdir, pathname=%s", path);

// 	if (init)
// 	{
// 		slog_debug("Calling _chdir, pathname=%s", path);
// 	}

// 	return real__chdir(path);
// }

// static int (*real_do_system)(const char *line);
// static int do_system(const char *line)
// {

// 	fprintf(stderr, "Calling do_system\n");
// 	real_do_system = dlsym(RTLD_NEXT, __func__);

// 	return real_do_system(line);
// }

// static int (*real___libc_system)(const char *line) = NULL;
// int __libc_system(const char *line)
// {
// 	fprintf(stderr, "[PRELOAD] Calling '__libc_system'\n");
// 	real___libc_system = dlsym(RTLD_NEXT, __func__);
// 	return real___libc_system(line);
// }

// static long int (*real_ptrace)(enum __ptrace_request __request, ...) = NULL;
// // extern long ptrace(enum __ptrace_request request, pid_t pid, void *addr, void *data) {
// long int ptrace (enum __ptrace_request __request, ...)
// {
// 	fprintf(stderr, "[PRELOAD] Calling 'ptrace'\n");
// 	real_ptrace = dlsym(RTLD_NEXT, __func__);
// 	return real_ptrace(__request);
// }

// int pthread_create(pthread_t *restrict thread,
// 				   const pthread_attr_t *restrict attr,
// 				   void *(*start_routine)(void *),
// 				   void *restrict arg)
// {
// 	if (!real_pthread_create)
// 		real_pthread_create = dlsym(RTLD_NEXT, __func__);

// 	// fprintf(stderr, "Calling 'pthread_create', fd=%d, errno=%d:%s\n", fd, errno, strerror(errno));
// 	if (!init)
// 	{
// 		// slog_debug("Calling pthread_create, fd=%d", fd);
// 		return real_pthread_create(thread, attr, start_routine, arg);
// 	}

// 	fprintf(stderr, "Calling 'pthread_create'\n");

// 	return real_pthread_create(thread, attr, start_routine, arg);
// }

int posix_fadvise(int fd, off_t offset, off_t len, int advice)
{
	if (!real_posix_fadvise)
		real_posix_fadvise = dlsym(RTLD_NEXT, "posix_fadvise");

	// fprintf(stderr, "Calling 'posix_fadvise', fd=%d, errno=%d:%s\n", fd, errno, strerror(errno));
	if (!init)
	{
		// slog_debug("Calling posix_fadvise, fd=%d", fd);
		return real_posix_fadvise(fd, offset, len, advice);
	}

	errno = 0;
	int ret = -1;
	char *pathname = map_fd_search_by_val(map_fd, fd);
	if (pathname != NULL)
	{
		slog_warn("[POSIX][TODO] Calling Hercules 'posix_fadvise', pathname=%s, fd=%d", pathname, fd);
		// fprintf(stderr, "[POSIX] Calling Hercules 'posix_fadvise', pathname=%s, fd=%d\n", pathname, fd);
		//  imss_refresh(pathname);
		//  ret = imss_getattr(pathname, buf);
		//  if (ret < 0)
		//  {
		//  	errno = -ret;
		//  	ret = -1;
		//  	slog_error("[POSIX] Error Hercules '__fxstat'	: %s", strerror(errno));
		//  }
		// ret = 9;
		ret = 0;
		// fprintf(stderr, "[POSIX] Hercules 'posix_fadvise', fd=%d, errno=%d:%s, ret=%d\n", fd, errno, strerror(errno), ret);
		//  slog_debug("[POSIX] End Hercules '__fxstat', pathname=%s, fd=%d, errno=%d:%s, ret=%d\n", pathname, fd, errno, strerror(errno), ret);
	}
	else
	{
		// slog_debug("[POSIX] Calling Real 'posix_fadvise', fd=%d", fd);
		// fprintf(stderr, "[POSIX] 'posix_fadvise', fd=%d\n", fd);
		ret = real_posix_fadvise(fd, offset, len, advice);
		// fprintf(stderr, "[POSIX] Real 'posix_fadvise', fd=%d, errno=%d:%s, ret=%d\n", fd, errno, strerror(errno), ret);
		//  slog_debug("[POSIX] End Real 'posix_fadvise', fd=%d, errno=%d:%s, ret=%d", fd, errno, strerror(errno), ret);
	}

	return ret;
}

// int fcntl(int fd, int cmd, ... /* arg */)
// {
// 	if (!real_fcntl)
// 		real_fcntl = dlsym(RTLD_NEXT, "fcntl");
// 	// FIX ME!

// 	va_list ap;
// 	void *arg;
// 	va_start(ap, cmd);
// 	arg = va_arg(ap, void *);
// 	va_end(ap);

// 	if (!init)
// 	{
// 		if (!arg)
// 			return real_fcntl(fd, cmd);
// 		else
// 			return real_fcntl(fd, cmd, arg);
// 	}

// 	errno = 0;
// 	int ret = 0;
// 	char *pathname = map_fd_search_by_val(map_fd, fd);
// 	// if (pathname = map_fd_search_by_val(map_fd, fd))
// 	if (pathname != NULL)
// 	{
// 		slog_debug("[POSIX]. Calling Hercules 'fcntl', pathname=%s, fd=%d", pathname, fd);
// 		cmd = FCNTL_ADJUST_CMD(cmd);
// 		switch (cmd)
// 		{
// 		// case F_SETLKW:
// 		case F_SETLKW64: // release the existence of record locks.
// 			// return SYSCALL_CANCEL(fcntl64, fd, cmd, arg);
// 			slog_debug("[POSIX][fcntl] pathname=%s, fd=%d, F_SETLKW", pathname, fd);
// 			// ret = 0;//real_fcntl(fd, cmd, arg);
// 			ret = -1;
// 			errno = EDEADLK;
// 			break;
// 		case F_OFD_SETLKW:
// 		{
// 			struct flock *flk = (struct flock *)arg;
// 			slog_debug("[POSIX][fcntl] pathname=%s, fd=%d, F_OFD_SETLKW", pathname, fd);
// 			// struct flock64 flk64 =
// 			// 	{
// 			// 		.l_type = flk->l_type,
// 			// 		.l_whence = flk->l_whence,
// 			// 		.l_start = flk->l_start,
// 			// 		.l_len = flk->l_len,
// 			// 		.l_pid = flk->l_pid
// 			// 	};
// 			// return SYSCALL_CANCEL(fcntl64, fd, cmd, &flk64);
// 			ret = 0; // real_fcntl(fd, cmd, &flk);
// 			break;
// 		}
// 		case F_OFD_GETLK:
// 		case F_OFD_SETLK:
// 		{
// 			slog_debug("[POSIX][fcntl], pathname=%s, fd=%d, F_OFD_SETLK", pathname, fd);
// 			struct flock *flk = (struct flock *)arg;
// 			// struct flock64 flk64 =
// 			// 	{
// 			// 		.l_type = flk->l_type,
// 			// 		.l_whence = flk->l_whence,
// 			// 		.l_start = flk->l_start,
// 			// 		.l_len = flk->l_len,
// 			// 		.l_pid = flk->l_pid
// 			// 	};
// 			int ret = real_fcntl(fd, cmd, &flk); // INLINE_SYSCALL_CALL(fcntl64, fd, cmd, &flk64);
// 			if (ret == -1)
// 				return -1;
// 			if ((off_t)flk->l_start != flk->l_start || (off_t)flk->l_len != flk->l_len)
// 			{
// 				//__set_errno(EOVERFLOW);
// 				errno = EOVERFLOW;
// 				return -1;
// 			}
// 			// flk->l_type = flk64.l_type;
// 			// flk->l_whence = flk64.l_whence;
// 			// flk->l_start = flk64.l_start;
// 			// flk->l_len = flk64.l_len;
// 			// flk->l_pid = flk64.l_pid;
// 			// return ret;
// 			break;
// 		}
// 		case F_DUPFD_CLOEXEC:
// 		{
// 			slog_debug("[POSIX][fcntl], pathname=%s, fd=%d, F_DUPFD_CLOEXEC", pathname, fd);
// 			// ret = real_fcntl(fd, cmd, arg);
// 			ret = dup(fd);
// 			break;
// 		}
// 		/* Since only F_SETLKW{64}/F_OLD_SETLK are cancellation entrypoints and
// 	   only OFD locks require LFS handling, all others flags are handled
// 	   unmodified by calling __NR_fcntl64.  */
// 		default:
// 			slog_debug("[POSIX][fcntl], pathname=%s, fd=%d, default", pathname, fd);
// 			// ret = real_fcntl(fd, cmd, arg);
// 			if (!arg)
// 				ret = real_fcntl(fd, cmd);
// 			else
// 				ret = real_fcntl(fd, cmd, arg);
// 			break;
// 			// return __fcntl64_nocancel_adjusted(fd, cmd, arg);
// 		}
// 		slog_debug("[POSIX]. Ending Hercules 'fcntl', pathname=%s, fd=%d, ret=%d, errno=%d:%s\n", pathname, fd, ret, errno, strerror(errno));
// 	}
// 	else
// 	{
// 		slog_full("[POSIX]. Calling real 'fcntl', fd=%d", fd);
// 		// cmd = FCNTL_ADJUST_CMD(cmd);
// 		// switch (cmd)
// 		// {
// 		// // case F_SETLKW:
// 		// case F_SETLKW64: // release the existence of record locks.
// 		// 	slog_debug("[POSIX][fcntl] fd=%d, F_SETLKW", fd);
// 		// 	break;
// 		// case F_OFD_SETLKW:
// 		// {
// 		// 	struct flock *flk = (struct flock *)arg;
// 		// 	slog_debug("[POSIX][fcntl] fd=%d, F_OFD_SETLKW", fd);
// 		// 	break;
// 		// }
// 		// case F_OFD_GETLK:
// 		// case F_OFD_SETLK:
// 		// {
// 		// 	slog_debug("[POSIX][fcntl] fd=%d, F_OFD_SETLK", fd);
// 		// 	break;
// 		// }
// 		// default:
// 		// 	slog_debug("[POSIX][fcntl] fd=%d, default", fd);
// 		// 	break;
// 		// }
// 		if (!arg)
// 			ret = real_fcntl(fd, cmd);
// 		else
// 			ret = real_fcntl(fd, cmd, arg);
// 		slog_full("[POSIX]. Ending Real 'fcntl', fd=%d, ret=%d", fd, ret);
// 	}

// 	return ret;
// }