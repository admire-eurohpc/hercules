#define _GNU_SOURCE

#include "map.hpp"
#include "mapfd.hpp"
#include "cfg_parse.h"
#include <stdio.h>
#include <stdint.h>
#include <dlfcn.h>
#include <errno.h>
// #include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/xattr.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/vfs.h> // statfs
// #include <limits.h>	 // realpath
#include "imss.h"
#include <imss_posix_api.h>
#include <stdarg.h>

#include "mapprefetch.hpp"
#include <math.h>
#include <sys/utsname.h>

#include <sys/epoll.h>

#undef _FILE_OFFSET_BITS
#undef __USE_LARGEFILE64
#undef __USE_FILE_OFFSET64
#include <dirent.h>
// #ifndef _STAT_VER
// #define _STAT_VER 0
// #endif

#include <fcntl.h>

// #define O_RDONLY 00000000
// #ifndef O_CREAT
// #define O_CREAT 00000100 /* Not fcntl.  */
// #endif
// #ifndef O_EXCL
// #define O_EXCL 00000200 /* not fcntl */
// #endif
// #ifndef O_TRUNC
// #define O_TRUNC 00001000 /* not fcntl */
// #endif

#define KB 1024
#define GB 1073741824
uint32_t deployment = 2; // Default 1=ATACHED, 0=DETACHED ONLY METADATA SERVER 2=DETACHED METADATA AND DATA SERVERS
char *POLICY = "RR";	 // Default RR
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

#define MAX_PATH 256
uint64_t IMSS_DATA_BSIZE;
// char *aux_refresh;
// char *imss_path_refresh;

int LD_PRELOAD = 0;
void *map;
void *map_prefetch;

char MOUNT_POINT[32];
char HERCULES_PATH[256];
void *map_fd;

uint32_t rank = -1;
static int init = 0;
int release = 1;

// log path.
char log_path[1000];
// char pwd_init[1000];
pid_t g_pid = -1;

void getConfiguration();
char *checkHerculesPath(const char *pathname);
char *convert_path(const char *name, char *replace);
static off_t (*real_lseek)(int fd, off_t offset, int whence) = NULL;
static int (*real__lxstat)(int fd, const char *pathname, struct stat *buf) = NULL;
static int (*real__lxstat64)(int ver, const char *pathname, struct stat64 *buf) = NULL;
static int (*real_lstat)(const char *file_name, struct stat *buf) = NULL;
static int (*real_xstat)(int fd, const char *path, struct stat *buf) = NULL;
static int (*real_stat)(const char *pathname, struct stat *buf) = NULL;
static int (*real__xstat64)(int ver, const char *path, struct stat64 *stat_buf) = NULL;
static int (*real_stat64)(const char *__restrict__ pathname, struct stat64 *__restrict__ info) = NULL;
static int (*real_fstat)(int fd, struct stat *buf) = NULL;
static int (*real_fstatat)(int dirfd, const char *pathname, struct stat *buf, int flags) = NULL;
static int (*real__fxstat64)(int ver, int fd, struct stat64 *buf) = NULL;
static int (*real__fxstat)(int ver, int fd, struct stat *buf) = NULL;
static int (*real_close)(int fd) = NULL;
static int (*real_puts)(const char *str) = NULL;
static int (*real__open_2)(const char *pathname, int flags, ...) = NULL;
static int (*real_open64)(const char *pathname, int flags, ...) = NULL;
static int (*real_open)(const char *pathname, int flags, ...) = NULL;
static FILE *(*real_fopen)(const char *restrict pathname, const char *restrict mode) = NULL;
// static FILE *(*real_fopen64)(const char *restrict pathname, const char *restrict mode) = NULL;
static int (*real_access)(const char *pathname, int mode) = NULL;
static int (*real_mkdir)(const char *path, mode_t mode) = NULL;
static ssize_t (*real_write)(int fd, const void *buf, size_t size) = NULL;
static ssize_t (*real_read)(int fd, const void *buf, size_t size) = NULL;
static int (*real_remove)(const char *name) = NULL;
static int (*real_unlink)(const char *name) = NULL;
static int (*real_rmdir)(const char *path) = NULL;
static int (*real_unlinkat)(int fd, const char *name, int flag) = NULL;
static int (*real_rename)(const char *old, const char *new) = NULL;
static int (*real_fchmodat)(int dirfd, const char *pathname, mode_t mode, int flags) = NULL;
static int (*real_fchownat)(int dirfd, const char *pathname, uid_t owner, gid_t group, int flags) = NULL;
static DIR *(*real_opendir)(const char *name) = NULL;
static struct dirent *(*real_readdir)(DIR *dirp) = NULL;
static struct dirent64 *(*real_readdir64)(DIR *dirp) = NULL;
// static int (*real_readdir)(unsigned int fd, struct old_linux_dirent *dirp, unsigned int count);

static int (*real_closedir)(DIR *dirp) = NULL;
static int (*real_statvfs)(const char *restrict path, struct statvfs *restrict buf) = NULL;
static int (*real_statfs)(const char *path, struct statfs *buf) = NULL;
static char *(*real_realpath)(const char *restrict path, char *restrict resolved_path) = NULL;
// static int (*real__openat)(int dirfd, const char *pathname, int flags, ...) = NULL;
// static int (*real_openat)(int dirfd, const char *pathname, int flags, ...) = NULL;
// static int (*real__openat64)(int fd, const char *file, int oflag, ...) = NULL;
// static int (*real__openat64_2)(int fd, const char *file, int oflag) = NULL;
// static int (*real__libc_openat)(int fd, const char *file, int oflag, ...) = NULL;
static int (*real__libc_open64)(const char *file, int oflag, ...) = NULL;
// static int (*real_openat)(int dirfd, const char *pathname, int flags) = NULL;
static int (*real_fclose)(FILE *fp) = NULL;
static size_t (*real_fread)(void *buf, size_t size, size_t count, FILE *fp) = NULL;
static size_t (*real_fwrite)(const void *buf, size_t size, size_t count, FILE *fp) = NULL;
static int (*real_ferror)(FILE *fp) = NULL;
static int (*real_feof)(FILE *fp) = NULL;

static void *(*real_mmap)(void *addr, size_t length, int prot, int flags, int fd, off_t offset) = NULL;

static int (*real_symlink)(const char *name1, const char *name2) = NULL;

static int (*real_symlinkat)(const char *name1, int fd, const char *name2) = NULL;

static int (*real_chdir)(const char *path) = NULL;
static int (*real_fchdir)(int fd) = NULL;
static int (*real__chdir)(const char *path) = NULL;
static int (*real___chdir)(const char *path) = NULL;
static int (*real_sys_chdir)(const char *filename) = NULL;
static int (*real_wchdir)(const wchar_t *dirname) = NULL;

static int (*real_chmod)(const char *pathname, mode_t mode) = NULL;
static int (*real_fchmod)(int fd, mode_t mode) = NULL;

// static int (*real_execve)(const char *pathname, char *const argv[], char *const envp[]) = NULL;
// static int (*real_execv)(const char *pathname, char *const argv[]) = NULL;

static char *(*real_getcwd)(char *buf, size_t size) = NULL;

static int (*real_change_to_directory)(char *, int, int) = NULL;
static int (*real_bindpwd)(int) = NULL;

static int (*real_epoll_ctl)(int epfd, int op, int fd, struct epoll_event *event) = NULL;

static pid_t (*real_fork)(void) = NULL;

static int (*real___fwprintf_chk)(FILE *stream, int flag, const wchar_t *format) = NULL;

static ssize_t (*real_pwrite)(int fd, const void *buf, size_t count, off_t offset) = NULL;

static int (*real_truncate)(const char *path, off_t length) = NULL;
static int (*real_ftruncate)(int fd, off_t length) = NULL;

static int (*real_flock)(int fd, int operation) = NULL; 

int epoll_ctl(int epfd, int op, int fd, struct epoll_event *event)
{
	real_epoll_ctl = dlsym(RTLD_NEXT, "epoll_ctl");
	// fprintf(stderr, "Calling 'epoll_ctl'\n");
	return real_epoll_ctl(epfd, op, fd, event);
}

static int change_to_directory(char *newdir, int nolinks, int xattr)
{
	real_change_to_directory = dlsym(RTLD_NEXT, "change_to_directory");
	fprintf(stderr, "Calling change_to_directory\n");
	return real_change_to_directory(newdir, nolinks, xattr);
}

static int bindpwd(int no_symlinks)
{
	real_bindpwd = dlsym(RTLD_NEXT, "bindpwd");
	fprintf(stderr, "Calling bindpwd\n");
	return real_bindpwd(no_symlinks);
}

int sys_chdir(const char *filename)
{
	real_sys_chdir = dlsym(RTLD_NEXT, "sys_chdir");
	fprintf(stderr, "Calling sys_chdir\n");
	return real_sys_chdir(filename);
}

int _wchdir(const wchar_t *dirname)
{
	real_wchdir = dlsym(RTLD_NEXT, "wchdir");
	fprintf(stderr, "Calling _wchdir\n");
	return real_wchdir(dirname);
}

char *getcwd(char *buf, size_t size)
{
	real_getcwd = dlsym(RTLD_NEXT, "getcwd");
	// fprintf(stderr, "Calling getcwd, size=%ld\n", size);
	// buf = real_getcwd(buf, size);
	// *** buf = getenv("PWD");
	// fprintf(stderr, "End getcwd, buf=%s\n", buf);
	// ***return buf;
	return real_getcwd(buf, size);
}

int chdir(const char *path)
{
	real_chdir = dlsym(RTLD_NEXT, "chdir");

	if (!init)
	{
		// fprintf(stderr, "Calling chdir, pathname=%s\n", path);
		// slog_debug("Calling chdir, pathname=%s", path);
		return real_chdir(path);
	}

	// char *workdir = getenv("PWD");
	int ret = 0;
	if (!strncmp(path, MOUNT_POINT, strlen(MOUNT_POINT)))
	// if (!strncmp(path, MOUNT_POINT, strlen(MOUNT_POINT)) || !strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT)))
	{
		fprintf(stderr, "[%d] Calling Hercules 'chdir', pathname=%s\n", rank, path);
		slog_debug("Calling Hercules 'chdir', pathname=%s", path);
		setenv("PWD", path, 1);
		// ret = setenv("PWD", path, 1);
		// setenv("PWD_HERCULES", MOUNT_POINT, 1);
		slog_debug("End Hercules 'chdir', pathname=%s, ret=%d", path, ret);
		fprintf(stderr, "[%d] End Hercules 'chdir', pathname=%s\n", rank, path);

		// ret = 0;
	}
	else
	{
		fprintf(stderr, "Calling Real 'chdir', pathname=%s\n", path);
		ret = real_chdir(path);
		fprintf(stderr, "End Real 'chdir', pathname=%s, ret=%d\n", path, ret);
	}

	return ret;
}

int fchdir(int fd)
{
	real_fchdir = dlsym(RTLD_NEXT, "fchdir");

	if (init)
	{
		slog_debug("Calling fchdir");
	}

	return real_fchdir(fd);
}

int __chdir(const char *path)
{
	real___chdir = dlsym(RTLD_NEXT, "__chdir");

	if (init)
	{
		slog_debug("Calling __chdir, pathname=%s", path);
	}

	return real___chdir(path);
}

int _chdir(const char *path)
{
	real__chdir = dlsym(RTLD_NEXT, "_chdir");
	fprintf(stderr, "Calling _chdir, pathname=%s", path);

	if (init)
	{
		slog_debug("Calling _chdir, pathname=%s", path);
	}

	return real__chdir(path);
}

// int _openat(int dirfd, const char *pathname, int flags, ...)
// {
// 	real__openat = dlsym(RTLD_NEXT, "_openat");
// 	int mode = 0;

// 	if (flags & O_CREAT)
// 	{
// 		va_list ap;
// 		va_start(ap, flags);
// 		mode = va_arg(ap, unsigned);
// 		va_end(ap);
// 	}

// 	fprintf(stderr, "Calling _openat %s\n", pathname);
// 	return real__openat(dirfd, pathname, flags);
// }

// static int (*real_fsync)(int fd) = NULL;

void checkOpenFlags(const char *pathname, int flags)
{
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
}

char *checkHerculesPath(const char *pathname)
{
	char *new_path = NULL;
	char *workdir = getenv("PWD");
	// fprintf(stderr,"\t[IMSS] Checking %s\n", pathname);
	if (!strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT) - 1) || (pathname[0] != '/' && !strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT) - 1)))
	{
		// if (pathname[0] == '.')
		if (!strncmp(pathname, ".", strlen(pathname)))
		{
			// slog_debug("[IMSS][checkHerculesPath] workdir=%s", workdir);
			new_path = convert_path(workdir, MOUNT_POINT);
		}
		else if (!strncmp(pathname, "./", strlen("./")))
		{
			// slog_debug("[IMSS][checkHerculesPath] ./ case=%s", pathname);
			new_path = convert_path(pathname + strlen("./"), MOUNT_POINT);
		}
		else
		{
			new_path = convert_path(pathname, MOUNT_POINT);
		}
		// fprintf(stderr, "[POSIX][checkHerculesPath]. workdir=%s, pathname=%s, new_path=%s\n", workdir, pathname, new_path);
	}
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

char *convert_path(const char *name, char *replace)
{
	char *path = calloc(256, sizeof(char));
	strcpy(path, name);
	// printf("path=%s\n",path);
	size_t len = strlen(MOUNT_POINT);
	if (len > 0)
	{
		char *p = path;
		while ((p = strstr(p, MOUNT_POINT)) != NULL)
		{
			memmove(p, p + len, strlen(p + len) + 1);
		}
	}

	char *new_path = calloc(256, sizeof(char));

	if (!strncmp(path, "/", strlen("/")))
	{
		strcat(new_path, "imss:/");
	}
	else
	{
		strcat(new_path, "imss://");
	}

	strcat(new_path, path);
	return new_path;
}

__attribute__((constructor)) void imss_posix_init(void)
{
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
	getConfiguration();

	// fprintf(stderr, "IMSS_DEBUG_LEVEL=%d\n", IMSS_DEBUG_LEVEL);

	IMSS_DATA_BSIZE = IMSS_BLKSIZE * KB;
	// aux_refresh = (char *)malloc(IMSS_DATA_BSIZE); // global buffer to refresh metadata.
	// imss_path_refresh = calloc(MAX_PATH, sizeof(char));
	// Hercules init -- Attached deploy
	if (deployment == 1)
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

	sprintf(log_path, "%s/client.%02d-%02d.%d", HERCULES_PATH, tm.tm_hour, tm.tm_min, rank);
	// sprintf(log_path, "./client.%02d-%02d-%02d.%d", tm.tm_hour, tm.tm_min, tm.tm_sec, rank);
	slog_init(log_path, IMSS_DEBUG_LEVEL, IMSS_DEBUG_FILE, IMSS_DEBUG_SCREEN, 1, 1, 1, rank);
	slog_info(",Time(msec), Comment, RetCode");

	slog_debug(" -- IMSS_MOUNT_POINT: %s", MOUNT_POINT);
	slog_debug(" -- IMSS_ROOT: %s", IMSS_ROOT);
	slog_debug(" -- IMSS_HOSTFILE: %s", IMSS_HOSTFILE);
	slog_debug(" -- IMSS_N_SERVERS: %d", N_SERVERS);
	slog_debug(" -- IMSS_SRV_PORT: %d", IMSS_SRV_PORT);
	slog_debug(" -- IMSS_BUFFSIZE: %ld", IMSS_BUFFSIZE);
	slog_debug(" -- META_HOSTFILE: %s", META_HOSTFILE);
	slog_debug(" -- IMSS_META_PORT: %d", METADATA_PORT);
	slog_debug(" -- IMSS_META_SERVERS: %d", N_META_SERVERS);
	slog_debug(" -- IMSS_BLKSIZE: %ld", IMSS_BLKSIZE);
	slog_debug(" -- IMSS_STORAGE_SIZE: %ld", STORAGE_SIZE);
	slog_debug(" -- IMSS_METADATA_FILE: %s", METADATA_FILE);
	slog_debug(" -- IMSS_DEPLOYMENT: %d", deployment);
	slog_debug(" -- IMSS_MALLEABILITY: %d", MALLEABILITY);
	slog_debug(" -- UPPER_BOUND_SERVERS: %d", UPPER_BOUND_SERVERS);
	slog_debug(" -- LOWER_BOUND_SERVERS: %d", LOWER_BOUND_SERVERS);
	slog_debug(" -- REPL_FACTOR: %d", REPL_FACTOR);

	// Metadata server
	if (stat_init(META_HOSTFILE, METADATA_PORT, N_META_SERVERS, rank) == -1)
	{
		// In case of error notify and exit
		slog_error("Stat init failed, cannot connect to Metadata server.");
	}

	if (deployment == 2)
	{
		ret = open_imss(IMSS_ROOT);
		if (ret < 0)
		{
			slog_fatal("Error creating IMSS's resources, the process cannot be started");
			return;
		}
	}

	if (deployment != 2)
	{
		// Initialize the IMSS servers
		if (init_imss(IMSS_ROOT, IMSS_HOSTFILE, META_HOSTFILE, N_SERVERS, IMSS_SRV_PORT, IMSS_BUFFSIZE, deployment, "hercules_server", METADATA_PORT) < 0)
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
	// fprintf(stderr, "[CLIENT %d] ready!\n", rank);
	init = 1;
}

void getConfiguration()
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
			fprintf(stderr, "[CLIENT %d] Configuration file not found\n", rank);
			perror("ERRIMSS_CONF_NOT_FOUND");
			return;
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
		deployment = atoi(getenv("IMSS_DEPLOYMENT"));
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
}

void __attribute__((destructor)) run_me_last()
{
	// fprintf(stderr, "\t[%d] Run me last..., pid=%d, release=%d\n", rank, g_pid, release);
	slog_live("Calling 'run_me_last', pid=%d, release=%d", g_pid, release);
	if (release)
	{
		// fprintf(stderr, "\t[%ld] Release..., pid=%d\n", rank, g_pid);
		release = -1;
		// char *workdir = getenv("PWD");
		// slog_live("[%ld]********************** Calling 'run_me_last', pid=%d **********************\n", rank, g_pid);

		// if (!strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT)))
		// {
		slog_live("[POSIX] release_imss()");
		release_imss("imss://", CLOSE_DETACHED);
		slog_live("[POSIX] stat_release()");
		stat_release();
		// }
		// fprintf(stderr, "\tWaiting...\n");
		// // sleep(20);
		// fprintf(stderr, "\tFinish...\n");
		imss_comm_cleanup();

		// slog_live("********************** End 'run_me_last' **********************\n");
	}
	slog_live("End 'run_me_last', pid=%d, release=%d", g_pid, release);
}

void check_ld_preload(void)
{

	if (LD_PRELOAD == 0)
	{
		DPRINT("\nActivating... ld_preload=%d\n\n", LD_PRELOAD);
		LD_PRELOAD = 1;
		imss_posix_init();
	}
}

int close(int fd)
{
	real_close = dlsym(RTLD_NEXT, "close");
	if (!init)
	{
		return real_close(fd);
	}

	clock_t t;
	t = clock();
	int ret = 0;
	errno = 0;

	char *path = (char *)calloc(256, sizeof(char));
	if (TIMING(map_fd_search_by_val(map_fd, path, fd), "[POSIX] Searching path by fd", int) == 1)
	{
		slog_debug("[POSIX]. Calling Hercules 'close', pathname=%s, fd=%d, errno=%d:%s", path, fd, errno, strerror(errno));
		ret = TIMING(imss_close(path), "imss_close", int);
		slog_debug("[POSIX]. Ending Hercules 'close', pathname=%s, ret=%d, errno=%d:%s", path, ret, errno, strerror(errno));

		map_fd_update_value(map_fd, path, fd, 0);
		// fprintf(stderr, "[POSIX] Closing %s in Hercules, ret=%d, fd=%d, errno=%d:%s\n", path, ret, fd, errno, strerror(errno));
		// fprintf(stderr,"*** CLOSE ***\n");
		//  t = clock() - t;
		//  double time_taken = ((double)t) / CLOCKS_PER_SEC; // in seconds
		//  slog_info("[POSIX %d] close time total %f s", rank, time_taken);
	}
	else
	{
		slog_debug("[POSIX]. Calling Real 'close', fd=%d", fd);
		ret = real_close(fd);
		slog_debug("[POSIX]. Ending Real 'close', ret=%d", ret);
	}
	free(path);
	// fprintf(stderr, "[POSIX] ret=%d, fd=%d, errno=%d:%s\n", ret, fd, errno, strerror(errno));
	return ret;
}

int __lxstat(int fd, const char *pathname, struct stat *buf)
{

	real__lxstat = dlsym(RTLD_NEXT, "__lxstat");

	if (!init)
	{
		return real__lxstat(fd, pathname, buf);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		unsigned long p = 0;
		slog_debug("[POSIX]. Calling Hercules '__lxstat', pathname=%s, new_path=%s, fd=%d, errno=%d:%s", pathname, new_path, fd, errno, strerror(errno));
		imss_refresh(new_path);
		ret = imss_getattr(new_path, buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
		}
		slog_debug("[POSIX]. End Hercules '__lxstat', ret=%d, errno=%d:%s, file_size=%lu", ret, errno, strerror(errno), buf->st_size);
		free(new_path);
	}
	else
	{
		slog_debug("[POSIX]. Calling Real '__lxstat', pathname=%s", pathname);
		ret = real__lxstat(fd, pathname, buf);
		slog_debug("[POSIX]. End Real '__lxstat', pathname=%s", pathname);
	}

	return ret;
}

int __lxstat64(int fd, const char *pathname, struct stat64 *buf)
{
	real__lxstat64 = dlsym(RTLD_NEXT, "__lxstat64");

	if (!init)
	{
		return real__lxstat64(fd, pathname, buf);
	}

	errno = 0;
	int ret = 0;
	unsigned long p = 0;
	// char *workdir = getenv("PWD");
	// if (!strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT)))
	// // if (!strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT)) || !strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT)))
	// {
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_debug("[POSIX %d]. Calling Hercules '__lxstat64', pathname=%s.", rank, pathname);

		// char *new_path;
		// new_path = convert_path(pathname, MOUNT_POINT);
		// int exist = map_fd_search(map_fd, new_path, &ret, &p);
		imss_refresh(new_path);
		ret = imss_getattr(new_path, buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
		}
		slog_debug("[POSIX %d]. End Hercules '__lxstat64', ret=%d, errno=%d.", rank, ret, errno);
		free(new_path);
	}
	else
	{
		slog_debug("[POSIX %d]. Calling Real '__lxstat64', pathname=%s.", rank, pathname);
		ret = real__lxstat64(fd, pathname, buf);
		slog_debug("[POSIX %d]. End Real '__lxstat64', pathname=%s.", rank, pathname);
	}

	return ret;
}

int __xstat(int fd, const char *pathname, struct stat *buf)
{
	real_xstat = dlsym(RTLD_NEXT, "__xstat");

	if (!init)
	{
		return real_xstat(fd, pathname, buf);
	}

	errno = 0;

	// clock_t t;
	// t = clock();

	// if(!strcmp(pathname, MOUNT_POINT) || (!strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT)-1) && strlen(pathname)== strlen(MOUNT_POINT)-1)) {
	// 	return 0;
	// }
	int ret = -1;
	unsigned long p = 0;
	// char *workdir = getenv("PWD");
	// slog_debug("[__xstat] pwd=%s", workdir);
	// // if (!strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT)))
	// // if (!strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT)) || (pathname[0] != '/' && !strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT))))
	// if (!strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT)) || (pathname[0] != '/' && !strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT))))
	// {
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{

		slog_debug("[POSIX] Calling Hercules '__xstat', pathname=%s, fd=%d, new_path=%s", pathname, fd, new_path);
		// slog_debug("[_xstat] new_path=%s", new_path);
		// int exist = map_fd_search(map_fd, new_path, &ret, &p);
		imss_refresh(new_path);
		ret = imss_getattr(new_path, buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_error("[POSIX] Error Hercules '__xstat': %s", strerror(errno));
		}

		// slog_info("[LD_PRELOAD] _xstat time  total %f s", time_taken);
		slog_debug("[POSIX] End Hercules '__xstat', pathname=%s, fd=%d, new_path=%s, errno=%d:%s, ret=%d", pathname, fd, new_path, errno, strerror(errno), ret);
		free(new_path);
	}
	else
	{
		slog_debug("[POSIX]. Calling Real '__xstat', pathname=%s, fd=%d.", pathname, fd);
		ret = real_xstat(fd, pathname, buf);
		slog_debug("[POSIX]. End Real '__xstat', pathname=%s, fd=%d, errno=%d, ret=%d.", pathname, fd, errno, ret);
	}
	// slog_debug("Stat->dev=%d, buf->st_ino=%d", buf->st_dev, buf->st_ino);
	return ret;
}

pid_t fork(void)
{
	real_fork = dlsym(RTLD_NEXT, "fork");

	pid_t pid = real_fork();
	g_pid = pid;

	if (pid != 0)
	{
		release = 0;
		// fprintf(stderr, "[%d] Calling Real 'fork', pid=%d\n", rank, pid);

		char hostname_[512], hostname[1024];
		int ret = gethostname(&hostname_[0], 512);
		if (ret == -1)
		{
			perror("gethostname");
			exit(EXIT_FAILURE);
		}
		sprintf(hostname, "%s:%d", hostname_, pid);

		int new_rank = MurmurOAAT32(hostname);

		// fill global variables with the enviroment variables value.
		getConfiguration();

		time_t t = time(NULL);
		struct tm tm = *localtime(&t);
		// char *workdir = getenv("PWD");
		sprintf(log_path, "%s/client-child.%02d-%02d.%d", HERCULES_PATH, tm.tm_hour, tm.tm_min, new_rank);
		// sprintf(log_path, "./client.%02d-%02d-%02d.%d", tm.tm_hour, tm.tm_min, tm.tm_sec, rank);

		slog_init(log_path, IMSS_DEBUG_LEVEL, IMSS_DEBUG_FILE, IMSS_DEBUG_SCREEN, 1, 1, 1, new_rank);

		// fprintf(stderr, "[%d] End Real 'fork', pid=%d, new_rank=%d, log_path=%s\n", rank, pid, new_rank, log_path);
	}

	return pid;
}

int lstat(const char *pathname, struct stat *buf)
{
	// unsigned long p = 0;
	// char *workdir = getenv("PWD");
	// fprintf(stderr, "[POSIX %d]. Calling 'lstat'.\n", rank);
	real_lstat = dlsym(RTLD_NEXT, "lstat");

	if (!init)
	{
		return real_lstat(pathname, buf);
	}

	int ret;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		// {
		// int exist = map_fd_search(map_fd, new_path, &ret, &p);
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
		ret = real_lstat(pathname, buf);
	}

	return ret;
}

int stat(const char *pathname, struct stat *buf)
{
	// unsigned long p = 0;
	// char *workdir = getenv("PWD");
	real_stat = dlsym(RTLD_NEXT, "stat");

	if (!init)
	{
		return real_stat(pathname, buf);
	}

	int ret;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_debug("[POSIX %d]. Calling Hercules 'stat', new_path=%s.", rank, new_path);
		// int exist = map_fd_search(map_fd, new_path, &ret, &p);
		imss_refresh(new_path);
		ret = imss_getattr(new_path, buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
		}
		slog_debug("[POSIX %d]. Ending Hercules 'stat', new_path=%s, errno=%d:%s", rank, new_path, errno, strerror(errno));
		free(new_path);
	}
	else
	{
		slog_debug("[POSIX %d]. Calling Real 'stat', pathname=%s.", rank, pathname);
		ret = real_stat(pathname, buf);
		slog_debug("[POSIX %d]. Ending Real 'stat', pathname=%s.", rank, pathname);
	}

	return ret;
}

int statvfs(const char *restrict path, struct statvfs *restrict buf)
{
	real_statvfs = dlsym(RTLD_NEXT, "statvfs");

	// fprintf(stderr, "[POSIX] Calling 'statvfs', path=%s, init=%d\n", path, init);

	if (!init)
	{
		return real_statvfs(path, buf);
	}

	// char *workdir = getenv("PWD");
	// if (!strncmp(path, MOUNT_POINT, strlen(MOUNT_POINT)))
	// // if (!strncmp(path, MOUNT_POINT, strlen(MOUNT_POINT)) || !strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT)))
	// {
	char *new_path = checkHerculesPath(path);
	if (new_path != NULL)
	{

		fprintf(stderr, "[POSIX] Calling Hercules 'statvfs', path=%s, init=%d, new_path=%s\n", path, init, new_path);
		slog_debug("[POSIX %d]. Calling Hercules 'statvfs', path=%s.", rank, path);

		buf->f_bsize = IMSS_BLKSIZE * KB;
		buf->f_namemax = URI_;
		slog_debug("[POSIX %d]. End Hercules 'statvfs', path=%s.", rank, path);
		free(new_path);
		return 0;
	}
	else
	{
		// fprintf(stderr, "[POSIX] Calling Real 'statvfs', path=%s, init=%d\n", path, init);
		slog_debug("[POSIX %d]. Calling Real 'statvfs', path=%s.", rank, path);
		return real_statvfs(path, buf);
		slog_debug("[POSIX %d]. Ending Real 'statvfs', path=%s.", rank, path);
	}
}

int statfs(const char *restrict path, struct statfs *restrict buf)
{
	// fprintf(stderr, "[POSIX]. Calling 'statfs', path=%s, init=%d\n", path, init);
	real_statfs = dlsym(RTLD_NEXT, "statfs");
	// if (real_statfs)
	//{
	//	fprintf(stderr, "dlsym works\n");
	// }
	if (!init)
	{
		return real_statfs(path, buf);
	}

	// char *workdir = getenv("PWD");
	// slog_debug("[POSIX %d]. Calling 'statfs', path=%s.", rank, path);
	int ret = 0;

	// if (!strncmp(path, MOUNT_POINT, strlen(MOUNT_POINT)))
	// // if (!strncmp(path, MOUNT_POINT, strlen(MOUNT_POINT)) || !strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT)))
	// {
	char *new_path = checkHerculesPath(path);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'statfs', path=%s, new_path=%s", path, new_path);
		// fprintf(stderr, "[POSIX %d]. 'statfs', buf->f_type=%ld, buf->f_bsize=%ld.\n", rank, buf->f_type,  buf->f_bsize);
		// int ret = real_statfs(path, buf);
		buf->f_bsize = IMSS_BLKSIZE * KB;
		buf->f_namelen = URI_;
		// fprintf(stderr, "[POSIX %d]. Calling IMSS 'statfs', ret=%d.\n", rank, ret);
		// return ret;
		// fprintf(stderr, "[POSIX %d]. 'statfs', buf->f_type=%ld, buf->f_bsize=%ld.\n", rank, buf->f_type,  buf->f_bsize);
		// buf->f_type =
		// buf->f_bsize = IMSS_BLKSIZE * KB;
		// buf->f_namelen = URI_;
		// return 0;
		slog_debug("[POSIX]. Ending Hercules 'statfs', path=%s.", path);
		free(new_path);
	}
	else
	{
		slog_debug("[POSIX %d]. Calling Real 'statfs', path=%s.", rank, path);
		ret = real_statfs(path, buf);
		slog_debug("[POSIX %d]. Ending Real 'statfs', path=%s.", rank, path);
	}
	// fprintf(stderr, "[POSIX %d]. Exit 'statfs', path=%s.\n", rank, path);
	return ret;
}

int __xstat64(int ver, const char *pathname, struct stat64 *stat_buf)
{
	real__xstat64 = dlsym(RTLD_NEXT, "__xstat64");

	if (!init)
	{
		// slog_debug("[POSIX %d] Calling Real '__xstat64', pathname=%s", rank, pathname);
		return real__xstat64(ver, pathname, stat_buf);
	}

	int ret;
	unsigned long p = 0;
	// char *workdir = getenv("PWD");

	// if (!strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT)))
	// // if (!strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT)) || !strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT)))
	// {
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules '__xstat64', pathname=%s, new_path=%s", pathname, new_path);
		// char *new_path;
		// new_path = convert_path(pathname, MOUNT_POINT);
		// int exist = map_fd_search(map_fd, new_path, &ret, &p);
		imss_refresh(new_path);
		ret = imss_getattr(new_path, stat_buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
		}
		slog_debug("[POSIX]. Ending Hercules '__xstat64', pathname=%s, ret=%d, errno=%d:%s.", pathname, ret, errno, strerror(errno));
		free(new_path);
	}
	else
	{
		slog_debug("[POSIX]. Calling Real '__xstat64', pathname=%s.", pathname);
		ret = real__xstat64(ver, pathname, stat_buf);
		slog_debug("[POSIX]. Ending Real '__xstat64', pathname=%s, ret=%d.", pathname, ret);
	}

	return ret;
}

char *realpath(const char *path, char *resolved_path)
{
	real_realpath = dlsym(RTLD_NEXT, "realpath");
	fprintf(stderr, "Calling realpath\n");
	if ((resolved_path != NULL) && (resolved_path[0] == '\0'))
	{
		fprintf(stderr, "[POSIX] Calling Real 'realpath', path=%s, resolved_path=%s\n", path, resolved_path);
	}
	else
	{
		fprintf(stderr, "[POSIX] Calling Real 'realpath', path=%s\n", path);
	}

	if (init)
	{
		slog_debug("[POSIX]. Calling Real 'realpath', path=%s.", path);
	}
	char *p;
	p = real_realpath(path, resolved_path);

	fprintf(stderr, "[POSIX] Ending Real 'realpath', path=%s\n", path);
	return p;
}

int __open_2(const char *pathname, int flags, ...)
{
	real__open_2 = dlsym(RTLD_NEXT, "__open_2");

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

	int ret = 0;
	errno = 0;

	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		uint64_t ret_ds = 0;
		unsigned long p = 0;
		slog_debug("[POSIX]. Calling Hercules '__open_2', new_path=%s.", new_path);

		checkOpenFlags(pathname, flags);

		int exist = map_fd_search(map_fd, new_path, &ret, &p);
		if (exist == -1)
		{
			int create_flag = (flags & O_CREAT);
			if (create_flag == O_CREAT)
			{
				slog_debug("[POSIX %d]. O_CREAT", rank);
				int err_create = imss_create(new_path, mode, &ret_ds);
				if (err_create == -EEXIST)
				{
					ret = TIMING(imss_open(new_path, &ret_ds), "imss_open O_CREAT", int);
				}
			}
			else
			{
				slog_debug("[POSIX %d]. Not O_CREAT", rank);
				ret = TIMING(imss_open(new_path, &ret_ds), "imss_open Not O_CREAT", int);

				if (ret < 0)
				{
					errno = -ret;
					ret = -1;
				}
				// If we get a "ret_ds" equal to "-2", we are in the case of symbolic link pointing to a file stored in the system.
				if (ret_ds == -2)
				{
					// Calling the real open.
					if (!mode)
						ret = real__open_2(new_path, flags);
					else
						ret = real__open_2(new_path, flags, mode);
					// stores the file descriptor "ret" into the map "map_fd".
					map_fd_put(map_fd, new_path, ret, p); // TO CHECK!
														  // free(new_path);
														  // return ret;
				}
			}
			if (ret > -1)
			{
				ret = real__open_2("/dev/null", 0); // Get a file descriptor
				// stores the file descriptor "ret" into the map "map_fd".
				map_fd_put(map_fd, new_path, ret, p); // TO CHECK!
			}
		}
		slog_debug("[POSIX]. Ending Hercules '__open_2', new_path=%s, ret=%d, errno=%d:%s", new_path, ret, errno, strerror(errno));
		free(new_path);
	}
	else
	{
		slog_debug("Calling Real '__open_2', pathname=%s\n", pathname);
		if (!mode)
			ret = real__open_2(pathname, flags);
		else
			ret = real__open_2(pathname, flags, mode);
	}
	return ret;
}

int open64(const char *pathname, int flags, ...)
{
	real_open64 = dlsym(RTLD_NEXT, "open64");

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

	int ret;
	errno = 0;

	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		uint64_t ret_ds;
		unsigned long p = 0;
		slog_debug("[POSIX]. Calling Hercules 'open64', pathname=%s, new_path=%s", pathname, new_path);

		checkOpenFlags(pathname, flags);

		int exist = map_fd_search(map_fd, new_path, &ret, &p);
		if (exist == -1) // if the "new_path" was not find in the local map:
		{
			int create_flag = (flags & O_CREAT);
			if (create_flag == O_CREAT) // if the file does not exist, then we create it.
			{
				int err_create = imss_create(new_path, mode, &ret_ds);
				if (err_create == -EEXIST)
				{
					ret = imss_open(new_path, &ret_ds);
				}
			}
			else // the file must exist.
			{
				ret = imss_open(new_path, &ret_ds);
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
						ret = real_open64(new_path, flags);
					else
						ret = real_open64(new_path, flags, mode);
					// stores the file descriptor "ret" into the map "map_fd".
					map_fd_put(map_fd, new_path, ret, p); // TO CHECK!
														  // free(new_path);
														  // return ret;
				}
			}
			if (ret > -1)
			{
				ret = real_open64("/dev/null", 0); // Get a file descriptor.
				// stores the file descriptor "ret" into the map "map_fd".
				map_fd_put(map_fd, new_path, ret, p); // TO CHECK!
			}
		}
		slog_debug("[POSIX]. Ending Hercules 'open64', pathname=%s, fd=%ld.", pathname, ret);
		free(new_path);
	}
	else
	{
		slog_debug("[POSIX %d]. Calling Real 'open64', pathname=%s.", rank, pathname);
		if (!mode)
			ret = real_open64(pathname, flags);
		else
			ret = real_open64(pathname, flags, mode);
		slog_debug("[POSIX %d]. Ending Real 'open64', pathname=%s.", rank, pathname);
	}
	return ret;
}

int flock(int fd, int operation) {
	real_flock = dlsym(RTLD_NEXT, "flock");
	if (!init)
	{
		return real_flock(fd, operation);
	}

	int ret = 0;
	char *pathname = (char *)calloc(256, sizeof(char));
	if (map_fd_search_by_val(map_fd, pathname, fd) == 1)
	{
		fprintf(stderr, "[POSIX]. Calling Hercules 'flock', pathname=%s\n", pathname);
		// imss_close(pathname);
		fprintf(stderr,"[POSIX]. Ending Hercules 'flock', pathname=%s\n", pathname);
	}
	else
	{
		ret = real_flock(fd, operation);
	}

	free(pathname);
	return ret;
}

int fclose(FILE *fp)
{
	real_fclose = dlsym(RTLD_NEXT, "fclose");
	if (!init)
	{
		return real_fclose(fp);
	}

	int ret = 0;
	char *pathname = (char *)calloc(256, sizeof(char));
	if (map_fd_search_by_val(map_fd, pathname, fp->_fileno) == 1)
	{
		slog_debug("[POSIX]. Calling Hercules 'fclose', pathname=%s", pathname);
		imss_close(pathname);
		slog_debug("[POSIX]. Ending Hercules 'fclose' pathname=%s",  pathname);
		free(fp);
	}
	else
	{
		ret = real_fclose(fp);
	}

	free(pathname);

	return ret;
}

size_t fread(void *buf, size_t size, size_t count, FILE *fp)
{
	real_fread = dlsym(RTLD_NEXT, "fread");

	if (!init)
	{
		return real_fread(buf, size, count, fp);
	}

	size_t ret;
	unsigned long p = 0;
	char *path = (char *)calloc(256, sizeof(char));

	if (map_fd_search_by_val(map_fd, path, fp->_fileno) == 1)
	{
		slog_debug("[POSIX]. Calling Hercules 'fread', path=%s", path);

		// printf("CUSTOM read worked! path=%s fd=%d, size=%ld\n",path, fd, size);
		map_fd_search(map_fd, path, &fp->_fileno, &p);
		ret = imss_read(path, buf, count, p);
		slog_debug("[POSIX]. End Hercules 'fread'  %ld", ret);
		if (ret < count - 1) // FIX: Python read.
			ret = 0;
	}
	else
	{
		ret = real_fread(buf, size, count, fp);
	}
	free(path);

	return ret;

	// return real_fread;
}

size_t fwrite(const void *buf, size_t size, size_t count, FILE *fp)
{
	real_fwrite = dlsym(RTLD_NEXT, "fwrite");

	if (!init)
	{
		return real_fwrite(buf, size, count, fp);
	}

	size_t ret = -1;
	unsigned long p = 0;
	char *path = (char *)calloc(256, sizeof(char));
	if (map_fd_search_by_val(map_fd, path, fp->_fileno) == 1)
	{
		slog_debug("[POSIX]. Calling Hercules 'fwrite', path=%s", path);

		// struct stat ds_stat_n;
		// ret = imss_getattr(path, &ds_stat_n);
		// if (ret < 0)
		// {
		// 	errno = -ret;
		// 	ret = 0;
		// 	slog_error("[POSIX] Error Hercules '__fxstat', errno=%d:%s", errno, strerror(errno));
		// 	free(path);
		// 	return ret;
		// }
		map_fd_search(map_fd, path, &fp->_fileno, &p);
		// printf("CUSTOM write worked! path=%s fd=%d, size=%ld offset=%ld, buffer=%s\n", path, fd, size, p, buf);
		ret = imss_write(path, buf, count, p);
		// imss_release(path);
		slog_debug("[POSIX %d]. Ending Hercules 'fwrite', ret=%ld,  errno=%d:%s", ret, path, errno, strerror(errno));
	}
	else
	{
		ret = real_fwrite(buf, size, count, fp);
	}
	free(path);

	return ret;
}

int ferror(FILE *fp)
{
	real_ferror = dlsym(RTLD_NEXT, "ferror");
	if (!init)
	{
		return real_ferror(fp);
	}

	int ret = 0;
	char *pathname = (char *)calloc(256, sizeof(char));
	if (map_fd_search_by_val(map_fd, pathname, fp->_fileno) == 1)
	{
		// ret = real_ferror(fp);
		slog_debug("[POSIX]. Calling Hercules 'ferror', pathname=%s", pathname);
		return 0;
		// slog_debug("[POSIX %d]. Ending 'ferror' %s.\n", rank, pathname);
	}
	else
	{
		ret = real_ferror(fp);
	}

	free(pathname);

	return ret;
}

int feof(FILE *fp)
{
	real_feof = dlsym(RTLD_NEXT, "feof");
	if (!init)
	{
		return real_feof(fp);
	}

	int ret = 0;
	char *pathname = (char *)calloc(256, sizeof(char));
	if (map_fd_search_by_val(map_fd, pathname, fp->_fileno) == 1)
	{
		// ret = real_ferror(fp);
		slog_debug("[POSIX]. Calling Hercules 'feof', pathname=%s", pathname);
		return 0;
		// slog_debug("[POSIX %d]. Ending Hercules 'feof' %s.\n", rank, pathname);
	}
	else
	{
		ret = real_feof(fp);
	}

	free(pathname);

	return ret;
}

// static FILE *fopen(const char *restrict pathname, const char *restrict mode)
FILE *fopen(const char *restrict pathname, const char *restrict mode)
{
	real_fopen = dlsym(RTLD_NEXT, "fopen");

	if (!init)
	{
		return real_fopen(pathname, mode);
	}

	FILE *file = NULL;
	int ret = 0;

	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		uint64_t ret_ds;
		unsigned long p = 0;
		slog_debug("Calling Hercules 'fopen', pathname=%s", pathname);

		// Search for the path "new_path" on the map "map_fd".
		slog_debug("[POSIX %d]. Searching for the %s on the map", rank, new_path);
		int exist = map_fd_search(map_fd, new_path, &ret, &p);
		if (exist == -1) // if the "new_path" was not find:
		{
			ret = real_open("/dev/null", 0); // Get a file descriptor
			// stores the file descriptor "ret" into the map "map_fd".
			if (strstr(mode, "+"))
			{
				p = 0; // TODO
			}
			else
			{
				p = 0;
			}

			int create_flag = 0;
			if (strstr(mode, "w"))
				create_flag = O_CREAT;

			slog_debug("[POSIX %d] new_path:%s, exist: %d, create_flag: %d", rank, new_path, exist, create_flag);
			if (create_flag == O_CREAT)
			{
				slog_debug("[POSIX %d] map_fd_put", rank);
				map_fd_put(map_fd, new_path, ret, p);
				slog_debug("[POSIX %d]. New file %s", rank, new_path);
				int err_create = imss_create(new_path, mode, &ret_ds);
				slog_debug("[POSIX %d] imss_create(%s, %s, %ld), err_create: %d\n", rank, new_path, mode, ret_ds, err_create);
				if (err_create == -EEXIST)
				{
					slog_debug("[POSIX %d] dataset already exists.\n", rank);
					slog_debug("[POSIX %d] 1 - imss_fopen(%s, %ld)\n", rank, new_path, ret_ds);
					ret = TIMING(imss_open(new_path, &ret_ds), "imss_open op1", int);
				}
			}
			else
			{
				slog_debug("[POSIX %d] 2 - imss_fopen(%s, %ld)\n", rank, new_path, ret_ds);
				ret = TIMING(imss_open(new_path, &ret_ds), "imss_open op2", int);
			}
		}

		slog_debug("File descriptor=%d", ret);

		if (ret < 0)
		{
			return NULL;
		}

		file = malloc(sizeof(struct _IO_FILE));

		file->_fileno = ret;
		file->_flags2 = IMSS_BLKSIZE * KB;
		file->_offset = p;
		file->_mode = 0;

		if (file == NULL)
		{
			slog_debug("File %s was not found\n", pathname);
		}

		slog_debug("Ending Hercules 'fopen', pathname=%s\n", pathname);
		free(new_path);
	}
	else /* Do not try to use slog_ here! This function uses 'fopen' internally. */
	{
		// fprintf(stderr, "pathname=%s, pathname+%d-3=%s, strlen=%d, strlen-4=%d\n", pathname, strlen(pathname), pathname+strlen(pathname)-3, strlen(pathname), strlen(pathname) - 3);
		if (strncmp(pathname + strlen(pathname) - 3, "log", strlen("log")))
		{
			// fprintf(stderr, "Calling Real 'fopen', pathname=%s\n", pathname);
		}
		// slog_debug("Calling Real 'fopen', pathname=%s\n", pathname);
		file = real_fopen(pathname, mode);
		// fprintf(stderr,"Ending Real 'fopen', pathname=%s\n", pathname);
	}

	return file;
}

int open(const char *pathname, int flags, ...)
{
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

	int ret = 0;
	errno = 0;

	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		uint64_t ret_ds = 0;
		unsigned long p = 0;

		slog_debug("[POSIX]. Calling Hercules 'open' flags=%d, mode=%o, pathname=%s, new_path=%s", flags, mode, pathname, new_path);

		checkOpenFlags(pathname, flags);

		// Search for the path "new_path" on the map "map_fd".
		slog_debug("[POSIX]. Searching for the %s on the map", new_path);
		int exist = map_fd_search(map_fd, new_path, &ret, &p);
		if (exist == -1) // if the "new_path" was not find:
		{
			// slog_debug("Map add=%x", &map_fd);
			int create_flag = (flags & O_CREAT);
			slog_debug("[POSIX] new_path:%s, exist: %d, create_flag: %d", new_path, exist, create_flag);
			if (create_flag == O_CREAT) // if the file does not exist, then we create it.
			{
				slog_debug("[POSIX]. New file %s, ret=%d", new_path, ret);
				int err_create = imss_create(new_path, mode, &ret_ds);
				slog_debug("[POSIX] imss_create(%s, %d, %ld), err_create: %d", new_path, mode, ret_ds, err_create);
				// ret = ret_ds;
				if (err_create == -EEXIST)
				{
					slog_debug("[POSIX] dataset already exists.");
					slog_debug("[POSIX] 1 - imss_open(%s, %ld)", new_path, ret_ds);
					ret = TIMING(imss_open(new_path, &ret_ds), "imss_open op1", int);
				}
			}
			else // the file must exist.
			{
				slog_debug("[POSIX] File must exists - imss_open(%s, %ld)", new_path, ret_ds);
				ret = TIMING(imss_open(new_path, &ret_ds), "imss_open op2", int);
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
					// ret = real_open(new_path, flags);
					if (!mode)
						ret = real_open(new_path, flags);
					else
						ret = real_open(new_path, flags, mode);
					// stores the file descriptor "ret" into the map "map_fd".
					map_fd_put(map_fd, new_path, ret, p); // TO CHECK!
														  // free(new_path);
														  // return ret;
				}
			}
			if (ret > -1)
			{
				ret = real_open("/dev/null", 0); // Get a file descriptor
				// stores the file descriptor "ret" into the map "map_fd".
				slog_debug("[POSIX] Puting fd %d into map", ret);
				map_fd_put(map_fd, new_path, ret, p); // TO CHECK!
			}
		}
		else
		{
			if (flags & O_TRUNC)
			{
				map_fd_update_value(map_fd, new_path, ret, 0);
				struct stat stats;
				ret = imss_getattr(new_path, &stats);
				if (ret < 0)
				{
					errno = -ret;
					ret = -1;
					slog_error("[POSIX] Error Hercules '__fxstat', errno=%d:%s", errno, strerror(errno));
					free(new_path);
					return ret;
				}
				stats.st_size = 0;
				stats.st_blocks = 0;
				map_update(map, new_path, ret, stats);
			}
		}
		// pthread_mutex_unlock(&lock2);
		slog_debug("[POSIX] Ending Hercules 'open', exist=%d, mode=%d, ret=%d, errno=%d:%s", exist, mode, ret, errno, strerror(errno));
		free(new_path);
	}
	else
	{
		slog_debug("[POSIX]. Calling real 'open', mode=%d, pathname=%s.", mode, pathname);
		if (!mode)
			ret = real_open(pathname, flags);
		else
			ret = real_open(pathname, flags, mode);
		slog_debug("[POSIX]. Ending real 'open', mode=%d, pathname=%s, ret=%d, errno=%d:%s", mode, pathname, ret, errno, strerror(errno));
	}
	// slog_debug("Ending Open, errno=%d", errno);
	return ret;
}

int mkdir(const char *path, mode_t mode)
{
	real_mkdir = dlsym(RTLD_NEXT, "mkdir");
	size_t ret;

	if (!init)
	{
		return real_mkdir(path, mode);
	}

	char *workdir = getenv("PWD");
	// if (!strncmp(path, MOUNT_POINT, strlen(MOUNT_POINT)))
	// // if (!strncmp(path, MOUNT_POINT, strlen(MOUNT_POINT)) || !strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT)))
	// {
	char *new_path = checkHerculesPath(path);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling hercules 'mkdir', path=%s", path);

		// char *new_path;
		// new_path = convert_path(path, MOUNT_POINT);
		ret = imss_mkdir(new_path, mode);
		slog_debug("[POSIX]. Ending hercules 'mkdir', path=%s.", path);
		free(new_path);
	}
	else
	{
		slog_debug("[POSIX %d]. Calling real 'mkdir', path=%s.", rank, path);
		ret = real_mkdir(path, mode);
		slog_debug("[POSIX %d]. Ending real 'mkdir', path=%s.", rank, path);
	}
	return ret;
}

int symlink(const char *name1, const char *name2)
{

	real_symlink = dlsym(RTLD_NEXT, "symlink");

	fprintf(stderr, "Hola symlink \t ******");

	return real_symlink(name1, name2);
}

int symlinkat(const char *name1, int fd, const char *name2)
{

	real_symlinkat = dlsym(RTLD_NEXT, "symlinkat");

	if (!init)
	{
		return real_symlinkat(name1, fd, name2);
	}

	// char *workdir = getenv("PWD");
	int ret;

	char *new_path_1 = checkHerculesPath(name1);
	char *new_path_2 = checkHerculesPath(name2);
	if (new_path_1 != NULL || new_path_2 != NULL)
	{
		// if (!strncmp(name1, MOUNT_POINT, strlen(MOUNT_POINT)) || !strncmp(name2, MOUNT_POINT, strlen(MOUNT_POINT)))
		// if (!strncmp(name1, MOUNT_POINT, strlen(MOUNT_POINT)) || !strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT)) || !strncmp(name2, MOUNT_POINT, strlen(MOUNT_POINT)))
		// {
		slog_debug("[POSIX %d]. Calling Hercules 'symlinkat', name1=%s, name2=%s.", rank, name1, name2);

		// char *new_path_1, *new_path_2;

		// if (!strncmp(name1, MOUNT_POINT, strlen(MOUNT_POINT)) && !strncmp(name2, MOUNT_POINT, strlen(MOUNT_POINT)))
		if (new_path_1 != NULL && new_path_2 != NULL)
		{
			slog_debug("[POSIX %d]. Both new_path_1=%s, new_path_2=%s", rank, new_path_1, new_path_2);
			// new_path_1 = convert_path(name1, MOUNT_POINT);
			// new_path_2 = convert_path(name2, MOUNT_POINT);
			ret = imss_symlinkat(new_path_1, new_path_2, 0);
			free(new_path_1);
			free(new_path_2);
		}

		// if (!strncmp(name2, MOUNT_POINT, strlen(MOUNT_POINT)))
		if (new_path_1 == NULL && new_path_2 != NULL)
		{
			slog_debug("[POSIX %d]. Only second new_path_2=%s", rank, new_path_2);
			new_path_1 = name1;
			// new_path_2 = convert_path(name2, MOUNT_POINT);
			ret = imss_symlinkat(new_path_1, new_path_2, 1);
			free(new_path_2);
		}

		slog_debug("[POSIX %d]. Ending Hercules 'symlinkat', name1=%s, name2=%s.", rank, name1, name2);
	}
	else
	{
		slog_debug("[POSIX %d]. Calling Real 'symlinkat', name1=%s, name2=%s.", rank, name1, name2);
		ret = real_symlinkat(name1, fd, name2);
		slog_debug("[POSIX %d]. Ending Real 'symlinkat', name1=%s, name2=%s.", rank, name1, name2);
	}
	return ret;
}

off_t lseek(int fd, off_t offset, int whence)
{
	real_lseek = dlsym(RTLD_NEXT, "lseek");

	if (!init)
	{
		return real_lseek(fd, offset, whence);
	}

	off_t ret;
	errno = 0;
	char *path = (char *)calloc(256, sizeof(char));
	if (map_fd_search_by_val(map_fd, path, fd) == 1)
	{
		unsigned long p = 0;
		slog_debug("[POSIX]. Calling Hercules 'lseek', path=%s, fd=%d, errno=%d:%s", path, fd, errno, strerror(errno));
		slog_info("[POSIX]. whence=%d, offset=%ld", whence, offset);
		if (whence == SEEK_SET)
		{
			slog_debug("[POSIX]. SEEK_SET=%ld", offset);
			ret = offset;
			map_fd_update_value(map_fd, path, fd, ret);
		}
		else if (whence == SEEK_CUR)
		{
			ret = map_fd_search(map_fd, path, &fd, &p);
			slog_debug("[POSIX]. SEEK_CUR=%ld, ret=%ld, p=%ld", offset, ret);
			if (ret < 0)
			{
				errno = -ret;
				ret = -1;
				free(path);
				slog_debug("[POSIX]. Error in 'lseek', ret=%ld, errno=%d:%s", ret, errno, strerror(errno));
				return ret;
			}
			ret = p + offset;
			slog_debug("[POSIX]. SEEK_CUR=%ld, p+offset=%ld", offset, ret);
			map_fd_update_value(map_fd, path, fd, ret);
		}
		else if (whence == SEEK_END)
		{
			slog_debug("SEEK_END=%ld", offset);
			struct stat ds_stat_n;
			ret = imss_getattr(path, &ds_stat_n);
			if (ret < 0)
			{
				errno = -ret;
				ret = -1;
				free(path);
				slog_debug("[POSIX]. Error in 'lseek', ret=%ld, errno=%d:%s", ret, errno, strerror(errno));
				return ret;
			}
			ret = offset + ds_stat_n.st_size;
			map_fd_update_value(map_fd, path, fd, ret);
		}

		slog_debug("[POSIX]. Ending Hercules 'lseek', ret=%ld, errno=%d:%s", ret, errno, strerror(errno));
	}
	else
	{
		ret = real_lseek(fd, offset, whence);
	}
	free(path);
	return ret;
}

int truncate(const char *path, off_t length)
{
	real_truncate = dlsym(RTLD_NEXT, "truncate");

	if (init)
	{
		fprintf(stderr, "[POSIX] Calling truncate, path=%s, length=%ld", path, length);
	}

	return real_truncate(path, length);
}

int ftruncate(int fd, off_t length)
{
	real_ftruncate = dlsym(RTLD_NEXT, "ftruncate");

	if (!init)
	{
		return real_ftruncate(fd, length);
		// fprintf(stderr, "[POSIX] Calling ftruncate, fd=%d, length=%ld", fd, length);
	}

	int ret;
	char *path = (char *)calloc(256, sizeof(char));
	if (TIMING(map_fd_search_by_val(map_fd, path, fd), "[POSIX] Searching path by fd", int) == 1)
	{
		// fprintf(stderr, "[POSIX] Calling Hercules 'ftruncate', fd=%d, length=%ld, errno=%d:%s\n", fd, length, errno, strerror(errno));
		slog_debug("[POSIX] Calling Hercules 'ftruncate', fd=%d, length=%ld, errno=%d:%s\n", fd, length, errno, strerror(errno));
		// struct stat ds_stat_n;
		// imss_getattr(path, &ds_stat_n);
		// map_fd_search(map_fd, path, &fd, &p);
		// printf("CUSTOM write worked! path=%s fd=%d, size=%ld offset=%ld, buffer=%s\n", path, fd, size, p, buf);
		ret = 1;
		// fprintf(stderr, "[POSIX] Ending Hercules 'ftruncate', ret=%d, fd=%d, length=%ld errno=%d:%s\n", ret, fd, length, errno, strerror(errno));
		slog_debug("[POSIX] Ending Hercules 'ftruncate', ret=%d, fd=%d, length=%ld, errno=%d:%s\n", ret, fd, length, errno, strerror(errno));
	}
	else
	{
		ret = real_ftruncate(fd, length);
	}
	free(path);

	return ret;
}

ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset)
{
	real_pwrite = dlsym(RTLD_NEXT, "pwrite");

	if (!init)
	{
		return real_pwrite(fd, buf, count, offset);
	}

	ssize_t ret;
	// errno = 0;
	char *path = (char *)calloc(256, sizeof(char));
	if (TIMING(map_fd_search_by_val(map_fd, path, fd), "[POSIX] Searching path by fd", int) == 1)
	{
		// fprintf(stderr, "[POSIX] Calling Hercules 'pwrite', fd=%d, count=%ld, offset=%ld, errno=%d:%s\n", fd, count, offset, errno, strerror(errno));
		slog_debug("[POSIX] Calling Hercules 'pwrite', fd=%d, count=%ld, offset=%ld, errno=%d:%s", fd, count, offset, errno, strerror(errno));
		// struct stat ds_stat_n;
		// imss_getattr(path, &ds_stat_n);
		// map_fd_search(map_fd, path, &fd, &p);
		// printf("CUSTOM write worked! path=%s fd=%d, size=%ld offset=%ld, buffer=%s\n", path, fd, size, p, buf);
		ret = TIMING(imss_write(path, buf, count, offset), "imss_write", int);
		// fprintf(stderr, "[POSIX] Ending Hercules 'pwrite', ret=%ld, fd=%d, count=%ld, offset=%ld, errno=%d:%s\n", ret, fd, count, offset, errno, strerror(errno));
		slog_debug("[POSIX] Ending Hercules 'pwrite', ret=%ld, fd=%d, count=%ld, offset=%ld, errno=%d:%s", ret, fd, count, offset, errno, strerror(errno));
	}
	else
	{
		slog_debug("[POSIX] Calling Real 'pwrite', fd=%d, count=%ld, offset=%ld, errno=%d:%s", fd, count, offset, errno, strerror(errno));
		ret = real_pwrite(fd, buf, count, offset);
	}
	free(path);
	return ret;
}

ssize_t write(int fd, const void *buf, size_t size)
{
	real_write = dlsym(RTLD_NEXT, "write");
	if (!init)
	{
		return real_write(fd, buf, size);
	}

	size_t ret = -1;
	char *path = (char *)calloc(256, sizeof(char));
	if (map_fd_search_by_val(map_fd, path, fd) == 1)
	{
		unsigned long p = -1;
		slog_debug("[POSIX]. Calling Hercules 'write', path=%s, size=%lu", path, size);

		struct stat ds_stat_n;
		imss_getattr(path, &ds_stat_n);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_error("[POSIX] Error Hercules 'write'	: %d:%s", errno, strerror(errno));
			free(path);
			return ret;
		}
		map_fd_search(map_fd, path, &fd, &p);
		slog_debug("[POSIX]. path=%s, size=%lu, current_file_size=%lu, offset=%d", path, size, ds_stat_n.st_size, p);

		// printf("CUSTOM write worked! path=%s fd=%d, size=%ld offset=%ld, buffer=%s\n", path, fd, size, p, buf);
		ret = TIMING(imss_write(path, buf, size, p), "imss_write", int);

		if (ds_stat_n.st_size + size > ds_stat_n.st_size)
		{
			map_fd_update_value(map_fd, path, fd, ds_stat_n.st_size + size);
		}

		// imss_release(path);
		slog_debug("[POSIX]. Ending Hercules 'write', path=%s, ret=%ld", path, ret);
	}
	else
	{
		ret = real_write(fd, buf, size);
	}
	free(path);

	return ret;
}

void *mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset)
{
	real_mmap = dlsym(RTLD_NEXT, "mmap");

	if (init)
	{
		slog_debug("[POSIX %d] Calling Real 'mmap'", rank);
	}

	return mmap(addr, length, prot, flags, fd, offset);
}

ssize_t read(int fd, void *buf, size_t size)
{
	real_read = dlsym(RTLD_NEXT, "read");
	// printf("read=%d\n",fd);

	if (!init)
	{
		return real_read(fd, buf, size);
	}

	size_t ret;
	unsigned long p = 0;
	char *path = (char *)calloc(256, sizeof(char));

	if (map_fd_search_by_val(map_fd, path, fd) == 1)
	{
		slog_debug("[POSIX]. Calling Hercules 'read', pathname=%s, size=%ld, fd=%ld.", path, size, fd);
		// printf("CUSTOM read worked! path=%s fd=%d, size=%ld\n",path, fd, size);
		TIMING(map_fd_search(map_fd, path, &fd, &p), "[read]map_fd_search", int);
		struct stat ds_stat_n;
		ret = imss_getattr(path, &ds_stat_n);
		slog_debug("[POSIX]. pathname=%s, stat.size=%ld.", path, ds_stat_n.st_size);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_error("[POSIX] Error Hercules 'read'	: %s", strerror(errno));
		}
		else if (p >= ds_stat_n.st_size)
		{
			ret = 0;
		}
		else
		{
			ret = TIMING(imss_read(path, buf, size, p), "[read]imss_read", int);

			// map_fd_search(map_fd, path, &fd, &p);
			p += ret;
			slog_debug("[POSIX] Updating map_fd, offset=%d", p);
			map_fd_update_value(map_fd, path, fd, p);
		}

		slog_debug("[POSIX]. End Hercules 'read', pathname=%s, ret=%ld, size=%ld, errno=%d:%s", path, ret, size, errno, strerror(errno));
	}
	else
	{
		// slog_debug("[POSIX]. Calling Real 'read', fd=%d, size=%ld", fd, size);
		ret = real_read(fd, buf, size);
		// slog_debug("[POSIX]. End Real 'read', ret=%ld, size=%ld, fd=%ld", ret, size, fd);
	}
	free(path);
	return ret;
}

int unlink(const char *name)
{ // unlink
	real_unlink = dlsym(RTLD_NEXT, "unlink");
	// char *workdir = getenv("PWD");

	if (!init)
	{
		return real_unlink(name);
	}

	int ret = 0;
	char *new_path = checkHerculesPath(name);
	if (new_path != NULL)
	{
		int32_t type = get_type(new_path);
		slog_debug("[POSIX]. Calling Hercules 'unlink', type=%d, name=%s, new_path=%s", type, name, new_path);
		// fprintf(stderr, "[POSIX]. Calling Hercules 'unlink', type=%d, name=%s, new_path=%s\n", type, name, new_path);
		if (type == 0)
		{
			strcat(new_path, "/");
			type = get_type(new_path);
			slog_debug("[POSIX]. Calling Hercules 'unlink' type %ld, name=%s, new_path=%s", type, name, new_path);
			if (type == 2)
			{
				ret = imss_rmdir(new_path);
			}

			if (type != 0)
			{
				ret = imss_unlink(new_path);
			}

			// fprintf(stderr, "[POSIX] Calling Hercules 'unlink', type=%d, new_path=%s, ret=%d\n", type, new_path, ret);
		}
		else
		{
			ret = imss_unlink(new_path);
		}

		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_error("[POSIX]. Error Hercules 'unlink', errno=%d:%s", errno, strerror(errno));
		}

		// remove the file descriptor from the local map.
		map_fd_erase(map_fd, new_path);

		slog_debug("[POSIX]. Ending Hercules 'unlink', type %ld, name=%s, new_path=%s, ret=%d", type, name, new_path, ret);
		// fprintf(stderr, "[POSIX]. Ending Hercules 'unlink', new_path=%s, ret=%d\n", new_path, ret);
		free(new_path);
		// fprintf(stderr,"After free', new_path=%s\n", new_path);
	}
	else if (!strncmp(name, "imss://", strlen("imss://"))) // TO REVIEW!
	{
		slog_debug("[POSIX]. Calling 'unlink' op 2, name=%s.", name);
		// fprintf(stderr, "[POSIX]. Calling 'unlink' op 2, name=%s\n", name);
		char *path = (char *)calloc(256, sizeof(char));
		strcpy(path, name);
		int32_t type = get_type(path);
		if (type == 0)
		{
			strcat(path, "/");
			type = get_type(path);

			if (type == 2)
			{
				ret = imss_rmdir(path);
			}
		}
		else
		{
			ret = imss_unlink(path);
		}
		free(path);
	}
	else
	{
		slog_debug("[POSIX]. Calling Real 'unlink', name=%s", name);
		ret = real_unlink(name);
		slog_debug("[POSIX]. Ending Real 'unlink', name=%s", name);
	}
	// slog_debug("[POSIX %d]. End 'unlink'.", rank);
	return ret;
}

int rmdir(const char *path)
{

	real_rmdir = dlsym(RTLD_NEXT, "rmdir");
	int ret;
	char *workdir = getenv("PWD");

	if (!init)
	{
		return real_rmdir(path);
	}

	// if (!strncmp(path, MOUNT_POINT, strlen(MOUNT_POINT)))
	// if (!strncmp(path, MOUNT_POINT, strlen(MOUNT_POINT)) || !strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT)))
	char *new_path = checkHerculesPath(path);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'rmdir', new_path=%s", new_path);
		// char *new_path;
		// new_path = convert_path(path, MOUNT_POINT);
		ret = imss_rmdir(new_path);
		if (ret == -1)
		{ // special case io500
			ret = unlinkat(0, path, 0);
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

int unlinkat(int fd, const char *name, int flag)
{ // rm & rm -r
	real_unlinkat = dlsym(RTLD_NEXT, "unlinkat");
	int ret = 0;
	char *workdir = getenv("PWD");

	// fprintf(stderr, "unlinkat");

	if (!init)
	{
		return real_unlinkat(fd, name, flag);
	}

	// if (!strncmp(name, MOUNT_POINT, strlen(MOUNT_POINT)))
	// if (!strncmp(name, MOUNT_POINT, strlen(MOUNT_POINT)) || !strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT)))
	char *new_path = checkHerculesPath(name);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'unlinkat', new_path=%s", new_path);
		// char *new_path;
		// new_path = convert_path(name, MOUNT_POINT);
		int n_ent = 0;
		char *buffer;
		char **refs;

		if ((n_ent = get_dir((char *)new_path, &buffer, &refs)) < 0)
		{
			strcat(new_path, "/");
			if ((n_ent = get_dir((char *)new_path, &buffer, &refs)) < 0)
			{
				return -ENOENT;
			}
		}

		for (int i = n_ent - 1; i > -1; --i)
		{
			char *last = refs[i] + strlen(refs[i]) - 1;

			if (refs[i][strlen(refs[i]) - 1] == '/')
			{
				rmdir(refs[i]);
			}
			else
			{
				unlink(refs[i]);
			}
		}
		free(new_path);
	}
	else
	{
		ret = real_unlinkat(fd, name, flag);
	}

	return ret;
}

int rename(const char *old, const char *new)
{

	real_rename = dlsym(RTLD_NEXT, "rename");
	// char *workdir = getenv("PWD");

	if (!init)
	{
		return real_rename(old, new);
	}

	int ret;
	// if ((!strncmp(old, MOUNT_POINT, strlen(MOUNT_POINT)) && !strncmp(new, MOUNT_POINT, strlen(MOUNT_POINT))))
	// if ((!strncmp(old, MOUNT_POINT, strlen(MOUNT_POINT)) && !strncmp(new, MOUNT_POINT, strlen(MOUNT_POINT))) || !strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT)))
	char *old_path = checkHerculesPath(old);
	char *new_path = checkHerculesPath(new);
	if (old_path != NULL && new_path != NULL)
	{
		slog_debug("[POSIX %d]. Calling Hercules 'rename', old path=%s, new path=%s", rank, old_path, new_path);
		// char *old_path;
		// old_path = convert_path(old, MOUNT_POINT);
		// char *new_path;
		// new_path = convert_path(new, MOUNT_POINT);
		ret = imss_rename(old_path, new_path);
		slog_debug("[POSIX %d]. End Hercules 'rename', old path=%s, new path=%s, ret=%d", rank, old_path, new_path, ret);
		free(old_path);
		free(new_path);
	}
	else
	{
		slog_debug("[POSIX %d]. Calling Real 'rename', old path=%s, new path=%s", rank, old, new);
		ret = real_rename(old, new);
		slog_debug("[POSIX %d]. End Real 'rename', old path=%s, new path=%s", rank, old, new);
	}
	return ret;
}

int fchmodat(int dirfd, const char *pathname, mode_t mode, int flags)
{
	real_fchmodat = dlsym(RTLD_NEXT, "fchmodat");

	if (!init)
	{
		return real_fchmodat(dirfd, pathname, mode, flags);
	}

	int ret;
	char *workdir = getenv("PWD");
	// if (!strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT)))
	// if (!strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT)) || !strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT)))
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'fchmodat', pathname=%s", pathname);
		// char *new_path;
		// new_path = convert_path(pathname, MOUNT_POINT);
		ret = imss_chmod(new_path, mode);
		slog_debug("[POSIX]. End Hercules 'fchmodat', pathname=%s, ret=%d", pathname, ret);
		free(new_path);
	}
	else
	{
		ret = real_fchmodat(dirfd, pathname, mode, flags);
	}

	return ret;
}

int chmod(const char *pathname, mode_t mode)
{
	real_chmod = dlsym(RTLD_NEXT, "chmod");

	if (!init)
	{
		return real_chmod(pathname, mode);
	}

	int ret;
	// char *workdir = getenv("PWD");
	// if (!strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT)))
	// if (!strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT)) || !strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT)))
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_debug("[POSIX]. Calling Hercules 'chmod', pathname=%s.", pathname);
		// char *new_path;
		// new_path = convert_path(pathname, MOUNT_POINT);
		ret = imss_chmod(new_path, mode);
		slog_debug("[POSIX]. End Hercules 'chmod', pathname=%s, ret=%d.", pathname, ret);
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

// int execve(const char *pathname, char *const argv[], char *const envp[])
// {

// 	// real_execve = dlsym(RTLD_NEXT, "execve");

// 	fprintf(stderr, "*********** Running execve, pathname=%s\n", pathname);

// 	// return real_execve(pathname, argv, envp);
// 	return dlsym(RTLD_NEXT, "execve");

// 	// int ret;

// 	// return ret;
// }

int fchmod(int fd, mode_t mode)
{
	real_fchmod = dlsym(RTLD_NEXT, "fchmod");

	if (!init)
	{
		return real_fchmod(fd, mode);
	}

	int ret;
	char *pathname = calloc(256, sizeof(char));
	if (map_fd_search_by_val(map_fd, pathname, fd) == 1)
	{
		slog_debug("[POSIX]. Calling Hercules 'fchmod', pathname=%s.", pathname);
		// char *new_path;
		// new_path = convert_path(pathname, MOUNT_POINT);
		ret = imss_chmod(pathname, mode);
		slog_debug("[POSIX]. End Hercules 'fchmod', pathname=%s, ret=%d.", pathname, ret);
	}
	else
	{
		ret = real_fchmod(fd, mode);
	}

	free(pathname);

	return ret;
}

int fchownat(int dirfd, const char *pathname, uid_t owner, gid_t group, int flags)
{
	real_fchownat = dlsym(RTLD_NEXT, "chown");
	int ret;
	char *workdir = getenv("PWD");

	if (!init)
	{
		return real_fchownat(dirfd, pathname, owner, group, flags);
	}

	// if (!strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT)))
	// if (!strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT)) || !strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT)))
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_debug("[POSIX %d]. Calling Hercules 'fchownat'.", rank);
		// char *new_path;
		// new_path = convert_path(pathname, MOUNT_POINT);
		ret = imss_chown(new_path, owner, group);
	}
	else
	{
		ret = real_fchownat(dirfd, pathname, owner, group, flags);
	}

	return ret;
}

DIR *opendir(const char *name)
{

	real_opendir = dlsym(RTLD_NEXT, "opendir");

	if (!init)
	{
		return real_opendir(name);
	}

	DIR *dirp;
	char *new_path = checkHerculesPath(name);
	if (new_path != NULL)
	{
		// slog_debug("[POSIX %d]. Calling Hercules 'opendir', pathname=%s", rank, name);

		slog_debug("[POSIX]. Calling Hercules 'opendir', new_path=%s", new_path);
		int a = 1;
		int ret = 0;
		dirp = real_opendir("/tmp");
		seekdir(dirp, 0);
		unsigned long p = 0;
		int fd = 0;
		// Search for the path "new_path" on the map "map_fd",
		// if it exists then a file descriptor "fd" is going to point it.
		if (map_fd_search(map_fd, new_path, &fd, &p) == 1)
		{
			slog_debug("[POSIX] map_fd_update_value, new_path=%s", new_path);
			map_fd_update_value(map_fd, new_path, dirfd(dirp), p);
		}
		else
		{
			slog_debug("[POSIX] map_fd_put, new_path=%s", new_path);
			map_fd_put(map_fd, new_path, dirfd(dirp), p);
		}
		slog_debug("[POSIX]. End Hercules 'opendir', pathname=%s", name);
		free(new_path);
	}
	else
	{
		slog_debug("[POSIX]. Calling Real 'opendir', pathname=%s", name);
		dirp = real_opendir(name);
		slog_debug("[POSIX]. Ending Real 'opendir', pathname=%s", name);
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
	real_readdir = dlsym(RTLD_NEXT, "readdir");

	if (!init)
	{
		return real_readdir(dirp);
	}
	// slog_debug("[POSIX %d]. 1 . Calling 'readdir'.", rank);

	size_t ret;
	struct dirent *entry = (struct dirent *)malloc(sizeof(struct dirent));
	char *path = calloc(256, sizeof(char));
	if (map_fd_search_by_val(map_fd, path, dirfd(dirp)) == 1)
	{
		slog_debug("[POSIX]. Calling Hercules 'readdir', pathname=%s", path);
		char buf[KB * KB] = {0};
		char *token;
		// slog_fatal("CUSTOM IMSS_READDIR\n");
		imss_readdir(path, buf, myfiller, 0);
		unsigned long pos = telldir(dirp);

		token = strtok(buf, "$");
		// printf("readddir token=%s\n",token);
		int i = 0;

		while (token != NULL)
		{
			if (i == pos)
			{
				entry->d_ino = 0;
				entry->d_off = pos;

				// name of file
				strcpy(entry->d_name, token);

				char path_search[256] = {0};
				sprintf(path_search, "imss://%s", token);
				// type of file;
				int32_t type = get_type(path_search);

				if (!strncmp(token, ".", strlen(token)))
				{
					entry->d_type = DT_DIR;
				}
				else if (!strncmp(token, "..", strlen(token)))
				{
					entry->d_type = DT_DIR;
				}
				else if (type == 0)
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

				// length of this record
				if (strlen(token) < 5)
				{
					entry->d_reclen = 24;
				}
				else
				{
					entry->d_reclen = ceil((double)(strlen(token) - 4) / 8) * 8 + 24;
				}

				// fprintf(stderr, "[POSIX] token=%s, d_reclen=%d\n", token, entry->d_reclen);
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
		slog_debug("[POSIX]. Ending Hercules 'readdir', pathname=%s", path);
	}
	else
	{
		slog_debug("[POSIX]. Calling Real 'readdir'.");
		entry = real_readdir(dirp);
		slog_debug("[POSIX]. Ending Real 'readdir'.");
	}
	/* if(entry!=NULL){
	   printf("entry->d_ino=%ld\n",entry->d_ino);
	   printf("entry->d_off=%ld\n",entry->d_off);
	   printf("entry->d_reclen=%d\n",entry->d_reclen);
	   printf("entry->d_type=%d\n",entry->d_type);
	   printf("entry->d_name:%s\n",entry->d_name);
	   }*/
	// slog_debug("[POSIX %d]. End 'readdir'.", rank);

	free(path);
	return entry;
}

struct dirent64 *readdir64(DIR *dirp)
{
	real_readdir64 = dlsym(RTLD_NEXT, "readdir64");

	if (!init)
	{
		// slog_debug("[POSIX %d]. 1 . Calling 'readdir64'.", rank);
		return real_readdir64(dirp);
	}

	char *pathname = calloc(256, sizeof(char));
	if (map_fd_search_by_val(map_fd, pathname, dirfd(dirp)) == 1)
	{
		slog_debug("[POSIX %d]. Calling Hercules 'readdir64', pathname=%s.", rank, pathname);
		struct dirent *entry = (struct dirent *)malloc(sizeof(struct dirent));
		char buf[KB * KB] = {0};
		char *token;
		// slog_fatal("CUSTOM IMSS_READDIR\n");
		imss_readdir(pathname, buf, myfiller, 0);
		unsigned long pos = telldir(dirp);

		token = strtok(buf, "$");
		// printf("readddir token=%s\n",token);
		int i = 0;

		while (token != NULL)
		{
			if (i == pos)
			{
				entry->d_ino = 0;
				entry->d_off = pos;

				// name of file
				strcpy(entry->d_name, token);

				char path_search[256] = {0};
				sprintf(path_search, "imss://%s", token);
				// type of file;
				int32_t type = get_type(path_search);

				if (!strncmp(token, ".", strlen(token)))
				{
					entry->d_type = DT_DIR;
				}
				else if (!strncmp(token, "..", strlen(token)))
				{
					entry->d_type = DT_DIR;
				}
				else if (type == 0)
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

				// length of this record
				if (strlen(token) < 5)
				{
					entry->d_reclen = 24;
				}
				else
				{
					entry->d_reclen = ceil((double)(strlen(token) - 4) / 8) * 8 + 24;
				}
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
		return (struct dirent64 *)entry;
	}
	else
	{
		return real_readdir64(dirp);
	}
}

int closedir(DIR *dirp)
{
	real_closedir = dlsym(RTLD_NEXT, "closedir");

	if (!init)
	{
		return real_closedir(dirp);
	}

	// slog_debug("[POSIX %d]. Calling 'closedir'.", rank);

	map_fd_search_by_val_close(map_fd, dirfd(dirp));
	// printf("closedir worked! fd=%d\n",dirfd(dirp));
	int ret = real_closedir(dirp);

	return ret;
}

/************************/

// int openat(int dirfd, const char *pathname, int flags)
// {
// 	real_openat = dlsym(RTLD_NEXT, "openat");
// 	fprintf(stderr, "Calling openat %s\n", pathname);

// 	return real_openat;
// }

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

int stat64(const char *pathname, struct stat64 *info)
{
	// int ret = 0;
	// unsigned long p = 0;
	// char *workdir = getenv("PWD");
	real_stat64 = dlsym(RTLD_NEXT, "stat64");

	fprintf(stderr, " stat64\n");
	if (init)
	{
		slog_debug("[POSIX %d] Calling Real 'stat64'", rank);
	}
	return real_stat64(pathname, info);
}

int fstat(int fd, struct stat *buf)
{
	// int ret = 0;
	// unsigned long p = 0;
	// char *workdir = getenv("PWD");
	real_fstat = dlsym(RTLD_NEXT, "fstat");

	fprintf(stderr, " Calling Real fstat\n");
	if (init)
	{
		slog_debug("[POSIX %d] Calling Real 'fstat'", rank);
	}
	return real_fstat(fd, buf);
}

int fstatat(int dirfd, const char *pathname, struct stat *buf, int flags)
{
	// int ret = 0;
	// unsigned long p = 0;
	// char *workdir = getenv("PWD");
	real_fstatat = dlsym(RTLD_NEXT, "fstatat");

	// fprintf(stderr, " fstatat\n");
	if (init)
	{
		slog_debug("[POSIX %d] Calling Real 'fstatat', pathname=%s", rank, pathname);
	}
	return real_fstatat(dirfd, pathname, buf, flags);
}

int __fxstat(int ver, int fd, struct stat *buf)
{
	// char *workdir = getenv("PWD");
	real__fxstat = dlsym(RTLD_NEXT, "__fxstat");

	if (!init)
	{
		return real__fxstat(ver, fd, buf);
	}

	errno = 0;
	int ret;
	char *pathname = calloc(256, sizeof(char));
	if (map_fd_search_by_val(map_fd, pathname, fd) == 1)
	{
		unsigned long p = 0;
		slog_debug("[POSIX] Calling Hercules '__fxstat', pathname=%s, fd=%d.", pathname, fd);

		// char *new_path;
		// new_path = convert_path(pathname, MOUNT_POINT);
		// int exist = map_fd_search(map_fd, new_path, &ret, &p);
		imss_refresh(pathname);
		// slog_debug("[POSIX %d] Calling Hercules '__fxstat', new_path=%s, fd=%d.", rank, pathname, fd);
		ret = imss_getattr(pathname, buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_error("[POSIX] Error Hercules '__fxstat'	: %s", strerror(errno));
		}

		slog_debug("[POSIX] End Hercules '__fxstat', pathname=%s, fd=%d, errno=%d:%s, ret=%d, st_size=%ld", pathname, fd, errno, strerror(errno), ret, buf->st_size);
	}
	else
	{
		slog_debug("[POSIX] Calling Real '__fxstat', fd=%d", fd);
		ret = real__fxstat(ver, fd, buf);
		slog_debug("[POSIX] End Real '__fxstat', fd=%d, errno=%d, ret=%d", fd, errno, ret);
	}
	// slog_debug("Stat->dev=%d, buf->st_ino=%d", buf->st_dev, buf->st_ino);
	free(pathname);
	return ret;
}

int __fwprintf_chk(FILE *stream, int flag, const wchar_t *format)
{
	real___fwprintf_chk = dlsym(RTLD_NEXT, "__fwprintf_chk");
	fprintf(stderr, "Calling __fwprintf_chk\n");
	return real___fwprintf_chk(stream, flag, format);
}

// int __fxstat64(int ver, int fd, struct stat64 *buf)
// {
// 	real__fxstat64 = dlsym(RTLD_NEXT, "__fxstat64");

// 	fprintf(stderr, " __fxstat64, fd=%d\n", fd);
// 	if (init)
// 	{
// 		slog_debug("[POSIX %d] Calling Real '__fxstat64'", rank);
// 	}
// 	return real__fxstat64(ver, fd, buf);
// }

int access(const char *path, int mode)
{
	//	char *workdir = getenv("PWD");
	real_access = dlsym(RTLD_NEXT, "access");

	// fprintf(stderr, "access\n");
	if (!init)
	{
		return real_access(path, mode);
	}

	errno = 0;
	int ret = 0;
	// unsigned long p = 0;
	char *new_path = checkHerculesPath(path);
	if (new_path != NULL)
	{
		struct stat stat_buf;
		int permissions = 0;
		slog_debug("[POSIX]. Calling Hercules 'access', path=%s, new_path=%s", path, new_path);
		//		char *new_path;
		//		new_path = convert_path(path, MOUNT_POINT);
		// int exist = map_fd_search(map_fd, new_path, &ret, &p);
		imss_refresh(new_path);
		ret = imss_getattr(new_path, &stat_buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
		}
		else
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
		slog_debug("[POSIX]. End Hercules 'access', path=%s, new_path=%s ret=%d, errno=%d:%s.", path, new_path, ret, errno, strerror(errno));
		free(new_path);
	}
	else
	{
		slog_debug("[POSIX]. Calling Real 'access', path=%s, errno=%d:%s", path, errno, strerror(errno));
		ret = real_access(path, mode);
		slog_debug("[POSIX]. End Real 'access', ret=%d, errno=%d:%s", ret, errno, strerror(errno));
	}

	return ret;
}

// int fsync(int fd)
// {
// 	int ret = 0;
// 	unsigned long p = 0;
// 	char *workdir = getenv("PWD");
// 	real_fsync = dlsym(RTLD_NEXT, "fsync");

// 	fprintf(stderr, "fsync\n");
// 	if (!init)
// 	{
// 		return real_fsync(fd);
// 	}
// }
