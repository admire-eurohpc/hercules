/*
FUSE: Filesystem in Userspace
Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

This program can be distributed under the terms of the GNU GPL.
See the file COPYING.

gcc -Wall imss.c `pkg-config fuse --cflags --libs` -o imss
 */

#define FUSE_USE_VERSION 26
#include "map.hpp"
#include "mapprefetch.hpp"
#include "imss.h"
#include "hercules.h"
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
#include <sys/time.h>

#include <dirent.h>
#include <sys/types.h>
#include "imss_posix_api.h"

#define KB 1024
#define GB 1073741824
#define HEADER sizeof(uint32_t)

/*
   -----------	IMSS Global variables, filled at the beggining or by default -----------
 */
int32_t error_print = 0; // Temporal solution maybe to change in the future.

extern int32_t REPL_FACTOR;

extern int32_t N_SERVERS;
extern int32_t N_BLKS;
extern char *POLICY;
extern uint64_t IMSS_BLKSIZE;
extern uint64_t IMSS_DATA_BSIZE;
extern void *map;
extern void *map_prefetch;

extern uint16_t PREFETCH;
extern uint16_t threshold_read_servers;
extern uint16_t BEST_PERFORMANCE_READ;
extern uint16_t MULTIPLE_READ;
extern uint16_t MULTIPLE_WRITE;

extern char *BUFFERPREFETCH;
extern char prefetch_path[256];
extern int32_t prefetch_first_block;
extern int32_t prefetch_last_block;
extern int32_t prefetch_pos;

extern int16_t prefetch_ds;
extern int32_t prefetch_offset;

extern pthread_cond_t cond_prefetch;
extern pthread_mutex_t mutex_prefetch;
// char fd_table[1024][MAX_PATH];

#define MAX_PATH 256
extern pthread_mutex_t lock;
pthread_mutex_t lock_file = PTHREAD_MUTEX_INITIALIZER;

extern int32_t IMSS_DEBUG;

/*
   (*) Mapping for REPL_FACTOR values:
   NONE = 1;
   DRM = 2;
   TRM = 3;
 */

/*
   -----------	Auxiliar Functions -----------
 */

void fd_lookup(const char *path, int *fd, struct stat *s, char **aux)
{
	//pthread_mutex_lock(&lock_file);
	*fd = -1;
	int found = map_search(map, path, fd, s, aux);
	if (!found)
		*fd = -1;
	//pthread_mutex_unlock(&lock_file);
}

void get_iuri(const char *path, /*output*/ char *uri)
{

	if (!strncmp(path, "imss:/", strlen("imss:/")))
	{
		strcat(uri, path);
	}
	else
	{
		// Copying protocol
		strcpy(uri, "imss:/");
		strcat(uri, path);
	}
}

/*
   -----------	FUSE IMSS implementation -----------
 */

int imss_truncate(const char *path, off_t offset)
{
	return 0;
}
int imss_access(const char *path, int permission)
{
	return 0;
}

int imss_refresh(const char *path)
{

	struct stat *stats;
	struct stat old_stats;
	uint32_t ds;
	int fd;
	char *aux2;
	char *imss_path = calloc(MAX_PATH, sizeof(char));
	char *aux = (char *)malloc(IMSS_DATA_BSIZE);

	get_iuri(path, imss_path);

	fd_lookup(imss_path, &fd, &old_stats, &aux2);
	if (fd >= 0)
		ds = fd;
	else
		return -ENOENT;

	get_data(ds, 0, aux);
	stats = (struct stat *)aux;

	map_update(map, imss_path, ds, *stats);

	free(aux);
	free(imss_path);
	return 0;
}

int imss_getattr(const char *path, struct stat *stbuf)
{
	// Needed variables for the call
	char *buffer;
	char **refs;
	char head[IMSS_DATA_BSIZE];
	int n_ent;
	char *imss_path = calloc(MAX_PATH, sizeof(char));
	dataset_info metadata;
	struct timespec spec;
	get_iuri(path, imss_path);
	memset(stbuf, 0, sizeof(struct stat));
	clock_gettime(CLOCK_REALTIME, &spec);

	stbuf->st_atim = spec;
	stbuf->st_mtim = spec;
	stbuf->st_ctim = spec;
	stbuf->st_uid = getuid();
	stbuf->st_gid = getgid();
	stbuf->st_blksize = IMSS_BLKSIZE;
	// printf("imss_getattr=%s\n",imss_path);
	uint32_t type = get_type(imss_path);
	slog_live("[imss_getattr] get_type(%s):%ld", imss_path, type);
	if (type == 0)
	{
		strcat(imss_path, "/");
		// printf("imss_getattr=%s\n",imss_path);
		type = get_type(imss_path);
		slog_live("[imss_getattr] get_type(imss_path=%s):%ld", imss_path, type);
	}

	uint32_t ds;

	int fd;
	struct stat stats;
	char *aux;
	switch (type)
	{

	case 0:
		return -ENOENT;
	case 1:
		if ((n_ent = get_dir((char *)imss_path, &buffer, &refs)) != -1)
		{
			stbuf->st_size = 4;
			stbuf->st_nlink = 1;
			stbuf->st_mode = S_IFDIR | 0755;
			// Free resources
			// free(buffer);
			free(refs);
			return 0;
		}
		else
		{
			// fprintf(stderr,"imss_getattr get_dir ERROR\n");
			return -ENOENT;
		}
	case 2: // Case file

		/*if(stat_dataset(imss_path, &metadata) == -1){
		  fprintf(stderr, "[IMSS-FUSE]	Cannot get dataset metadata.");
		  return -ENOENT;
		  }*/

		// Get header
		// FIXME! Not always possible!!!!
		//
		//
		fd_lookup(imss_path, &fd, &stats, &aux);
		if (fd >= 0)
			ds = fd;
		else
		{
			ds = open_dataset(imss_path);
			if (ds >= 0)
			{
				aux = (char *)malloc(IMSS_DATA_BSIZE);
				get_data(ds, 0, (char *)aux);
				memcpy(&stats, aux, sizeof(struct stat));
				pthread_mutex_lock(&lock_file);
				map_put(map, imss_path, ds, stats, aux);
				pthread_mutex_unlock(&lock_file);
				// free(aux);
			}
			else
			{
				if (IMSS_DEBUG)
					fprintf(stderr, "[IMSS-FUSE]	Cannot get dataset metadata.");
				return -ENOENT;
			}
		}
		if (stats.st_nlink != 0)
		{
			memcpy(stbuf, &stats, sizeof(struct stat));
		}
		else
		{
			if (IMSS_DEBUG)
				fprintf(stderr, "[IMSS-FUSE]	Cannot get dataset metadata.");
			return -ENOENT;
		}
		return 0;
	default:
		return -1;
	}
}

/*
   Read directory

   The filesystem may choose between two modes of operation:

   -->	1) The readdir implementation ignores the offset parameter, and passes zero to the filler function's offset.
   The filler function will not return '1' (unless an error happens), so the whole directory is read in a single readdir operation.

   2) The readdir implementation keeps track of the offsets of the directory entries. It uses the offset parameter
   and always passes non-zero offset to the filler function. When the buffer is full (or an error happens) the filler function will return '1'.
   */

int imss_readdir(const char *path, void *buf, posix_fill_dir_t filler, off_t offset)
{
	// fprintf(stderr,"imss_readdir=%s\n",path);
	// Needed variables for the call
	char *buffer;
	char **refs;
	int n_ent = 0;

	char *imss_path = calloc(MAX_PATH, sizeof(char));
	get_iuri(path, imss_path);
	// Call IMSS to get metadata
	if ((n_ent = get_dir((char *)imss_path, &buffer, &refs)) < 0)
	{
		strcat(imss_path, "/");
		// fprintf(stderr,"try again imss_path=%s\n",imss_path);
		if ((n_ent = get_dir((char *)imss_path, &buffer, &refs)) < 0)
		{
			fprintf(stderr, "[IMSS-FUSE]	Error retrieving directories for URI=%s", path);
			return -ENOENT;
		}
	}
	// Fill buffer
	// TODO: Check if subdirectory
	// printf("[FUSE] imss_readdir %s has=%d\n",path, n_ent);
	for (int i = 0; i < n_ent; ++i)
	{
		if (i == 0)
		{
			// printf("[readdir]. y ..\n");
			filler(buf, "..", NULL, 0);
			filler(buf, ".", NULL, 0);
		}
		else
		{
			// printf("[readdir]%s\n",refs[i]+6);
			struct stat stbuf;
			int error = imss_getattr(refs[i] + 6, &stbuf);
			if (!error)
			{
				char *last = refs[i] + strlen(refs[i]) - 1;
				if (last[0] == '/')
				{
					last[0] = '\0';
				}
				int offset = 0;
				for (int j = 0; j < strlen(refs[i]); ++j)
				{
					if (refs[i][j] == '/')
					{
						offset = j;
					}
				}

				filler(buf, refs[i] + offset + 1, &stbuf, 0);
			}
			free(refs[i]);
		}
	}
	// Free resources
	free(imss_path);
	free(refs);
	return 0;

	/*(void) offset;
	  (void) fi;

	  if (strcmp(path, "/") != 0)
	  return -ENOENT;

	  filler(buf, ".", NULL, 0);
	  filler(buf, "..", NULL, 0);
	  filler(buf, imss_path + 1, NULL, 0);

	  return 0;*/
}

int imss_open(const char *path, uint64_t *fh)
{
	// printf("imss_open=%s\n",path);
	// TODO -> Access control
	// DEBUG
	char *imss_path = (char *)calloc(MAX_PATH, sizeof(char));
	int32_t file_desc;
	get_iuri(path, imss_path);

	int fd;
	struct stat stats;
	char *aux;
	fd_lookup(imss_path, &fd, &stats, &aux);
	if (fd >= 0)
	{
		file_desc = fd;
	}
	else if (fd == -2)
		return -1;
	else
	{

		file_desc = open_dataset(imss_path);
		aux = (char *)malloc(IMSS_DATA_BSIZE);
		get_data(file_desc, 0, (char *)aux);
		memcpy(&stats, aux, sizeof(struct stat));
		pthread_mutex_lock(&lock_file);
		map_put(map, imss_path, file_desc, stats, aux);
		if (PREFETCH != 0)
		{
			char *buff = (char *)malloc(PREFETCH * IMSS_DATA_BSIZE);
			map_init_prefetch(map_prefetch, imss_path, buff);
		}

		pthread_mutex_unlock(&lock_file);

		// free(aux);
	}

	// File does not exist
	if (file_desc < 0)
		return -ENOENT;

	// Assign file descriptor
	*fh = file_desc;
	/*if ((fi->flags & 3) != O_RDONLY)
	  return -EACCES;*/
	free(imss_path);
	return 0;
}

int imss_sread(const char *path, char *buf, size_t size, off_t offset)
{
	clock_t t, tm, tmm;
	t = clock();

	dataset_info new_dataset;

	char *rpath = (char *)calloc(MAX_PATH, sizeof(char));

	get_iuri(path, rpath);

	int64_t curr_blk, end_blk, start_offset, end_offset;
	int64_t first = 0;
	int ds = 0;
	curr_blk = offset / IMSS_DATA_BSIZE + 1; // Plus one to skip the header (0) block
	start_offset = offset % IMSS_DATA_BSIZE;
	// end_blk = (offset+size) / IMSS_DATA_BSIZE + 1; //Plus one to skip the header (0) block
	end_blk = ceil((double)(offset + size) / IMSS_DATA_BSIZE);
	end_offset = (offset + size) % IMSS_DATA_BSIZE;
	size_t to_read = 0;

	// printf("[CLIENT] [SREAD] start block=%ld, end_block=%ld\n",curr_blk, end_blk);
	// Needed variables
	size_t byte_count = 0;
	int64_t rbytes;

	int fd;
	struct stat stats;
	char *aux;

	fd_lookup(rpath, &fd, &stats, &aux);
	// printf("stats_size=%ld\n",stats.st_size);
	if (stats.st_size < size)
	{
		end_blk = ceil((double)(offset + stats.st_size) / IMSS_DATA_BSIZE);
	}

	// Check if offset is bigger than filled, return 0 because is EOF case
	if (start_offset >= stats.st_size)
	{
		return 0;
	}

	if (fd >= 0)
		ds = fd;
	else if (fd == -2)
		return -ENOENT;

	//	gettimeofday(&start, NULL);
	tm = clock();
	//memset(buf, 0, size);
	tm = clock() - tm;
        tmm += tm;

	/*	struct timeval start1, end1;
		long delta_us1;
		gettimeofday(&start1, NULL);*/
	while (curr_blk <= end_blk)
	{
		// if( err != -1){
		// First block case
		if (first == 0)
		{
			get_data(ds, curr_blk, (char *)aux);
			if (size < (stats.st_size - start_offset) && size < IMSS_DATA_BSIZE)
			{
				to_read = size;
				// to_read = IMSS_DATA_BSIZE - start_offset;
			}
			else
			{
				if (stats.st_size < IMSS_DATA_BSIZE)
				{

					to_read = stats.st_size - start_offset;
				}
				else
				{

					to_read = IMSS_DATA_BSIZE - start_offset;
				}
			}
			memcpy(buf, aux + start_offset, to_read);
			byte_count += to_read;
			++first;
			// Check if offset is bigger than filled, return 0 because is EOF case
			// printf("start_offset=%ld, to_read=%ld, stats.st_size=%ld\n",start_offset,to_read,stats.st_size);
			if (start_offset + to_read > stats.st_size)
			{
				return 0;
			}
			// Middle block case
		}
		else if (curr_blk != end_blk)
		{
			get_data(ds, curr_blk, buf + byte_count);
			// memcpy(buf + byte_count, aux, IMSS_DATA_BSIZE);
			byte_count += IMSS_DATA_BSIZE;
			// End block case
		}
		else
		{
			get_data(ds, curr_blk, (char *)aux);
			// Read the minimum between end_offset and filled (read_ = min(end_offset, filled))
			int64_t pending = size - byte_count;
			/*struct timeval start, end;
			  long delta_us;
			  gettimeofday(&start, NULL);*/
			tm = clock();
			memcpy(buf + byte_count, aux, pending);
			tm = clock() - tm;
			//tmm += tm;
			/*gettimeofday(&end, NULL);
			  delta_us = (long) (end.tv_usec - start.tv_usec);
			  printf("[CLIENT] [SREAD MEMCPY 1 BLOCK] delta_us=%6.3f\n",(delta_us/1000.0F));*/

			byte_count += pending;
		}

		//}
		++curr_blk;
	}
	/*	gettimeofday(&end1, NULL);
		delta_us1 = (long) (end1.tv_usec - start1.tv_usec);
		printf("[CLIENT] [SREAD_END] delta_us=%6.3f\n",(delta_us1/1000.0F));*/



	t = clock() - t;
        double time_taken = ((double)t) / CLOCKS_PER_SEC; // in seconds
        double time_mem = ((double)tmm) / CLOCKS_PER_SEC; // in seconds

        slog_info("[API] imss_read time  total %f mem %f  s", time_taken, time_mem);

	free(rpath);
	return byte_count;
}

int imss_vread_prefetch(const char *path, char *buf, size_t size, off_t offset)
{

	// printf("IMSS_READV size=%ld, path=%s\n", size, path);
	// Compute current block and offsets
	int64_t curr_blk, end_blk, start_offset, end_offset;
	int64_t first = 0;
	int ds = 0;
	curr_blk = offset / IMSS_DATA_BSIZE + 1; // Plus one to skip the header (0) block
	start_offset = offset % IMSS_DATA_BSIZE;
	// end_blk = (offset+size) / IMSS_DATA_BSIZE+1; //Plus one to skip the header (0) block
	end_blk = ceil((double)(offset + size) / IMSS_DATA_BSIZE);
	end_offset = (offset + size) % IMSS_DATA_BSIZE;
	size_t to_read = 0;

	// Needed variables
	size_t byte_count = 0;
	int64_t rbytes;

	char *rpath = (char *)calloc(MAX_PATH, sizeof(char));
	get_iuri(path, rpath);

	int fd;
	struct stat stats;
	char *aux;
	// printf("imss_read aux before %p\n", aux);
	fd_lookup(rpath, &fd, &stats, &aux);
	// printf("imss_read aux after %p\n", aux);
	if (fd >= 0)
		ds = fd;
	else if (fd == -2)
		return -ENOENT;

	memset(buf, 0, size);
	// Read remaining blocks
	if (stats.st_size < size && stats.st_size < IMSS_DATA_BSIZE)
	{
		// total = IMSS_DATA_BSIZE;
		size = stats.st_size;
		end_blk = ceil((double)(offset + size) / IMSS_DATA_BSIZE);
		end_offset = (offset + size) % IMSS_DATA_BSIZE;
	}
	// Check if offset is bigger than filled, return 0 because is EOF case
	if (start_offset >= stats.st_size)
	{
		free(rpath);
		return 0;
	}

	int err;
	// printf("READ path:%s, start block=%ld, end_block=%ld, size=%ld\n",rpath, curr_blk, end_blk, size);

	// printf("READ curr_block=%ld end_block=%ld numbers of block=%ld\n",curr_blk, end_blk,(end_blk-curr_blk+1));
	while (curr_blk <= end_blk)
	{

		int exist_first_block, exist_last_block;
		aux = map_get_buffer_prefetch(map_prefetch, rpath, &exist_first_block, &exist_last_block);

		if (aux != NULL)
		{ // Existe fichero es normal esta creado anteriormente

			if (curr_blk >= exist_first_block && curr_blk <= exist_last_block)
			{ // Tiene el bloque
				// Tengo que mover el puntero al bloque correspondiente
				// printf("Existe se lo doy bloque=%ld\n", curr_blk);
				int pos = curr_blk - exist_first_block;
				aux = aux + (IMSS_DATA_BSIZE * pos);
				err = 1;
			}
			else
			{ // Existe pero no tiene ese bloque especifico

				if (first == 0)
				{ // readv si es el primero leo todos
					// printf("READV TODOS\n");
					pthread_mutex_lock(&lock);
					err = readv_multiple(ds, curr_blk, end_blk, buf, IMSS_BLKSIZE, start_offset, size);
					pthread_mutex_unlock(&lock);
					// PREFETCH
					pthread_mutex_lock(&lock);
					prefetch_ds = ds;
					strcpy(prefetch_path, rpath);
					prefetch_first_block = end_blk + 1;
					// prefetch_last_block = min ((end_blk + PREFETCH), stats.st_blocks);
					if ((end_blk + PREFETCH) <= stats.st_blocks)
					{
						prefetch_last_block = (end_blk + PREFETCH);
					}
					else
					{
						prefetch_last_block = stats.st_blocks;
					}
					prefetch_offset = start_offset;
					pthread_cond_signal(&cond_prefetch);
					pthread_mutex_unlock(&lock);
					if (err == -1)
					{
						free(rpath);
						return -1;
					}
					free(rpath);
					return size;
				}
				else
				{ // si ya tengo alguno guardado
					// printf("READV LOS QUE FALTABAN\n");
					pthread_mutex_lock(&lock);
					err = readv_multiple(ds, curr_blk, end_blk, buf + byte_count, IMSS_BLKSIZE,
										 start_offset, IMSS_DATA_BSIZE * (end_blk - curr_blk + 1));
					pthread_mutex_unlock(&lock);
					// PREFETCH
					pthread_mutex_lock(&lock);
					prefetch_ds = ds;
					strcpy(prefetch_path, rpath);
					prefetch_first_block = end_blk + 1;
					// prefetch_last_block = min ((end_blk + PREFETCH), stats.st_blocks);
					if ((end_blk + PREFETCH) <= stats.st_blocks)
					{
						prefetch_last_block = (end_blk + PREFETCH);
					}
					else
					{
						prefetch_last_block = stats.st_blocks;
					}
					prefetch_offset = start_offset;
					pthread_cond_signal(&cond_prefetch);
					pthread_mutex_unlock(&lock);
					if (err == -1)
					{
						free(rpath);
						return -1;
					}
					free(rpath);
					return size;
				}
			}
		}
		else
		{
			if (err != -1)
			{
			}
			else
			{
				free(rpath);
				return -ENOENT;
			}
		}

		if (err != -1)
		{
			// First block case
			if (first == 0)
			{
				if (size < stats.st_size - start_offset)
				{
					// to_read = size;
					to_read = IMSS_DATA_BSIZE - start_offset;
				}
				else
				{
					if (stats.st_size < IMSS_DATA_BSIZE)
					{
						to_read = stats.st_size - start_offset;
					}
					else
					{
						to_read = IMSS_DATA_BSIZE - start_offset;
					}
				}

				// Check if offset is bigger than filled, return 0 because is EOF case
				if (start_offset > stats.st_size)
				{
					free(rpath);
					return 0;
				}
				memcpy(buf, aux + start_offset, to_read);
				byte_count += to_read;
				++first;

				// Middle block case
			}
			else if (curr_blk != end_blk)
			{
				// memcpy(buf + byte_count, aux + HEADER, IMSS_DATA_BSIZE);
				memcpy(buf + byte_count, aux, IMSS_DATA_BSIZE);
				byte_count += IMSS_DATA_BSIZE;
				// End block case
			}
			else
			{

				// Read the minimum between end_offset and filled (read_ = min(end_offset, filled))
				int64_t pending = size - byte_count;
				memcpy(buf + byte_count, aux, pending);
				byte_count += pending;
			}
		}
		++curr_blk;
	}

	// PREFETCH
	pthread_mutex_lock(&lock);
	prefetch_ds = ds;
	strcpy(prefetch_path, rpath);
	prefetch_first_block = end_blk + 1;
	if ((end_blk + PREFETCH) <= stats.st_blocks)
	{
		prefetch_last_block = (end_blk + PREFETCH);
	}
	else
	{
		prefetch_last_block = stats.st_blocks;
	}
	prefetch_offset = start_offset;
	pthread_cond_signal(&cond_prefetch);
	pthread_mutex_unlock(&lock);
	free(rpath);
	return byte_count;
}

int imss_vread_no_prefetch(const char *path, char *buf, size_t size, off_t offset)
{
	// printf("IMSS_READV size=%ld, path=%s, offset=%ld\n", size, path, offset);
	// Compute current block and offsets
	int64_t curr_blk, end_blk, start_offset, end_offset;
	int64_t first = 0;
	int ds = 0;
	curr_blk = offset / IMSS_DATA_BSIZE + 1; // Plus one to skip the header (0) block
	start_offset = offset % IMSS_DATA_BSIZE;
	// end_blk = (offset+size) / IMSS_DATA_BSIZE+1; //Plus one to skip the header (0) block
	end_blk = ceil((double)(offset + size) / IMSS_DATA_BSIZE);
	end_offset = (offset + size) % IMSS_DATA_BSIZE;
	size_t to_read = 0;

	// Needed variables
	size_t byte_count = 0;
	int64_t rbytes;

	char *rpath = (char *)calloc(MAX_PATH, sizeof(char));
	get_iuri(path, rpath);
	// printf("rpath=%s\n",rpath);
	int fd;
	struct stat stats;
	char *aux;
	// printf("imss_read aux before %p\n", aux);
	fd_lookup(rpath, &fd, &stats, &aux);

	// printf("imss_read aux after %p\n", aux);
	if (fd >= 0)
		ds = fd;
	else if (fd == -2)
		return -ENOENT;

	memset(buf, 0, size);
	int total = size;
	// Read remaining blocks
	if (stats.st_size < size && stats.st_size < IMSS_DATA_BSIZE)
	{
		// total = IMSS_DATA_BSIZE;
		size = stats.st_size;
		end_blk = ceil((double)(offset + size) / IMSS_DATA_BSIZE);
		end_offset = (offset + size) % IMSS_DATA_BSIZE;
	}
	// Check if offset is bigger than filled, return 0 because is EOF case
	if (start_offset >= stats.st_size)
	{
		free(rpath);
		return 0;
	}
	int err;
	// printf("READ path:%s, start block=%ld, end_block=%ld, size=%ld\n",rpath, curr_blk, end_blk, size);

	// printf("READ curr_block=%ld end_block=%ld numbers of block=%ld\n",curr_blk, end_blk,(end_blk-curr_blk+1));
	while (curr_blk <= end_blk)
	{

		if (first == 0)
		{ // readv si es el primero leo todos
			// printf("READV TODOS\n");
			pthread_mutex_lock(&lock);
			err = readv_multiple(ds, curr_blk, end_blk, buf, IMSS_BLKSIZE, start_offset, total);
			pthread_mutex_unlock(&lock);
			if (err == -1)
			{
				return -1;
			}
			free(rpath);
			return size;
		}
		else
		{ // si ya tengo alguno guardado
			// printf("READV LOS QUE FALTABAN\n");
			pthread_mutex_lock(&lock);
			err = readv_multiple(ds, curr_blk, end_blk, buf + byte_count, IMSS_BLKSIZE,
								 start_offset, IMSS_BLKSIZE * KB * (end_blk - curr_blk + 1));
			pthread_mutex_unlock(&lock);
			if (err == -1)
			{
				return -1;
			}
			free(rpath);
			return size;
		}

		if (err != -1)
		{
			// First block case
			if (first == 0)
			{
				if (size < stats.st_size - start_offset)
				{
					// to_read = size;
					to_read = IMSS_DATA_BSIZE - start_offset;
				}
				else
				{
					if (stats.st_size < IMSS_DATA_BSIZE)
					{
						to_read = stats.st_size - start_offset;
					}
					else
					{
						to_read = IMSS_DATA_BSIZE - start_offset;
					}
				}

				// Check if offset is bigger than filled, return 0 because is EOF case
				if (start_offset > stats.st_size)
				{
					free(rpath);
					return 0;
				}
				memcpy(buf, aux + start_offset, to_read);
				byte_count += to_read;
				++first;

				// Middle block case
			}
			else if (curr_blk != end_blk)
			{
				// memcpy(buf + byte_count, aux + HEADER, IMSS_DATA_BSIZE);
				memcpy(buf + byte_count, aux, IMSS_DATA_BSIZE);
				byte_count += IMSS_DATA_BSIZE;
				// End block case
			}
			else
			{

				// Read the minimum between end_offset and filled (read_ = min(end_offset, filled))
				int64_t pending = size - byte_count;
				memcpy(buf + byte_count, aux, pending);
				byte_count += pending;
			}
		}
		++curr_blk;
	}
	free(rpath);
	return byte_count;
}

int imss_vread_2x(const char *path, char *buf, size_t size, off_t offset)
{

	// printf("IMSS_READV size=%ld, path=%s, offset=%ld\n", size, path, offset);
	// Compute current block and offsets
	int64_t curr_blk, end_blk, start_offset, end_offset;
	int64_t first = 0;
	int ds = 0;
	curr_blk = offset / IMSS_DATA_BSIZE + 1; // Plus one to skip the header (0) block
	start_offset = offset % IMSS_DATA_BSIZE;
	// end_blk = (offset+size) / IMSS_DATA_BSIZE+1; //Plus one to skip the header (0) block
	end_blk = ceil((double)(offset + size) / IMSS_DATA_BSIZE);
	end_offset = (offset + size) % IMSS_DATA_BSIZE;
	size_t to_read = 0;

	// Needed variables
	size_t byte_count = 0;
	int64_t rbytes;

	char *rpath = (char *)calloc(MAX_PATH, sizeof(char));
	get_iuri(path, rpath);

	int fd;
	struct stat stats;
	char *aux;
	// printf("imss_read aux before %p\n", aux);
	fd_lookup(rpath, &fd, &stats, &aux);
	// printf("imss_read aux after %p\n", aux);
	if (fd >= 0)
		ds = fd;
	else if (fd == -2)
		return -ENOENT;

	memset(buf, 0, size);
	// Read remaining blocks
	if (stats.st_size < size && stats.st_size < IMSS_DATA_BSIZE)
	{
		// total = IMSS_DATA_BSIZE;
		size = stats.st_size;
		end_blk = ceil((double)(offset + size) / IMSS_DATA_BSIZE);
		end_offset = (offset + size) % IMSS_DATA_BSIZE;
	}
	// Check if offset is bigger than filled, return 0 because is EOF case
	if (start_offset >= stats.st_size)
	{
		free(rpath);
		return 0;
	}
	PREFETCH = (end_blk - curr_blk) * 3;
	int err;
	// printf("READ curr_block=%ld end_block=%ld numbers of block=%ld\n",curr_blk, end_blk,(end_blk-curr_blk+1));
	while (curr_blk <= end_blk)
	{

		int exist_first_block, exist_last_block;
		aux = map_get_buffer_prefetch(map_prefetch, rpath, &exist_first_block, &exist_last_block);
		// curr_blk, exist_first_block, end_blk, exist_last_block);
		if (curr_blk >= exist_first_block && curr_blk <= exist_last_block)
		{ // Tiene el bloque
			// Tengo que mover el puntero al bloque correspondiente
			int pos = curr_blk - exist_first_block;
			aux = aux + (IMSS_BLKSIZE * KB * pos);
			err = 1;
		}
		else
		{
			if (first == 0)
			{ // readv si es el primero leo todos
				// printf("READV TODOS\n");
				pthread_mutex_lock(&lock);
				err = readv_multiple(ds, curr_blk, end_blk, buf, IMSS_BLKSIZE, start_offset, size);
				pthread_mutex_unlock(&lock);

				// PERSONAL PREFETCH
				prefetch_ds = ds;
				strcpy(prefetch_path, rpath);
				prefetch_first_block = end_blk;
				// prefetch_last_block = min ((end_blk + PREFETCH), stats.st_blocks);
				if ((end_blk + PREFETCH) <= stats.st_blocks)
				{
					prefetch_last_block = (end_blk + PREFETCH);
				}
				else
				{
					prefetch_last_block = stats.st_blocks;
				}
				if (prefetch_last_block < prefetch_first_block)
				{
					free(rpath);
					return size;
				}

				prefetch_offset = start_offset;
				char *buf = map_get_buffer_prefetch(map_prefetch, prefetch_path, &exist_first_block, &exist_last_block);
				int err = readv_multiple(prefetch_ds, prefetch_first_block, prefetch_last_block, buf, IMSS_BLKSIZE, prefetch_offset, IMSS_BLKSIZE * KB * (prefetch_last_block - prefetch_first_block));
				map_update_prefetch(map_prefetch, prefetch_path, prefetch_first_block, prefetch_last_block);
				if (err == -1)
					return -1;
				return size;
			}
			else
			{ // si ya tengo alguno guardado
				// printf("READV LOS QUE FALTABAN\n");
				pthread_mutex_lock(&lock);
				// printf("2ASK real ones\n");
				err = readv_multiple(ds, curr_blk, end_blk, buf + byte_count, IMSS_BLKSIZE,
									 start_offset, IMSS_BLKSIZE * KB * (end_blk - curr_blk));
				pthread_mutex_unlock(&lock);

				// PERSONAL PREFETCH
				prefetch_ds = ds;
				strcpy(prefetch_path, rpath);
				prefetch_first_block = end_blk + 1;
				if ((end_blk + PREFETCH) <= stats.st_blocks)
				{
					prefetch_last_block = (end_blk + PREFETCH);
				}
				else
				{
					prefetch_last_block = stats.st_blocks;
				}
				if (prefetch_last_block < prefetch_first_block)
				{
					free(rpath);
					return size;
				}

				prefetch_offset = start_offset;
				char *buf = map_get_buffer_prefetch(map_prefetch, prefetch_path, &exist_first_block, &exist_last_block);
				int err = readv_multiple(prefetch_ds, prefetch_first_block, prefetch_last_block, buf, IMSS_BLKSIZE, prefetch_offset, IMSS_BLKSIZE * KB * (prefetch_last_block - prefetch_first_block));
				map_update_prefetch(map_prefetch, prefetch_path, prefetch_first_block, prefetch_last_block);

				if (err == -1)
				{
					free(rpath);
					return -1;
				}
				free(rpath);
				return size;
			}
		}
		// printf("End update_prefetch\n");
		if (err != -1)
		{
			// First block case
			if (first == 0)
			{
				if (size < stats.st_size - start_offset)
				{
					// to_read = size;
					to_read = IMSS_DATA_BSIZE - start_offset;
				}
				else
				{
					if (stats.st_size < IMSS_DATA_BSIZE)
					{
						to_read = stats.st_size - start_offset;
					}
					else
					{
						to_read = IMSS_DATA_BSIZE - start_offset;
					}
				}

				// Check if offset is bigger than filled, return 0 because is EOF case
				if (start_offset > stats.st_size)
					return 0;

				memcpy(buf, aux + start_offset, to_read);
				byte_count += to_read;
				++first;

				// Middle block case
			}
			else if (curr_blk != end_blk)
			{
				// memcpy(buf + byte_count, aux + HEADER, IMSS_DATA_BSIZE);
				memcpy(buf + byte_count, aux, IMSS_DATA_BSIZE);
				byte_count += IMSS_DATA_BSIZE;
				// End block case
			}
			else
			{

				// Read the minimum between end_offset and filled (read_ = min(end_offset, filled))
				int64_t pending = size - byte_count;
				memcpy(buf + byte_count, aux, pending);
				byte_count += pending;
			}
		}
		++curr_blk;
	}
	free(rpath);
	return byte_count;
}

int imss_read(const char *path, char *buf, size_t size, off_t offset)
{
	int ret;

	if (BEST_PERFORMANCE_READ == 0)
	{
		if (MULTIPLE_READ == 1)
		{
			ret = imss_vread_prefetch(path, buf, size, offset);
		}
		else if (MULTIPLE_READ == 2)
		{
			ret = imss_vread_no_prefetch(path, buf, size, offset);
		}
		else if (MULTIPLE_READ == 3)
		{
			ret = imss_vread_2x(path, buf, size, offset);
		}
		else if (MULTIPLE_READ == 4)
		{
			// printf("ENTER IMSS_SPLIT_READV\n");
			ret = imss_split_readv(path, buf, size, offset);
		}
		else
		{
			ret = imss_sread(path, buf, size, offset);
		}
	}
	else if (BEST_PERFORMANCE_READ == 1)
	{
		if (N_SERVERS < threshold_read_servers)
		{
			// printf("[BEST_PERFORMANCE_READ] SREAD\n");
			ret = imss_sread(path, buf, size, offset);
		}
		else
		{
			// printf("[BEST_PERFORMANCE_READ] SPLIT_READV\n");
			ret = imss_split_readv(path, buf, size, offset);
		}
	}

	return ret;
}

int imss_write(const char *path, const char *buf, size_t size, off_t off)
{
	int ret;
	clock_t t, tm, tmm;

	tmm = 0;

	t = clock();

	if (MULTIPLE_WRITE == 2)
	{
		ret = imss_split_writev(path, buf, size, off);
		return ret;
	}

	char *aux_block;

	// Compute offsets to write
	int64_t curr_blk, end_blk, start_offset, end_offset;
	int64_t start_blk = off / IMSS_DATA_BSIZE + 1; // Add one to skip block 0
	start_offset = off % IMSS_DATA_BSIZE;
	// end_blk = (off+size) / IMSS_DATA_BSIZE + 1; //Add one to skip block 0
	end_blk = ceil((double)(off + size) / IMSS_DATA_BSIZE);
	end_offset = (off + size) % IMSS_DATA_BSIZE;
	curr_blk = start_blk;

	slog_live("[imss_write] size %ld, start_blk %ld, start_offset %ld, end_blk %ld, end_offset %ld, curr_blk %ld", size, start_blk, start_offset, end_blk, end_offset, curr_blk);

	// Needed variables
	size_t byte_count = 0;
	int first = 0;
	int ds = 0;
	int64_t to_copy = 0;
	uint32_t filled = 0;
	struct stat header;
	char *aux;
	char *rpath = (char *)calloc(MAX_PATH, sizeof(char));
	get_iuri(path, rpath);
	int middle  = 0;

	int fd;
	struct stat stats;

	fd_lookup(rpath, &fd, &stats, &aux);
	if (fd >= 0)
		ds = fd;
	else if (fd == -2)
		return -ENOENT;

	if (MULTIPLE_WRITE == 1)
	{
		slog_live("[imss_write] MULTIPLE_WRITE %d", MULTIPLE_WRITE);
		if ((end_blk - curr_blk) > 1)
		{
			writev_multiple(buf, ds, curr_blk, end_blk, start_offset, end_offset, IMSS_DATA_BSIZE, size);

			// Update header count if the file has become bigger
			if (size + off > stats.st_size)
			{
				stats.st_size = size + off;
				stats.st_blocks = end_blk - 1;
				pthread_mutex_lock(&lock);
				map_update(map, rpath, ds, stats);
				pthread_mutex_unlock(&lock);
			}
			free(rpath);
			return size;
		}
	}

	// For the rest of blocks
	while (curr_blk <= end_blk)
	{
		middle = 0;
		// First fragmented block
		if (first == 0 && start_offset && stats.st_size != 0)
		{
			slog_live("[imss_write] first block %ld fragmented", curr_blk);
			// Get previous block
			if (get_data(ds, curr_blk, (char *)aux) < 0)
			{
				fprintf(stderr, "[IMSS-FUSE]	Error reading from imss.\n");
				error_print = -ENOENT;
				pthread_mutex_unlock(&lock);
				free(rpath);
				return -ENOENT;
			}
			// Bytes to write are the minimum between the size parameter and the remaining space in the block (BLOCKSIZE-start_offset)
			to_copy = (size < IMSS_DATA_BSIZE - start_offset) ? size : IMSS_DATA_BSIZE - start_offset;

			memcpy(aux + start_offset, buf + byte_count, to_copy);
		}
		// Last Block
		else if (curr_blk == end_blk)
		{
			slog_live("[imss_write] last block %ld", curr_blk);
			if (end_offset != 0)
			{
				to_copy = end_offset;
			}
			else
			{
				to_copy = IMSS_DATA_BSIZE;
			}

			// Only if last block has contents
			if (curr_blk <= stats.st_blocks && start_offset)
			{
				pthread_mutex_lock(&lock);
				if (get_data(ds, curr_blk, (char *)aux) < 0)
				{
					fprintf(stderr, "[IMSS-FUSE]	Error reading from imss.\n");
					error_print = -ENOENT;
					pthread_mutex_unlock(&lock);
					free(rpath);
					return -ENOENT;
				}
				pthread_mutex_unlock(&lock);
			}
			else
			{
				memset(aux, 0, IMSS_DATA_BSIZE);
			}
			if (byte_count == size)
			{
				to_copy = 0;
			}
			memcpy(aux, buf + byte_count, to_copy);
		}
		// middle block
		else
		{
			//slog_live("[imss_write] middle block %ld", curr_blk);	
			to_copy = IMSS_DATA_BSIZE;
			middle = 1;

			int64_t pending = size - byte_count;
		}

		// Write and update variables
		//slog_live("[imss_write] set_data, block %ld", curr_blk);	

		if (middle) {
			char * aux_middle = (char *)buf + byte_count;
			if (set_data(ds, curr_blk, aux_middle) < 0)
                        {
                                fprintf(stderr, "[IMSS-FUSE]    Error writing to imss.\n");
                                error_print = -ENOENT;
                                return -ENOENT;
                        }

		} else {
			if (set_data(ds, curr_blk, aux) < 0)
			{
				fprintf(stderr, "[IMSS-FUSE]	Error writing to imss.\n");
				error_print = -ENOENT;
				return -ENOENT;
			}
		}

		byte_count += to_copy;
		++curr_blk;
		++first;
	}

	// Update header count if the file has become bigger
	if (size + off > stats.st_size)
	{
		// if(size + off != stats.st_size){
		stats.st_size = size + off;
		stats.st_blocks = curr_blk - 1;
		map_update(map, rpath, ds, stats);
	}
	free(rpath);

	t = clock() - t;
	double time_taken = ((double)t) / CLOCKS_PER_SEC; // in seconds
	double time_mem = ((double)tmm) / CLOCKS_PER_SEC; // in seconds

	slog_info("[API] imss_write time  total %f mem %f  s", time_taken, time_mem);

	return byte_count;
	}

	int imss_split_writev(const char *path, const char *buf, size_t size, off_t off)
	{
		// printf("IMSS SPLIT WRITEV\n");
		// Compute offsets to write
		int64_t curr_blk, end_blk, start_offset, end_offset;
		int64_t start_blk = off / IMSS_DATA_BSIZE + 1; // Add one to skip block 0
		start_offset = off % IMSS_DATA_BSIZE;
		// end_blk = (off+size) / IMSS_DATA_BSIZE + 1; //Add one to skip block 0
		end_blk = ceil((double)(off + size) / IMSS_DATA_BSIZE);
		end_offset = (off + size) % IMSS_DATA_BSIZE;
		curr_blk = start_blk;

		// Needed variables
		size_t byte_count = 0;
		int first = 0;
		int ds = 0;
		int64_t to_copy = 0;
		uint32_t filled = 0;
		struct stat header;
		char *aux;
		char *rpath = (char *)calloc(MAX_PATH, sizeof(char));
		get_iuri(path, rpath);

		int fd;
		struct stat stats;
		fd_lookup(rpath, &fd, &stats, &aux);
		if (fd >= 0)
			ds = fd;
		else if (fd == -2)
			return -ENOENT;

		// List of servers for the blocks
		int **list_servers = (int **)calloc(N_SERVERS, sizeof(int *));
		int total = end_blk - curr_blk + 1;

		// Initialization to 0.
		for (int i = 0; i < N_SERVERS; i++)
		{
			list_servers[i] = (int *)calloc(size, sizeof(int));
		}

		// Getting list of servers for each block
		split_location_servers(list_servers, ds, curr_blk, end_blk);

		int lenght_message = 102400;
		char **msg; // save block read for each server
		msg = calloc(N_SERVERS, sizeof(char *));
		for (int z = 0; z < N_SERVERS; z++)
		{
			msg[z] = calloc(lenght_message, sizeof(char));
		}
		int amount[N_SERVERS]; // save how many are sent to each server.

		// Preparing message for the server
		int count;

		char *all_blocks = calloc(lenght_message, sizeof(char));
		char *block = calloc(64, sizeof(char));
		char *number = calloc(64, sizeof(char));
		for (int server = 0; server < N_SERVERS; server++)
		{

			memset(all_blocks, '\0', lenght_message);
			count = 0;
			for (int i = 0; i < total; i++)
			{
				if (list_servers[server][i] > 0)
				{
					// printf("**list_servers[%d][%d]=%d\n", server,i,list_servers[server][i]);
					sprintf(block, "$%d", list_servers[server][i]);
					// printf("block=%s\n",block);
					// printf("all_block=%s length=%ld\n",all_blocks, strlen(all_blocks));
					strcat(all_blocks, block);

					count++;
				}
			}
			amount[server] = count;
			sprintf(number, "%d", count);
			strcat(msg[server], number);
			strcat(msg[server], all_blocks);
			// printf("server=%d msg_full=%s\n",server, msg[server]);
			// printf("amount=%d\n",amount[server]);
		}

		free(block);
		free(number);
		free(all_blocks);

		char **buffer_servers; // save blocks to write for each server
		buffer_servers = calloc(N_SERVERS, sizeof(char *));
		for (int z = 0; z < N_SERVERS; z++)
		{
			// printf("buffer_servers[%d]=%ld\n",z, amount[z]*IMSS_DATA_BSIZE);
			buffer_servers[z] = calloc(amount[z] * IMSS_DATA_BSIZE, sizeof(char));
		}

		for (int server = 0; server < N_SERVERS; server++)
		{
			count = 0;

			for (int i = 0; i < total; i++)
			{
				if (list_servers[server][i] > 0)
				{
					/*******COPYING DATA TO EACH SERVER BUFFER******/
					// printf("buffer_servers[%d]+%ld, buf+%ld, copy=%ld\n",server,count*IMSS_DATA_BSIZE, i*IMSS_DATA_BSIZE, IMSS_DATA_BSIZE);
					memcpy(buffer_servers[server] + count, buf + (i * IMSS_DATA_BSIZE), IMSS_DATA_BSIZE);
					/*******COPYING DATA TO EACH SERVER BUFFER******/
					count = count + IMSS_DATA_BSIZE;
				}
			}
		}

		//*********************Threads*******************************
		// Initialize pool of threads.
		pthread_t threads[(N_SERVERS)];
		thread_argv arguments[(N_SERVERS)];

		for (int server = 0; server < N_SERVERS; server++)
		{
			arguments[server].n_server = server;
			arguments[server].path = path;
			arguments[server].msg = msg[server];
			arguments[server].buffer = buffer_servers[server];
			arguments[server].size = amount[server];
			arguments[server].BLKSIZE = IMSS_BLKSIZE;
			arguments[server].start_offset = start_offset;
			arguments[server].stats_size = stats.st_size;
			arguments[server].lenght_key = lenght_message;

			if (arguments[server].size > 0)
			{
				if (pthread_create(&threads[server], NULL, split_writev, (void *)&arguments[server]) == -1)
				{
					perror("ERRIMSS_METAWORKER_DEPLOY");
					pthread_exit(NULL);
				}
			}
		}
		// Wait for the threads to conclude.
		for (int32_t server = 0; server < (N_SERVERS); server++)
		{

			if (arguments[server].size > 0)
			{
				// printf("Esperando hilo=%d\n",server);
				if (pthread_join(threads[server], NULL) != 0)
				{
					perror("ERRIMSS_METATH_JOIN");
					pthread_exit(NULL);
				}
			}
		}

		//*********************Threads*******************************

		// UPDATE FILE
		stats.st_size = size + off;
		stats.st_blocks = end_blk - 1;
		pthread_mutex_lock(&lock);
		map_update(map, rpath, ds, stats);
		pthread_mutex_unlock(&lock);
		return size; /////////////
	}
	int imss_split_readv(const char *path, char *buf, size_t size, off_t offset)
	{
		int64_t curr_blk, end_blk, start_offset, end_offset;
		int64_t first = 0;
		int ds = 0;
		curr_blk = offset / IMSS_DATA_BSIZE + 1; // Plus one to skip the header (0) block
		start_offset = offset % IMSS_DATA_BSIZE;
		// end_blk = (offset+size) / IMSS_DATA_BSIZE + 1; //Plus one to skip the header (0) block
		end_blk = ceil((double)(offset + size) / IMSS_DATA_BSIZE);
		end_offset = (offset + size) % IMSS_DATA_BSIZE;
		size_t to_read = 0;

		// printf("\n[CLIENT] [SPLIT_READ] size=%ld  offset=%ld start block=%ld, end_block=%ld\n",size, offset, curr_blk, end_blk);
		// Needed variables
		size_t byte_count = 0;
		int64_t rbytes;

		char *rpath = (char *)calloc(MAX_PATH, sizeof(char));

		get_iuri(path, rpath);

		int fd;
		struct stat stats;
		char *aux;

		fd_lookup(rpath, &fd, &stats, &aux);
		// printf("stats_size=%ld\n",stats.st_size);
		if (stats.st_size < size)
		{
			end_blk = ceil((double)(offset + stats.st_size) / IMSS_DATA_BSIZE);
		}

		// Check if offset is bigger than filled, return 0 because is EOF case
		if (start_offset >= stats.st_size)
		{
			return 0;
		}

		if (fd >= 0)
			ds = fd;
		else if (fd == -2)
			return -ENOENT;

		// List of servers for the blocks
		int **list_servers = (int **)calloc(N_SERVERS, sizeof(int *));
		int total = end_blk - curr_blk + 1;

		// Initialization to 0.
		for (int i = 0; i < N_SERVERS; i++)
		{
			list_servers[i] = (int *)calloc(size, sizeof(int));
			// bzero(list_servers[i],size*sizeof(int));
		}

		// Getting list of servers for each block
		split_location_servers(list_servers, ds, curr_blk, end_blk);

		int lenght_message = 102400;
		char **msg; // save block read for each server
		msg = calloc(N_SERVERS, sizeof(char *));
		for (int z = 0; z < N_SERVERS; z++)
		{
			msg[z] = calloc(lenght_message, sizeof(char));
		}
		int amount[N_SERVERS]; // save how many are sent to each server.

		// Preparing message for the server
		int count;

		char *all_blocks = calloc(lenght_message, sizeof(char));
		char *block = calloc(64, sizeof(char));
		char *number = calloc(64, sizeof(char));
		for (int server = 0; server < N_SERVERS; server++)
		{
			/*char all_blocks[1024];*/
			memset(all_blocks, '\0', lenght_message);
			count = 0;
			for (int i = 0; i < total; i++)
			{
				if (list_servers[server][i] > 0)
				{
					// printf("**list_servers[%d][%d]=%d\n", server,i,list_servers[server][i]);
					sprintf(block, "$%d", list_servers[server][i]);
					//	printf("block=%s\n",block);
					//	printf("all_block=%s length=%ld\n",all_blocks, strlen(all_blocks));
					strcat(all_blocks, block);
					count++;
				}
			}
			amount[server] = count;
			sprintf(number, "%d", count);
			strcat(msg[server], number);
			strcat(msg[server], all_blocks);
			printf("server=%d msg_full=%s\n", server, msg[server]);
			//	printf("amount=%d\n",amount[server]);
		}

		free(block);
		free(number);
		free(all_blocks);

		char **buffer_servers; // save block read for each server
		buffer_servers = calloc(N_SERVERS, sizeof(char *));
		for (int z = 0; z < N_SERVERS; z++)
		{
			buffer_servers[z] = calloc(amount[z] * IMSS_DATA_BSIZE, sizeof(char));
		}
		//*********************Lineal*******************************
		/*for(int server = 0; server < N_SERVERS; server++){
		  printf("server=%d, N_SERVER=%d\n",server, N_SERVERS);
		  int err = split_readv(server, path, msg[server],buffer_servers[server], amount[server], IMSS_BLKSIZE, start_offset, stats.st_size);
		  if(err == -1)
		  return -1;
		  }*/
		//*********************Lineal*******************************

		//*********************Threads*******************************
		// Initialize pool of threads.
		pthread_t threads[(N_SERVERS)];
		thread_argv arguments[(N_SERVERS)];

		for (int server = 0; server < N_SERVERS; server++)
		{
			arguments[server].n_server = server;
			arguments[server].path = path;
			arguments[server].msg = msg[server];
			arguments[server].buffer = buffer_servers[server];
			arguments[server].size = amount[server];
			arguments[server].BLKSIZE = IMSS_BLKSIZE;
			arguments[server].start_offset = start_offset;
			arguments[server].stats_size = stats.st_size;
			arguments[server].lenght_key = lenght_message;

			/*printf("\nCustom   ->buffer %p\n", buffer_servers[server]);
			  printf("arguments->buffer %p\n", arguments[server].buffer);
			  printf("arguments.n_server=%d\n",arguments[server].n_server);
			  printf("arguments.path=%s\n",arguments[server].path);
			  printf("arguments.msg=%s\n",arguments[server].msg);
			  printf("arguments.size=%d\n",arguments[server].size);
			  printf("arguments.BLKSIZE=%ld\n",arguments[server].BLKSIZE);
			  printf("arguments.start_offset=%ld\n",arguments[server].start_offset);
			  printf("arguments.stats-size=%d\n",arguments[server].stats_size);*/

			if (arguments[server].size > 0)
			{
				if (pthread_create(&threads[server], NULL, split_readv, (void *)&arguments[server]) == -1)
				{
					perror("ERRIMSS_METAWORKER_DEPLOY");
					pthread_exit(NULL);
				}
			}
		}

		// Wait for the threads to conclude.
		for (int32_t server = 0; server < (N_SERVERS); server++)
		{

			if (arguments[server].size > 0)
			{
				//	printf("Esperando hilo=%d\n",server);
				if (pthread_join(threads[server], NULL) != 0)
				{
					perror("ERRIMSS_METATH_JOIN");
					pthread_exit(NULL);
				}
			}
		}

		//*********************Threads*******************************

		size_t byte_count_servers[N_SERVERS]; // offset telling me how many i have write
		for (int server = 0; server < N_SERVERS; server++)
		{
			byte_count_servers[server] = 0;
		}

		for (int i = 0; i < total; i++)
		{
			for (int server = 0; server < N_SERVERS; server++)
			{

				if (list_servers[server][i] == curr_blk)
				{
					// printf("block find list_servers[%d][%d]=%d=%ld\n",server,i,list_servers[server][i],curr_blk);

					// First block case
					if (first == 0)
					{
						//	printf("FIRST BLOCK\n");
						if (size < (stats.st_size - start_offset) && size < IMSS_DATA_BSIZE && total == 1)
						{
							//		printf("*First block 1 case to_read=size=%ld\n",size);
							to_read = size;
						}
						else
						{
							if (stats.st_size < IMSS_DATA_BSIZE)
							{
								//			printf("*First block 2 case to_read=stats.st_size - start_offset=%ld\n",stats.st_size-start_offset);
								to_read = stats.st_size - start_offset;
							}
							else
							{
								//			printf("*First block 3 case to_read=IMSS_DATA_BSIZE- start_offset=%ld\n",IMSS_DATA_BSIZE-start_offset);
								to_read = IMSS_DATA_BSIZE - start_offset;
							}
						}
						// Check if offset is bigger than filled, return 0 because is EOF case
						if (start_offset > stats.st_size)
							return 0;

						memcpy(buf, buffer_servers[server] + start_offset, to_read);
						byte_count_servers[server] += to_read;
						byte_count += to_read;
						++first;
						// Middle block case
					}
					else if (curr_blk != end_blk)
					{
						//	printf("MIDDLE BLOCK\n");
						//	printf("curr_block=%ld, end_block=%ld\n",curr_blk, end_blk);
						//	printf("byte_count=%ld, byte_count_servers[%d]=%ld\n",byte_count,server,byte_count_servers[server]);
						memcpy(buf + byte_count, buffer_servers[server] + byte_count_servers[server], IMSS_DATA_BSIZE);
						byte_count_servers[server] += IMSS_DATA_BSIZE;
						byte_count += IMSS_DATA_BSIZE;
						// End block case
					}
					else
					{
						//	printf("LAST BLOCK\n");
						// Read the minimum between end_offset and filled (read_ = min(end_offset, filled))
						int64_t pending = size - byte_count;
						memcpy(buf + byte_count, buffer_servers[server] + byte_count_servers[server], pending);
						byte_count_servers[server] += pending;
						byte_count += pending;
					}
				}
			}
			curr_blk++;
		}

		// Releasing
		for (int i = 0; i < N_SERVERS; i++)
		{
			free(list_servers[i]);
		}
		free(list_servers);

		for (int z = 0; z < N_SERVERS; z++)
		{
			free(buffer_servers[z]);
		}
		free(buffer_servers);

		for (int z = 0; z < N_SERVERS; z++)
		{
			free(msg[z]);
		}
		free(msg);
		free(rpath);
		return byte_count;
	}

	int imss_release(const char *path)
	{
		// Update dates
		int ds = 0;
		char *rpath = (char *)calloc(MAX_PATH, sizeof(char));
		get_iuri(path, rpath);

		int fd;
		struct stat stats;
		char *aux;
		fd_lookup(rpath, &fd, &stats, &aux);
		if (fd >= 0)
			ds = fd;
		else
			return -ENOENT;

		char head[IMSS_DATA_BSIZE];

		// Get time
		struct timespec spec;
		clock_gettime(CLOCK_REALTIME, &spec);

		// Update time
		stats.st_mtim = spec;
		stats.st_ctim = spec;

		// write metadata
		memcpy(head, &stats, sizeof(struct stat));
		//pthread_mutex_lock(&lock);
		// fprintf(stderr,"[Client for file %s] file size %ld\n", path, stats.st_size);
		if (set_data(ds, 0, head) < 0)
		{
			fprintf(stderr, "[IMSS-FUSE][release]	Error writing to imss.\n");
			error_print = -ENOENT;
			pthread_mutex_unlock(&lock);
			free(rpath);
			return -ENOENT;
		}

		//pthread_mutex_unlock(&lock);
		free(rpath);
		return 0;
	}


	int imss_close(const char *path)
	{
		clock_t t;
		t = clock();
		flush_data();
		imss_release(path);
		imss_refresh(path);
		t = clock() -t ;

		double time_taken = ((double)t) / CLOCKS_PER_SEC; // in seconds

        slog_info("[API] imss_close time  total %f s", time_taken);

		return 0;
	}

	int imss_create(const char *path, mode_t mode, uint64_t *fh)
	{
		int ret = 0;
		struct timespec spec;
		// TODO check mode
		struct stat ds_stat;
		// Check if already created!
		char *rpath = (char *)calloc(MAX_PATH, sizeof(char));
		get_iuri(path, rpath);
		slog_live("[imss_create] get_iuri(path:%s, rpath:%s)", path, rpath);

		// Assing file handler and create dataset
		int res = 0;
		res = create_dataset((char *)rpath, POLICY, N_BLKS, IMSS_BLKSIZE, REPL_FACTOR);
		slog_live("[imss_create] create_dataset((char*)rpath:%s, POLICY:%s,  N_BLKS:%ld, IMSS_BLKSIZE:%d, REPL_FACTOR:%ld), res:%d", (char *)rpath, POLICY, N_BLKS, IMSS_BLKSIZE, REPL_FACTOR, res);
		if (res < 0)
		{
			if (IMSS_DEBUG)
				fprintf(stderr, "[imss_create]	Cannot create new dataset.\n");
			slog_error("[imss_create] Cannot create new dataset.\n");
			free(rpath);
			return res;
		}
		else
		{
			*fh = res;
		}
		clock_gettime(CLOCK_REALTIME, &spec);

		memset(&ds_stat, 0, sizeof(struct stat));

		// Create initial block
		ds_stat.st_size = 0;
		ds_stat.st_nlink = 1;
		ds_stat.st_atime = spec.tv_sec;
		ds_stat.st_mtime = spec.tv_sec;
		ds_stat.st_ctime = spec.tv_sec;
		ds_stat.st_uid = getuid();
		ds_stat.st_gid = getgid();
		ds_stat.st_blocks = 0;
		ds_stat.st_blksize = IMSS_DATA_BSIZE;
		slog_live("[imss_create] IMSS_DATA_BSIZE:%ld", IMSS_DATA_BSIZE);
		if (!S_ISDIR(mode))
			mode |= S_IFREG;
		ds_stat.st_mode = mode;

		// Write initial block
		char *buff = (char *)malloc(IMSS_DATA_BSIZE); //[IMSS_DATA_BSIZE];
		memcpy(buff, &ds_stat, sizeof(struct stat));
		pthread_mutex_lock(&lock); // lock.
		set_data(*fh, 0, (char *)buff);
		pthread_mutex_unlock(&lock); // unlock.

		//ret = 
		map_erase(map, rpath);
		slog_live("[imss_create] map_erase(map, rpath:%s), ret:%d", rpath, ret);
		// if(ret < 1){
		// 	slog_live("No elements erased by map_erase, ret:%d", ret);
		// }

		pthread_mutex_lock(&lock_file); // lock.
		map_put(map, rpath, *fh, ds_stat, buff);
		slog_live("[imss_create] map_put(map, rpath:%s, fh:%ld, ds_stat, buff:%s)", rpath, *fh, buff);
		if (PREFETCH != 0)
		{
			char *buff = (char *)malloc(PREFETCH * IMSS_BLKSIZE * KB);
			map_init_prefetch(map_prefetch, rpath, buff);
			slog_live("[imss_create] PREFETCH:%ld, map_init_prefetch(map_prefetch, rpath:%s, buff:%s)", PREFETCH, rpath, buff);
		}
		pthread_mutex_unlock(&lock_file); // unlock.
		free(rpath);
		return 0;
	}

	// Does nothing
	int imss_opendir(const char *path)
	{
		return 0;
	}

	// Does nothing
	int imss_releasedir(const char *path)
	{
		return 0;
	}

	// Remove directory
	int imss_rmdir(const char *path)
	{

		// Needed variables for the call
		char *buffer;
		char **refs;
		int n_ent = 0;
		char *imss_path = (char *)calloc(MAX_PATH, sizeof(char));
		get_iuri(path, imss_path);

		if (imss_path[strlen(imss_path) - 1] != '/')
		{
			strcat(imss_path, "/");
		}

		if ((n_ent = get_dir((char *)imss_path, &buffer, &refs)) > 0)
		{
			if (n_ent > 1)
			{
				return -EPERM;
			}
		}
		else
		{
			free(imss_path);
			return -ENOENT;
		}
		imss_unlink(imss_path);
		free(imss_path);
		return 0;
	}

	int imss_unlink(const char *path)
	{

		char *imss_path = (char *)calloc(MAX_PATH, sizeof(char));

		get_iuri(path, imss_path);

		uint32_t ds;
		int fd;
		struct stat stats;
		char *buff;
		fd_lookup(imss_path, &fd, &stats, &buff);
		if (fd >= 0)
			ds = fd;
		else
			return -ENOENT;

		pthread_mutex_lock(&lock);
		get_data(ds, 0, buff);
		pthread_mutex_unlock(&lock);

		// Read header
		struct stat header;
		memcpy(&header, buff, sizeof(struct stat));

		// header.st_blocks = INT32_MAX;
		// header.st_nlink = 0;
		header.st_nlink = header.st_nlink - 1;

		// Write initial block
		memcpy(buff, &header, sizeof(struct stat));
		pthread_mutex_lock(&lock);
		set_data(ds, 0, (char *)buff);
		pthread_mutex_unlock(&lock);

		pthread_mutex_lock(&lock_file);
		map_erase(map, imss_path);
		pthread_mutex_unlock(&lock_file);

		map_release_prefetch(map_prefetch, path);

		delete_dataset(imss_path);

		release_dataset(ds);
		free(imss_path);
		return 0;
	}

	int imss_utimens(const char *path, const struct timespec tv[2])
	{
		struct stat ds_stat;
		struct timespec spec;
		clock_gettime(CLOCK_REALTIME, &spec);
		uint32_t file_desc;

		char *rpath = (char *)calloc(MAX_PATH, sizeof(char));
		get_iuri(path, rpath);

		// Assing file handler and create dataset
		int fd;
		struct stat stats;
		char *buff;
		fd_lookup(rpath, &fd, &stats, &buff);

		if (fd >= 0)
			file_desc = fd;
		else if (fd == -2)
			return -ENOENT;
		else
			file_desc = open_dataset(rpath);
		if (file_desc < 0)
		{
			if (IMSS_DEBUG)
				fprintf(stderr, "[IMSS-FUSE]    Cannot open dataset.\n");
		}

		// char *buff = malloc(IMSS_DATA_BSIZE);
		pthread_mutex_lock(&lock);
		get_data(file_desc, 0, (char *)buff);
		pthread_mutex_unlock(&lock);

		memcpy(&ds_stat, buff, sizeof(struct stat));

		ds_stat.st_mtime = spec.tv_sec;

		// Write initial block
		memcpy(buff, &ds_stat, sizeof(struct stat));

		pthread_mutex_lock(&lock);
		set_data(file_desc, 0, (char *)buff);
		pthread_mutex_unlock(&lock);

		free(rpath);

		return 0;
	}

	int imss_mkdir(const char *path, mode_t mode)
	{
		char *rpath = (char *)calloc(MAX_PATH, sizeof(char));
		uint64_t fi;
		strcpy(rpath, path);
		if (path[strlen(path) - 1] != '/')
		{
			strcat(rpath, "/");
		}
		imss_create(rpath, mode | S_IFDIR, &fi);
		free(rpath);
		return 0;
	}

	int imss_flush(const char *path)
	{

		if (error_print != 0)
		{
			int32_t err = error_print;
			error_print = 0;
			return err;
		}
		// struct stat ds_stat;
		struct timespec spec;
		clock_gettime(CLOCK_REALTIME, &spec);
		uint32_t file_desc;

		char *rpath = (char *)calloc(MAX_PATH, sizeof(char));
		get_iuri(path, rpath);

		// Assing file handler and create dataset
		int fd;
		struct stat stats;
		char *buff;
		fd_lookup(rpath, &fd, &stats, &buff);

		if (fd >= 0)
			file_desc = fd;
		else if (fd == -2)
			return -ENOENT;
		else
			file_desc = open_dataset(rpath);
		if (file_desc < 0)
		{
			if (IMSS_DEBUG)
				fprintf(stderr, "[IMSS-FUSE]    Cannot open dataset.\n");
			return -EACCES;
		}

		// char *buff = malloc(IMSS_DATA_BSIZE);

		stats.st_mtime = spec.tv_sec;

		// Write initial block
		memcpy(buff, &stats, sizeof(struct stat));

		pthread_mutex_lock(&lock);
		if (set_data(file_desc, 0, (char *)buff) < 0)
		{
			if (IMSS_DEBUG)
				fprintf(stderr, "[IMSS-FUSE]	Error writing to imss.\n");
			error_print = -ENOENT;
			pthread_mutex_unlock(&lock);
			return -ENOENT;
		}

		pthread_mutex_unlock(&lock);
		free(rpath);
		return 0;
	}

	int imss_getxattr(const char *path, const char *attr, char *value, size_t s)
	{
		return 0;
	}

	int imss_chmod(const char *path, mode_t mode)
	{

		struct stat ds_stat;
		uint32_t file_desc;
		char *rpath = (char *)calloc(MAX_PATH, sizeof(char));
		get_iuri(path, rpath);

		// Assing file handler and create dataset
		int fd;
		struct stat stats;
		char *buff;
		fd_lookup(rpath, &fd, &stats, &buff);

		if (fd >= 0)
			file_desc = fd;
		else if (fd == -2)
			return -ENOENT;
		else
			file_desc = open_dataset(rpath);
		if (file_desc < 0)
		{
			if (IMSS_DEBUG)
				fprintf(stderr, "[IMSS-FUSE]    Cannot open dataset.\n");
		}

		pthread_mutex_lock(&lock);
		get_data(file_desc, 0, (char *)buff);
		pthread_mutex_unlock(&lock);

		memcpy(&ds_stat, buff, sizeof(struct stat));

		ds_stat.st_mode = mode;

		// Write initial block
		memcpy(buff, &ds_stat, sizeof(struct stat));

		pthread_mutex_lock(&lock);
		set_data(file_desc, 0, (char *)buff);
		pthread_mutex_unlock(&lock);

		free(rpath);
		return 0;
	}

	int imss_chown(const char *path, uid_t uid, gid_t gid)
	{
		struct stat ds_stat;
		uint32_t file_desc;
		char *rpath = (char *)calloc(MAX_PATH, sizeof(char));
		get_iuri(path, rpath);

		// Assing file handler and create dataset
		int fd;
		struct stat stats;
		char *buff;
		fd_lookup(rpath, &fd, &stats, &buff);

		if (fd >= 0)
			file_desc = fd;
		else if (fd == -2)
			return -ENOENT;
		else
			file_desc = open_dataset(rpath);
		if (file_desc < 0)
		{
			if (IMSS_DEBUG)
				fprintf(stderr, "[IMSS-FUSE]    Cannot open dataset.\n");
		}

		pthread_mutex_lock(&lock);
		get_data(file_desc, 0, (char *)buff);
		pthread_mutex_unlock(&lock);

		memcpy(&ds_stat, buff, sizeof(struct stat));

		ds_stat.st_uid = uid;
		if (gid != -1)
		{
			ds_stat.st_gid = gid;
		}

		// Write initial block
		memcpy(buff, &ds_stat, sizeof(struct stat));

		pthread_mutex_lock(&lock);
		set_data(file_desc, 0, (char *)buff);
		pthread_mutex_unlock(&lock);
		free(rpath);
		return 0;
	}

	int imss_rename(const char *old_path, const char *new_path)
	{
		// printf("Imss rename\n");
		struct stat ds_stat_n;
		int file_desc_o, file_desc_n;
		int fd = 0;
		char *old_rpath = (char *)calloc(MAX_PATH, sizeof(char));
		get_iuri(old_path, old_rpath);

		char *new_rpath = (char *)calloc(MAX_PATH, sizeof(char));
		get_iuri(new_path, new_rpath);

		// CHECKING IF IS MV DIR TO DIR
		// check old_path if it is a directory if it is add / at the end
		int res = imss_getattr(old_path, &ds_stat_n);

		if (res == 0)
		{
			if (S_ISDIR(ds_stat_n.st_mode))
			{

				strcat(old_rpath, "/");

				int fd;
				struct stat stats;
				char *aux;
				fd_lookup(old_rpath, &fd, &stats, &aux);
				if (fd >= 0)
					file_desc_o = fd;
				else if (fd == -2)
					return -ENOENT;
				else
					file_desc_o = open_dataset(old_rpath);

				if (file_desc_o < 0)
				{
					if (IMSS_DEBUG)
						fprintf(stderr, "[IMSS-FUSE]    Cannot open dataset.\n");
					free(new_rpath);
					return -ENOENT;
				}

				// If origin path is a directory then we are in the case of mv dir to dir
				// Extract destination directory from path
				int pos = 0;
				for (int c = 0; c < strlen(new_path); ++c)
				{
					if (new_path[c] == '/')
					{
						if (c + 1 < strlen(new_path))
							pos = c;
					}
				}
				char *dir_dest = (char *)calloc(MAX_PATH, sizeof(char));
				memcpy(dir_dest, &new_path[0], pos + 1);

				char *rdir_dest = (char *)calloc(MAX_PATH, sizeof(char));
				get_iuri(dir_dest, rdir_dest);

				res = imss_getattr(dir_dest, &ds_stat_n);
				if (res == 0)
				{
					if (S_ISDIR(ds_stat_n.st_mode))
					{
						// WE ARE IN MV DIR TO DIR
						map_rename_dir_dir(map, old_rpath, new_rpath);
						if (MULTIPLE_READ == 1)
						{
							map_rename_dir_dir_prefetch(map_prefetch, old_rpath, new_rpath);
						}
						// RENAME LOCAL_IMSS(GARRAY), SRV_STAT(MAP & TREE)
						rename_dataset_metadata_dir_dir(old_rpath, new_rpath);

						// RENAME SRV_WORKER(MAP)

						rename_dataset_srv_worker_dir_dir(old_rpath, new_rpath, fd, 0);
						free(dir_dest);
						free(rdir_dest);
						free(old_rpath);
						free(new_rpath);
						return 0;
					}
				}
			}
		}

		// MV FILE TO FILE OR MV FILE TO DIR
		// Assing file handler
		fd;
		struct stat stats;
		char *aux;
		fd_lookup(old_rpath, &fd, &stats, &aux);

		if (fd >= 0)
			file_desc_o = fd;
		else if (fd == -2)
			return -ENOENT;
		else
			file_desc_o = open_dataset(old_rpath);

		if (file_desc_o < 0)
		{
			if (IMSS_DEBUG)
				fprintf(stderr, "[IMSS-FUSE]    Cannot open dataset.\n");
			free(old_rpath);
			free(new_rpath);
			return -ENOENT;
		}
		res = imss_getattr(new_path, &ds_stat_n);

		if (res == 0)
		{
			// printf("**************EXISTE EL DESTINO=%s\n",new_path);
			// printf("new_path[last]=%c\n",new_path[strlen(new_path) -1]);
			if (S_ISDIR(ds_stat_n.st_mode))
			{
				// Because of how the path arrive never get here.
			}
			else
			{
				// printf("**************TENGO QUE BORRARLO ES UN FICHERO=%s\n",new_path);
				imss_unlink(new_path);
			}
		}
		else
		{
			// printf("**************NO EXISTE EL DESTINO=%s\n",new_path);
		}

		// printf("old_rpath=%s, new_rpath=%s\n",old_rpath, new_rpath);

		map_rename(map, old_rpath, new_rpath);
		//TODO   map_rename_prefetch(map_prefetch, old_rpath, new_rpath);

		// RENAME LOCAL_IMSS(GARRAY), SRV_STAT(MAP & TREE)
		rename_dataset_metadata(old_rpath, new_rpath);
		// RENAME SRV_WORKER(MAP)
		rename_dataset_srv_worker(old_rpath, new_rpath, fd, 0);

		free(old_rpath);
		free(new_rpath);
		return 0;
	}
