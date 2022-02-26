/*
FUSE: Filesystem in Userspace
Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

This program can be distributed under the terms of the GNU GPL.
See the file COPYING.

gcc -Wall imss.c `pkg-config fuse --cflags --libs` -o imss
 */

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

#include "imss_posix_api.h"

#define KB 1024
#define HEADER sizeof(uint32_t)


/*
   -----------	IMSS Global variables, filled at the beggining or by default -----------
 */
int32_t error_print=0;//Temporal solution maybe to change in the future.

extern int32_t REPL_FACTOR;


extern int32_t N_BLKS;
extern char * POLICY; 
extern uint64_t IMSS_BLKSIZE;
extern uint64_t IMSS_DATA_BSIZE; 
extern void * map;

//char fd_table[1024][MAX_PATH]; 

#define MAX_PATH 256

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t lock_fileopen = PTHREAD_MUTEX_INITIALIZER;


/*
   (*) Mapping for REPL_FACTOR values:
   NONE = 1;
   DRM = 2;
   TRM = 3;
 */

/*
   -----------	Auxiliar Functions -----------
 */




long fd_lookup(const char * path) {
	pthread_mutex_lock(&lock_fileopen);
	int fd = -1;
	int found;

	found = map_search(map, path, &fd);
	if (!found)
		fd = -1;
	pthread_mutex_unlock(&lock_fileopen);
	return fd;
}

void get_iuri(const char * path, /*output*/ char * uri){

	//Copying protocol
	strcpy(uri, "imss:/");
	strcat(uri, path);
}

/*
   -----------	FUSE IMSS implementation -----------
 */


int imss_truncate(const char * path, off_t offset) {
	return 0;
}
int imss_access(const char *path, int permission)
{
	return 0;
}

int imss_getattr(const char *path, struct stat *stbuf)
{

	struct fuse_context * ctx;
	ctx = fuse_get_context();

	//Needed variables for the call
	char * buffer;
	char ** refs;
	char head[IMSS_BLKSIZE*KB];
	int n_ent;
	char imss_path[MAX_PATH] = {0};
	dataset_info metadata;
	struct timespec spec;

	bzero(imss_path, MAX_PATH);
	get_iuri(path, imss_path);

	memset(stbuf, 0, sizeof(struct stat));
	clock_gettime(CLOCK_REALTIME, &spec);

	stbuf->st_atim = spec; 
	stbuf->st_mtim = spec;
	stbuf->st_ctim = spec;
	stbuf->st_uid = ctx->uid;
	stbuf->st_gid = ctx->gid;
	stbuf->st_blksize = IMSS_BLKSIZE;

	uint32_t type = get_type(imss_path);
	if (type == 0) {
		strcat(imss_path,"/");
		type = get_type(imss_path);
	}

	switch(type){

		case 0:
			return -ENOENT;
		case 1:
			if((n_ent = get_dir((char*)imss_path, &buffer, &refs)) != -1){
				stbuf->st_size = 4;
				stbuf->st_nlink = 1;
				stbuf->st_mode = S_IFDIR | 0755;

				//Free resources
				//free(buffer);
				free(refs);

				return 0;
			} 
			else return -ENOENT;

		case 2: //Case file
			printf("GetAttribute: path %s\n", imss_path);
			if(stat_dataset(imss_path, &metadata) == -1){
				fprintf(stderr, "[IMSS-FUSE]	Cannot get dataset metadata.");
				return -ENOENT;
			}



			//Get header
			//FIXME! Not always possible!!!!
			//
			//
			uint32_t ds;

			int fd = fd_lookup(imss_path);
			if (fd >= 0) 
				ds = fd;
			else {
				ds = open_dataset(imss_path);
				if (ds >=0) {
					pthread_mutex_lock(&lock_fileopen);
					map_put(map, imss_path, ds);
					pthread_mutex_unlock(&lock_fileopen);
				}	else{
					fprintf(stderr, "[IMSS-FUSE]	Cannot get dataset metadata.");
					return -ENOENT;
				}
					
			}

			pthread_mutex_lock(&lock);
			get_data(ds, 0, head);
			pthread_mutex_unlock(&lock);
			//Read header
			struct stat header;
			memcpy(&header, head, sizeof(struct stat));

			if (header.st_nlink != 0) {
				memcpy(stbuf, &header, sizeof(struct stat));
			} else {
				fprintf(stderr, "[IMSS-FUSE]	Cannot get dataset metadata.");
				return -ENOENT;
			}

			return 0;

		default:
			return -1;
	}


	/*

	   memset(stbuf, 0, sizeof(struct stat));
	   if (strcmp(path, "/") == 0) {
	   stbuf->st_mode = S_IFDIR | 0755;
	   stbuf->st_nlink = 2;
	   } else if (strcmp(path, imss_path) == 0) {
	   stbuf->st_mode = S_IFREG | 0444;
	   stbuf->st_nlink = 1;
	   stbuf->st_size = strlen(imss_str);
	   } else
	   res = -ENOENT;*/

}

/*
   Read directory

   The filesystem may choose between two modes of operation:

   -->	1) The readdir implementation ignores the offset parameter, and passes zero to the filler function's offset. 
   The filler function will not return '1' (unless an error happens), so the whole directory is read in a single readdir operation.

   2) The readdir implementation keeps track of the offsets of the directory entries. It uses the offset parameter
   and always passes non-zero offset to the filler function. When the buffer is full (or an error happens) the filler function will return '1'. 
 */
int imss_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
		off_t offset, struct fuse_file_info *fi)
{
	//Needed variables for the call
	char * buffer;
	char ** refs;
	int n_ent = 0;

	char imss_path[MAX_PATH] = {0};

	bzero(imss_path, MAX_PATH);
	get_iuri(path, imss_path);

	//Call IMSS to get metadata
	if((n_ent = get_dir((char*)imss_path, &buffer, &refs)) < 0){
		strcat(imss_path,"/");
		if((n_ent = get_dir((char*)imss_path, &buffer, &refs)) < 0){	
			fprintf(stderr, "[IMSS-FUSE]	Error retrieving directories for URI=%s", path);
			return -ENOENT;
		}
	}

	//Fill buffer
	//TODO: Check if subdirectory
	for(int i = 0; i < n_ent; ++i) {
		if (i == 0) {
			filler(buf, ".", NULL, 0);
			filler(buf, "..", NULL, 0);
		} else {
			struct stat stbuf;
			int error = imss_getattr(refs[i]+6, &stbuf); 
			if (!error) {
				char *last = refs[i] + strlen(refs[i]) - 1;
				if (last[0] == '/') {
					last[0] = '\0';
				}
				int offset = 0;
				for (int j = 0; j < strlen(refs[i]); ++j) {
					if (refs[i][j] == '/'){
						offset = j;
					}

				}

				filler(buf, refs[i] + offset + 1,  &stbuf, 0); 
			}
			free(refs[i]);
		}
	}
	//Free resources
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

int imss_open(const char *path, struct fuse_file_info *fi)
{
	//TODO -> Access control
	//DEBUG
	char imss_path[MAX_PATH] = {0};
	int32_t file_desc;

	bzero(imss_path, MAX_PATH);
	get_iuri(path, imss_path);


	int fd = fd_lookup(imss_path);

	if (fd >= 0) 
		file_desc = fd;
	else if (fd == -2)
		return -1;
	else {
		file_desc = open_dataset(imss_path);
		pthread_mutex_lock(&lock_fileopen);
		map_put(map, imss_path, file_desc);
		pthread_mutex_unlock(&lock_fileopen);
	}


	//File does not exist	
	if(file_desc < 0) return -ENOENT;

	//Assign file descriptor
	fi->fh = file_desc;

	/*if ((fi->flags & 3) != O_RDONLY)
	  return -EACCES;*/

	return 0;
}

int imss_read(const char *path, char *buf, size_t size, off_t offset,
		struct fuse_file_info *fi)
{

	//printf("offset=%d\n",offset);
	//Compute current block and offsets
	int64_t curr_blk, end_blk, start_offset, end_offset;
	int64_t first = 0; 
	int ds = 0;
	curr_blk = offset / IMSS_DATA_BSIZE +1; //Plus one to skip the header (0) block
	start_offset = offset % IMSS_DATA_BSIZE;
	end_blk = (offset+size) / IMSS_DATA_BSIZE +1; //Plus one to skip the header (0) block
	end_offset = (offset+size) % IMSS_DATA_BSIZE;
	size_t to_read = 0;

	//Needed variables
	size_t byte_count = 0;
	int64_t rbytes;
	char * aux = (char *) malloc(IMSS_BLKSIZE*KB);

	//Read header
	//get_data(fi->fh, 0, (unsigned char*)aux);
	//memcpy(&header, aux, sizeof(struct stat));

	char rpath[MAX_PATH];
	get_iuri(path, rpath);

	int fd = fd_lookup(rpath);
	if (fd >= 0) 
		ds = fd;
	else if (fd == -2)
		return -ENOENT;
	else
		ds = open_dataset(rpath);


	memset(buf, 0, size);
	//Read remaining blocks
	while(curr_blk <= end_blk){


		pthread_mutex_lock(&lock);
		int err = get_data(ds, curr_blk, (unsigned char*)aux);
		pthread_mutex_unlock(&lock);

		if( err != -1){

			//Parse data
			uint32_t filled;
			memcpy (&filled, aux, HEADER);

			//Parse buffer

			//First block case
			if (first == 0) {
				if(size < filled - start_offset) to_read = size;
				else to_read = filled - start_offset;
				//Check if offset is bigger than filled, return 0 because is EOF case
				if(start_offset > filled) 
					return 0; 
				memcpy(buf, aux + HEADER + start_offset, to_read);
				byte_count += to_read;
				++first;

				//Middle block case
			} else if (curr_blk != end_blk) {

				//Read only filled
				to_read = filled;
				memcpy(buf + byte_count, aux + HEADER, to_read);
				byte_count += filled;


				//End block case
			}  else {

				//Read the minimum between end_offset and filled (read_ = min(end_offset, filled))
				int64_t pending = filled;

				if (filled >= end_offset)
					pending = end_offset;

				if (size < pending)
					to_read = size;
				else
					to_read = pending;

				memcpy(buf + byte_count, aux + sizeof(uint32_t), to_read);
				byte_count += to_read;

			}

		} 

		++curr_blk;
		size-=to_read;
	}
	free(aux);

	return byte_count;
}

int imss_write(const char *path, const char *buf, size_t size,//if buffer is "" it has \nola\n why?¿ echo ""> /tmp/testimss/prueba ver foto
		off_t off, struct fuse_file_info *fi)
{
	//Compute offsets to write
	int64_t curr_blk, end_blk, start_offset, end_offset;
	curr_blk = off / IMSS_DATA_BSIZE + 1; //Add one to skip block 0
	start_offset = off % IMSS_DATA_BSIZE;
	end_blk = (off+size) / IMSS_DATA_BSIZE + 1; //Add one to skip block 0
	end_offset = (off+size) % IMSS_DATA_BSIZE; 

	//Needed variables
	size_t byte_count = 0;
	int64_t wbytes;
	int first = 0;
	int ds = 0;
	int64_t to_copy;
	uint32_t filled;
	struct stat header;
	char *aux = malloc(IMSS_BLKSIZE*KB);

	char rpath[MAX_PATH];
	bzero(rpath, MAX_PATH);
	get_iuri(path, rpath);


	int fd = fd_lookup(rpath);
	if (fd >= 0) 
		ds = fd;
	else if (fd == -2)
		return -ENOENT;
	else
		ds = open_dataset(rpath);


	//Read header
	pthread_mutex_lock(&lock);
	if(get_data(ds, 0, (unsigned char*)aux) < 0){
		fprintf(stderr, "[IMSS-FUSE]	Error reading metadata.\n");
		error_print=-ENOENT;
		pthread_mutex_unlock(&lock);
		return -ENOENT;
	}
	pthread_mutex_unlock(&lock);
	memcpy(&header, aux, sizeof(struct stat));

	//For the rest of blocks
	while(curr_blk <= end_blk){

		//Fisr block
		if (first==0 && start_offset && header.st_size != 0) {
			//Get previous block
			pthread_mutex_lock(&lock);
			if(get_data(ds, curr_blk, (unsigned char *)aux) < 0){
				fprintf(stderr, "[IMSS-FUSE]	Error reading from imss.\n");
				error_print=-ENOENT;
				pthread_mutex_unlock(&lock);
				return -ENOENT;
			}
			pthread_mutex_unlock(&lock);
			//Bytes to write are the minimum between the size parameter and the remaining space in the block (BLOCKSIZE-start_offset)
			to_copy = (size < IMSS_DATA_BSIZE-start_offset) ? size : IMSS_DATA_BSIZE-start_offset;

			memcpy(&filled, aux, sizeof(uint32_t));
			memcpy(aux + HEADER + start_offset, buf + byte_count, to_copy);

			//Check if there is need to update the offset
			if(start_offset + to_copy > filled){

				filled = start_offset + to_copy;
				memcpy(aux, &filled, sizeof(uint32_t));
			} 
		}
		//Last Block
		else if(curr_blk == end_blk){
			to_copy = end_offset;
			//Only if last block has contents
			if(curr_blk <= header.st_blocks){
				pthread_mutex_lock(&lock);
				if(get_data(ds, curr_blk, (unsigned char *)aux) < 0){
					fprintf(stderr, "[IMSS-FUSE]	Error reading from imss.\n");
					error_print=-ENOENT;
					pthread_mutex_unlock(&lock);
					return -ENOENT;
				}
				pthread_mutex_unlock(&lock);
				memcpy(&filled, aux, sizeof(uint32_t));
			}
			else{
				memset(aux, 0, IMSS_BLKSIZE*KB);
				filled = to_copy;
				memcpy(aux, &filled, sizeof(uint32_t));
			}

			memcpy(aux + HEADER, buf + byte_count, to_copy);

			//When copied bytes exceed offset, update filled 
			if(to_copy > filled){
				filled = to_copy;
				memcpy(aux, &filled, sizeof(uint32_t));
			}
		}
		//middle block
		else{
			to_copy = IMSS_DATA_BSIZE;
			memcpy(aux, &to_copy, sizeof(uint32_t));
			memcpy(aux + HEADER, buf + byte_count, to_copy);
		}

		//Write and update variables
		pthread_mutex_lock(&lock);
		if(set_data(ds, curr_blk, (unsigned char *)aux) < 0){
			fprintf(stderr, "[IMSS-FUSE]	Error writing to imss.\n");
			error_print=-ENOENT;
			pthread_mutex_unlock(&lock);
			return -ENOENT;
		}
		pthread_mutex_unlock(&lock);

		byte_count += to_copy;
		++curr_blk;
		++first;
	}

	//Update header count if the file has become bigger
	if(size + off > header.st_size){
		header.st_size = size + off;
		header.st_blocks = curr_blk-1;
		memcpy(aux, &header, sizeof(struct stat));
		pthread_mutex_lock(&lock);
		if(set_data(ds, 0, (unsigned char*)aux) < 0){
			fprintf(stderr, "[IMSS-FUSE]	Error writing to imss.\n");
			error_print=-ENOENT;
			pthread_mutex_unlock(&lock);
			return -ENOENT;
		}
		pthread_mutex_unlock(&lock);
	}

	free(aux);
	return byte_count;
}

int imss_release(const char * path, struct fuse_file_info *fi)
{
	//Update dates

	int ds = 0;
	char rpath[MAX_PATH];
	bzero(rpath, MAX_PATH);
	get_iuri(path, rpath);

	int fd = fd_lookup(rpath);
	if (fd >= 0) 
		ds = fd;
	else 
		return  -ENOENT;

	char head[IMSS_BLKSIZE*KB];
	pthread_mutex_lock(&lock);
	if(get_data(ds, 0, head) < 0){
		fprintf(stderr, "[IMSS-FUSE][release]	Error reading from imss.\n");
		error_print=-ENOENT;
		pthread_mutex_unlock(&lock);
		return -ENOENT;
	}

	pthread_mutex_unlock(&lock);
	struct stat header;
	memcpy(&header, head, sizeof(struct stat));

	//Get time
	struct timespec spec;
	clock_gettime(CLOCK_REALTIME, &spec);

	//Update time
	header.st_mtim = spec;
	header.st_ctim = spec;


	//write metadata
	memcpy(head, &header, sizeof(struct stat));
	pthread_mutex_lock(&lock);
	if(set_data(ds, 0, head) < 0){
		fprintf(stderr, "[IMSS-FUSE][release]	Error writing to imss.\n");
		error_print=-ENOENT;
		pthread_mutex_unlock(&lock);
		return -ENOENT;
	}
	pthread_mutex_unlock(&lock);


	//strcpy(fd_table[fi->fh], "");

	/*pthread_mutex_lock(&lock_fileopen);
	  unsigned long index = fileopen_index(rpath);
	  strcpy(fileopen_table[index].path, "");
	  fileopen_table[index].fd=-1;
	  pthread_mutex_unlock(&lock_fileopen);
	 */

	//Release resources
	//return release_dataset(fi->fh);
	return 0;
}

int imss_create(const char * path, mode_t mode, struct fuse_file_info * fi)
{
	struct timespec spec;
	struct fuse_context * ctx;
	ctx = fuse_get_context();

	//TODO check mode
	struct stat ds_stat;

	//Check if already created!
	char rpath[MAX_PATH];
	bzero(rpath, MAX_PATH);
	get_iuri(path, rpath);

	//Assing file handler and create dataset
	int res = 0;


	res = create_dataset((char*)rpath, POLICY,  N_BLKS, IMSS_BLKSIZE, REPL_FACTOR);
	if(res < 0) {
		fprintf(stderr, "[IMSS-FUSE]	Cannot create new dataset.\n");
		return res;
	} else
		fi->fh = res;

	clock_gettime(CLOCK_REALTIME, &spec);

	memset(&ds_stat, 0, sizeof(struct stat));

	//Create initial block
	ds_stat.st_size = 0;
	ds_stat.st_nlink = 1;
	ds_stat.st_atime = spec.tv_sec;
	ds_stat.st_mtime = spec.tv_sec;
	ds_stat.st_ctime = spec.tv_sec;
	ds_stat.st_uid = ctx->uid;
	ds_stat.st_gid = ctx->gid;
	ds_stat.st_blocks = 0;
	ds_stat.st_blksize = IMSS_BLKSIZE*KB;	
	if (!S_ISDIR(mode))
		mode |= S_IFREG;
	ds_stat.st_mode = mode;


	//Write initial block
	char *buff = (char *) malloc(IMSS_BLKSIZE*KB);//[IMSS_BLKSIZE*KB];
	memcpy(buff, &ds_stat, sizeof(struct stat));
	pthread_mutex_lock(&lock);
	set_data(fi->fh, 0, (unsigned char *)buff);
	pthread_mutex_unlock(&lock);
	//set_data(fi->fh, 0, (unsigned char *)buff);//first file handler return 1
	//strcpy(fd_table[fi->fh], rpath);

	pthread_mutex_lock(&lock_fileopen);
	map_put(map, rpath, fi->fh);
	/*if (!S_ISDIR(mode)){
	  fileopen_table[index].type=2;
	  }else{
	  fileopen_table[index].type=1;
	  }*/


	pthread_mutex_unlock(&lock_fileopen);

	free(buff);
	return 0; 

}

//Does nothing
int imss_opendir(const char * path, struct fuse_file_info * fi) {
	return 0;
}

//Does nothing
int imss_releasedir(const char * path, struct fuse_file_info * fi) {
	//printf("-----------Release_dir %s\n",path);
	return 0;
}

//Remove directory
int imss_rmdir(const char * path){

	//Needed variables for the call
	char * buffer;
	char ** refs;
	int n_ent = 0;
	char imss_path[MAX_PATH] = {0};
	bzero(imss_path, MAX_PATH);
	get_iuri(path, imss_path);

	//printf("-----------Remove dir %s\n",imss_path);
	strcat(imss_path,"/");
	if((n_ent = get_dir((char*)imss_path, &buffer, &refs)) > 0){

		if(n_ent > 1){
			return -EPERM;
		}
	}else{
		return -ENOENT;
	}

	if(n_ent==1){
        char new_path[MAX_PATH];
		strcpy(new_path,path);
		strcat(new_path,"/");
		imss_unlink(new_path);
	}

	return 0;
}

int imss_unlink(const char * path){

	char imss_path[MAX_PATH] = {0};

	get_iuri(path, imss_path);

	char *buff = malloc(IMSS_BLKSIZE*KB);

	uint32_t ds;
	int fd = fd_lookup(imss_path);
	if (fd >= 0) 
		ds = fd;
	else 
		return  -ENOENT;

	pthread_mutex_lock(&lock);
	get_data(ds, 0, buff);
	pthread_mutex_unlock(&lock);

	//Read header
	struct stat header;
	memcpy(&header, buff, sizeof(struct stat));

	//header.st_blocks = INT32_MAX;
	//header.st_nlink = 0;
	header.st_nlink = header.st_nlink - 1 ;

	//Write initial block
	memcpy(buff, &header, sizeof(struct stat));
	pthread_mutex_lock(&lock);
	set_data(ds, 0, (unsigned char *)buff);
	pthread_mutex_unlock(&lock);

	pthread_mutex_lock(&lock_fileopen);
	map_erase(map, imss_path);
	pthread_mutex_unlock(&lock_fileopen);		

	delete_dataset(imss_path);

	release_dataset(ds);
	free (buff);
	return 0;
}

int imss_utimens(const char * path, const struct timespec tv[2]) {
	struct stat ds_stat;
	struct timespec spec;
	clock_gettime(CLOCK_REALTIME, &spec);
	uint32_t file_desc;

	char rpath[MAX_PATH];
	bzero(rpath, MAX_PATH);
	get_iuri(path, rpath);

	//Assing file handler and create dataset
	int fd = fd_lookup(rpath);

	if (fd >= 0) 
		file_desc = fd;
	else if (fd == -2)
		return -ENOENT;
	else 
		file_desc = open_dataset(rpath);
	if(file_desc < 0) {
		fprintf(stderr, "[IMSS-FUSE]    Cannot open dataset.\n");
	}


	char *buff = malloc(IMSS_BLKSIZE*KB);
	pthread_mutex_lock(&lock);
	get_data(file_desc, 0, (unsigned char *)buff);
	pthread_mutex_unlock(&lock);

	memcpy(&ds_stat, buff, sizeof(struct stat));

	ds_stat.st_mtime = spec.tv_sec;

	//Write initial block
	memcpy(buff, &ds_stat, sizeof(struct stat));

	pthread_mutex_lock(&lock);
	set_data(file_desc, 0, (unsigned char *)buff);
	pthread_mutex_unlock(&lock);


	free(buff);

	return 0;
}


int imss_mkdir(const char * path, mode_t mode) {
	char rpath[MAX_PATH];
	bzero(rpath, MAX_PATH);
	struct fuse_file_info fi;
	strcpy(rpath,path);
	strcat(rpath,"/");
	imss_create(rpath,  mode | S_IFDIR, &fi);
	return 0;
}

int imss_flush(const char * path, struct fuse_file_info * fi){

	if(error_print!=0){
		int32_t err=error_print;
		error_print=0;
		return err;
	}
	struct stat ds_stat;
	struct timespec spec;
	clock_gettime(CLOCK_REALTIME, &spec);
	uint32_t file_desc;




	char rpath[MAX_PATH];
	bzero(rpath, MAX_PATH);
	get_iuri(path, rpath);

	//Assing file handler and create dataset
	int fd = fd_lookup(rpath);

	if (fd >= 0) 
		file_desc = fd;
	else if (fd == -2)
		return -ENOENT;
	else 
		file_desc = open_dataset(rpath);
	if(file_desc < 0) {
		fprintf(stderr, "[IMSS-FUSE]    Cannot open dataset.\n");
		return -EACCES;
	}


	char *buff = malloc(IMSS_BLKSIZE*KB);
	pthread_mutex_lock(&lock);
	if(get_data(file_desc, 0, (unsigned char *)buff) < 0){
		fprintf(stderr, "[IMSS-FUSE]	Error reading from imss.\n");
		error_print=-ENOENT;
		pthread_mutex_unlock(&lock);
		return -ENOENT;
	}
	pthread_mutex_unlock(&lock);

	memcpy(&ds_stat, buff, sizeof(struct stat));

	ds_stat.st_mtime = spec.tv_sec;

	//Write initial block
	memcpy(buff, &ds_stat, sizeof(struct stat));

	pthread_mutex_lock(&lock);
	if(set_data(file_desc, 0, (unsigned char *)buff) < 0){
		fprintf(stderr, "[IMSS-FUSE]	Error writing to imss.\n");
		error_print=-ENOENT;
		pthread_mutex_unlock(&lock);
		return -ENOENT;
	}

	pthread_mutex_unlock(&lock);


	free(buff);

	return 0;
}

int imss_getxattr(const char * path, const char *attr, char *value, size_t s) {
	return 0;
}



int imss_chmod(const char *path, mode_t mode){

	struct stat ds_stat;
	uint32_t file_desc;
	printf("chmod_path=%s\n",path);
	char rpath[MAX_PATH];
	bzero(rpath, MAX_PATH);
	get_iuri(path, rpath);

	//Assing file handler and create dataset
	int fd = fd_lookup(rpath);

	if (fd >= 0) 
		file_desc = fd;
	else if (fd == -2)
		return -ENOENT;
	else 
		file_desc = open_dataset(rpath);
	if(file_desc < 0) {
		fprintf(stderr, "[IMSS-FUSE]    Cannot open dataset.\n");
	}


	char *buff = malloc(IMSS_BLKSIZE*KB);
	pthread_mutex_lock(&lock);
	get_data(file_desc, 0, (unsigned char *)buff);
	pthread_mutex_unlock(&lock);

	memcpy(&ds_stat, buff, sizeof(struct stat));

	ds_stat.st_mode = mode;

	//Write initial block
	memcpy(buff, &ds_stat, sizeof(struct stat));

	pthread_mutex_lock(&lock);
	set_data(file_desc, 0, (unsigned char *)buff);
	pthread_mutex_unlock(&lock);


	free(buff);

	return 0;
}

int imss_chown(const char *path, uid_t uid, gid_t gid) {
	struct stat ds_stat;
	uint32_t file_desc;
	char rpath[MAX_PATH];
	bzero(rpath, MAX_PATH);
	get_iuri(path, rpath);

	//Assing file handler and create dataset
	int fd = fd_lookup(rpath);

	if (fd >= 0) 
		file_desc = fd;
	else if (fd == -2)
		return -ENOENT;
	else 
		file_desc = open_dataset(rpath);
	if(file_desc < 0) {
		fprintf(stderr, "[IMSS-FUSE]    Cannot open dataset.\n");
	}


	char *buff = malloc(IMSS_BLKSIZE*KB);
	pthread_mutex_lock(&lock);
	get_data(file_desc, 0, (unsigned char *)buff);
	pthread_mutex_unlock(&lock);

	memcpy(&ds_stat, buff, sizeof(struct stat));

	ds_stat.st_uid = uid;
	if(gid!=-1){
		ds_stat.st_gid = gid;
	}



	//Write initial block
	memcpy(buff, &ds_stat, sizeof(struct stat));

	pthread_mutex_lock(&lock);
	set_data(file_desc, 0, (unsigned char *)buff);
	pthread_mutex_unlock(&lock);


	free(buff);

	return 0;

}

int imss_rename(const char *old_path, const char *new_path){
	struct stat ds_stat_n;
	printf("old_path=%s, new_path=%s\n",old_path, new_path);
	int file_desc_o, file_desc_n;
	int fd=0;
	char old_rpath[MAX_PATH];
	bzero(old_rpath, MAX_PATH);
	get_iuri(old_path, old_rpath);

	char new_rpath[MAX_PATH];
	bzero(new_rpath, MAX_PATH);
	get_iuri(new_path, new_rpath);


	

    //CHECKING IF IS MV DIR TO DIR
	//check old_path if it is a directory if it is add / at the end
	int res = imss_getattr(old_path, &ds_stat_n);
    if (res == 0) {
		printf("old_path[last]=%c\n",old_path[strlen(old_path) -1]);
		if (S_ISDIR(ds_stat_n.st_mode)) {
			
			
			printf("**************EXISTE EL ORIGEN ES DIRECTORIO=%s\n",old_path);
			strcat(old_rpath, "/");
			printf("fd_lookup=%s\n",old_rpath);
			fd = fd_lookup(old_rpath);
			if (fd >= 0) 
				file_desc_o = fd;
			else if (fd == -2)
				return -ENOENT;
			else 
				file_desc_o = open_dataset(old_rpath);

			if(file_desc_o < 0) {
				fprintf(stderr, "[IMSS-FUSE]    Cannot open dataset.\n");

				return -ENOENT;
			}
			
			//If origin path is a directory then we are in the case of mv dir to dir
			//Extract destination directory from path
			int pos = 0;
			for (int c = 0; c < strlen(new_path); ++c) {
				if (new_path[c] == '/') {
					if (c + 1 < strlen(new_path))
					   pos=c;
				}
			}
			char dir_dest[256] = {0};
			memcpy(dir_dest,&new_path[0],pos+1);
			printf("dir_dest=%s\n",dir_dest);

			char rdir_dest[MAX_PATH];
			bzero(rdir_dest, MAX_PATH);
			get_iuri(dir_dest,rdir_dest);
			
			printf("rdir_dest=%s\n",rdir_dest);

			res = imss_getattr(dir_dest, &ds_stat_n);
			if(res == 0){
				if (S_ISDIR(ds_stat_n.st_mode)) {
					//WE ARE IN MV DIR TO DIR
					printf("********CASO DIR TO DIR********\n");

					map_rename_dir_dir(map, old_rpath,new_rpath);

					//RENAME LOCAL_IMSS(GARRAY), SRV_STAT(MAP & TREE)
					rename_dataset_metadata_dir_dir(old_rpath,new_rpath);

					//RENAME SRV_WORKER(MAP)
					
					rename_dataset_srv_worker_dir_dir(old_rpath,new_rpath,fd,0);
					return 0;
				}
			}

		}
	}

	//MV FILE TO FILE OR MV FILE TO DIR
	//Assing file handler
	fd = fd_lookup(old_rpath);

	if (fd >= 0) 
		file_desc_o = fd;
	else if (fd == -2)
		return -ENOENT;
	else 
		file_desc_o = open_dataset(old_rpath);

	if(file_desc_o < 0) {
		fprintf(stderr, "[IMSS-FUSE]    Cannot open dataset.\n");

		return -ENOENT;
	}
	res = imss_getattr(new_path, &ds_stat_n);
	
    if (res == 0) {
		printf("**************EXISTE EL DESTINO=%s\n",new_path);
		printf("new_path[last]=%c\n",new_path[strlen(new_path) -1]);
		if (S_ISDIR(ds_stat_n.st_mode)) {
			//Because of how the path arrive never get here.
		}else{
			printf("**************TENGO QUE BORRARLO ES UN FICHERO=%s\n",new_path);
			imss_unlink(new_path);
		}
	}else{
		printf("**************NO EXISTE EL DESTINO=%s\n",new_path);
	}


	printf("old_rpath=%s, new_rpath=%s\n",old_rpath, new_rpath);

	map_rename(map, old_rpath,new_rpath);
	/*map_erase(map,old_rpath);
	map_put(map,new_rpath,fd);

	found=map_search(map,new_rpath,&fd);
	if(found!=-1){
		printf("añadido %s\n",new_rpath);
	}*/

	//RENAME LOCAL_IMSS(GARRAY), SRV_STAT(MAP & TREE)
	rename_dataset_metadata(old_rpath,new_rpath);
	//RENAME SRV_WORKER(MAP)
	rename_dataset_srv_worker(old_rpath,new_rpath,fd,0);


	return 0;
}
