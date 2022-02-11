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


#define KB 1024
#define HEADER sizeof(uint32_t)

//Macro to compute real offsets
#define IMSS_DATA_BSIZE (IMSS_BLKSIZE*KB-sizeof(uint32_t))

//Struct for first block dataset metadata
typedef struct {

	int32_t last_block; /*	Last block with data, -1 if none block used */
	struct timespec st_atim;  /* Time of last access */
	struct timespec st_mtim;  /* Time of last modification */
	struct timespec st_ctim;  /* Time of last status change */

} dataset_header;



/*
   -----------	IMSS Global variables, filled at the beggining or by default -----------
   */

uint16_t IMSS_SRV_PORT = 1; //Not default, 1 will fail
uint16_t METADATA_PORT = 1; //Not default, 1 will fail
int32_t N_SERVERS = 1; //Default
int32_t N_META_SERVERS = 1; //Default 1 1
int32_t N_BLKS = 1; //Default 1
char * METADATA_FILE = NULL; //Not default
char * IMSS_HOSTFILE = NULL; //Not default
char * IMSS_ROOT = NULL;//Not default
char * META_HOSTFILE = NULL; //Not default 
char * POLICY = "RR"; //Default RR
uint64_t STORAGE_SIZE = 1024*1024*16; //In Kb, Default 2 MB
uint64_t META_BUFFSIZE = 1024 * 16; //In Kb, Default 16MB
//uint64_t IMSS_BLKSIZE = 1024; //In Kb, Default 1 MB
uint64_t IMSS_BLKSIZE = 4;
//uint64_t IMSS_BUFFSIZE = 1024*1024*32; //In Kb, Default 2Gb
uint64_t IMSS_BUFFSIZE = 1024*2048; //In Kb, Default 2Gb
int32_t REPL_FACTOR = 1; //Default none
char * MOUNTPOINT[4] = {"imssfs", "-f" , "-s",  NULL}; // {"f", mountpoint} Not default ({"f", NULL})


void * map;

//char fd_table[1024][MAX_PATH]; 

#define MAX_PATH 256

typedef struct{
	char path[MAX_PATH];
	long fd;
	//int32_t type;
} fileopen_table_entry;

//int fileopen_table_amount = 0;
//int fileopen_table_size = ELEMENTS;
//fileopen_table_entry fileopen_table[ELEMENTS];


pthread_mutex_t lock;
pthread_mutex_t lock_fileopen;


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


static int imss_truncate(const char * path, off_t offset) {
	return 0;
}
static int imss_access(const char *path, int permission)
{
	return 0;
}

static int imss_getattr(const char *path, struct stat *stbuf)
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
		//printf("GetAttribute: path %s\n", imss_path);
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
				}	else
				   return -ENOENT;
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
static int imss_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
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

static int imss_open(const char *path, struct fuse_file_info *fi)
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

	//strcpy(fd_table[file_desc], imss_path);


	/*if ((fi->flags & 3) != O_RDONLY)
	  return -EACCES;*/

	return 0;
}

static int imss_read(const char *path, char *buf, size_t size, off_t offset,
		struct fuse_file_info *fi)
{


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
	    return -1;
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

static int imss_write(const char *path, const char *buf, size_t size,//if buffer is "" it has \nola\n why?¿ echo ""> /tmp/testimss/prueba ver foto
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
	    return -1;
	else
		ds = open_dataset(rpath);


	//Read header
	pthread_mutex_lock(&lock);
	if(get_data(ds, 0, (unsigned char*)aux) < 0){
		fprintf(stderr, "[IMSS-FUSE]	Error reading metadata.\n");
		return -1;
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
				fprintf(stderr, "[IMSS-FUSE]	Error writing to imss.\n");
				return -1;
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
					return -1;
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
		//pthread_mutex_lock(&lock);
		if(set_data(ds, curr_blk, (unsigned char *)aux) < 0){
			fprintf(stderr, "[IMSS-FUSE]	Error writing to imss.\n");
			return -1;
		}
		//pthread_mutex_unlock(&lock);

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
		set_data(ds, 0, (unsigned char*)aux);
		pthread_mutex_unlock(&lock);
	}

	free(aux);
	return byte_count;
}

static int imss_release(const char * path, struct fuse_file_info *fi)
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
	get_data(ds, 0, head);
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
	set_data(ds, 0, head);
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

static int imss_create(const char * path, mode_t mode, struct fuse_file_info * fi)
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
		return -1;
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
	header.st_nlink = 0;

	//Write initial block
	memcpy(buff, &header, sizeof(struct stat));
	pthread_mutex_lock(&lock);
	set_data(ds, 0, (unsigned char *)buff);
	pthread_mutex_unlock(&lock);

	pthread_mutex_lock(&lock_fileopen);
	map_erase(map, imss_path);
	pthread_mutex_unlock(&lock_fileopen);		

    delete_dataset(imss_path);
	//Delete metadata from client from GArray

	release_dataset(ds);
	free (buff);
	return 0;
}

static int imss_utimens(const char * path, const struct timespec tv[2]) {
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
	     return -1;
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

	struct stat ds_stat;
	struct timespec tv;

	clock_gettime(CLOCK_REALTIME, &tv);
	struct timespec spec;
	clock_gettime(CLOCK_REALTIME, &spec);

	char rpath[MAX_PATH];
	bzero(rpath, MAX_PATH);
	get_iuri(path, rpath);


	char *buff = malloc(IMSS_BLKSIZE*KB);
	pthread_mutex_lock(&lock);
	get_data(fi->fh, 0, (unsigned char *)buff);
	pthread_mutex_unlock(&lock);

	memcpy(&ds_stat, buff, sizeof(struct stat));

	ds_stat.st_mtime = spec.tv_sec;

	//Write initial block
	memcpy(buff, &ds_stat, sizeof(struct stat));

	pthread_mutex_lock(&lock);
	set_data(fi->fh, 0, (unsigned char *)buff);
	pthread_mutex_unlock(&lock);

	free(buff);

	return 0;
}

static struct fuse_operations imss_oper = {
	.getattr	= imss_getattr,
	.truncate	= imss_truncate,
	.utimens        = imss_utimens,
	.readdir	= imss_readdir,
	.open		= imss_open,
	.read		= imss_read, //
	.write		= imss_write, 
	.release	= imss_release,
	.create		= imss_create,
	.mkdir		= imss_mkdir,
	.opendir 	= imss_opendir,
	.releasedir     = imss_releasedir,
	.unlink		= imss_unlink,
	.access		= imss_access
};

/*
   ----------- Parsing arguments and help functions -----------
   */

void print_help(){

	printf("IMSS FUSE HELP\n\n");

	printf("\t-p	IMSS port (*).\n");
	printf("\t-m	Metadata port (*).\n");
	printf("\t-s	Number of servers (1 default).\n");
	printf("\t-b	Number of blocks (1 default).\n");
	printf("\t-M	Metadata file path (*).\n");
	printf("\t-h	Host file path(*).\n");
	printf("\t-r	IMSS root path (*).\n");
	printf("\t-a	Metadata hostfile (*).\n");
	printf("\t-P	IMSS policy (RR by default).\n");
	printf("\t-S	IMSS storage size in KB (by default 2048).\n");
	printf("\t-B	IMSS buffer size in KB (by default 1024).\n");
	printf("\t-e	Metadata buffer size in KB (by default 1024).\n");
	printf("\t-o	IMSS block size in KB (by default 1024).\n");
	printf("\t-R	Replication factor (by default NONE).\n");
	printf("\t-x	Metadata server number (by default 1).\n");

	printf("\n\t-l	Mountpoint (*).\n");

	printf("\n\t-H	Print this message.\n");

	printf("\n(*) Argument is compulsory.\n");

}


//Function checking arguments, return 1 if everything is filled, 0 otherwise
int check_args(){

	//Check all non optional parameters
	return IMSS_SRV_PORT != 1 &&
		METADATA_PORT != 1 &&
		METADATA_FILE &&
		IMSS_HOSTFILE &&
		IMSS_ROOT &&
		META_HOSTFILE;
}

/**
 *	Parse arguments function.
 *	Returns 1 if the parsing was correct, 0 otherwise.
 */
int parse_args(int argc, char ** argv){

	int opt;

	while((opt = getopt(argc, argv, "p:m:s:b:M:h:r:a:P:S:B:e:o:R:x:Hl:")) != -1){ 
		switch(opt) { 
			case 'p':
				if(!sscanf(optarg, "%" SCNu16, &IMSS_SRV_PORT)){
					print_help();
					return 0;
				}
				break;
			case 'm':
				if(!sscanf(optarg, "%" SCNu16, &METADATA_PORT)){
					print_help();
					return 0;
				}
				break;
			case 's':
				if(!sscanf(optarg, "%" SCNu32, &N_SERVERS)){
					print_help();
					return 0;
				}
				break;
			case 'b':
				if(!sscanf(optarg, "%" SCNu32, &N_BLKS)){
					print_help();
					return 0;
				}
				break;
			case 'M':
				METADATA_FILE = optarg;            	
				break;
			case 'h':
				IMSS_HOSTFILE = optarg;
				break;
			case 'r':
				IMSS_ROOT = optarg;
				break;
			case 'a':
				META_HOSTFILE = optarg;
				break;
			case 'P':
				POLICY = optarg; //We lost "RR", but not significative
				break;
			case 'S':
				if(!sscanf(optarg, "%" SCNu64, &STORAGE_SIZE)){
					print_help();
					return 0;
				}
				break;
			case 'B':
				if(!sscanf(optarg, "%" SCNu64, &IMSS_BUFFSIZE)){
					print_help();
					return 0;
				}
				break;
			case 'e':
				if(!sscanf(optarg, "%" SCNu64, &META_BUFFSIZE)){
					print_help();
					return 0;
				}
				break;
			case 'o':
				if(!sscanf(optarg, "%" SCNu64, &IMSS_BLKSIZE)){
					print_help();
					return 0;
				}
				break;
			case 'R':
				if(!sscanf(optarg, "%" SCNu32, &REPL_FACTOR)){
					print_help();
					return 0;
				}
				break;
			case 'l':
				MOUNTPOINT[2] = optarg; //We lost "RR", but not significative
				break;
			case 'x':
				if(!sscanf(optarg, "%" SCNu32, &N_META_SERVERS)){
					print_help();
					return 0;
				}
				break;
			case 'H':
				print_help();
				return 0;
			case ':':
				return 0;
			case '?':
				print_help();
				return 0;
		} 
	} 

	//Check if all compulsory args are filled
	if(!check_args()) {
		fprintf(stderr, "[IMSS-FUSE]	Please, fill all the mandatory arguments.\n");
		print_help();
		return 0;
	}

	return 1;
}


/*
   ----------- MAIN -----------
   */

int main(int argc, char *argv[])
{	
	//Parse input arguments
	if(!parse_args(argc, argv)) return -1;

	pthread_mutex_init(&lock, NULL);
	pthread_mutex_init(&lock_fileopen, NULL);

	//Hercules init -- Attached deploy
	if (hercules_init(0, STORAGE_SIZE, IMSS_SRV_PORT, 1, METADATA_PORT, META_BUFFSIZE, METADATA_FILE) == -1){
		//In case of error notify and exit
		fprintf(stderr, "[IMSS-FUSE]	Hercules init failed, cannot deploy IMSS.\n");
		return -1;
	} 

	//Metadata server
	if (stat_init(META_HOSTFILE, METADATA_PORT, N_META_SERVERS,1) == -1){
		//In case of error notify and exit
		fprintf(stderr, "[IMSS-FUSE]	Stat init failed, cannot connect to Metadata server.\n");
		return -1;
	} 

	//Initialize the IMSS servers
	if(init_imss(IMSS_ROOT, IMSS_HOSTFILE, N_SERVERS, IMSS_SRV_PORT, IMSS_BUFFSIZE, ATTACHED, NULL) < 0) {
		//Notify error and exit
		fprintf(stderr, "[IMSS-FUSE]	IMSS init failed, cannot create servers.\n");
		return -1;
	} 

	char * test = get_deployed();
	if(test) {free(test);}

        map = map_create(); 

	return fuse_main(3, MOUNTPOINT, &imss_oper, NULL);
}

