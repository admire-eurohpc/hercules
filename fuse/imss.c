/*
FUSE: Filesystem in Userspace
Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

This program can be distributed under the terms of the GNU GPL.
See the file COPYING.

gcc -Wall imss.c `pkg-config fuse --cflags --libs` -o imss
*/

#define FUSE_USE_VERSION 26
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

#define KB 1024
#define HEADER sizeof(uint32_t)

//Macro to compute real offsets
#define IMSS_DATA_BSIZE (IMSS_BLKSIZE*KB-sizeof(uint32_t))

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
uint64_t STORAGE_SIZE = 2048; //In Kb, Default 2 MB
uint64_t META_BUFFSIZE = 1024; //In Kb, Default 1 MB
uint64_t IMSS_BUFFSIZE = 1024; //In Kb, Default 1 MB
uint64_t IMSS_BLKSIZE = 1024; //In Kb, Default 1 MB
int32_t REPL_FACTOR = 1; //Default none
char * MOUNTPOINT[4] = {"f", "-d", "-s", NULL}; // {"f", mountpoint} Not default ({"f", NULL})


pthread_mutex_t lock;

/*
   (*) Mapping for REPL_FACTOR values:
   NONE = 1;
   DRM = 2;
   TRM = 3;
   */

/*
   -----------	Auxiliar Functions -----------
   */

void get_iuri(const char * path, /*output*/ char * uri){

	//Copying protocol
	memcpy(uri, "imss:/", 6);
	//memcpy(uri, "imss://fuse", 11);
	//Copying path
	strcpy(uri+6, path);
	int l = strlen(uri);
	if (uri[l-1] != '/') {
		uri[l] = '/';
		uri[l+1] = 0;
	}
}

/*
   -----------	FUSE IMSS implementation -----------
   */


static int imss_truncate(const char * path, off_t offset) {
	return 0;
}

static int imss_getattr(const char *path, struct stat *stbuf)
{

	struct fuse_context * ctx;
	ctx = fuse_get_context();

	//Needed variables for the call
	char * buffer;
	char ** refs;
	int n_ent;
	char imss_path[256] = {0};
	dataset_info  metadata;
	struct timespec spec;

	get_iuri(path, imss_path);

	memset(stbuf, 0, sizeof(struct stat));
	clock_gettime(CLOCK_REALTIME, &spec);

	stbuf->st_uid = ctx->uid;
	stbuf->st_gid = ctx->gid;
	stbuf->st_blksize = IMSS_BLKSIZE;
	stbuf->st_atim	= spec;
	stbuf->st_mtime	= spec.tv_sec;
	stbuf->st_ctime	= spec.tv_sec;

	uint32_t type = get_type(imss_path);

	switch(type){

		case 0:
			return -ENOENT;
		case 1:
			if((n_ent = get_dir((char*)imss_path, &buffer, &refs)) != -1){
				stbuf->st_size = 4;
				stbuf->st_nlink = 1;
				stbuf->st_mode = S_IFDIR | 0666;

				//Free resources
				free(buffer);
				free(refs);

				return 0;
			} 
			else return -ENOENT;

		case 2:
			if(stat_dataset(imss_path, &metadata) == -1){
				fprintf(stderr, "[IMSS-FUSE]	Cannot get dataset metadata.");
				return -ENOENT;
			}
			stbuf->st_size = 200;//metadata.size;
			stbuf->st_mode = S_IFREG | 0666;
			stbuf->st_nlink = 1;
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
	int n_ent;

	char imss_path[256] = {0};

	get_iuri(path, imss_path);

	//Call IMSS to get metadata
	if((n_ent = get_dir((char*)imss_path, &buffer, &refs)) < 0){
		//In case of error
		fprintf(stderr, "[IMSS-FUSE]	Error retrieving directories for URI=%s", path);
		return -ENOENT;
	}

	//Fill buffer
	//TODO: Check if subdirectory
	for(int i = 0; i < n_ent; ++i) {
		if (i == 0) {
			filler(buf, ".", NULL, 0);
			filler(buf, "..", NULL, 0);
		} else {
			struct stat stbuf;
			imss_getattr(refs[i]+6, &stbuf); 
			refs[i][strlen(refs[i])-1] = '\0';
			filler(buf, refs[i]+6+1,  &stbuf, 0); 
		}
	}
	//Free resources
	free(buffer);
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
	char imss_path[256] = {0};

	get_iuri(path, imss_path);
	//ONLY OPENING IF ALREADY EXISTS
	int32_t file_desc = open_dataset(imss_path);
	//File does not exist	
	if(file_desc < 0) return -ENOENT;

	//Assign file descriptor
	fi->fh = file_desc;

	/*if ((fi->flags & 3) != O_RDONLY)
	  return -EACCES;*/

	return 0;
}

static int imss_read(const char *path, char *buf, size_t size, off_t offset,
		struct fuse_file_info *fi)
{
	/*size_t len;
	  (void) fi;
	  if(strcmp(path, imss_path) != 0)
	  return -ENOENT;

	  len = strlen(imss_str);
	  if (offset < len) {
	  if (offset + size > len)
	  size = len - offset;
	  memcpy(buf, imss_str + offset, size);
	  } else
	  size = 0;

	  return size;*/

	//TODO -- Previous checks

	pthread_mutex_lock(&lock);

	//Compute current block and offsets
	int64_t curr_blk, end_blk, start_offset, end_offset;
	int64_t first = 0; 
	curr_blk = offset / IMSS_DATA_BSIZE;
	start_offset = offset / IMSS_DATA_BSIZE;
	end_blk = (offset+size) / IMSS_DATA_BSIZE;
	end_offset = (offset+size) % IMSS_DATA_BSIZE;

	//Needed variables
	size_t byte_count = 0;
	int64_t rbytes;
	char aux[(IMSS_BLKSIZE*KB)];

	memset(buf, 0, size);
	//Read remaining blocks
	while(curr_blk <= end_blk){

		if(get_data(fi->fh, curr_blk, (unsigned char*)aux) == -1){
			//Notify error and stop loop
			fprintf(stderr, "[IMSS-FUSE]	Error reding from imss.\n");
			break;
		}

		//if (rbytes) {
			//Parse data
			uint32_t filled;
			memcpy (&filled, aux, sizeof(uint32_t));

			//Parse buffer

			//First block case
			if (first == 0) {
				//Check if offset is bigger than filled, return 0 because is EOF case
				if(start_offset > filled) 
					return 0; 
				memcpy(buf, aux + HEADER + start_offset, filled - start_offset);
				byte_count += filled - start_offset;

				//Middle block case
			} else if (curr_blk != end_blk) {

				//Read only filled
				memcpy(buf + byte_count, aux + HEADER, filled);
				byte_count += filled;

				//End block case
			}  else {

				//Read the minimum between end_offset and filled (read_ = min(end_offset, filled))
				int64_t read_ = (filled >= end_offset) ? end_offset : filled;
				memcpy(buf + byte_count, aux + sizeof(uint32_t), read_);
				byte_count += read_;
			} 
		//}
		++first;
		++curr_blk;
	}

	pthread_mutex_unlock(&lock);
	return byte_count;
}

static int imss_write(const char *path, const char *buf, size_t size,
		off_t off, struct fuse_file_info *fi)
{

	//TODO -- Previous checks

	//Compute offsets to write
	int64_t curr_blk, end_blk, start_offset, end_offset;
	curr_blk = off / IMSS_DATA_BSIZE;
    start_offset = off / IMSS_DATA_BSIZE;
    end_blk = (off+size) / IMSS_DATA_BSIZE;
    end_offset = (off+size) % IMSS_DATA_BSIZE;

	//Needed variables
	size_t byte_count = 0;
	int64_t wbytes;
	int first = 0;
	char aux[(IMSS_BLKSIZE*KB)];
    int64_t to_copy;
    uint32_t filled;
	//For the rest of blocks
	while(curr_blk <= end_blk){

		//Fisr block
        if (first==0 && start_offset) {
        	//Get previous block
        	if(get_data(fi->fh, curr_blk, (unsigned char *)aux) < 0){
				fprintf(stderr, "[IMSS-FUSE]	Error writing to imss.\n");
				return -1;
			}
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

        	if(get_data(fi->fh, curr_blk, (unsigned char *)aux) < 0){
				//If block does not exist set to 0 and update filled
				memset(aux, 0, IMSS_BLKSIZE*KB);
				filled = to_copy;
				memcpy(aux, &filled, sizeof(uint32_t));
			}
			else{
				memcpy(&filled, aux, sizeof(uint32_t));
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


        /*if (size > IMSS_DATA_BSIZE)
		    to_copy = IMSS_DATA_BSIZE;
		else
		    to_copy = size%IMSS_DATA_BSIZE;*/ 

		//Create block, calloc -> padded with 0's
        

		//Write and update variables
		if(set_data(fi->fh, curr_blk, (unsigned char *)aux) < 0){
			fprintf(stderr, "[IMSS-FUSE]	Error writing to imss.\n");
			return -1;
		}

		byte_count += to_copy;
		++curr_blk;
		++first;
	}

	return byte_count;
}

static int imss_release(const char * path, struct fuse_file_info *fi)
{
	//Release resources
	return release_dataset(fi->fh);
}

static int imss_create(const char * path, mode_t mode, struct fuse_file_info * fi)
{
	//TODO check mode
	//DEBUG

	//Check if already created!
	char rpath[strlen(path)+8];
	get_iuri(path, rpath);

	int32_t creat = create_dataset((char*)rpath, POLICY,  N_BLKS, IMSS_BLKSIZE, REPL_FACTOR);
	if(creat < 0) {
		fprintf(stderr, "[IMSS-FUSE]	Cannot create new dataset.\n");
	}

	release_dataset(creat);
	return 0; //FIXME check return value!

}

//Does nothing
int imss_opendir(const char * path, struct fuse_file_info * fi) {
	return 0;
}

//Does nothing
int imss_releasedir(const char * path, struct fuse_file_info * fi) {
	return 0;
}

static int imss_utimens(const char * path, const struct timespec tv[2]) {
    return 0;
}

static struct fuse_operations imss_oper = {
	.getattr	= imss_getattr,
	.truncate	= imss_truncate,
    .utimens    = imss_utimens,
	.readdir	= imss_readdir,
	.open		= imss_open,
	.read		= imss_read, //
	.write		= imss_write, 
	.release	= imss_release,
	.create		= imss_create,
	.opendir 	= imss_opendir,
	.releasedir = imss_releasedir
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
		META_HOSTFILE &&
		MOUNTPOINT[4];
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

	int ds = create_dataset("imss://fuse/", POLICY,  N_BLKS, IMSS_BLKSIZE, REPL_FACTOR);

	if(ds < 0) {
		fprintf(stderr, "[IMSS-FUSE]	IMSS init failed, cannot create servers.\n");
		return -1;
	}
	char * test = get_deployed();
	if(test) {printf("%s\n", test); free(test);}

	release_dataset(ds);

	return fuse_main(3, MOUNTPOINT, &imss_oper, NULL);
}

