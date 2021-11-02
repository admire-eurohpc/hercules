/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.

  gcc -Wall imss.c `pkg-config fuse --cflags --libs` -o imss
*/

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include "imss.h"

/*
	-----------	IMSS Global variables, filled at the beggining or by default -----------
*/

 uint16_t IMSS_SRV_PORT = 1; //Not default, 1 will fail
 uint16_t METADATA_PORT = 1; //Not default, 1 will fail
 int32_t N_SERVERS = 1; //Default 1
 int32_t N_BLKS = 1; //Default 1
 char * METADATA_FILE = ""; //Not default, "" will fail
 char * IMSS_HOSTFILE = ""; //Not default, "" will fail
 char * IMSS_ROOT = ""; //Not default, "" will fail
 char * IMSS_SRV_ADDR = ""; //Not default, "" will fail 
 char * POLICY = "RR"; //Default RR
 uint64_t STORAGE_SIZE = 2048; //In Kb, Default 2 MB
 uint64_t META_BUFFSIZE = 1024; //In Kb, Default 1 MB
 uint64_t IMSS_BUFFSIZE = 1024; //In Kb, Default 1 MB
 uint64_t IMSS_BLKSIZE = 1024; //In Kb, Default 1 MB
 int32_t REPL_FACTOR = NONE; //Default none (*)

/*
  	(*) Mapping for REPL_FACTOR values:
  		NONE = 1;
		DRM = 2;
		TRM = 3;
*/

 /*
	-----------	FUSE IMSS implementation -----------
*/


static int imss_getattr(const char *path, struct stat *stbuf)
{
	//Get metadata from IMSS
	//FIXME!! -> Esto solo vale para datasets, ¿Qué hacemos para distinguir directorios? ¿Si de error intentamos de nuevo?
	dataset_info * metadata;
	if(stat_dataset(path, metadata) == -1){
		fprintf(stderr, "[IMSS-FUSE]	Cannot get dataset metadata.");
		return -ENOENT;
	}

	//Copy metadata
	//TODO

	//Release metadata
	free_dataset(metadata);

	//¿Hacer aquí si es directorio? 


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

	return res;
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

	//Call IMSS to get metadata
	if((n_ent = get_dir(path, &buffer, &refs)) < 0){
		//In case of error
		fprintf(stderr, "[IMSS-FUSE]	Error retrieving directories for URI=%s", path);
		return -ENOENT
	}

	//Fill buffer
	//TODO: Check if subdirectory
	for(int i = 0; i < n_ent; ++i) filler(buf, refs[i], NULL, 0);
	
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
	
	//ONLY OPENING IF ALREADY EXISTS
	int32_t file_desc = open_dataset(path);
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

	//Compute current block and offsets
	int64_t curr_blk, end_blk, start_offset, end_offset;
	curr_blk = offset / BLK_SIZE;
	start_offset = offset / BLK_SIZE;
	end_blk = (offset+size) / BLK_SIZE;
	end_offset = (offset+size) % BLK_SIZE;

	//Needed variables
	size_t byte_count = 0;
	int64_t rbytes;
	char aux[BLK_SIZE];

	//When the starting offset requires less than a block
	if(start_offset) {
		//Read the st block
		if(get_ndata(fi->fh, curr_blk, (unsigned char*)aux, &rbytes) == -1){
			//Notify error and stop loop
			fprintf(stderr, "[IMSS-FUSE]	Error reding from imss.\n");
			break;
		}
		//Copy
		memcpy(buf, aux+start_offset, BLK_SIZE-start_offset);

		//Update variables
		++curr_blk;
		byte_count = BLK_SIZE-start_offset;
	}

	//Read remaining blocks
	while(curr_blk <= end_blk){

		//If there exist end offset, read in aux
		if(curr_blk == end_blk && end_offset){
			if(get_ndata(fi->fh, curr_blk, (unsigned char*)aux, &rbytes) == -1){
				//Notify error and stop loop
				fprintf(stderr, "[IMSS-FUSE]	Error reding from imss.\n");
				break;
			}
			//Copy
			memcpy(buf, aux, end_offset);

			//Update variables;
			byte_count += end_offset
		}

		else {

			if(get_ndata(fi->fh, curr_blk, (unsigned char*)(buf+byte_count), &rbytes) == -1){
				//Notify error and stop loop
				fprintf(stderr, "[IMSS-FUSE]	Error reding from imss.\n");
				break;
			}
			//Update buffer offset and byte count
			byte_count += rbytes; 
			
		}

		++curr_blk;

	}

	return byte_count;

}


static int imss_write(const char *path, const char *buf, size_t size,
			  off_t off, struct fuse_file_info *fi)
{

	//TODO -- Previous checks


	//Compute offsets to write
	int64_t curr_blk, end_blk, start_offset, end_offset;
	curr_blk = off / BLK_SIZE;
	start_offset = off / BLK_SIZE;
	end_blk = (off+size) / BLK_SIZE;
	end_offset = (off+size) % BLK_SIZE;

	//Needed variables
	size_t byte_count = 0;
	int64_t wbytes;
	char aux[BLK_SIZE];

	//If there is offset in the first block
	if(start_offset){
		//Read previous block
		if(get_ndata(fi->fh, curr_blk, (unsigned char*)aux, &wbytes) == -1){
			//Notify error and stop loop
			fprintf(stderr, "[IMSS-FUSE]	Error reding from imss.\n");
			break;
		}

		//Copy data to the writing buffer
		//Remark: this buffer is managed by IMSS and freed by it.
		char * to_write = (char *) malloc(BLK_SIZE);
		memcpy(to_write, aux, BLK_SIZE);

		//Copy the new data
		memcpy(to_write+start_offset, buf, BLK_SIZE-start_offset);

		//Write and update variables
		if(set_data(fi->fh, curr_blk, (unsigned char *)to_write) < 0){
			fprintf(stderr, "[IMSS-FUSE]	Error writing to imss.\n");
			return -1;
		}

		byte_count += BLK_SIZE-start_offset;
		++curr_blk

	}

	//For the rest of blocks
	while(curr_blk <= end_blk){

		//Create block, calloc -> padded with 0's
		char * to_write = (char *) calloc(BLK_SIZE, sizeof(char));

		//Copy data, check if last to copy only remaining bytes
		size_t to_copy = (curr_blk == end_blk && end_offset) ? end_offset : BLK_SIZE;
		memcpy(to_write, buffer+byte_count, to_copy);

		//Write and update variables
		if(set_data(fi->fh, curr_blk, (unsigned char *)to_write) < 0){
			fprintf(stderr, "[IMSS-FUSE]	Error writing to imss.\n");
			return -1;
		}

		byte_count += to_copy;
		++curr_blk

	}

	return byte_count;

}

static int imss_close(const char * path, struct fuse_file_info *fi)
{
	//Release resources
	return release_dataset(fi->fh);
}

static int imss_create(const char * path, mode_t mode, struct fuse_file_info * fi)
{
	//TODO check mode

	//Check if already created!

	int32_t creat = create_dataset(path, POLICY,  N_BLKS, IMSS_BLKSIZE, REPL_FACTOR);
	if(creat < 0) {
		fprintf(stderr, "[IMSS-FUSE]	Cannot create new dataset.\n")
	}

	return 1; //FIXME check return value!

}

static struct fuse_operations imss_oper = {
	.getattr	= imss_getattr,
	.readdir	= imss_readdir,
	.open		= imss_open,
	.read		= imss_read, //
	.write		= imss_write, 
	.close		= imss_close,
	.create		= imss_create,
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
	printf("\t-M	Metada file path (*).\n");
	printf("\t-h	Host file path(*).\n");
	printf("\t-r	IMSS root path (*).\n");
	printf("\t-a	IMSS server adderss (*).\n");
	printf("\t-P	IMSS policy (RR by default).\n");
	printf("\t-S	IMSS storage size in KB (by default 2048).\n");
	printf("\t-B	IMSS buffer size in KB (by default 1024).\n");
	printf("\t-e	Metadata buffer size in KB (by default 1024).\n");
	printf("\t-o	IMSS block size in KB (by default 1024).\n");
	printf("\t-R	Replication factor (by default NONE).\n");

	printf("\t-H	Print this message.\n");

	printf("\n\n(*) Argument is compulsory.\n");

}

int parse_args(char ** argv){

	//optstring -> "p:m:s:b:M:h:r:a:P:S:B:e:o:R"
	int option;
	//EXAMPLE -->
	while((opt = getopt(argc, argv, “:if:lrx”)) != -1){ 
        switch(opt) { 
            case ‘i’: 
            case ‘l’: 
            case ‘r’: 
                printf(“option: %c\n”, opt); 
                break; 
            case ‘f’: 
                printf(“filename: %s\n”, optarg); 
                break; 
            case ‘:’: 
                printf(“option needs a value\n”); 
                break; 
            case ‘?’: 
                printf(“unknown option: %c\n”, optopt);
                break; 
        } 
    } 
}

/*
 	----------- MAIN -----------
 */

int main(int argc, char *argv[])
{	
	//Parse input arguments
	int parse = parse_args(argv);

	//Hercules init -- Attached deploy
	if (hercules_init(0, STORAGE_SIZE, IMSS_SRV_PORT, METADATA_PORT, META_BUFFSIZE, METADATA_FILE) == -1){
		//In case of error notify and exit
		fprintf(stderr, "[IMSS-FUSE]	Hercules init failed, cannot deploy IMSS.\n");
		return -1;
	} 

	//Metadata server
	if (stat_init( IMSS_SRV_ADDR, METADATA_PORT, rank) == -1){
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


	return fuse_main(argc, argv, &imss_oper, NULL);
}

