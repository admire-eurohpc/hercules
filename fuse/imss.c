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
#include <unistd.h>
#include <fcntl.h>
#include "imss.h"

#define MAX_LEN 1024

/*
	-----------	IMSS Global variables, filled at the beggining or by default -----------
*/

 uint16_t IMSS_SRV_PORT = 1; //Not default, 1 will fail
 uint16_t METADATA_PORT = 1; //Not default, 1 will fail
 int32_t N_SERVERS = 1; //Default 1
 int32_t N_BLKS = 1; //Default 1
 char * METADATA_FILE = NULL //Not default
 char * IMSS_HOSTFILE = NULL //Not default
 char * IMSS_ROOT = NULL//Not default
 char * IMSS_SRV_ADDR = NULL; //Not default 
 char * POLICY = "RR"; //Default RR
 uint64_t STORAGE_SIZE = 2048; //In Kb, Default 2 MB
 uint64_t META_BUFFSIZE = 1024; //In Kb, Default 1 MB
 uint64_t IMSS_BUFFSIZE = 1024; //In Kb, Default 1 MB
 uint64_t IMSS_BLKSIZE = 1024; //In Kb, Default 1 MB
 int32_t REPL_FACTOR = NONE; //Default none

/*
  	(*) Mapping for REPL_FACTOR values:
  		NONE = 1;
		DRM = 2;
		TRM = 3;
*/

 /*
	-----------	FUSE IMSS implementation -----------
*/

 char * DEF_POL = "RR";


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
	IMSS_SRV_ADDR;
}

/**
 *	Parse arguments function.
 *	Returns 1 if the parsing was correct, 0 otherwise.
 */
int parse_args(char ** argv){

	int opt;

	while((opt = getopt(argc, argv, "p:m:s:b:M:h:r:a:P:S:B:e:o:R")) != -1){ 
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
				IMSS_SRV_ADDR = optarg;
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
            case 'H':
            	print_help();
            	return 0;
            case ':':
            	fprintf(stderr, "[IMSS-FUSE]	Option %s requires value\n", optarg);
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

