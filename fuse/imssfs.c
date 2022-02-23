/*
FUSE: Filesystem in Userspace
Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

This program can be distributed under the terms of the GNU GPL.
See the file COPYING.

gcc -Wall imss.c `pkg-config fuse --cflags --libs` -o imss
*/

#define FUSE_USE_VERSION 26
#include "map.hpp"
#include "hercules.h"
#include "imss_posix_api.h"
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
uint64_t STORAGE_SIZE = 1024*1024*16; //In Kb, Default 16 GB
uint64_t META_BUFFSIZE = 1024 * 16; //In Kb, Default 16MB
//uint64_t IMSS_BLKSIZE = 1024; //In Kb, Default 1 MB
uint64_t IMSS_BLKSIZE = 8;//4
//uint64_t IMSS_BUFFSIZE = 1024*1024*2; //In Kb, Default 2Gb
uint64_t IMSS_BUFFSIZE = 1024*2048; //In Kb, Default 2Gb
int32_t REPL_FACTOR = 1; //Default none
char * MOUNTPOINT[6] = {"imssfs", "-f" , "XXXX", "-s", "-d", NULL}; // {"f", mountpoint} Not default ({"f", NULL})

uint64_t IMSS_DATA_BSIZE;

void * map;


#define MAX_PATH 256

static struct fuse_operations imss_oper = {
	.getattr	= imss_getattr,
	.chmod      = imss_chmod,
	.chown      = imss_chown,
	.rename		= imss_rename,
	.truncate	= imss_truncate,
	.utimens    = imss_utimens,
	.readdir	= imss_readdir,
	.open		= imss_open,
	.read		= imss_read, 
	.write		= imss_write, 
	.release	= imss_release,
	.create		= imss_create,
	.flush      = imss_flush,
	.mkdir		= imss_mkdir,
	.opendir 	= imss_opendir,
	.releasedir = imss_releasedir,
	.rmdir		= imss_rmdir,
	.unlink		= imss_unlink,
    .getxattr   = imss_getxattr,
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
	printf("\t-B	IMSS buffer size in MB (by default 2GB).\n");
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
	int argument;

	while((opt = getopt(argc, argv, "p:m:s:b:M:h:r:a:P:S:B:e:o:R:x:Hl:")) != -1){ 
		switch(opt) { 
			case 'p':
				
				if(!sscanf(optarg, "%" SCNu16, &IMSS_SRV_PORT)){
					print_help();
					return 0;
				}
				argument = atoi(optarg);
				if(argument<0){
					print_help();
					return 0;
				}
				break;
			case 'm':
				if(!sscanf(optarg, "%" SCNu16, &METADATA_PORT)){
					print_help();
					return 0;
				}
				argument = atoi(optarg);
				if(argument<0){
					print_help();
					return 0;
				}
				break;
			case 's':
				if(!sscanf(optarg, "%" SCNu32, &N_SERVERS)){
					print_help();
					return 0;
				}
				argument = atoi(optarg);
				if(argument<1){
					print_help();
					return 0;
				}
				break;
			case 'b':
				if(!sscanf(optarg, "%" SCNu32, &N_BLKS)){
					print_help();
					return 0;
				}
				argument = atoi(optarg);
				if(argument<0){
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
				argument = atoi(optarg);
				if(argument<0){
					print_help();
					return 0;
				}
				break;
			case 'B':
				if(!sscanf(optarg, "%" SCNu64, &IMSS_BUFFSIZE)){
					print_help();
					return 0;
				}
				argument = atoi(optarg);
				if(argument<0){
					print_help();
					return 0;
				}
				printf("IMSS_BUFFSIZE=%ld, STORAGE_SIZE=%ld\n",IMSS_BUFFSIZE,STORAGE_SIZE);
				if(IMSS_BUFFSIZE>STORAGE_SIZE){
					print_help();
					fprintf(stderr, "[IMSS-FUSE]	Total HERCULES storage size must be larger than IMSS_STORAGE_SIZE, %ld KB\n",IMSS_BUFFSIZE+META_BUFFSIZE);
					return 0;
				}
				break;
			case 'e':
				if(!sscanf(optarg, "%" SCNu64, &META_BUFFSIZE)){
					print_help();
					return 0;
				}
				
				argument = atoi(optarg);
				if(argument<0){
					print_help();
					return 0;
				}
				if(META_BUFFSIZE>STORAGE_SIZE){
					print_help();
					fprintf(stderr, "[IMSS-FUSE]	Total HERCULES storage size must be larger than IMSS_STORAGE_SIZE, %ld KB\n",META_BUFFSIZE+IMSS_BUFFSIZE);
					return 0;
				}
				break;
			case 'o':
				if(!sscanf(optarg, "%" SCNu64, &IMSS_BLKSIZE)){
					print_help();
					return 0;
				}
				argument = atoi(optarg);
				if(argument<0){
					print_help();
					return 0;
				}
				break;
			case 'R':
				if(!sscanf(optarg, "%" SCNu32, &REPL_FACTOR)){
					print_help();
					return 0;
				}
				argument = atoi(optarg);
				if(argument<0){
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
				argument = atoi(optarg);
				if(argument<0){
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


	//Check storage size
	if((META_BUFFSIZE+IMSS_BUFFSIZE)>STORAGE_SIZE){
		print_help();
		fprintf(stderr, "[IMSS-FUSE]	Total HERCULES storage size must be larger than IMSS_METADATA + DATA STORAGE, %ld\n", IMSS_BUFFSIZE+META_BUFFSIZE);
		//perror("Total HERCULES storage size must be larger than IMSS_METADATA + DATA STORAGE\n");
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
	if(!parse_args(argc, argv)) return -EINVAL;

	//Hercules init -- Attached deploy
	if (hercules_init(0, STORAGE_SIZE, IMSS_SRV_PORT, 1, METADATA_PORT, META_BUFFSIZE, METADATA_FILE) == -1){
		//In case of error notify and exit
		fprintf(stderr, "[IMSS-FUSE]	Hercules init failed, cannot deploy IMSS.\n");
		return -EIO;
	} 

	//Metadata server
	if (stat_init(META_HOSTFILE, METADATA_PORT, N_META_SERVERS,1) == -1){
		//In case of error notify and exit
		fprintf(stderr, "[IMSS-FUSE]	Stat init failed, cannot connect to Metadata server.\n");
		return -EIO;
	} 

	//Initialize the IMSS servers
	if(init_imss(IMSS_ROOT, IMSS_HOSTFILE, N_SERVERS, IMSS_SRV_PORT, IMSS_BUFFSIZE, ATTACHED, NULL) < 0) {
		//Notify error and exit
		fprintf(stderr, "[IMSS-FUSE]	IMSS init failed, cannot create servers.\n");
		return -EIO;
	} 

	char * test = get_deployed();
	if(test) {free(test);}

    map = map_create(); 

    IMSS_DATA_BSIZE = IMSS_BLKSIZE*KB-sizeof(uint32_t);

	return fuse_main(5, MOUNTPOINT, &imss_oper, NULL);
}

