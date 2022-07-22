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
#include "hercules.h"
#include "imss_fuse_api.h"
#include <fuse.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <inttypes.h>
#include <stdlib.h>
#include <time.h>
#include <limits.h>


#define KB 1024
#define GB 1073741824
/*
   -----------	IMSS Global variables, filled at the beggining or by default -----------
*/
uint32_t deployment = 1;//Default 1=ATACHED, 0=DETACHED ONLY METADATA SERVER 2=DETACHED METADATA AND DATA SERVERS
uint16_t IMSS_SRV_PORT = 1; //Not default, 1 will fail
uint16_t METADATA_PORT = 1; //Not default, 1 will fail
int32_t N_SERVERS = 1; //Default 1
int32_t N_META_SERVERS = 1; //Default 1 1
int32_t N_BLKS = 1; //Default 1
char * METADATA_FILE = NULL; //Not default
char * IMSS_HOSTFILE = NULL; //Not default
char * IMSS_ROOT = NULL;//Not default
char * META_HOSTFILE = NULL; //Not default 
char * POLICY = "RR"; //Default RR
uint64_t STORAGE_SIZE = 16; //In GB
uint64_t META_BUFFSIZE = 16; //In GB
//uint64_t META_BUFFSIZE = 1024 * 1000;
//uint64_t IMSS_BLKSIZE = 1024; //In Kb, Default 1 MB
uint64_t IMSS_BLKSIZE = 1024;
//uint64_t IMSS_BUFFSIZE = 1024*1024*2; //In Kb, Default 2Gb
uint64_t IMSS_BUFFSIZE = 2; //In GB
int32_t REPL_FACTOR = 1; //Default none
char * MOUNTPOINT[7] = {"imssfs", "-f" , "XXXX", "-s", NULL}; // {"f", mountpoint} Not default ({"f", NULL})

uint16_t PREFETCH = 6;
uint16_t threshold_read_servers = 4;
uint16_t BEST_PERFORMANCE_READ = 0;//if 1    then n_servers < threshold => SREAD, else if n_servers > threshold => SPLIT_READV 
                                   //if 0 only one method of read applied specified in MULTIPLE_READ

uint16_t MULTIPLE_READ = 0;//1=vread with prefetch, 2=vread without prefetch, 3=vread_2x else sread
uint16_t MULTIPLE_WRITE = 0;//1=writev, else sread
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

void * map;
void * map_prefetch;


#define MAX_PATH 256

static struct fuse_operations imss_oper = {
	.getattr		= imss_fuse_getattr,
	.chmod		= imss_fuse_chmod,
	.chown		= imss_fuse_chown,
	.rename		= imss_fuse_rename,
	.truncate	= imss_fuse_truncate,
	.utimens	= imss_fuse_utimens,
	.readdir	= imss_fuse_readdir,
	.open		= imss_fuse_open,
	.read		= imss_fuse_read, 
	.write		= imss_fuse_write, 
	.release	= imss_fuse_release,
	.create		= imss_fuse_create,
	.flush		= imss_fuse_flush,
	.mkdir		= imss_fuse_mkdir,
	.opendir 	= imss_fuse_opendir,
	.releasedir	= imss_fuse_releasedir,
	.rmdir		= imss_fuse_rmdir,
	.unlink		= imss_fuse_unlink,
    .getxattr	= imss_fuse_getxattr,
	.access		= imss_fuse_access
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
	printf("\t-d	Deployment (by default 1).\n");

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
	while((opt = getopt(argc, argv, "p:m:s:b:M:h:r:a:P:S:B:e:o:R:x:d:Hl:")) != -1){ 
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
				if(IMSS_BUFFSIZE>STORAGE_SIZE){
					print_help();
					fprintf(stderr, "[IMSS-FUSE]	1Total HERCULES storage size must be larger than IMSS_STORAGE_SIZE, %ld KB\n",IMSS_BUFFSIZE+META_BUFFSIZE);
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
					fprintf(stderr, "[IMSS-FUSE]	2Total HERCULES storage size must be larger than IMSS_STORAGE_SIZE, %ld KB\n",META_BUFFSIZE+IMSS_BUFFSIZE);
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
			case 'd'://0=ATTACHED 1=DETACHED ONLY SERVER-METADATA 2=FULL DETACHED
				if(!sscanf(optarg, "%" SCNu32, &deployment)){
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

/*
   ----------- MAIN -----------
   */

int main(int argc, char *argv[])
{	
	//Parse input arguments
	if(!parse_args(argc, argv)) return -EINVAL;

	if(deployment==1){
		//Hercules init -- Attached deploy
		if (hercules_init(0, STORAGE_SIZE, IMSS_SRV_PORT, 1, METADATA_PORT, META_BUFFSIZE, METADATA_FILE) == -1){
			//In case of error notify and exit
			fprintf(stderr, "[IMSS-FUSE]	Hercules init failed, cannot deploy IMSS.\n");
			return -EIO;
		}
	}

	//Metadata server
	if (stat_init(META_HOSTFILE, METADATA_PORT, N_META_SERVERS,1) == -1){
		//In case of error notify and exit
		fprintf(stderr, "[IMSS-FUSE]	Stat init failed, cannot connect to Metadata server.\n");
		return -EIO;
	} 

	if(deployment==2){
		open_imss(IMSS_ROOT);
	}

	if(deployment!=2){
		//Initialize the IMSS servers
		if(init_imss(IMSS_ROOT, IMSS_HOSTFILE, META_HOSTFILE, N_SERVERS, IMSS_SRV_PORT, IMSS_BUFFSIZE, deployment, "/home/hcristobal/imss/build/server", METADATA_PORT) < 0) {
		//if(init_imss(IMSS_ROOT, IMSS_HOSTFILE, N_SERVERS, IMSS_SRV_PORT, IMSS_BUFFSIZE, deployment, NULL) < 0) {
			//Notify error and exit
			fprintf(stderr, "[IMSS-FUSE]	IMSS init failed, cannot create servers.\n");
			return -EIO;
		}
	}

	char * test = get_deployed();
	if(test) {free(test);}

    map = map_create(); 
	map_prefetch = map_create_prefetch();

	IMSS_DATA_BSIZE = IMSS_BLKSIZE*KB;

    int ret;
    pthread_attr_t tattr;
    /* initialized with default attributes */
    ret = pthread_attr_init(&tattr);
    ret = pthread_attr_setdetachstate(&tattr,PTHREAD_CREATE_DETACHED);

	/*if (pthread_create(&prefetch_t, &tattr, prefetch_function, NULL) == -1)
	{
		perror("ERRIMSS_PREFETCH_DEPLOY");
		pthread_exit(NULL);
	}*/
	//struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
	struct fuse_args args = FUSE_ARGS_INIT(0, NULL);
	fuse_opt_add_arg(&args, MOUNTPOINT[0]);
	fuse_opt_add_arg(&args, MOUNTPOINT[1]);
	fuse_opt_add_arg(&args, MOUNTPOINT[2]);
	fuse_opt_add_arg(&args, MOUNTPOINT[3]);
	/*fuse_opt_add_arg(&args, "-obig_writes"); // allow Big Writes
	fuse_opt_add_arg(&args, "-omax_write=131072");*/
	fuse_opt_add_arg(&args, "-odirect_io");

	//#define FUSE_MAX_PAGES_PER_REQ 256
	return fuse_main(args.argc, args.argv, &imss_oper, NULL);
	//return fuse_main(5, args.argv, &imss_oper, NULL);

	//return fuse_main(4, MOUNTPOINT, &imss_oper,  NULL);
}
