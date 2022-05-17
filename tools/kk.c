#define _GNU_SOURCE


#include <stdio.h>
#include <dlfcn.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/stat.h>
//#include <fcntl.h>
#include <string.h>

#include "imss.h"


char IMSS_HOSTFILE[512]; //Not default
char META_HOSTFILE[512];
uint16_t IMSS_SRV_PORT = 1; //Not default, 1 will fail
uint16_t METADATA_PORT = 1; //Not default, 1 will fail
char IMSS_ROOT[32];
int32_t N_SERVERS = 1; //Default
int32_t N_META_SERVERS = 1;
uint64_t IMSS_BUFFSIZE = 1024*2048; //In Kb, Default 2Gb
int32_t IMSS_DEBUG = 0;


int main (void)
{

    fprintf(stderr,"IMSS client starting\n");

	strcpy(IMSS_ROOT, "imss://");

    if (getenv("IMSS_HOSTFILE") != NULL) {
		strcpy(IMSS_HOSTFILE, getenv("IMSS_HOSTFILE"));
	}

    if (getenv("IMSS_N_SERVERS") != NULL) {
		N_SERVERS = atoi(getenv("IMSS_N_SERVERS"));
	}

	if (getenv("IMSS_SRV_PORT") != NULL) {
        IMSS_SRV_PORT = atoi(getenv("IMSS_SRV_PORT"));
    }

    if (getenv("IMSS_BUFFSIZE") != NULL) {
        IMSS_BUFFSIZE = atol(getenv("IMSS_BUFFSIZE"));
    }

	if (getenv("IMSS_META_HOSTFILE") != NULL) {
        strcpy(META_HOSTFILE, getenv("IMSS_META_HOSTFILE"));
    }

    if (getenv("IMSS_META_PORT") != NULL) {
        METADATA_PORT = atoi(getenv("IMSS_META_PORT"));
    }

    if (getenv("IMSS_META_SERVERS") != NULL) {
        N_META_SERVERS = atoi(getenv("IMSS_META_SERVERS"));
    }

    if (getenv("IMSS_DEBUG") != NULL) {
        IMSS_DEBUG = 1;
    }


    fprintf(stderr," -- Hostfile: %s\n", IMSS_HOSTFILE);
    fprintf(stderr," -- # Servers: %d\n", N_SERVERS );
    fprintf(stderr," -- Server port: %d\n", IMSS_SRV_PORT );
    fprintf(stderr," -- Buffer size: %ld\n", IMSS_BUFFSIZE );

	//Metadata server
    if (stat_init(META_HOSTFILE, METADATA_PORT, N_META_SERVERS,1) == -1){
        //In case of error notify and exit
        fprintf(stderr, "Stat init failed, cannot connect to Metadata server.\n");
    }


    if(init_imss(IMSS_ROOT, IMSS_HOSTFILE, META_HOSTFILE, N_SERVERS, IMSS_SRV_PORT, IMSS_BUFFSIZE, ATTACHED, NULL, METADATA_PORT) < 0) {
        fprintf(stderr,"IMSS init failed, cannot create servers.\n");
    }

    fprintf(stderr,"IMSS client running\n");

   return 1;
}


