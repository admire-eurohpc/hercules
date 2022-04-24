/*
 * daemonize.c
 * This example daemonizes a process, writes a few log messages,
 * sleeps 20 seconds and terminates afterwards.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <syslog.h>
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
#include<sys/wait.h>

#include "imss.h"
#include "hercules.h"

#define MAX_PATH 256

/*
   -----------  IMSS Global variables, filled at the beggining or by default -----------
   */

uint16_t IMSS_SRV_PORT = 1; //Not default, 1 will fail
uint16_t METADATA_PORT = 1; //Not default, 1 will fail
int32_t N_SERVERS = 2; //Default
int32_t N_META_SERVERS = 1; //Default 1 1
int32_t N_BLKS = 1; //Default 1
char * METADATA_FILE = NULL; //Not default
char * IMSS_HOSTFILE = NULL; //Not default
char * IMSS_ROOT = NULL;//Not default
char * META_HOSTFILE = NULL; //Not default 
char * POLICY = "RR"; //Default RR
uint64_t STORAGE_SIZE = 2048; //In Kb, Default 2 MB
uint64_t META_BUFFSIZE = 1024; //In Kb, Default 1 MB
uint64_t IMSS_BLKSIZE = 1024; //In Kb, Default 1 MB
uint64_t IMSS_BUFFSIZE = 1024*2048; //In Kb, Default 2Gb
int32_t REPL_FACTOR = 1; //Default none
char MOUNTPOINT[MAX_PATH];

//char fd_table[1024][MAX_PATH]; 



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

/*
   ----------- Parsing arguments and help functions -----------
   */

void print_help(){

    printf("IMSS FUSE HELP\n\n");

    printf("\t-p    IMSS port (*).\n");
    printf("\t-m    Metadata port (*).\n");
    printf("\t-s    Number of servers (1 default).\n");
    printf("\t-b    Number of blocks (1 default).\n");
    printf("\t-M    Metadata file path (*).\n");
    printf("\t-h    Host file path(*).\n");
    printf("\t-r    IMSS root path (*).\n");
    printf("\t-a    Metadata hostfile (*).\n");
    printf("\t-P    IMSS policy (RR by default).\n");
    printf("\t-S    IMSS storage size in KB (by default 2048).\n");
    printf("\t-B    IMSS buffer size in KB (by default 1024).\n");
    printf("\t-e    Metadata buffer size in KB (by default 1024).\n");
    printf("\t-o    IMSS block size in KB (by default 1024).\n");
    printf("\t-R    Replication factor (by default NONE).\n");
    printf("\t-x    Metadata server number (by default 1).\n");

    printf("\n\t-l  Mountpoint (*).\n");

    printf("\n\t-H  Print this message.\n");

    printf("\n(*) Argument is compulsory.\n");

}

/**
 *  Parse arguments function.
 *  Returns 1 if the parsing was correct, 0 otherwise.
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
			    strcpy(MOUNTPOINT, optarg);
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
        fprintf(stderr, "[IMSS-FUSE]    Please, fill all the mandatory arguments.\n");
        print_help();
        return 0;
    }

    return 1;
}

static int skeleton_daemon(int argc, char ** argv)
{
    pid_t pid;

    /* Fork off the parent process */
    pid = fork();

    /* An error occurred */
    if (pid < 0)
        exit(EXIT_FAILURE);

    /* Success: Let the parent terminate */
    if (pid > 0)
        exit(EXIT_SUCCESS);

    /* On success: The child process becomes session leader */
    if (setsid() < 0)
        exit(EXIT_FAILURE);

    /* Catch, ignore and handle signals */
    //TODO: Implement a working signal handler */
    //signal(SIGCHLD, SIG_IGN);
    //signal(SIGHUP, SIG_IGN);

    /* Fork off for the second time*/
    pid = fork();

    /* An error occurred */
    if (pid < 0)
        exit(EXIT_FAILURE);

    /* Success: Let the parent terminate */
    if (pid > 0)
        exit(EXIT_SUCCESS);

    /* Set new file permissions */
    umask(0);

    /* Close all open file descriptors */
    int x;
    for (x = sysconf(_SC_OPEN_MAX); x>=0; x--)
    {
        close (x);
    }

    /* Open the log file */
    openlog ("firstdaemon", LOG_PID, LOG_DAEMON);
    syslog (LOG_NOTICE, "IMSS daemon starting.");

    syslog (LOG_NOTICE, "Hercules starting.");
    //Hercules init -- Attached deploy
    if (hercules_init(0, STORAGE_SIZE, IMSS_SRV_PORT, 1, METADATA_PORT, META_BUFFSIZE, METADATA_FILE) == -1){
        //In case of error notify and exit
        syslog (LOG_NOTICE,  "Hercules init failed, cannot deploy IMSS.\n");
        return -1;
    }

	syslog (LOG_NOTICE, "IMSS server starting.");


    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;

    int run = creat("/var/run/imss.pid", mode);
	char buff[16];

	pid_t pid_daemon = getpid();

	sprintf(buff,"%u\n",pid_daemon);
    write (run, buff, strlen(buff));
    close(run);

    syslog (LOG_NOTICE, "IMSS daemon started.");

    pause();

    return 0;
}



int main(int argc, char ** argv)
{
	//Parse input arguments
    if(!parse_args(argc, argv)) return -1;

    skeleton_daemon(argc, argv);

    closelog();

    return EXIT_SUCCESS;
}
