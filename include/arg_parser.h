#ifndef ARG_PARSER_H
#define ARG_PARSER_H

#include <stdint.h>
#include <argp.h>

/********** argp options **********/
/* common options */
#define PORT                    'p'
#define BUFSIZE                 'b'
#define ID                      'r'

/* data server options */
#define IMSS_URI                'i'
#define STAT_HOST               'H'
#define STAT_PORT               'P'
#define NUM_SERVERS             'n'
#define DEPLOY_HOSTFILE         'd'
#define BLOCK_SIZE              'B'
#define STORAGE_SIZE            's'

/* metadata server options */
#define STAT_LOGFILE            'l'


/*** TYPE argument legal values ***/
#define TYPE_DATA_SERVER        'd'
#define TYPE_METADATA_SERVER    'm'


struct arguments
{
    char        type;               /* type arg */
	int			id;                 /* server ID arg to -r */
    uint16_t    port;               /* port arg to '-p' */
    int64_t     bufsize;            /* buffer size arg to '-b' */
    char        imss_uri[32];       /* IMSS URI arg to '-i' */
    char *      stat_host;          /* Metadata server hostname arg to '-H' */
    int64_t     stat_port;          /* Metadata server port number arg to '-P' */
    int64_t     num_servers;        /* number of data servers arg to '-n' */
    char *      deploy_hostfile;    /* deploy hostfile arg to '-d' */
    char *      stat_logfile;       /* metadata logfile arg to '-l' */
    uint64_t    block_size;         /* block size in KB arg to -B */
	uint64_t    storage_size;       /* total storage size in GB to -s */
};


int parse_args (int argc, char ** argv, struct arguments * args);

#endif
