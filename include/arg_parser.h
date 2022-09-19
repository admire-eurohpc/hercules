#ifndef ARG_PARSER_H
#define ARG_PARSER_H

#include <stdint.h>
#include <argp.h>

/* argp options */
#define TYPE                    't'
#define PORT                    'p'
#define BUFSIZE                 'b'
#define IMSS_URI                'i'
#define STAT_HOST               'h'
#define STAT_PORT               's'
#define NUM_SERVERS             'n'
#define DEPLOY_HOSTFILE         'd'
#define STAT_LOGFILE            'm'

/* TYPE option args */
#define TYPE_DATA_SERVER        'd'
#define TYPE_METADATA_SERVER    'm'


struct arguments
{
    char        type;               /* type arg to '-t' */
    uint16_t    port;               /* port arg to '-p' */
    int64_t     bufsize;            /* buffer size arg to '-b' */
    char        imss_uri[32];       /* IMSS URI arg to '-i' */
    char *      stat_host;          /* Metadata server hostname arg to '-h' */
    int64_t     stat_port;          /* Metadata server port number arg to '-s' */
    int64_t     num_servers;        /* number of data servers arg to '-n' */
    char *      deploy_hostfile;    /* deploy hostfile arg to '-d' */
    char *      stat_logfile;       /* metadata logfile arg to '-m' */
};


int parse_args (int argc, char ** argv, struct arguments * args);

#endif