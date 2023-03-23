#include <stdlib.h>
#include <string.h>
#include "arg_parser.h"

/* Parse a single option. */
static error_t parse_opt (int key, char * arg, struct argp_state * state);


const char *argp_program_version =
    "IMSS 2.0";

/* Program documentation. */
static char doc[] =
    "IMSS -- <short description>\
    \v<long description>";

/* A description of the arguments we accept. */
static char args_doc[] = "<type>";


/* The options we understand */
static struct argp_option options[] = {
    {0,                     0,                  0,              0,  "Arguments:" },
    {0,                     0,                  0,              0,  "<types>:" },
    {0,                     0,                  0,              0,  "d : Data server" },
    {0,                     0,                  0,              0,  "m : Metadata server" },

    {0,                     0,                  0,              0,  "\nOptions:\n" },
    {0,                     0,                  0,              0,  "Common options:" },
    {"port",                PORT,               "PORT",         0,  "Listening port number" },
    //TODO: figure out what bufsize ACTUALLY does
    {"bufsize",             BUFSIZE,            "BUFFER_SIZE",  0,  "Buffer size; max RAM size that can be used; 0 if omitted, which means NO LIMIT" },
    {"server-id",           ID,                 "ID",     		0,  "0 if omitted" },

    {0,                     0,                  0,              0,  "Data server options (required only if type=d):" },
    {"imss-uri",            IMSS_URI,           "IMSS_URI",     0,  "IMSS URI (data server); 'imss://' if omitted" },
    {"stat-host",           STAT_HOST,          "HOSTNAME",     0,  "Metadata server hostname" },
    {"stat-port",           STAT_PORT,          "PORT",         0,  "Metadata server port number" },
    {"num-servers",         NUM_SERVERS,        "NUM_SERVERS",  0,  "Number of data servers for this IMSS deployment" },
    {"deploy-hostfile",     DEPLOY_HOSTFILE,    "FILE",         0,  "IMSS MPI deployment file; contains hostnames of data servers" },
    {"block-size",          BLOCK_SIZE,         "BLOCK_SIZE",   0,  "Size of each data block in KB" },
    {"storage-size",        STORAGE_SIZE,       "STORAGE_SIZE", 0,  "Total amount of RAM in GB to be used as storage" },

    {0,                     0,                  0,              0,  "Metadata server options (required only if type=m):" },
    {"stat-logfile",        STAT_LOGFILE,       "FILE",         0,  "Metadata server logfile" },
    { 0 }
};


/* Parse a single option. */
static error_t parse_opt (int key, char * arg, struct argp_state * state)
{
    /* Get the input argument from argp_parse, which we
        know is a pointer to our arguments structure */
    struct arguments *args = (struct arguments *) state->input;

    switch (key)
        {
        case PORT:
            args->port = (uint16_t) atoi(arg);
            break;
        case BUFSIZE:
            args->bufsize = atoi(arg);
            break;
        case ID:
		    args->id = atoi(arg);
			break;
        case IMSS_URI:
            strcpy(args->imss_uri, arg);
            break;
        case STAT_HOST:
            args->stat_host = arg;
            break;
        case STAT_PORT:
            args->stat_port = atoi(arg);
            break;
        case NUM_SERVERS:
            args->num_servers = atoi(arg);
            break;
        case DEPLOY_HOSTFILE:
            args->deploy_hostfile = arg;
            break;
        case BLOCK_SIZE:
            args->block_size = atoi(arg);
            break;
        case STORAGE_SIZE:
            args->storage_size = atoi(arg);
            break;
        case STAT_LOGFILE:
            args->stat_logfile = arg;
            break;
        case ARGP_KEY_ARG:
            if (state->arg_num >= 1) {
                argp_usage (state);
            }

            args->type = *arg;
            if (args->type != TYPE_DATA_SERVER && args->type != TYPE_METADATA_SERVER) {
                argp_failure(state, 1, 0, "Invalid argument for 'type'. \nSee --help for more detail");
            }
            break;

        /* check that all mandatory options have been provided;
        *  in case of a data server, more options are mandatory */
        case ARGP_KEY_END:
            if (state->arg_num < 1) {
                argp_usage (state);
            }
            if (!args->type || !args->port) {
                argp_failure(state, 1, 0, "Required options: -p. \nSee --help for more detail");
            }
            if (args-> type == TYPE_DATA_SERVER &&
            (!args->stat_host || !args->stat_port || !args->num_servers ||
            !args->deploy_hostfile || !args->block_size)) {
                argp_failure(state, 1, 0, "Required options for data server type: -H, -P, -n, -d, -B, -s. \nSee --help for more detail");
                exit (ARGP_ERR_UNKNOWN);

            } else if (args->type == TYPE_METADATA_SERVER && !args->stat_logfile) {
                argp_failure(state, 1, 0, "Required options for metadata server type: -l. \nSee --help for more detail");
                exit (ARGP_ERR_UNKNOWN);
            }
            break;
        default:
            return ARGP_ERR_UNKNOWN;
        }
    return 0;
}


/* argp parser */
static struct argp argp = { options, parse_opt, args_doc, doc };


int parse_args (int argc, char ** argv, struct arguments * args)
{
    /* Default values */
    args->type = 0;
	args->id = 0;
    args->port = 0;
    args->bufsize = 0;
    strcpy(args->imss_uri, "imss://");
    args->stat_port = 0;
    args->num_servers = 0;
    args->block_size = 64; //In KB
    args->storage_size = 8; // In GB

    /* Parse arguments; every option seen by parse_opt will be
        reflected in arguments */
    argp_parse (&argp, argc, argv, 0, 0, args);

    return 0;
}
