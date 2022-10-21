#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/signal.h>
#include "imss.h"
#include "stat.h"
#include "comms.h"
#include "workers.h"
#include "memalloc.h"
#include "directory.h"
#include "records.hpp"
#include "arg_parser.h"
#include "map_ep.hpp"
#include <inttypes.h>


//Pointer to the tree's root node.
extern GNode * 		tree_root;
extern pthread_mutex_t 	tree_mut;
//URI of the created IMSS.
char * 			imss_uri;

//Initial buffer address.
extern char *   buffer_address;
//Set of locks dealing with the memory buffer access.
extern pthread_mutex_t * region_locks;
//Segment size (amount of memory assigned to each thread).
extern uint64_t 	 buffer_segment;

/* UCP objects */
ucp_context_h ucp_context;
ucp_worker_h  ucp_worker;

ucp_ep_h     pub_ep;

void * map_ep; // map_ep used for async write; server doesn't use it
int32_t is_client = 0; // also used for async write

int32_t  IMSS_DEBUG = 0;


int32_t main(int32_t argc, char **argv)
{
	// Print off a hello world message

	uint16_t bind_port, aux_bind_port;
	char *   stat_add;
	char *   metadata_file;
	char *   deployfile;
	int64_t  buffer_size, stat_port, num_servers;
	void * 	 socket;

	// memory pool stuff
	uint64_t block_size; //In KB 
	uint64_t storage_size; //In GB

	ucs_status_t status;
	ucp_ep_h     client_ep;

	ucp_am_handler_param_t param;
	int              ret;
	/***************************************************************/
	/******************** PARSE INPUT ARGUMENTS ********************/
	/***************************************************************/
	struct arguments args;

	if (getenv("IMSS_DEBUG") != NULL) {
		IMSS_DEBUG = 1;
	}

	parse_args(argc, argv, &args);

	DPRINT("[SERVER] Starting server.\n");
	DPRINT("[CLI PARAMS] type = %c port = %" PRIu16 " bufsize = %" PRId64 " ", args.type, args.port, args.bufsize);
	if (args.type == TYPE_DATA_SERVER) {
		DPRINT("imss_uri = %s stat-host = %s stat-port = %" PRId64 " num-servers = %" PRId64 " deploy-hostfile = %s block-size = %" PRIu64 " storage-size = %" PRIu64 "\n",
		args.imss_uri, args.stat_host, args.stat_port, args.num_servers, args.deploy_hostfile, args.block_size, args.storage_size);
	} else {
		DPRINT("stat-logfile = %s\n", args.stat_logfile);
	}

	//bind port number.
	bind_port = args.port;
	aux_bind_port	= bind_port;
	//buffer size provided
	buffer_size = args.bufsize;
	//set up imss uri (default value is already set up in args)
	imss_uri 	= (char *) calloc(32, sizeof(char));

	/* Initialize the UCX required objects */
	ret = init_context(&ucp_context, &ucp_worker, CLIENT_SERVER_SEND_RECV_STREAM);
	if (ret != 0) {
		perror("ERRIMSS_INIT_CONTEXT");
		return -1;
	}

	/* CHECK THIS OUT!
	 ***************************************************
	 In relation to the type argument provided, an IMSS or a metadata server will be deployed. */

	//IMSS server.
	if (args.type == TYPE_DATA_SERVER)
	{
		//IMSS name.
		strcpy(imss_uri, args.imss_uri);
		//machine name where the metadata server is being executed.
		stat_add	= args.stat_host;
		//port that the metadata server is listening on.
		stat_port 	= args.stat_port;
		//number of servers conforming the IMSS deployment.
		num_servers	= args.num_servers;
		//IMSS' MPI deployment file.
		deployfile	= args.deploy_hostfile;
		//data block size
		block_size  = args.block_size;
		//total storage size
		storage_size = args.storage_size;

		int32_t imss_exists = 0;

		//Check if the provided URI has been already reserved by any other instance.
		if (!args.id)
		{
			//Connect to the specified endpoint.
			status = start_client(ucp_worker, stat_add, stat_port + 1, &client_ep); // port, rank,
			if (status != UCS_OK) {
				fprintf(stderr, "failed to start client (%s)\n", ucs_status_string(status));
				return -1;
			}

			uint32_t id = CLOSE_EP;

			//Formated imss uri to be sent to the metadata server.
			char formated_uri[REQUEST_SIZE];
			sprintf(formated_uri, "%" PRIu32 " GET 0 %s", id, imss_uri);

			//Send the request.
			if (send_stream(ucp_worker, client_ep, formated_uri, REQUEST_SIZE) < 0)
			{
				perror("ERRIMSS_DATASET_REQ");
				return -1;
			}

			ep_flush(client_ep, ucp_worker);
			imss_info imss_info_;

			//Receive the associated structure.
			ret = recv_dynamic_stream(ucp_worker, client_ep, &imss_info_, BUFFER);

			if (ret >= sizeof(imss_info))
			{
				imss_exists = 1;
				for (int32_t i = 0; i < imss_info_.num_storages; i++)
					free(imss_info_.ips[i]);
				free(imss_info_.ips);
			}

			ep_close(ucp_worker, client_ep, UCP_EP_CLOSE_MODE_FLUSH);
		}

		if (imss_exists)
		{
			fprintf(stderr, "ERRIMSS_IMSSURITAKEN");
			return 0;
		}
	}
	//Metadata server.
	else
	{
		//metadata file.
		metadata_file	= args.stat_logfile;

		//Create the tree_root node.
		char * root_data = (char *) calloc(8, sizeof(char));
		strcpy(root_data,"imss://");
		tree_root = g_node_new((void *) root_data);

		if (pthread_mutex_init(&tree_mut, NULL) != 0)
		{
			perror("ERRIMSS_TREEMUT_INIT");
			pthread_exit(NULL);
		}
	}


	/***************************************************************/
	/******************** INPROC COMMUNICATIONS ********************/
	/***************************************************************/
	//ucx_server_ctx_t context;
	//ucp_worker_h     ucp_data_worker;


	//ret = init_worker(ucp_context, &ucp_data_worker);
	//if (ret != 0) {
	//	perror("ERRIMSS_WORKER_INIT");
	//	pthread_exit(NULL);
	//}

	/* Initialize the server's context. */
	//context.conn_request = NULL;

	//status = start_server(ucp_worker, &context, &context.listener, NULL, 0);
	//status = start_server(ucp_worker, &context, &context.listener, NULL, bind_port + 1000);
	//if (status != UCS_OK) {
	//	perror("ERRIMSS_STAR_SERVER");
	//	pthread_exit(NULL);
	//} 
	/*
	   while (context.conn_request == NULL) {
	   ucp_worker_progress(ucp_worker);
	   }
	   status = server_create_ep(ucp_data_worker, context.conn_request, &pub_ep);
	   if (status != UCS_OK) {
	   perror("ERRIMSS_SERVER_CREATE_EP");
	   pthread_exit(NULL);
	   }
	   */
	//Map tracking saved records.
	std::shared_ptr<map_records> map(new map_records(buffer_size*KB));

	int64_t data_reserved;
	//Pointer to the allocated buffer memory.
	char * buffer;
	//Size of the buffer involved.
	uint64_t size = (uint64_t) buffer_size*KB;
	//Check if the requested data is available in the current node.
	if ((data_reserved = memalloc(size, &buffer)) == -1)
		return -1;

	buffer_address = buffer;

	//Metadata bytes written into the buffer.
	uint64_t bytes_written = 0;

	if (args.type == TYPE_METADATA_SERVER)
	{
		if ((buffer_address = metadata_read(metadata_file, map.get(), buffer, &bytes_written)) == NULL)
			return -1;

		//Obtain the remaining free amount of data reserved to the buffer after the metadata read operation.
		data_reserved -= bytes_written;
	}

	//Buffer segment size assigned to each thread.
	buffer_segment = data_reserved/THREAD_POOL;

	//Initialize pool of threads.
	pthread_t threads[(THREAD_POOL+1)];
	//Thread arguments.
	p_argv arguments[(THREAD_POOL+1)];

	if (args.type == TYPE_DATA_SERVER)
		region_locks = (pthread_mutex_t *) calloc(THREAD_POOL, sizeof(pthread_mutex_t));

	//Execute all threads.
	for (int32_t i = 0; i < (THREAD_POOL+1); i++)
	{
		//Add port number to thread arguments.
		arguments[i].port = (bind_port)++;
		arguments[i].ucp_context = ucp_context;
		arguments[i].ucp_worker = ucp_worker;
		arguments[i].blocksize = block_size;
		arguments[i].storage_size = storage_size;
		//Add the instance URI to the thread arguments.
		strcpy(arguments[i].my_uri, imss_uri);

		//Deploy all dispatcher + service threads.
		if (!i)
		{
			DPRINT("[SERVER] Creating dispatcher thread.\n");
			//Deploy a thread distributing incomming clients among all ports.
			if (pthread_create(&threads[i], NULL, dispatcher, (void *) &arguments[i]) == -1)
			{
				//Notify thread error deployment.
				perror("ERRIMSS_DISPATCHER_DEPLOY");
				return -1;
			}
		}
		else
		{
			//Add the reference to the map into the set of thread arguments.
			arguments[i].map = map;
			//Specify the address used by each thread to write inside the buffer.
			arguments[i].pt = (char *) ((i-1)*buffer_segment + buffer_address);
			arguments[i].ucp_context = ucp_context;
			arguments[i].ucp_worker = ucp_worker;

			//IMSS server.
			if (args.type == TYPE_DATA_SERVER)
			{	
				DPRINT("[SERVER] Creating data thread.\n");
				if (pthread_create(&threads[i], NULL, srv_worker, (void *) &arguments[i]) == -1)
				{
					//Notify thread error deployment.
					perror("ERRIMSS_SRVWORKER_DEPLOY");
					return -1;
				}
			}
			//Metadata server.
			else
			{
				DPRINT("[SERVER] Creating metadata thread.\n");
				if (pthread_create(&threads[i], NULL, stat_worker, (void *) &arguments[i]) == -1)
				{
					//Notify thread error deployment.
					perror("ERRIMSS_STATWORKER_DEPLOY");
					return -1;
				}
			}
		}
	}

	//Notify to the metadata server the deployment of a new IMSS.
	if ((args.type == TYPE_DATA_SERVER) && !args.id && stat_port)
	{
		//Metadata structure containing the novel IMSS info.
		imss_info my_imss;

		strcpy(my_imss.uri_, imss_uri);
		my_imss.ips = (char **) calloc(num_servers, sizeof(char *));
		my_imss.num_storages 	= num_servers;
		my_imss.conn_port	= aux_bind_port;
		my_imss.type	= 'I';//extremely important
		//FILE entity managing the IMSS deployfile.
		FILE * svr_nodes;

		if ((svr_nodes = fopen(deployfile, "r+")) == NULL)
		{
			perror("ERRIMSS_DEPLOYFILE_OPEN");
			return -1;
		}

		//Number of characters successfully read from the line.
		int32_t n_chars;
		for (int32_t i = 0; i < num_servers; i++)
		{
			//Allocate resources in the metadata structure so as to store the current IMSS's IP.
			(my_imss.ips)[i] = (char *) calloc(LINE_LENGTH, sizeof(char));
			size_t l_size = LINE_LENGTH;

			//Save IMSS metadata deployment.
			n_chars = getline(&((my_imss.ips)[i]), &l_size, svr_nodes);

			//Erase the new line character ('\n') from the string.
			((my_imss.ips)[i])[n_chars - 1] = '\0';
		}

		//Close the file.
		if (fclose(svr_nodes) != 0)
		{
			perror("ERRIMSS_DEPLOYFILE_CLOSE");
			return -1;
		}

		status = start_client(ucp_worker, stat_add, stat_port + 1, &client_ep); // port, rank,
		if (status != UCS_OK) {
			fprintf(stderr, "failed to start client (%s)\n", ucs_status_string(status));
			return -1;
		}

		char key_plus_size[REQUEST_SIZE];
		uint32_t id = CLOSE_EP;
		//Send the created structure to the metadata server.
		sprintf(key_plus_size, "%" PRIu32 " SET %lu %s", id,  (sizeof(imss_info)+my_imss.num_storages*LINE_LENGTH), my_imss.uri_);

		if (send_stream(ucp_worker, client_ep, key_plus_size, REQUEST_SIZE) < 0) // SNDMORE
		{
			perror("ERRIMSS_SRV_SENDKEY");
			return -1;
		}

		DPRINT("[SERVER] Creating IMSS_INFO at metadata server. \n");
		//Send the new IMSS metadata structure to the metadata server entity.
		if (send_dynamic_stream(ucp_worker, client_ep, (char *) &my_imss, IMSS_INFO) == -1)
			return -1;

		ep_flush(client_ep, ucp_worker);

		for (int32_t i = 0; i < num_servers; i++)
			free(my_imss.ips[i]);
		free(my_imss.ips);

		usleep(250);
		ep_close(ucp_worker, client_ep, UCP_EP_CLOSE_MODE_FLUSH);
	}

	//Wait for threads to finish.
	for (int32_t i = 0; i < (THREAD_POOL+1); i++)
	{
		if (pthread_join(threads[i], NULL) != 0)
		{
			perror("ERRIMSS_SRVTH_JOIN");
			return -1;
		}
	}

	//Write the metadata structures retrieved by the metadata server threads.
	if (args.type == TYPE_METADATA_SERVER)
	{
		if (metadata_write(metadata_file, buffer, map.get(), arguments, buffer_segment, bytes_written) == -1)

			return -1;

		//free(imss_uri);

		//Freeing all resources of the tree structure.
		g_node_traverse(tree_root, G_PRE_ORDER, G_TRAVERSE_ALL, -1, gnodetraverse, NULL);

		if (pthread_mutex_destroy(&tree_mut) != 0)
		{
			perror("ERRIMSS_TREEMUT_DESTROY");
			pthread_exit(NULL);
		}
	}
	else
		free(region_locks);

	//Close publisher socket.
	//ep_close(ucp_worker, pub_ep, UCP_EP_CLOSE_MODE_FORCE);

	//Free the memory buffer.
	free(buffer);
	//Free the publisher release address.
	return 0;
}

