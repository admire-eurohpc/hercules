#include <mpi.h>
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

int32_t main(int32_t argc, char **argv)
{
	int32_t provide, world_size, rank;
	// Notify that threads will be deployed along the MPI process execution.
	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provide);
	// Obtain identifier inside the group.
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    // Obtain the current number of processes in the group.
	MPI_Comm_size(MPI_COMM_WORLD, &world_size);

	uint16_t bind_port, aux_bind_port;
	char *   stat_add;
	char *   metadata_file;
	char *   deployfile;
	int64_t  buffer_size, stat_port, num_servers;
	void * 	 socket;

	ucs_status_t status;
    ucp_ep_h     client_ep;

    ucp_am_handler_param_t param;
    int              ret;
	/***************************************************************/
	/******************** PARSE INPUT ARGUMENTS ********************/
	/***************************************************************/
	for(int i=0;i<argc;i++){
		printf("argv[%d]=%s\n",i,argv[i]);
	}
	printf("argc=%d\n",argc);
	
	//ARGV[2] = bind port number.
	bind_port	= (uint16_t) atoi(argv[2]);
	aux_bind_port	= bind_port;
	//ARGV[3] = buffer size provided.
	buffer_size	= atoi(argv[3]);

    // Default setup for imss uri
	imss_uri 	= (char *) calloc(32, sizeof(char));	
    strcpy(imss_uri, "imss://");

     /* Initialize the UCX required objects */
    ret = init_context(&ucp_context, &ucp_worker, CLIENT_SERVER_SEND_RECV_STREAM);
    if (ret != 0) {
        perror("ERRIMSS_INIT_CONTEXT");
        return -1;
    }

	/* CHECK THIS OUT!
	***************************************************
	In relation to the number of arguments provided, an IMSS or a metadata server will be deployed. */

	//IMSS server.
	if (argc == 9)
	{

		//ARGV[1] = IMSS name.
		imss_uri	= argv[1];
		//ARGV[6] = machine name where the metadata server is being executed.
		stat_add	= argv[4];
		//ARGV[7] = port that the metadata server is listening to.
		stat_port 	= atoi(argv[5]);
		//ARGV[8] = number of servers conforming the IMSS deployment.
		num_servers	= atoi(argv[6]);
		//ARGV[9] = IMSS' MPI deployment file.
		deployfile	= argv[7];

		int32_t imss_exists = 0;

		//Check if the provided URI has been already reserved by any other instance.
		if (!rank)
		{
			printf("stat_address=%s\n",stat_add);
			//Connect to the specified endpoint.

            status = start_client(ucp_worker, stat_add, stat_port + 1, &client_ep); // port, rank,
            if (status != UCS_OK) {
                fprintf(stderr, "failed to start client (%s)\n", ucs_status_string(status));
                return -1;
            }

            uint32_t id = CLOSE_EP;
            if (send_stream(ucp_worker, client_ep, (char*) &id, sizeof(uint32_t)) < 0)
            {
                perror("ERRIMSS_DATASET_REQ");
                return -1;
            }
          
		    char mode[] = "GET\0";
            if (send_stream(ucp_worker, client_ep, mode, MODE_SIZE) < 0)
            {
                perror("ERRIMSS_DATASET_REQ");
                return -1;
            }

			//Formated imss uri to be sent to the metadata server.
			char formated_uri[REQUEST_SIZE];
			sprintf(formated_uri, "0 %s%c", imss_uri, '\0');

			//Send the request.
			if (send_stream(ucp_worker, client_ep, formated_uri, REQUEST_SIZE) < 0)
			{
				perror("ERRIMSS_DATASET_REQ");
				return -1;
			}

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

		MPI_Bcast(&imss_exists, 1, MPI_INT, 0, MPI_COMM_WORLD);
		if (imss_exists)
		{
			if (!rank)
				fprintf(stderr, "ERRIMSS_IMSSURITAKEN");

			MPI_Barrier(MPI_COMM_WORLD);
			MPI_Finalize();	
			return 0;
		}
	}
	//Metadata server.
	else
	{
		//ARGV[1] = metadata file.
		metadata_file	= argv[1];

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
	ucx_server_ctx_t context;
    ucp_worker_h     ucp_data_worker;


    ret = init_worker(ucp_context, &ucp_data_worker);
    if (ret != 0) {
        perror("ERRIMSS_WORKER_INIT");
        pthread_exit(NULL);
    }

    /* Initialize the server's context. */
    context.conn_request = NULL;

    status = start_server(ucp_worker, &context, &context.listener, NULL, 0);
    //status = start_server(ucp_worker, &context, &context.listener, NULL, bind_port + 1000);
    if (status != UCS_OK) {
        perror("ERRIMSS_STAR_SERVER");
        pthread_exit(NULL);
    } 
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

	if (argc == 4)
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

	if (argc == 9)
		region_locks = (pthread_mutex_t *) calloc(THREAD_POOL, sizeof(pthread_mutex_t));
	
	//Execute all threads.
	for (int32_t i = 0; i < (THREAD_POOL+1); i++)
	{
		//Add port number to thread arguments.
		arguments[i].port = (bind_port)++;
		arguments[i].ucp_context = ucp_context;
		arguments[i].ucp_worker = ucp_worker;
        //Add the instance URI to the thread arguments.
        strcpy(arguments[i].my_uri, imss_uri);

		//Deploy all dispatcher + service threads.
		if (!i)
		{
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
			if (argc == 9)
			{	
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
	if ((argc == 9) && !rank && stat_port)
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
		//Send the created structure to the metadata server.
		sprintf(key_plus_size, "%lu %s", (sizeof(imss_info)+my_imss.num_storages*LINE_LENGTH), my_imss.uri_);

        uint32_t id = CLOSE_EP;
        send_stream(ucp_worker, client_ep, (char *) &id, sizeof(uint32_t));

        char mode[] = "SET\0";
        send_stream(ucp_worker, client_ep, mode, MODE_SIZE);

   
		if (send_stream(ucp_worker, client_ep, key_plus_size, REQUEST_SIZE) < 0) // SNDMORE
		{
			perror("ERRIMSS_SRV_SENDKEY");
			return -1;
		}

		//Send the new IMSS metadata structure to the metadata server entity.
		if (send_dynamic_stream(ucp_worker, client_ep, (char *) &my_imss, IMSS_INFO) == -1)
			return -1;

        //ep_close(ucp_worker, pub_ep, UCP_EP_CLOSE_MODE_FLUSH);

		for (int32_t i = 0; i < num_servers; i++)
			free(my_imss.ips[i]);
		free(my_imss.ips);

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
	if (argc == 4)
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
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();

	return 0;
}
