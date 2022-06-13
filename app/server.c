#include <mpi.h>
#include <zmq.h>
#include <stdio.h>
#include <utility>
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


//ZeroMQ context entity conforming all sockets.
extern void * 		context;
//INPROC bind address for pub-sub communications.
extern char * 		pub_dir;
//Publisher socket.
extern void * 		pub;
//Pointer to the tree's root node.
extern GNode * 		tree_root;
extern pthread_mutex_t 	tree_mut;
//URI of the created IMSS.
char * 			imss_uri;

//Initial buffer address.
extern unsigned char *   buffer_address;
//Set of locks dealing with the memory buffer access.
extern pthread_mutex_t * region_locks;
//Segment size (amount of memory assigned to each thread).
extern uint64_t 	 buffer_segment;


int32_t main(int32_t argc, char **argv)
{
	int32_t provide, world_size, rank;
	//Notify that threads will be deployed along the MPI process execution.
	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provide);
	//Obtain identifier inside the group.
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        //Obtain the current number of processes in the group.
	MPI_Comm_size(MPI_COMM_WORLD, &world_size);

	uint16_t bind_port, aux_bind_port;
	char *   stat_add;
	char *   metadata_file;
	char *   deployfile;
	int64_t  buffer_size, stat_port, num_servers;
	void * 	 socket;

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


	//ZeroMQ context intialization.
	if (!(context = zmq_ctx_new()))
	{
		perror("ERRIMSS_CTX_CREATE");
		return -1;
	}


	/* CHECK THIS OUT!
	***************************************************
	In relation to the number of arguments provided, an IMSS or a metadata server will be deployed. */

	//IMSS server.
	if (argc == 9)
	{
		if (comm_ctx_set(context, ZMQ_IO_THREADS, atoi(argv[8])))
		{
			perror("ERRIMSS_CTX_SETIOTHRS");
			return -1;
		}

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

		int32_t imss_exists;

		//Check if the provided URI has been already reserved by any other instance.
		if (!rank)
		{
			//Create a ZMQ socket to send the created IMSS structure.
			if ((socket = zmq_socket(context, ZMQ_DEALER)) == NULL)
			{
				perror("ERRIMSS_SRV_SOCKETCREATE");
				return -1;
			}

			int32_t identity = -1;
			//Set communication id.
			if (comm_setsockopt(socket, ZMQ_IDENTITY, &identity, sizeof(int32_t)) == -1)
			{
				perror("ERRIMSS_SRV_SETIDENT");
				return -1;
			}

			//Connection address.
			char stat_address[LINE_LENGTH];
			sprintf(stat_address, "%s%s%c%ld%c", "tcp://", stat_add, ':', stat_port+1, '\0');
			//sprintf(stat_address, "%s%s%c%ld%c", "inproc://", stat_add, ':', stat_port+1, '\0');
			printf("stat_address=%s\n",stat_address);
			//Connect to the specified endpoint.
			if (comm_connect(socket, (const char *) stat_address) == -1)
			{
				perror("ERRIMSS_SRV_CONNECT");
				return -1;
			}

			//Formated imss uri to be sent to the metadata server.
			char formated_uri[strlen(imss_uri)+2];
			sprintf(formated_uri, "0 %s%c", imss_uri, '\0');
			size_t formated_uri_length = strlen(formated_uri);

			//Send the request.
			if (comm_send(socket, formated_uri, formated_uri_length, 0)  != formated_uri_length)
			{
				perror("ERRIMSS_DATASET_REQ");
				return -1;
			}

			imss_info imss_info_;

			//Receive the associated structure.
			imss_exists = recv_dynamic_struct(socket, &imss_info_, IMSS_INFO);

			if (imss_exists)
			{
				for (int32_t i = 0; i < imss_info_.num_storages; i++)

					free(imss_info_.ips[i]);

				free(imss_info_.ips);
			}
		}

		MPI_Bcast(&imss_exists, 1, MPI_INT, 0, MPI_COMM_WORLD);
		if (imss_exists)
		{
			if (!rank)
			{
				if (zmq_close(socket) == -1)
				{
					perror("ERRIMSS_SRV_SOCKETCLOSE");
					return -1;
				}
			}

			if (zmq_ctx_destroy(context) == -1)
			{
				perror("ERRIMSS_CTX_DSTRY");
				return -1;
			}

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

	//Publisher address where the release will be triggered.
	pub_dir = (char *) calloc(33, sizeof(char));
	sprintf(pub_dir, "inproc://inproc-imss-comms-%d", bind_port);
	//Publisher socket creation.
	if ((pub = zmq_socket(context, ZMQ_PUB)) == NULL)
	{
		perror("ERRIMSS_PUBSOCK_CREATE");
		return -1;
	}
	//Bind the previous pub socket for inprocess communications.
	if (comm_bind(pub, pub_dir) == -1)
	{
		perror("ERRIMSS_PUBSOCK_BIND");
		return -1;
	}

	//Map tracking saved records.

	map_records map(buffer_size*KB);
	int64_t data_reserved;
	//Pointer to the allocated buffer memory.
	unsigned char * buffer;
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
		if ((buffer_address = metadata_read(metadata_file, &map, buffer, &bytes_written)) == NULL)

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
			arguments[i].map = &map;
			//Specify the address used by each thread to write inside the buffer.
			arguments[i].pt = (unsigned char *) ((i-1)*buffer_segment + buffer_address);

			//IMSS server.
			if (argc == 9)
			{	
				printf("Cree un srv_worker server\n");
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
				printf("Cree un stat_worker server\n");
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

		char key_plus_size[KEY+16];
		//Send the created structure to the metadata server.
		sprintf(key_plus_size, "%lu %s", (sizeof(imss_info)+my_imss.num_storages*LINE_LENGTH), my_imss.uri_);

		if (comm_send(socket, key_plus_size, KEY+16, ZMQ_SNDMORE) != (KEY+16))
		{
			perror("ERRIMSS_SRV_SENDKEY");
			return -1;
		}

		//Send the new IMSS metadata structure to the metadata server entity.
		if (send_dynamic_struct(socket, (void *) &my_imss, IMSS_INFO) == -1)

			return -1;

		//Close the provided socket.
		if (zmq_close(socket) == -1)
		{
			perror("ERRIMSS_SRV_SOCKETCLOSE");
			return -1;
		}

		for (int32_t i = 0; i < num_servers; i++)

			free(my_imss.ips[i]);

		free(my_imss.ips);
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
		if (metadata_write(metadata_file, buffer, &map, arguments, buffer_segment, bytes_written) == -1)

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
	if (zmq_close(pub) == -1)
	{
		perror("ERRIMSS_PUBSOCK_CLOSE");
		return -1;
	}

	//Close context holding all sockets.
	if (zmq_ctx_destroy(context) == -1)
	{
		perror("ERRIMSS_CTX_DSTRY");
		return -1;
	}

	//Free the memory buffer.
	free(buffer);
	//Free the publisher release address.
	free(pub_dir);

	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();

	return 0;
}
