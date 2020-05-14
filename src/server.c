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
#include "records.hpp"


//ZeroMQ context entity conforming all sockets.
void * context;
//INPROC bind address for pub-sub communications.
char * pub_dir;
//URI of the created IMSS.
char * imss_uri;


int32_t main(int32_t argc, char **argv)
{
	int32_t provide, world_size, rank;
	//Notify that threads will be deployed along the MPI process execution.
	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provide);
	//Obtain identifier inside the group.
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        //Obtain the current number of processes in the group.
	MPI_Comm_size(MPI_COMM_WORLD, &world_size);

	uint16_t bind_port;
	char *   stat_add;
	char *   metadata_file;
	char *   release_add;
	char *   deployfile;
	int32_t  buffer_size, stat_port, release_port, num_servers;

	/***** PARSE INPUT ARGUMENTS *****/

	//ARGV[2] = bind port number.
	bind_port	= (uint16_t) atoi(argv[2]);
	//ARGV[3] = buffer size provided.
	buffer_size	= atoi(argv[3]);
	//ARGV[4] = release port.
	release_add	= argv[4];
	//ARGV[5] = release port.
	release_port	= atoi(argv[5]);


	/* CHECK THIS OUT!
	***************************************************
	In relation to the number of arguments provided, an IMSS or a metadata server  will be deployed. */


	//IMSS server.
	if (argc == 10)
	{
		//ARGV[1] = IMSS name.
		imss_uri	= argv[1];
		//ARGV[6] = machine name where the metadata server is being executed.
		stat_add	= argv[6];
		//ARGV[7] = port that the metadata server is listening to.
		stat_port 	= atoi(argv[7]);
		//ARGV[8] = number of servers conforming the IMSS deployment.
		num_servers	= atoi(argv[8]);
		//ARGV[9] = IMSS' MPI deployment file.
		deployfile	= argv[9];
	}
	//Metadata server.
	else
		//ARGV[1] = metadata file.
		metadata_file	= argv[1];


	//Publisher address where the release will be triggered.
	pub_dir = (char *) malloc(strlen(release_add)+16);
	sprintf(pub_dir, "tcp://%s:%d%c", release_add, release_port, '\0');

	//Map tracking saved records.
	map_records map;

	int64_t data_reserved;
	//Pointer to the allocated buffer memory.
	unsigned char * buffer;
	unsigned char * offset;
	//Size of the buffer involved.
	uint64_t size = (uint64_t) buffer_size*KB*KB*KB;
	//Check if the requested data is available in the current node.
	if ((data_reserved = memalloc(size, &buffer)) == -1)

		return -1;

	offset = buffer;

	//Metadata bytes written into the buffer.
	uint64_t bytes_written;

	if (argc == 6)
	{
		if ((offset = metadata_read(metadata_file, &map, buffer, &bytes_written)) == NULL)

			return -1;

		//Obtain the remaining free amount of data reserved to the buffer after the metadata read operation.
		data_reserved -= bytes_written;
	}

	//ZeroMQ context intialization.
	if (!(context = zmq_ctx_new()))
	{
		perror("ERRIMSS_CTX_CREATE");
		return -1;
	}

	//Buffer segment size assigned to each thread.
	int64_t buffer_segment = data_reserved/THREAD_POOL;

	//Initialize pool of threads.
	pthread_t threads[(THREAD_POOL+1)];
	//Thread arguments.
	p_argv arguments[(THREAD_POOL+1)];

	//Execute all threads.
	for (int32_t i = 0; i < (THREAD_POOL+1); i++)
	{
		//Add port number to thread arguments.
		arguments[i].port = (bind_port)++;

		//Deploy all dispatcher + service threads.
		if (!i)
		{
			//Deploy a thread distributing incomming clients among all ports.
			if (pthread_create(&threads[i], NULL, dispatcher, (void *) &arguments[i]) == -1)
			{
				//Notify thread error deployment.
				perror("ERRIMSS_SRVWORKER_DEPLOY");
				return -1;
			}
		}
		else
		{
			//Add the reference to the map into the set of thread arguments.
			arguments[i].map = &map;
			//Specify the address used by each thread to write inside the buffer.
			arguments[i].pt = (unsigned char *) ((i-1)*buffer_segment + offset);

			//Throw thread with the corresponding function and arguments.
			if (pthread_create(&threads[i], NULL, worker, (void *) &arguments[i]) == -1)
			{
				//Notify thread error deployment.
				perror("ERRIMSS_SRVDISPATCHER_DEPLOY");
				return -1;
			}
		}
	}

	//Notify to the metadata server the deployment of a new IMSS.
	if ((argc == 10) && !rank)
	{
		//Metadata structure containing the novel IMSS info.
		imss_info my_imss;

		strcpy(my_imss.uri_, imss_uri);
		my_imss.ips = (char **) malloc(num_servers*sizeof(char *));
		my_imss.num_storages 	= num_servers;
		my_imss.conn_port	= bind_port;

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
			(my_imss.ips)[i] = (char *) malloc(LINE_LENGTH);
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

		void * socket;
		//Create a ZMQ socket to send the created IMSS structure.
		if ((socket = zmq_socket(context, ZMQ_DEALER)) == NULL)
		{
			perror("ERRIMSS_SRV_SOCKETCREATE");
			return -1;
		}

		int32_t identity = -1;
		//Set communication id.
		if (zmq_setsockopt(socket, ZMQ_IDENTITY, &identity, sizeof(int32_t)) == -1)
		{
			perror("ERRIMSS_SRV_SETIDENT");
			return -1;
		}

		//Connection address.
		char stat_address[LINE_LENGTH]; 
		sprintf(stat_address, "%s%s%c%d%c", "tcp://", stat_add, ':', stat_port+1, '\0');
		//Connect to the specified endpoint.
		if (zmq_connect(socket, (const char *) stat_address) == -1)
		{
			perror("ERRIMSS_SRV_CONNECT");
			return -1;
		}

		char key_plus_size[KEY+16];
		//Send the created structure to the metadata server.
		sprintf(key_plus_size, "%lu %s", (sizeof(imss_info)+my_imss.num_storages*LINE_LENGTH), my_imss.uri_);

		if (zmq_send(socket, key_plus_size, KEY+16, ZMQ_SNDMORE) != (KEY+16))
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
	if (argc == 6)
	{
		if (metadata_write(metadata_file, buffer, &map, arguments, buffer_segment, bytes_written) == -1)

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
