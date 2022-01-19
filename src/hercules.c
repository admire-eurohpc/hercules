#include <zmq.h>
#include <stdio.h>
#include <utility>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/signal.h>
#include "stat.h"
#include "imss.h"
#include "workers.h"
#include "hercules.h"
#include "directory.h"
#include "records.hpp"



/***************************************************************************
*******************************  STRUCTURES  *******************************
***************************************************************************/


//Structure storing configuration info for the IMSS buffer servers.
typedef struct {

	//Port number that the server will be connecting to.
	int32_t port;

} server_arg;

//Structure storing configuration info for the IMSS metadata servers.
typedef struct {

	//Port number that the server will be connecting to.
	int32_t  port;
	//Memory amount that the process will be requesting to the corresponding node (in GB).
	uint64_t buffer_size;
	//Address of the matadata files' name.
	char 	 metadata_file[512];

} metadata_arg;



/***************************************************************************
****************************  GLOBAL VARIABLES  ****************************
***************************************************************************/


//IMSS server thread.
pthread_t server_th;
//IMSS metadata server thread.
pthread_t metadata_th;
//ZeroMQ context entity conforming all sockets.
extern void * context;
//PUB socket for ITC.
extern void * pub;
//IPC bind address for pub-sub communications.
extern char * pub_dir;

//Initial buffer address.
extern unsigned char *   buffer_address;
//Set of locks dealing with the memory buffer access.
extern pthread_mutex_t * region_locks;
//Segment size (amount of memory assigned to each thread).
extern uint64_t	  	 buffer_segment;

//Memory amount (in GB) assigned to the buffer process.
extern uint64_t 	buffer_KB;
//Flag stating that the previous parameter has been received.
extern int32_t 		size_received;

//Communication resources with worker threads.
pthread_mutex_t 	comms_mut;
pthread_cond_t 		comms_cond;
int32_t 		not_copied;
extern pthread_mutex_t 	buff_size_mut;
extern pthread_cond_t 	buff_size_cond;
extern int32_t 		copied;

//Pointer to the tree's root node.
extern GNode * 		tree_root;
extern pthread_mutex_t 	tree_mut;

//Number of bytes assigned to the IMSS backend storage (in KB).
uint64_t		backend_buffer_size;
pthread_mutex_t 	backend_buff_mut;

//URI of the attached deployment.
extern char att_imss_uri[URI_];

uint16_t		connection_port; //FIXME

//Metadata deployment flag.
uint32_t        metadata_server_deployed;

/***************************************************************************
*******************************  FUNCTIONS  ********************************
***************************************************************************/


//IMSS server process storing information.
void *
imss_server(void * arg_)
{
	server_arg arg;

	//Notify that the arguments have been copied.
	pthread_mutex_lock(&comms_mut);
	memcpy(&arg, arg_, sizeof(server_arg));
	not_copied = 0;
	pthread_cond_signal(&comms_cond);
	pthread_mutex_unlock(&comms_mut);

	copied = 0;

	//Initialize communication resources.
	if (pthread_mutex_init(&buff_size_mut, NULL) != 0)
	{
		perror("ERRIMSS_SRVMUT_INIT");
		pthread_exit(NULL);
	}

	if (pthread_cond_init(&buff_size_cond, NULL) != 0)
	{
		perror("ERRIMSS_SRVCOND_INIT");
		pthread_exit(NULL);
	}

	//Map tracking saved records.
	map_records buffer_map;

	//Amount of memory enabled for execution.
	uint64_t data_reserved;

	region_locks = (pthread_mutex_t *) malloc(THREAD_POOL * sizeof(pthread_mutex_t));

	//Initialize pool of threads.
	pthread_t threads[(THREAD_POOL+1)];
	//Thread arguments.
	p_argv arguments[(THREAD_POOL+1)];

	//Execute all threads.
	for (int32_t i = 0; i < (THREAD_POOL+1); i++)
	{
		//Add port number to thread arguments.
		arguments[i].port = (arg.port)++;

		//Deploy all dispatcher + service threads.
		if (!i)
		{
			//Deploy a thread distributing incomming clients among all ports.
			if (pthread_create(&threads[i], NULL, srv_attached_dispatcher, (void *) &arguments[i]) == -1)
			{
				perror("ERRIMSS_SRVDISPATCHER_DEPLOY");
				pthread_exit(NULL);
			}

			//Wait until the buffer size is provided.

			pthread_mutex_lock(&buff_size_mut);

			while (!copied)

				pthread_cond_wait(&buff_size_cond, &buff_size_mut);

			pthread_mutex_unlock(&buff_size_mut);

			pthread_mutex_lock(&backend_buff_mut);
			//Check if there is enough space to create a new IMSS server entity within the backend storage.
			if (buffer_KB > backend_buffer_size)
			{
				perror("ERRIMSS_BUFFTOOBIG");
				pthread_exit(NULL);
			}
			backend_buffer_size -= buffer_KB;
			pthread_mutex_unlock(&backend_buff_mut);

			data_reserved = buffer_KB * KB;

			buffer_address = (unsigned char *) malloc(sizeof(char)*data_reserved);

			buffer_segment = data_reserved/THREAD_POOL;
		}
		else
		{
			//Add the reference to the map into the set of thread arguments.
			arguments[i].map = &buffer_map;
			//Specify the address used by each thread to write inside the buffer.
			arguments[i].pt = (unsigned char *) ((i-1)*buffer_segment + buffer_address);
            //URI of the corresponding IMSS instance.
            strcpy(arguments[i].my_uri, att_imss_uri);

			//Throw thread with the corresponding function and arguments.
			if (pthread_create(&threads[i], NULL, srv_worker, (void *) &arguments[i]) == -1)
			{
				perror("ERRIMSS_SRVWORKER_DEPLOY");
				pthread_exit(NULL);
			}
		}
	}

	//Release communication resources.
	if (pthread_mutex_destroy(&buff_size_mut) != 0)
	{
		perror("ERRIMSS_SRVMUT_DESTROY");
		pthread_exit(NULL);
	}

	if (pthread_cond_destroy(&buff_size_cond) != 0)
	{
		perror("ERRIMSS_SRVCOND_DESTROY");
		pthread_exit(NULL);
	}

	//Wait for threads to finish.
	for (int32_t i = 0; i < (THREAD_POOL+1); i++)
	{
		if (pthread_join(threads[i], NULL) != 0)
		{
			perror("ERRIMSS_SRVTH_JOIN");
			pthread_exit(NULL);
		}
	}

	free(region_locks);
	//Free the memory buffer.
	free(buffer_address);

	pthread_exit(NULL);
}

//IMSS metadata server storing structure information.
void *
imss_metadata(void * arg_)
{
	metadata_arg arg;

	//Notify that the arguments have been copied.
	pthread_mutex_lock(&comms_mut);
	memcpy(&arg, arg_, sizeof(metadata_arg));
	not_copied = 0;
	pthread_cond_signal(&comms_cond);
	pthread_mutex_unlock(&comms_mut);

	//Map tracking metadata saved records.
	map_records metadata_map;
	//Pointer to the allocated metadata buffer memory.
	unsigned char * pt_met;

	pthread_mutex_lock(&backend_buff_mut);
	if (backend_buffer_size < arg.buffer_size)
	{
		perror("ERRIMSS_BUFFTOOBIG");
		pthread_exit(NULL);
	}
	backend_buffer_size -= arg.buffer_size;
	pthread_mutex_unlock(&backend_buff_mut);

	int64_t data_reserved = arg.buffer_size * KB;

	pt_met = (unsigned char *) malloc(sizeof(char) * data_reserved);

	if (pthread_mutex_init(&tree_mut, NULL) != 0)
	{
		perror("ERRIMSS_TREEMUT_INIT");
		pthread_exit(NULL);
	}

	//Create the tree_root node.
	char * root_data = (char *) malloc(8);
	strcpy(root_data,"imss://");
	tree_root = g_node_new((void *) root_data);

	//Address pointing to the end of the last metadata record.
	unsigned char * offset = pt_met;

	//Metadata bytes written into the buffer.
	uint64_t bytes_written;
	
	if ((offset = metadata_read(arg.metadata_file, &metadata_map, pt_met, &bytes_written)) == NULL)
		pthread_exit(NULL);

	//Obtain the remaining free amount of data reserved to the buffer after the metadata read operation.
	data_reserved -= bytes_written;

	//Buffer segment size assigned to each thread.
	int64_t buffer_segment_ = data_reserved/THREAD_POOL;

	//Initialize pool of threads.
	pthread_t threads[(THREAD_POOL+1)];
	//Thread arguments.
	p_argv arguments[(THREAD_POOL+1)];

	//Execute all threads.
	for (int32_t i = 0; i < (THREAD_POOL+1); i++)
	{
		//Add port number to set of thread arguments.
		arguments[i].port = (arg.port)++;

		//Deploy all dispatcher + service threads.
		if (!i)
		{
			//Deploy a thread distributing incomming clients among all ports.
			if (pthread_create(&threads[i], NULL, dispatcher, (void *) &arguments[i]) == -1)
			{
				perror("ERRIMSS_METADISPATCHER_DEPLOY");
				pthread_exit(NULL);
			}
		}
		else
		{
			//Add the reference to the map into the set of thread arguments.
			arguments[i].map = &metadata_map;
			//Specify the address used by each thread to write inside the buffer.
			arguments[i].pt = (unsigned char *) ((i-1)*(buffer_segment_) + offset);

			//Throw thread with the corresponding function and arguments.
			if (pthread_create(&threads[i], NULL, stat_worker, (void *) &arguments[i]) == -1)
			{
				perror("ERRIMSS_METAWORKER_DEPLOY");
				pthread_exit(NULL);
			}
		}
	}

	//Wait for the threads to conclude.
	for (int32_t i = 0; i < (THREAD_POOL+1); i++)
	{
		if (pthread_join(threads[i], NULL) != 0)
		{
			perror("ERRIMSS_METATH_JOIN");
			pthread_exit(NULL);
		}
	}

	//Save the current metadata information into a file.
	if (metadata_write(arg.metadata_file, pt_met, &metadata_map, arguments, buffer_segment_, bytes_written) == -1)
		pthread_exit(NULL);

	//Freeing all resources of the tree structure.
	g_node_traverse(tree_root, G_PRE_ORDER, G_TRAVERSE_ALL, -1, gnodetraverse, NULL);

	if (pthread_mutex_destroy(&tree_mut) != 0)
	{
		perror("ERRIMSS_TREEMUT_DESTROY");
		pthread_exit(NULL);
	}

	free(pt_met);

	pthread_exit(NULL);
}


//Method initializing an instance of the HERCULES in-memory storage system.
int32_t 
hercules_init(uint32_t rank,
              uint64_t backend_strg_size,
              uint16_t server_port_num,
              int32_t  deploy_metadata,
              uint16_t metadata_port_num,
              uint64_t metadata_buff_size,
              char *   metadata_file)
{
	//Save the total number of bytes that were assigned to the backend storage.
	backend_buffer_size = backend_strg_size;

	connection_port = server_port_num; //FIXME

	//ZeroMQ context intialization.
	if ((context = zmq_ctx_new()) == NULL)
	{
		perror("ERRHERCULES_CTX_CREATE");
		return -1;
	}

	//Publisher socket creation.
	if ((pub = zmq_socket(context, ZMQ_PUB)) == NULL)
	{
		perror("ERRHERCULES_PUBSOCK_CREATE");
		return -1;
	}

	pub_dir = (char *) malloc(32*sizeof(char));
	sprintf(pub_dir, "inproc://hercules_comms-%d", rank);

	if (zmq_bind(pub, pub_dir) == -1)
	{
		perror("ERRHERCULES_PUBSOCK_BIND");
		return -1;
	}

	//Initialize communication resources.
	if (pthread_mutex_init(&comms_mut, NULL) != 0)
	{
		perror("ERRHERCULES_MUT_INIT");
		return -1;
	}

	if (pthread_cond_init(&comms_cond, NULL) != 0)
	{
		perror("ERRHERCULES_COND_INIT");
		return -1;
	}

	if (pthread_mutex_init(&backend_buff_mut, NULL) != 0)
	{
		perror("ERRHERCULES_BACKENDMUT_INIT");
		return -1;
	}

	not_copied = 1;

	//Set of arguments provided to the server.
	server_arg srv_arg;
	srv_arg.port 		= server_port_num;

	//Deploy the previous thread.
	if (pthread_create(&server_th, NULL, imss_server, (void *) &srv_arg) != 0)
	{
		perror("ERRHERCULES_SRVTH_CREATE");
		return -1;
	}

	//Wait until the thread copies the provided arguments.

	pthread_mutex_lock(&comms_mut);

	while (not_copied)

		pthread_cond_wait(&comms_cond, &comms_mut);

	pthread_mutex_unlock(&comms_mut);

	not_copied = 1;

    //No metadata server is deployed by default.
    metadata_server_deployed = 0;

	//The rank zero process will be also deploying the metadata server.
	if (deploy_metadata)
	{
        //A metadata server has been deployed in the current process.
        metadata_server_deployed = 1;

		//Set of arguments provided to the metadata server.
		metadata_arg metadata_arg;
		metadata_arg.port 		= metadata_port_num;
		metadata_arg.buffer_size	= metadata_buff_size;
		strcpy(metadata_arg.metadata_file, metadata_file);

		//Deploy the previous thread.
		if (pthread_create(&metadata_th, NULL, imss_metadata, (void *) &metadata_arg) != 0)
		{
			perror("HERCULES_METATH_CREATE");
			return -1;
		}

		//Wait until the thread copies the provided arguments.

		pthread_mutex_lock(&comms_mut);

		while (not_copied)

			pthread_cond_wait(&comms_cond, &comms_mut);

		pthread_mutex_unlock(&comms_mut);
	}

	//Release communication resources.
	if (pthread_mutex_destroy(&comms_mut) != 0)
	{
		perror("ERRHERCULES_MUT_DESTROY");
		return -1;
	}

	if (pthread_cond_destroy(&comms_cond) != 0)
	{
		perror("ERRHERCULES_COND_DESTROY");
		return -1;
	}

	return 0;
}

//Method releasing an instance of the HERCULES in-memory storage system.
int32_t
hercules_release()
{
	//Publish RELEASE message to all worker threads.
	zmq_msg_t release_rsc;
	zmq_msg_init_size(&release_rsc, 8);
	memcpy(zmq_msg_data(&release_rsc), "RELEASE\0", 8);

	if (zmq_msg_send(&release_rsc, pub, 0) == -1)
	{
		perror("ERRHERCULES_PUBLISH_RELEASEMSG");
		return -1;
	}

	//Join threads.
	if (metadata_server_deployed)
	{
		if (pthread_join(metadata_th, NULL) != 0)
		{
			perror("ERRHERCULES_SRVTH_JOIN");
			return -1;
		}
	}

	if (pthread_join(server_th, NULL) != 0)
	{
		perror("ERRHERCULES_METATH_JOIN");
		return -1;
	}

	zmq_msg_close(&release_rsc);

	//Close publisher socket.
	if (zmq_close(pub) == -1)
	{
		perror("ERRHERCULES_PUBSOCK_CLOSE");
		return -1;
	}

	free(pub_dir);

	if (pthread_mutex_destroy(&backend_buff_mut) != 0)
	{
		perror("ERRHERCULES_BACKENDMUT_DESTROY");
		return -1;
	}

	//Close context holding all sockets.
	if (zmq_ctx_destroy(context) == -1)
	{
		perror("ERRHERCULES_CTX_DESTROY");
		return -1;
	}

	return 0;
}
