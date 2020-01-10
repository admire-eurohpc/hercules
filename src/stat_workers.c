#include <zmq.h>
#include <string.h>
#include <unistd.h>
#include "p_argv.h"
#include "assert.h"
#include "records.hpp"
#include "imss_data.h"
#include "th_workers.h"


//Variable specifying the ID that will be granted to the following client.
uint32_t client_id_;

//Thread method attending a client.
void * worker (void * th_argv)
{   
	//Cast from generic pointer type to p_argv struct type pointer.
	p_argv * arguments = (p_argv *) th_argv;
	//Obtain the current map class element from the set of arguments.
	map_records * map = arguments->map;

	//Initialize strings to be used by zmq_bind functions.
	char str1[] = "tcp://*:";
	//Turn port number (int) into a string.
	char buf[16]; sprintf(buf, "%d", arguments->port);
	//Append corresponding port number to each string.
	strcat(str1, buf); 

	//TODO consider adding a timeout to the socket.
	//ZeroMQ context intialization.
	void * context = zmq_ctx_new();
	//ZeroMQ socket creation (RECEIVER).
	void * server = zmq_socket (context, ZMQ_ROUTER);
	//Connection to a remote address.
	int32_t rc = zmq_bind(server, (const char *) (str1));
	assert(rc == 0);

	uint32_t hwm = 0;
	//Unset limit in the number of messages to be received in the socket.
	rc = zmq_setsockopt(server, ZMQ_RCVHWM, &hwm, sizeof(int32_t));	assert(rc == 0);
	//Unset limit in the number of messages to be sent from the socket.
	rc = zmq_setsockopt(server, ZMQ_SNDHWM, &hwm, sizeof(int32_t));	assert(rc == 0);

	//ZMQ message type.
	zmq_msg_t client_id, client_req, block;
	zmq_msg_init (&client_id);
	zmq_msg_init (&client_req);
	zmq_msg_init (&block);

	//Resources specifying if more messages will be received in the first attached.
	int64_t more; size_t more_size = sizeof(more);

	//Code to be sent if the requested to-be-read key does not exist.
	char err_code[] = "$ERRIMSS_NO_KEY_AVAIL$";

	for (;;)
	{
		//Save the identity of the requesting client.
		zmq_msg_recv(&client_id, server, 0);
		//Save the request to be served.
		zmq_msg_recv(&client_req, server, 0);

		//printf("CLIENT: %d\tREQUEST: %s\n", *((int *) zmq_msg_data(&client_id)), (char *) zmq_msg_data(&client_req));

		//Check the requests' type in relation to the number of remaining incomming messages.
		if ((zmq_getsockopt(server, ZMQ_RCVMORE, &more, &more_size)) == -1)
		{
			perror("ERRIMSS_MTDT_GETSOCKOPT");
			pthread_exit(NULL);
		}

		//Expeted format of the incomming message: "SIZE_IN_KB$KEY"

		//Compound request sent by the client (reason why requests must be sent with an '\0' at the end).
		char * request_ = (char *) zmq_msg_data(&client_req);

		//Elements conforming the request.
		uint64_t block_size_recv;
		char * uri_;

		//Obtain the size of the incomming record.
		sscanf(request_, "%lu", &block_size_recv);

		//Obtain the associated key of the incomming record.
		for (uint32_t i = 0; i < strlen(request_); i++)
		{
			if (request_[i] == '$')
			{
			    uri_ = request_ + i + 1;
			    break;
			}
		}

		//Create an std::string in order to be managed by the map structure.
		std::string key; key.assign((const char *) uri_);

		//Information associated to the arriving key.
		unsigned char * address_;
		uint64_t block_size_rtvd;

		printf("%s %lu\n", uri_, block_size_recv);

		//Differentiate between READ and WRITE operations. 
		switch (more)
		{
			//No more messages will arrive to the socket conforming the request.
			case READ_OP:
			{
				//Specify client to be answered.
				if (zmq_send(server, (int *) zmq_msg_data(&client_id), sizeof(int), ZMQ_SNDMORE) < 0)
				{
					perror("ERRIMSS_MTDT_READ_SNDID");
					pthread_exit(NULL);
				}

				//Check if there was an associated block to the key.
				if (!(map->get(key, &address_, &block_size_rtvd)))
				{
					//Send the error code block.
					if (zmq_send(server, err_code, strlen(err_code), 0) < 0)
					{
						perror("ERRIMSS_MTDT_READ_SNDERRCODE");
						pthread_exit(NULL);
					}
				}
				else
				{
					//Send the requested block.
					if (zmq_send(server, address_, block_size_rtvd, 0) < 0)
					{
						perror("ERRIMSS_MTDT_READ_SNDRESP");
						pthread_exit(NULL);
					}
				}

				break;
			}
			//More messages will arrive to the socket conforming the request.
			case WRITE_OP:
			{
				//If the record was not already stored, add the block.
				if (!map->get(key, &address_, &block_size_rtvd))
				{
					//Receive the block into the buffer.
					zmq_recv(server, arguments->pt, block_size_recv, 0);

					//Include the new record in the tracking structure.
					if (map->put(key, arguments->pt, block_size_recv) != 0)
					{
						perror("ERRIMSS_MTDT_WRITE_MAPPUT");
						pthread_exit(NULL);
					}

					//Update the pointer.
					arguments->pt += block_size_recv;
				}
				//If was already stored, update the block.
				else
				{
					//TODO: the following policy could be modified.

					//Clear the corresponding memory region.
					memset(address_, '\0', sizeof(block_size_rtvd));

					//Receive the block into the buffer.
					zmq_recv(server, address_, block_size_rtvd, 0);
				}

				break;
			}

			default:

				break;
		}

		//TODO: CLEAN COMMON RESOURCES BETWEEN ITERATIONS!
	}   
	
	pthread_exit(NULL);
}


//Dispatcher thread method.
void * dispatcher(void * th_argv)
{
	//Cast from generic pointer type to p_argv struct type pointer.
	p_argv * arguments = (p_argv *) th_argv;

	//Initialize strings to be used by zmq_bind functions.
	char str1[] = "tcp://*:";
	//Turn port number (int) into a string.
	char buf[16]; sprintf(buf, "%d", arguments->port);
	//Append corresponding port number to each string.
	strcat(str1, buf); 

	//TODO consider adding a timeout to the socket.
	//ZeroMQ context intialization.
	void * context = zmq_ctx_new();
	//ZeroMQ socket creation (RECEIVER).
	void * server = zmq_socket (context, ZMQ_ROUTER);
	//Connection to a remote address.
	int32_t rc = zmq_bind(server, (const char *) (str1));
	assert(rc == 0);

	uint32_t hwm = 0;
	//Unset limit in the number of messages to be received in the socket.
	rc = zmq_setsockopt(server, ZMQ_RCVHWM, &hwm, sizeof(int32_t)); assert(rc == 0);
	//Unset limit in the number of messages to be sent from the socket.
	rc = zmq_setsockopt(server, ZMQ_SNDHWM, &hwm, sizeof(int32_t)); assert(rc == 0);

	client_id_ = 0;

	//ZMQ message type.
	zmq_msg_t client_id, client_req, response;
	zmq_msg_init (&client_id);
	zmq_msg_init (&client_req);
	zmq_msg_init (&response);

	for (;;)
	{
		//Save the identity of the requesting client.
		zmq_msg_recv(&client_id, server, 0);
		//Save the request to be served.
		zmq_msg_recv(&client_req, server, 0);

		//Message containing the client's communication ID plus its connection port.
		char response_[8]; memset(response_, '\0', 8);
		//Port that the new client will be forwarded to.
		int32_t port_ = arguments->port + 1 + (client_id_ % THREAD_POOL);
		//Wrap the previous info into the ZMQ message.
		sprintf(response_, "%d%c%d", port_, '-', client_id_++);

		int32_t c_id = *((int32_t *) zmq_msg_data(&client_id));
		//Specify client to answered to.
		if (zmq_send(server, &c_id, sizeof(int32_t), ZMQ_SNDMORE) < 0)
		{
			perror("ERRIMSS_MTDT_DISP_SNDID");
			pthread_exit(NULL);
		}

		//Send all the communication specifications.
		if (zmq_send(server, response_, strlen(response_), 0) < 0)
		{
			perror("ERRIMSS_MTDT_DISP_SNDRESP");
			pthread_exit(NULL);
		}

		printf("%s\n", response_);
	}
	
	pthread_exit(NULL);
}

