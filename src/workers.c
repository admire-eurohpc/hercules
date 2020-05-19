#include <zmq.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <signal.h>
#include "imss.h"
#include "comms.h"
#include "workers.h"
#include "records.hpp"


//ZeroMQ context entity conforming all sockets.
extern void * context;
//INPROC bind address for pub-sub communications.
extern char * pub_dir;
//URI of the created IMSS.
extern char * imss_uri;


//Method creating a ROUTER socket and a PUBLISHER one.
int32_t
server_conn(void ** router,
	    char *  router_endpoint,
	    void ** subscriber)
{
	//Router socket creation.
	if ((*router = zmq_socket (context, ZMQ_ROUTER)) == NULL)
	{
		perror("ERRIMSS_THREAD_CRTROUTER");
		return -1;
	}
	//Set a receive timeout of 5 ms so as to perform a check iteration once in a while.
	int32_t rcvtimeo = 5;
	if (zmq_setsockopt(*router, ZMQ_RCVTIMEO, &rcvtimeo, sizeof(int32_t)) == -1)
	{
		perror("ERRIMSS_THREAD_RCVTIMEO");
		return -1;
	}
	//Connect the router socket to a certain endpoint.
	if (zmq_bind(*router, (const char *) router_endpoint) == -1)
	{
		perror("ERRIMSS_THREAD_ROUTERBIND");
		return -1;
	}
	uint32_t hwm = 0;
	//Unset the limit in the number of messages to be queued in the socket receive queue.
	if (zmq_setsockopt(*router, ZMQ_RCVHWM, &hwm, sizeof(int32_t)) == -1)
	{
		perror("ERRIMSS_THREAD_SETRCVHWM");
		return -1;
	}
	//Unset the limit in the number of messages to be queued in the socket send queue.
	if (zmq_setsockopt(*router, ZMQ_SNDHWM, &hwm, sizeof(int32_t)) == -1)
	{
		perror("ERRIMSS_THREAD_SETSNDHWM");
		return -1;
	}

	//Subscriber socket creation.
	if (!(*subscriber = zmq_socket (context, ZMQ_SUB)))
	{
		perror("ERRIMSS_THREAD_CRTSUB");
		return -1;
	}

	//Connect to the INPROC endpoint.
	if (zmq_connect(*subscriber, (const char *) pub_dir) == -1)
	{
		perror("ERRIMSS_THREAD_CONNECTSUB");
		return -1;
	}

	//Subscribe to publications associated with my IMSS uri.
	if (zmq_setsockopt (*subscriber, ZMQ_SUBSCRIBE, imss_uri, strlen(imss_uri)) == -1)
	{
		perror("ERRIMSS_THREAD_SUBSCRIBE");
		return -1;
	}

	return 0;
}


//Thread method attending client read-write requests.
void *
worker (void * th_argv)
{   
	//Cast from generic pointer type to p_argv struct type pointer.
	p_argv * arguments = (p_argv *) th_argv;
	//Obtain the current map class element from the set of arguments.
	map_records * map = arguments->map;

	//Format socket endpoint.
	char endpoint[24];
	sprintf(endpoint, "%s%d", "tcp://*:", arguments->port);

	//Communication sockets.
	void * socket;
	void * sub;

	//Create communication channels.
	if (server_conn(&socket, endpoint, &sub) == -1)

		pthread_exit(NULL);

	//ZeroMQ messages handling requests.
	zmq_msg_t client_id, client_req, release;

	//Resources specifying if the ZMQ_SNDMORE flag was set in the sender.
	int64_t more;
	size_t more_size = sizeof(more);

	//Code to be sent if the requested to-be-read key does not exist.
	char err_code[] = "$ERRIMSS_NO_KEY_AVAIL$";

	for (;;)
	{
		//Initialize ZeroMQ messages.
		zmq_msg_init (&client_id);
		zmq_msg_init (&client_req);

		//Save the identity of the requesting client.
		zmq_msg_recv(&client_id, socket, 0);

		//Check if a timeout was triggered in the previous receive operation.
		if ((errno == EAGAIN) && !zmq_msg_size(&client_id))
		{
			zmq_msg_init (&release);

			//If something was published, close sockets and exit.
			if (zmq_msg_recv(&release, sub, ZMQ_DONTWAIT) > 0)
			{
				if (zmq_close(socket) == -1)
				{
					perror("ERRIMSS_WORKER_ROUTERCLOSE");
					pthread_exit(NULL);
				}

				if (zmq_close(sub) == -1)
				{
					perror("ERRIMSS_WORKER_SUBCLOSE");
					pthread_exit(NULL);
				}

				pthread_exit(NULL);
			}

			//Overwrite errno value.
			errno = 1;

			continue;
		}

		//Save the request to be served.
		zmq_msg_recv(&client_req, socket, 0);

		//Determine if more messages are comming.
		if ((zmq_getsockopt(socket, ZMQ_RCVMORE, &more, &more_size)) == -1)
		{
			perror("ERRIMSS_WORKER_GETSOCKOPT");
			pthread_exit(NULL);
		}

		//Expeted incomming message format: "SIZE_IN_KB KEY"

		//Reference to the client request.
		char * req = (char *) zmq_msg_data(&client_req);

		//Elements conforming the request.
		uint64_t block_size_recv;
		char * uri_ = (char *) malloc(strlen(req)*sizeof(char));

		//Obtain the size of the incomming record and its associated key.
		sscanf(req, "%lu %s", &block_size_recv, uri_);

		//Create an std::string in order to be managed by the map structure.
		std::string key; key.assign((const char *) uri_);

		//Information associated to the arriving key.
		unsigned char * address_;
		uint64_t block_size_rtvd;

		//Differentiate between READ and WRITE operations. 
		switch (more)
		{
			//No more messages will arrive to the socket.
			case READ_OP:
			{
				//Specify client to be answered.
				if (zmq_send(socket, (int *) zmq_msg_data(&client_id), sizeof(int), ZMQ_SNDMORE) < 0)
				{
					perror("ERRIMSS_WORKER_SENDCLIENTID");
					pthread_exit(NULL);
				}

				//Check if there was an associated block to the key.
				if (!(map->get(key, &address_, &block_size_rtvd)))
				{
					//Send the error code block.
					if (zmq_send(socket, err_code, strlen(err_code), 0) < 0)
					{
						perror("ERRIMSS_WORKER_SENDERR");
						pthread_exit(NULL);
					}
				}
				else
				{
					//Send the requested block.
					if (zmq_send(socket, address_, block_size_rtvd, 0) < 0)
					{
						perror("ERRIMSS_WORKER_SENDBLOCK");
						pthread_exit(NULL);
					}
				}

				break;
			}
			//More messages will arrive to the socket.
			case WRITE_OP:
			{
				//If the record was not already stored, add the block.
				if (!map->get(key, &address_, &block_size_rtvd))
				{
					//Receive the block into the buffer.
					zmq_recv(socket, arguments->pt, block_size_recv, 0);

					//Include the new record in the tracking structure.
					if (map->put(key, arguments->pt, block_size_recv) != 0)
					{
						perror("ERRIMSS_WORKER_MAPPUT");
						pthread_exit(NULL);
					}

					//Update the pointer.
					arguments->pt += block_size_recv;
				}
				//If was already stored:
				else
				{
					//Follow a certain behavior if the received block was already stored.
					switch (block_size_recv)
					{
						//Update where the blocks of a LOCAL dataset have been stored.
						case LOCAL_DATASET_UPDATE:
						{
							zmq_msg_t data_locations_msg;

							if (zmq_msg_init(&data_locations_msg) != 0)
							{
								perror("ERRIMSS_WORKER_DATALOCATINIT");
								pthread_exit(NULL);
							}

							if (zmq_msg_recv(&data_locations_msg, socket, 0) == -1)
							{
								perror("ERRIMSS_WORKER_DATALOCATRECV");
								pthread_exit(NULL);
							}

							char * data_ref   = (char *) zmq_msg_data(&data_locations_msg);
							uint32_t data_size = zmq_msg_size(&data_locations_msg);

							//Value to be written in certain positions of the vector.
							uint16_t * update_value = (uint16_t *) (data_size + data_ref - 8);
							//Positions to be updated.
							uint32_t * update_positions = (uint32_t *) data_ref;

							//Set of positions that are going to be updated (those are just under the concerned dataset but not pointed by it).
							uint16_t * data_locations = (uint16_t *) (address_ + sizeof(dataset_info));

							//Number of positions to be updated.
							int32_t num_pos_toupdate = (data_size/sizeof(uint32_t)) - 2;

							//Perform the update operation.
							for (int32_t i = 0; i < num_pos_toupdate; i++)

								data_locations[update_positions[i]] = *update_value;

							if (zmq_msg_close(&data_locations_msg) == -1)
							{
								perror("ERRIMSS_WORKER_DATALOCATCLOSE");
								pthread_exit(NULL);
							}

							//Specify client to be answered.
							if (zmq_send(socket, (int *) zmq_msg_data(&client_id), sizeof(int), ZMQ_SNDMORE) < 0)
							{
								perror("ERRIMSS_WORKER_DATALOCATANSWER1");
								pthread_exit(NULL);
							}

							//Answer the client with the update.
							char answer[9] = "UPDATED!";
							if (zmq_send(socket, answer, 9, 0) < 0)
							{
								perror("ERRIMSS_WORKER_DATALOCATANSWER2");
								pthread_exit(NULL);
							}

							break;
						}

						default:
						{
							//Clear the corresponding memory region.
							memset(address_, '\0', block_size_rtvd);

							//Receive the block into the buffer.
							zmq_recv(socket, address_, block_size_rtvd, 0);

							break;
						}
					}
				}

				break;
			}

			default:

				break;
		}

		free(uri_);
	}   
	
	pthread_exit(NULL);
}


//Dispatcher thread method.
void *
dispatcher(void * th_argv)
{
	//Cast from generic pointer type to p_argv struct type pointer.
	p_argv * arguments = (p_argv *) th_argv;

	//Format socket endpoint.
	char endpoint[24];
	sprintf(endpoint, "%s%d", "tcp://*:", arguments->port);

	//Communication sockets
	void * socket;
	void * sub;

	//Create communication channels.
	if (server_conn(&socket, endpoint, &sub) == -1)

		pthread_exit(NULL);

	int32_t client_id_ = 0;

	//ZeroMQ messages handling requests.
	zmq_msg_t client_id, client_req, release;

	for (;;)
	{
		zmq_msg_init (&client_id);
		zmq_msg_init (&client_req);

		//Save the identity of the requesting client.
		zmq_msg_recv(&client_id, socket, 0);

		//Check if a timeout was triggered in the previous receive operation.
		if ((errno == EAGAIN) && !zmq_msg_size(&client_id))
		{
			zmq_msg_init (&release);

			//If something was published, close sockets and exit.
			if (zmq_msg_recv(&release, sub, ZMQ_DONTWAIT) > 0)
			{
				if (zmq_close(socket) == -1)
				{
					perror("ERRIMSS_DISP_ROUTERCLS");
					pthread_exit(NULL);
				}

				if (zmq_close(sub) == -1)
				{
					perror("ERRIMSS_DISP_SUBCLS");
					pthread_exit(NULL);
				}

				pthread_exit(NULL);
			}

			//Overwrite errno value.
			errno = 1;

			continue;
		}

		//Save the request to be served.
		zmq_msg_recv(&client_req, socket, 0);

		//Message containing the client's communication ID plus its connection port.
		char response_[32]; memset(response_, '\0', 32);
		//Port that the new client will be forwarded to.
		int32_t port_ = arguments->port + 1 + (client_id_ % THREAD_POOL);
		//Wrap the previous info into the ZMQ message.
		sprintf(response_, "%d%c%d", port_, '-', client_id_++);

		int32_t c_id = *((int32_t *) zmq_msg_data(&client_id));

		//Specify client to answered to.
		if (zmq_send(socket, &c_id, sizeof(int32_t), ZMQ_SNDMORE) < 0)
		{
			perror("ERRIMSS_DISP_SENDCLIENTID");
			pthread_exit(NULL);
		}

		//Send communication specifications.
		if (zmq_send(socket, response_, strlen(response_), 0) < 0)
		{
			perror("ERRIMSS_DISP_SENDBLOCK");
			pthread_exit(NULL);
		}
	}
	
	pthread_exit(NULL);
}

