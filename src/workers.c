#include <zmq.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <signal.h>
#include "imss.h"
#include "comms.h"
#include "workers.h"
#include "directory.h"
#include "records.hpp"

#define GARBAGE_COLLECTOR_PERIOD 120


//ZeroMQ context entity conforming all sockets.
void *	context;
//INPROC bind address for pub-sub communications.
char *	pub_dir;
//Publisher socket.
void * 	pub;

//Lock dealing when cleaning blocks
pthread_mutex_t mutex_garbage;

//Initial buffer address.
unsigned char *   buffer_address;
//Set of locks dealing with the memory buffer access.
pthread_mutex_t * region_locks;
//Segment size (amount of memory assigned to each thread).
uint64_t	  buffer_segment;


//Memory amount (in GB) assigned to the buffer process.
uint64_t 	buffer_KB;
//Flag stating that the previous parameter has been received.
int32_t 	size_received;
//Communication resources in order to retrieve the buffer_GB parameter.
pthread_mutex_t buff_size_mut;
pthread_cond_t 	buff_size_cond;
int32_t 	copied;

//URI of the attached deployment.
char att_imss_uri[URI_];


pthread_mutex_t tree_mut;

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
	//printf("WORKER BINDED to: %s\n", (const char *) router_endpoint);
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
	if ((*subscriber = zmq_socket (context, ZMQ_SUB)) == NULL)
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
	//Subscribe to all incomming topics.
	if (zmq_setsockopt (*subscriber, ZMQ_SUBSCRIBE, "", 0) == -1)
	{
		perror("ERRIMSS_THREAD_SUBSCRIBE");
		return -1;
	}

	return 0;
}

//Thread method attending client read-write data requests.
void *
srv_worker (void * th_argv)
{   
	//Cast from generic pointer type to p_argv struct type pointer.
	p_argv * arguments = (p_argv *) th_argv;
	//Obtain the current map class element from the set of arguments.
	map_records * map = arguments->map;

	//Initialize one of the mutexes.
	if (pthread_mutex_init(&region_locks[(arguments->port)%THREAD_POOL], NULL) != 0)
	{
		perror("ERRIMSS_WORKER_REGLOCKINIT");
		pthread_exit(NULL);
	}

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

				zmq_msg_close(&client_id);
				zmq_msg_close(&client_req);
				zmq_msg_close(&release);

				if (pthread_mutex_destroy(&region_locks[(arguments->port)%THREAD_POOL]) != 0)
				{
					perror("ERRIMSS_WORKER_REGLOCKDEST");
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
		char number[16];
		sscanf(req, "%s", number);
		int32_t number_length = (int32_t) strlen(number);

		//Elements conforming the request.
		char * uri_ = req + number_length + 1;
		uint64_t block_size_recv = (uint64_t) atoi(number);

		//Create an std::string in order to be managed by the map structure.
		std::string key; 
		key.assign((const char *) uri_);

		//printf("REQUEST: %s (%ld)\n", key.c_str(), block_size_recv);

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

				switch (block_size_recv)
				{
					case READ_OP:
					{
						//Check if there was an associated block to the key.
						if (!(map->get(key, &address_, &block_size_rtvd)))
						{
							//printf("ERROR2: %s (%ld)\n", key.c_str(), block_size_recv);
							
							//Send the error code block.
							if (zmq_send(socket, err_code, strlen(err_code), 0) < 0)
							{
								perror("ERRIMSS_WORKER_SENDERR");
								pthread_exit(NULL);
							}
						}
						else
						{
							/*int32_t lock = (address_ - buffer_address) / buffer_segment;
							pthread_mutex_lock(&region_locks[lock]);*/

							//Send the requested block.
							if (zmq_send(socket, address_, block_size_rtvd, 0) < 0)
							{
								perror("ERRIMSS_WORKER_SENDBLOCK");
								pthread_exit(NULL);
							}

							//pthread_mutex_unlock(&region_locks[lock]);
						}

						break;
					}

					case RELEASE:
					{
						//Publish RELEASE message to all threads.
						char release_msg[] = "RELEASE\0";

						if (zmq_send(pub, release_msg, strlen(release_msg), 0) < 0)
						{
							perror("ERRIMSS_PUBLISH_RELEASEMSG");
							pthread_exit(NULL);
						}

						break;
					}
					case RENAME_OP:
					{
						std::size_t found = key.find(' ');
						if (found!=std::string::npos){
							string old_key = key.substr(0,found);
							
							string new_key = key.substr(found+1,key.length());
							
							
							//RENAME MAP
							map->cleaning_specific(new_key);
							int32_t result = map->rename_data_srv_worker(old_key,new_key);
							if(result == 0){
							
							break;
							}
						}
							

						char release_msg[] = "RENAME\0";

						if (zmq_send(socket, release_msg, strlen(release_msg), 0) < 0)
						{
							perror("ERRIMSS_PUBLISH_RENAMEMSG");
							pthread_exit(NULL);
						}
			            break;
					}
					case RENAME_DIR_DIR_OP:
					{
						std::size_t found = key.find(' ');
						if (found!=std::string::npos){
							string old_dir = key.substr(0,found);
							
							string rdir_dest = key.substr(found+1,key.length());
						
							
							//RENAME MAP
							map->rename_data_dir_dir_srv_worker(old_dir,rdir_dest);
						
						}
							

						char release_msg[] = "RENAME\0";

						if (zmq_send(socket, release_msg, strlen(release_msg), 0) < 0)
						{
							perror("ERRIMSS_PUBLISH_RENAMEMSG");
							pthread_exit(NULL);
						}
			            break;
					}

                    case WHO:
                    {
                        //Provide the uri of this instance.
                        if (zmq_send(socket, arguments->my_uri, strlen(arguments->my_uri), 0) < 0)
						{
							perror("ERRIMSS_WHOREQUEST");
							pthread_exit(NULL);
						}

                        break;
                    }

					default:

						break;
				}

				break;
			}
			//More messages will arrive to the socket.
			case WRITE_OP:
			{
				//If the record was not already stored, add the block.
				if (!map->get(key, &address_, &block_size_rtvd))
				{
					
					
					//unsigned char * buffer = (unsigned char *) malloc(block_size_recv);
					unsigned char * buffer = (unsigned char *)aligned_alloc(1024, block_size_recv);
					//Receive the block into the buffer.
					int err = zmq_recv(socket, buffer, block_size_recv, 0);
				
					int32_t insert_successful;
					//Include the new record in the tracking structure.
				
					insert_successful=map->put(key, buffer, block_size_recv);
					//Include the new record in the tracking structure.
					if (insert_successful != 0)
					{
						perror("ERRIMSS_WORKER_MAPPUT");
						//pthread_exit(NULL);
						continue;
					}

					//Update the pointer.
					
					arguments->pt += block_size_recv;
					
				}
				//If was already stored:
				else
				{

					//Receive the block into the buffer.
					
					zmq_recv(socket, address_, block_size_rtvd, 0);

					//pthread_mutex_unlock(&region_locks[lock]);
				}

				break;
			}

			default:

				break;
		}
	}   
	
	pthread_exit(NULL);
}

//Thread method searching and cleaning nodes with st_nlink=0
void *
garbage_collector (void * th_argv)
{
	//Obtain the current map class element from the set of arguments.
	map_records * map = (map_records *)th_argv;
	

	for (;;)
	{
	//Gnodetraverse_garbage_collector(map);//Future
	sleep(GARBAGE_COLLECTOR_PERIOD);
	pthread_mutex_lock(&mutex_garbage);
	map->cleaning();
	pthread_mutex_unlock(&mutex_garbage);
	}
	pthread_exit(NULL);
}

//Thread method attending client read-write metadata requests.
void *
stat_worker (void * th_argv)
{   
	//Cast from generic pointer type to p_argv struct type pointer.
	p_argv * arguments = (p_argv *) th_argv;
	//Obtain the current map class element from the set of arguments.
	map_records * map = arguments->map;

    uint16_t current_offset = 0;


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

				zmq_msg_close(&client_id);
				zmq_msg_close(&client_req);
				zmq_msg_close(&release);

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

		int32_t req_size = zmq_msg_size(&client_req);

		char raw_msg[req_size+1];
		memcpy((void*) raw_msg,(void*) zmq_msg_data(&client_req), req_size);
		raw_msg[req_size] = '\0';

		//printf("*********worker_metadata raw_msg %s\n",raw_msg);

		//Reference to the client request.
		char number[16];
		sscanf(raw_msg, "%s", number);
		int32_t number_length = (int32_t) strlen(number);

		//Elements conforming the request.
		char * uri_ = raw_msg + number_length + 1;
		uint64_t block_size_recv = (uint64_t) atoi(number);

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

				switch (block_size_recv)
				{
					case GETDIR:
					{
						char * buffer;
						int32_t numelems_indir;

						//Retrieve all elements inside the requested directory.
						pthread_mutex_lock(&tree_mut);
						buffer = GTree_getdir((char *) key.c_str(), &numelems_indir);
						pthread_mutex_unlock(&tree_mut);

						if (buffer == NULL)
						{
							if (zmq_send(socket, err_code, strlen(err_code), 0) < 0)
							{
								perror("ERRIMSS_STATWORKER_NODIR");
								pthread_exit(NULL);
							}

							break;
						}

						//Send the serialized set of elements within the requested directory.
						if (zmq_send(socket, buffer, (numelems_indir*URI_), 0) < 0)
						{
							perror("ERRIMSS_WORKER_SENDBLOCK");
							pthread_exit(NULL);
						}

						free(buffer);

						break;
					}
					case READ_OP:
					{
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
							imss_info * data = (imss_info *) address_;
							//printf("READ_OP SEND data->type=%c\n",data->type);
							//Send the requested block.
							if (zmq_send(socket, address_, block_size_rtvd, 0) < 0)
							{
								perror("ERRIMSS_WORKER_SENDBLOCK");
								pthread_exit(NULL);
							}
						}

						break;
					}

					case RELEASE:
					{
						//Publish RELEASE message to all threads.
						char release_msg[] = "RELEASE\0";

						if (zmq_send(pub, release_msg, strlen(release_msg), 0) < 0)
						{
							perror("ERRIMSS_PUBLISH_RELEASEMSG");
							pthread_exit(NULL);
						}

						break;
					}
					case DELETE_OP:
					{
			           
						int32_t result = map->delete_metadata_stat_worker(key);
						GTree_delete((char *) key.c_str());
						

						char release_msg[] = "DELETE\0";

						if (zmq_send(socket, release_msg, strlen(release_msg), 0) < 0)
						{
							perror("ERRIMSS_PUBLISH_DELETEMSG");
							pthread_exit(NULL);
						}


			            break;
					}
					case RENAME_OP:
					{
						std::size_t found = key.find(' ');
						if (found!=std::string::npos){
							string old_key = key.substr(0,found);
							string new_key = key.substr(found+1,key.length());
							
							//RENAME MAP
							int32_t result = map->rename_metadata_stat_worker(old_key,new_key);
							if(result == 0){
							//printf("0 elements rename from stat_worker\n");
							break;
							}

							//RENAME TREE
							GTree_rename((char *)old_key.c_str(),(char *)new_key.c_str());
						}
							

						char release_msg[] = "RENAME\0";

						if (zmq_send(socket, release_msg, strlen(release_msg), 0) < 0)
						{
							perror("ERRIMSS_PUBLISH_RENAMEMSG");
							pthread_exit(NULL);
						}
			            break;
					}
					case RENAME_DIR_DIR_OP:
					{
						std::size_t found = key.find(' ');
						if (found!=std::string::npos){
							string old_dir = key.substr(0,found);
	
							string rdir_dest = key.substr(found+1,key.length());
							
							
							//RENAME MAP
							map->rename_metadata_dir_dir_stat_worker(old_dir,rdir_dest);

							//RENAME TREE
							GTree_rename_dir_dir((char *)old_dir.c_str(),(char *)rdir_dest.c_str());
						
						}
							

						char release_msg[] = "RENAME\0";

						if (zmq_send(socket, release_msg, strlen(release_msg), 0) < 0)
						{
							perror("ERRIMSS_PUBLISH_RENAMEMSG");
							pthread_exit(NULL);
						}
			            break;
					}


					default:

						break;
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
					unsigned char * buffer = (unsigned char *) malloc (block_size_recv);
					zmq_recv(socket, buffer, block_size_recv, 0);

					int32_t insert_successful;
					//Include the new record in the tracking structure.
					insert_successful=map->put(key, buffer, block_size_recv);
					if (insert_successful != 0)
					{
						perror("ERRIMSS_WORKER_MAPPUT");
						//pthread_exit(NULL);
						continue;
					}
					
									
					//Insert the received uri into the directory tree.
					pthread_mutex_lock(&tree_mut);
					insert_successful = GTree_insert((char *) key.c_str());
					pthread_mutex_unlock(&tree_mut);

					if (insert_successful == -1)
					{
						perror("ERRIMSS_STATWORKER_GTREEINSERT");
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
	}   
	
	pthread_exit(NULL);
}


//Server dispatcher thread method.
void *
srv_attached_dispatcher(void * th_argv)
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

	//Variable specifying the ID that will be granted to the next client.
	uint32_t client_id_ = 0;

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
					perror("ERRIMSS_SRVDISP_ROUTERCLS");
					pthread_exit(NULL);
				}

				if (zmq_close(sub) == -1)
				{
					perror("ERRIMSS_SRVDISP_SUBCLS");
					pthread_exit(NULL);
				}

				zmq_msg_close(&client_id);
				zmq_msg_close(&client_req);
				zmq_msg_close(&release);

				pthread_exit(NULL);
			}

			//Overwrite errno value.
			errno = 1;

			continue;
		}

		//Save the request to be served.
		zmq_msg_recv(&client_req, socket, 0);

		int32_t c_id = *((int32_t *) zmq_msg_data(&client_id));
		//Specify client to answered to.
		if (zmq_send(socket, &c_id, sizeof(int32_t), ZMQ_SNDMORE) < 0)
		{
		    perror("ERRIMSS_SRVDISP_SENDCLIENTID");
		    pthread_exit(NULL);
		}

		//printf("REQUEST RECEIVED: %s\n", (char *) zmq_msg_data(&client_req));

		//Check if the client is requesting connection resources.
		if (!strncmp((char *) zmq_msg_data(&client_req), "HELLO!", 6))
		{

			if (strncmp((char *) zmq_msg_data(&client_req), "HELLO!JOIN", 10) != 0)
			{
				//Retrieve the buffer size that will be asigned to the current server process.
				char buff[6];
				sscanf((char *) zmq_msg_data(&client_req), "%s %ld %s", buff, &buffer_KB, att_imss_uri);
				strcpy(arguments->my_uri, att_imss_uri);

				//printf("MU URI: %s\n", att_imss_uri);

				//Notify that the value has been received.
				pthread_mutex_lock(&buff_size_mut);
				copied = 1;
				pthread_cond_signal(&buff_size_cond);
				pthread_mutex_unlock(&buff_size_mut);
			}

			//Message containing the client's communication ID plus its connection port.
			char response_[32]; memset(response_, '\0', 32);
			//Port that the new client will be forwarded to.
			int32_t port_ = arguments->port + 1 + (client_id_ % THREAD_POOL);
			//Wrap the previous info into the ZMQ message.
			sprintf(response_, "%d%c%d", port_, '-', client_id_++);

			//Send communication specifications.
			if (zmq_send(socket, response_, strlen(response_), 0) < 0)
			{
				perror("ERRIMSS_SRVDISP_SENDBLOCK");
				pthread_exit(NULL);
			}

			continue;
		}
		//Check if someone is requesting identity resources.
		else if (*((int32_t *) zmq_msg_data(&client_req)) == WHO)
		{
		    //Provide the uri of this instance.
		    if (zmq_send(socket, arguments->my_uri, strlen(arguments->my_uri), 0) < 0)
		    {
			perror("ERRIMSS_WHOREQUEST");
			pthread_exit(NULL);
		    }
		}
	}
	
	pthread_exit(NULL);
}

//Metadata dispatcher thread method.
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
					perror("ERRIMSS_STATDISP_ROUTERCLS");
					pthread_exit(NULL);
				}

				if (zmq_close(sub) == -1)
				{
					perror("ERRIMSS_STATDISP_SUBCLS");
					pthread_exit(NULL);
				}

				zmq_msg_close(&client_id);
				zmq_msg_close(&client_req);
				zmq_msg_close(&release);

				pthread_exit(NULL);
			}

			//Overwrite errno value.
			errno = 1;

			continue;
		}

		//Save the request to be served.
		zmq_msg_recv(&client_req, socket, 0);

        int32_t c_id = *((int32_t *) zmq_msg_data(&client_id));

        //Specify client to answered to.
        if (zmq_send(socket, &c_id, sizeof(int32_t), ZMQ_SNDMORE) < 0)
        {
            perror("ERRIMSS_STATDISP_SENDCLIENTID");
            pthread_exit(NULL);
        }

		//Check if the client is requesting connection resources.
		if (!strncmp((char *) zmq_msg_data(&client_req), "HELLO!", 6))
		{
            //Message containing the client's communication ID plus its connection port.
            char response_[32]; memset(response_, '\0', 32);
            //Port that the new client will be forwarded to.
            int32_t port_ = arguments->port + 1 + (client_id_ % THREAD_POOL);
            //Wrap the previous info into the ZMQ message.
            sprintf(response_, "%d%c%d", port_, '-', client_id_++);

            //Send communication specifications.
            if (zmq_send(socket, response_, strlen(response_), 0) < 0)
            {
                perror("ERRIMSS_STATDISP_SENDBLOCK");
                pthread_exit(NULL);
            }

            continue;
        }
        //Check if someone is requesting identity resources.
		else if (*((int32_t *) zmq_msg_data(&client_req)) == WHO)
        {
            //Provide the uri of this instance.
            if (zmq_send(socket, arguments->my_uri, strlen(arguments->my_uri), 0) < 0)
            {
                perror("ERRIMSS_WHOREQUEST");
                pthread_exit(NULL);
            }
        }
	}
	
	pthread_exit(NULL);
}
