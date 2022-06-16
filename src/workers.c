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
#include <sys/time.h>

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
	//int32_t rcvtimeo = 5;
	int32_t rcvtimeo = -1;
	if (comm_setsockopt(*router, ZMQ_RCVTIMEO, &rcvtimeo, sizeof(int32_t)) == -1)
	{
		perror("ERRIMSS_THREAD_RCVTIMEO");
		return -1;
	}
	printf("WORKER BINDED to: %s\n", (const char *) router_endpoint);
	//Connect the router socket to a certain endpoint.
	if (comm_bind(*router, (const char *) router_endpoint) == -1)
	{
		perror("ERRIMSS_THREAD_ROUTERBIND");
		return -1;
	}
	uint32_t hwm = 0;
	//Unset the limit in the number of messages to be queued in the socket receive queue.
	if (comm_setsockopt(*router, ZMQ_RCVHWM, &hwm, sizeof(int32_t)) == -1)
	{
		perror("ERRIMSS_THREAD_SETRCVHWM");
		return -1;
	}
	//Unset the limit in the number of messages to be queued in the socket send queue.
	if (comm_setsockopt(*router, ZMQ_SNDHWM, &hwm, sizeof(int32_t)) == -1)
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
	if (comm_connect(*subscriber, (const char *) pub_dir) == -1)
	{
		perror("ERRIMSS_THREAD_CONNECTSUB");
		return -1;
	}
	//Subscribe to all incomming topics.
	if (comm_setsockopt (*subscriber, ZMQ_SUBSCRIBE, "", 0) == -1)
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
	//sprintf(endpoint, "%s%d", "inproc://kk-",arguments->port);
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
		printf("[SRV_WORKER] waiting_massage\n");
		//Save the identity of the requesting client.
		comm_msg_recv(&client_id, socket, 0);
		
		struct timeval start, end;
		float delta_us;
		gettimeofday(&start, NULL);
		
		
		//Check if a timeout was triggered in the previous receive operation.
		if ((errno == EAGAIN) && !zmq_msg_size(&client_id))
		{
			zmq_msg_init (&release);

			//If something was published, close sockets and exit.
			if (comm_msg_recv(&release, sub, ZMQ_DONTWAIT) > 0)
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

				comm_msg_close(&client_id);
				comm_msg_close(&client_req);
				comm_msg_close(&release);

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
		comm_msg_recv(&client_req, socket, 0);
		
		
		struct timeval start2, end2;
		float delta_us2;
		gettimeofday(&start2, NULL);
		
		//Determine if more messages are comming.
		if ((comm_getsockopt(socket, ZMQ_RCVMORE, &more, &more_size)) == -1)
		{
			perror("ERRIMSS_WORKER_GETSOCKOPT");
			pthread_exit(NULL);
		}


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
				if (comm_send(socket, (int *) zmq_msg_data(&client_id), sizeof(int), ZMQ_SNDMORE) < 0)
				{
					perror("ERRIMSS_WORKER_SENDCLIENTID");
					pthread_exit(NULL);
				}
				switch (block_size_recv)
				{
					case READ_OP:
					{
						//printf("SRV_WORKER READ_OP\n");

						gettimeofday(&start2, NULL);
						
						int ret = map->get(key, &address_, &block_size_rtvd);
						
						gettimeofday(&end2, NULL);
						delta_us2 = (float) (end2.tv_usec - start2.tv_usec);
						//printf("\n[SERVER] map-get delta_us=%6.3f\n",(delta_us2/1000.0F));
						
						//Check if there was an associated block to the key.
						//if (!(map->get(key, &address_, &block_size_rtvd)))
						if(ret == 0)
						{
							gettimeofday(&start2, NULL);
							//Send the error code block.
							if (comm_send(socket, err_code, strlen(err_code), 0) < 0)
							{
								perror("ERRIMSS_WORKER_SENDERR");
								pthread_exit(NULL);
							}
							
							gettimeofday(&end2, NULL);
							delta_us2 = (float) (end2.tv_usec - start2.tv_usec);
							//printf("\n[SERVER] send delta_us=%6.3f\n",(delta_us2/1000.0F));
						}
						else
						{
							gettimeofday(&start2, NULL);
							//Send the requested block.
							if (comm_send(socket, address_, block_size_rtvd, 0) < 0)
							{
								perror("ERRIMSS_WORKER_SENDBLOCK");
								pthread_exit(NULL);
							}
							gettimeofday(&end2, NULL);
							delta_us2 = (float) (end2.tv_usec - start2.tv_usec);
							printf("[SRV_WORKER] send delta_us=%6.3f\n",(delta_us2/1000.0F));
						}
						gettimeofday(&end, NULL);
						delta_us = (float) (end.tv_usec - start.tv_usec);
						printf("[SRV_WORKER] [END] delta_us=%6.3f\n\n",(delta_us/1000.0F));
						break;
					}

					case RELEASE:
					{
						//Publish RELEASE message to all threads.
						char release_msg[] = "RELEASE\0";

						if (comm_send(pub, release_msg, strlen(release_msg), 0) < 0)
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

						if (comm_send(socket, release_msg, strlen(release_msg), 0) < 0)
						{
							perror("ERRIMSS_PUBLISH_RENAMEMSG");
							pthread_exit(NULL);
						}
			            break;
					}
					case RENAME_DIR_DIR_OP:
					{
						//printf("SRV_WORKER RENAME_DIR_DIR_OP\n");
						std::size_t found = key.find(' ');
						if (found!=std::string::npos){
							string old_dir = key.substr(0,found);
							
							string rdir_dest = key.substr(found+1,key.length());
						
							
							//RENAME MAP
							map->rename_data_dir_dir_srv_worker(old_dir,rdir_dest);
						
						}
							

						char release_msg[] = "RENAME\0";

						if (comm_send(socket, release_msg, strlen(release_msg), 0) < 0)
						{
							perror("ERRIMSS_PUBLISH_RENAMEMSG");
							pthread_exit(NULL);
						}
			            break;
					}
					case READV:
					{
						//printf("READV CASE\n");
						std::size_t found = key.find('$');
						string path;
						if (found!=std::string::npos){
							path = key.substr(0,found+1);
							//std::cout <<"path:" << path << '\n';
							key.erase(0,found+1);
							std::size_t found = key.find(' ');
							int curr_blk = stoi(key.substr(0,found));
							key.erase(0,found+1);

							found = key.find(' ');
							int end_blk = stoi(key.substr(0,found));
							key.erase(0,found+1);

							found = key.find(' ');
							int blocksize = stoi(key.substr(0,found));
							key.erase(0,found+1);

							found = key.find(' ');
							int start_offset = stoi(key.substr(0,found));
							key.erase(0,found+1);

							found = key.find(' ');
							int64_t size = stoi(key.substr(0,found));
							key.erase(0,found+1);


							//Needed variables
							size_t byte_count = 0;
							int first = 0;
							int ds = 0;
							int64_t to_copy = 0;
							uint32_t filled = 0;
							size_t to_read = 0;

							int pos = path.find('$');
							std::string first_element = path.substr(0,pos+1);
							first_element = first_element + std::to_string(0);
							//printf("first_element=%s\n",first_element.c_str());
							map->get(first_element, &address_, &block_size_rtvd);
							struct stat * stats = (struct stat *) address_;
							unsigned char * buf = (unsigned char *)malloc(size);

							while(curr_blk <= end_blk){
									string element = path;
									element = element + std::to_string(curr_blk);
									//std::cout <<"SERVER READV element:" << element << '\n';
									if (map->get(element, &address_, &block_size_rtvd)==0)
									{//If dont exist 
										//Send the error code block.
										//std::cout <<"SERVER READV NO EXISTE element:" << element << '\n';
										if (comm_send(socket, err_code, strlen(err_code), 0) < 0)
										{
											perror("ERRIMSS_WORKER_SENDERR");
											pthread_exit(NULL);
										}
									}//If was already stored:
									else
									{
										//First block case
										if (first == 0) {
											if(size < stats->st_size - start_offset){
											//to_read = size;
											to_read = blocksize*KB - start_offset;
										}else{ 	
											if(stats->st_size<blocksize*KB){
												to_read = stats->st_size - start_offset;
											}else{
												to_read = blocksize*KB - start_offset;
											}																	
										}

											//Check if offset is bigger than filled, return 0 because is EOF case
											if(start_offset > stats->st_size) 
												return 0; 
											memcpy(buf, address_ + start_offset, to_read);
											byte_count += to_read;
											++first;

											//Middle block case
										} else if (curr_blk != end_blk) {
											//memcpy(buf + byte_count, aux + HEADER, IMSS_DATA_BSIZE);
											memcpy(buf + byte_count, address_, blocksize*KB);
											byte_count += blocksize*KB;
											//End block case
										}  else {

											//Read the minimum between end_offset and filled (read_ = min(end_offset, filled))
											int64_t pending = size - byte_count;
											memcpy(buf + byte_count, address_, pending);
											byte_count += pending;
										}
									}								
								++curr_blk;
								}
								//Send the requested block.
								if (comm_send(socket, buf, size, 0) < 0)
								{
									perror("ERRIMSS_WORKER_SENDBLOCK");
									pthread_exit(NULL);
								}
							}
							

			            break;
					}
					case SPLIT_READV:
					{
						//printf("SPLIT_READV CASE\n");
						//printf("key=%s\n",key.c_str());
						std::size_t found = key.find(' ');
						string path;
						if (found!=std::string::npos){
							
							path = key.substr(0,found);
							key.erase(0,found+1);

							found = key.find(' ');
							int blocksize = stoi(key.substr(0,found)) * KB;
							key.erase(0,found+1);

							found = key.find(' ');
							int start_offset = stoi(key.substr(0,found));
							key.erase(0,found+1);

							found = key.find(' ');
							int stats_size = stoi(key.substr(0,found));
							key.erase(0,found+1);


							found = key.find('$');
							int amount = stoi(key.substr(0,found));
							int size = amount * blocksize;
							key.erase(0,found+1);
							
							/*printf("amount=%d\n",amount);
							printf("path=%s\n",path.c_str());
							printf("blocksize=%d\n",blocksize);
							printf("start_offset=%d\n",start_offset);
							printf("size=%d\n",size);
							printf("rest=%s\n",key.c_str());*/

							unsigned char * buf = (unsigned char *)malloc(size);

							//Needed variables
							size_t byte_count = 0;
							int first = 0;
							int ds = 0;
							int64_t to_copy = 0;
							uint32_t filled = 0;
							size_t to_read = 0;
							int curr_blk = 0;

							for(int i = 0; i < amount; i++){
									
									//substract current block
									found = key.find('$');
									int curr_blk = stoi(key.substr(0,found));
									key.erase(0,found+1);
									
									string element = path;
									element = element + '$' + std::to_string(curr_blk);
									if (map->get(element, &address_, &block_size_rtvd)==0)
									{//If dont exist 
										//Send the error code block.
										if (comm_send(socket, err_code, strlen(err_code), 0) < 0)
										{
											perror("ERRIMSS_WORKER_SENDERR");
											pthread_exit(NULL);
										}
									}//If was already stored:
									
									memcpy(buf + byte_count, address_, blocksize);
									byte_count += blocksize;
									
							}
							//Send the requested block.
							if (comm_send(socket, buf, byte_count, 0) < 0)
							{
								perror("ERRIMSS_WORKER_SENDBLOCK");
								pthread_exit(NULL);
							}					

						}
			            break;
					}
                    case WHO:
                    {
                        //Provide the uri of this instance.
                        if (comm_send(socket, arguments->my_uri, strlen(arguments->my_uri), 0) < 0)
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
				//printf("WRITE_OP key=%s\n",key.c_str());
				//std::cout <<"WRITE_OP key:" << key << '\n';

				struct utsname detect;
				uname(&detect);

				std::size_t found = key.find(' ');
				if (found!=std::string::npos){
					
					//printf("WRITEV CASE\n");
					string path = key.substr(0,found);
					key.erase(0,found+1);
					//std::cout <<"path:" << key << '\n';
					
					std::size_t found = key.find(' ');
						int curr_blk = stoi(key.substr(0,found));
						key.erase(0,found+1);
						
						found = key.find(' ');
						int end_blk = stoi(key.substr(0,found));
						key.erase(0,found+1);

						found = key.find(' ');
						int start_offset = stoi(key.substr(0,found));
						key.erase(0,found+1);

						found = key.find(' ');
						int end_offset = stoi(key.substr(0,found));
						key.erase(0,found+1);

						found = key.find(' ');
						int IMSS_DATA_BSIZE = stoi(key.substr(0,found));
						key.erase(0,found+1);

						int size = stoi(key);

						unsigned char * buf = (unsigned char *)malloc(size);
						//Receive all blocks into the buffer.
						comm_recv(socket, buf, size, 0);
						//printf("WRITEV-buffer=%s\n",buf);
						int pos = path.find('$');
						std::string first_element = path.substr(0,pos+1);
						first_element = first_element + "0";
						map->get(first_element, &address_, &block_size_rtvd);
						//imss_info * data = (imss_info *) address_;
						//printf("READ_OP SEND data->type=%c\n",data->type);
						struct stat * stats = (struct stat *) address_;

						//Needed variables
						size_t byte_count = 0;
						int first = 0;
						int ds = 0;
						int64_t to_copy = 0;
						uint32_t filled = 0;
						unsigned char *aux = (unsigned char *)malloc(IMSS_DATA_BSIZE);
						int count = 0;
						//For the rest of blocks
						while(curr_blk <= end_blk){
							//printf("Nodename    - %s current_block=%d\n", detect.nodename, curr_blk);
							count=count +1;
							//printf("count=%d\n",count);
							pos = path.find('$');
							string element = path.substr(0,pos+1);
							element = element + std::to_string(curr_blk);
							//std::cout <<"element:" << element << '\n';

							//First fragmented block
							if (first==0 && start_offset && stats->st_size != 0) {
								
								//Get previous block
								map->get(element, &aux, &block_size_rtvd);//path por curr_block
								//Bytes to write are the minimum between the size parameter and the remaining space in the block (BLOCKSIZE-start_offset)
								to_copy = (size < IMSS_DATA_BSIZE-start_offset) ? size : IMSS_DATA_BSIZE-start_offset;

								memcpy(aux + start_offset, buf + byte_count, to_copy);

							
								
							}
							//Last Block
							else if(curr_blk == end_blk){
								if(end_offset != 0){
									to_copy = end_offset;
								}else{
									to_copy = IMSS_DATA_BSIZE;
								}
								//Only if last block has contents
								if(curr_blk <= stats->st_blocks && start_offset){
									map->get(element, &aux, &block_size_rtvd);//path por curr_block
									
								}
								else{
									memset(aux, 0, IMSS_DATA_BSIZE);
									
								}
								if(byte_count == size){
									to_copy=0;
								}
								//printf("curr_block=%d, end_block=%d, byte_count=%d\n",curr_blk, end_blk, byte_count);
								memcpy(aux , buf + byte_count, to_copy);

							}
							//middle block
							else{
								to_copy = IMSS_DATA_BSIZE;
								memcpy(aux, buf + byte_count, to_copy);
							}

							//Write and update variables
							if (!map->get(element, &address_, &block_size_rtvd))
							{	
								map->put(element,aux,block_size_rtvd);
								
								//printf("Nodename    - %s after put\n", detect.nodename);
							}else{
								memcpy(address_,aux,block_size_rtvd);
							}
							//printf("currblock=%d, byte_count=%d\n",curr_blk, byte_count);
							byte_count += to_copy;
							++curr_blk;
							++first;
						}
						int16_t off = (end_blk * IMSS_DATA_BSIZE) - 1 - size;
						if(size + off > stats->st_size){
							stats->st_size = size + off;
							stats->st_blocks = curr_blk-1;
						}
					
						free(buf);
				}else{
					//printf("WRITE NORMAL CASE\n");
					//If the record was not already stored, add the block.
					if (!map->get(key, &address_, &block_size_rtvd))
					{
						
						//unsigned char * buffer = (unsigned char *) malloc(block_size_recv);
						unsigned char * buffer = (unsigned char *)aligned_alloc(1024, block_size_recv);
						//Receive the block into the buffer.
						
						comm_recv(socket, buffer, block_size_recv, 0);
						struct stat * stats = (struct stat *) buffer;
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
						comm_recv(socket, address_, block_size_rtvd, 0);

						//pthread_mutex_unlock(&region_locks[lock]);
					}

					break;
				}
					


				
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
	//sprintf(endpoint, "%s%d", "inproc://kk-", arguments->port);
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
		comm_msg_recv(&client_id, socket, 0);

		//Check if a timeout was triggered in the previous receive operation.
		if ((errno == EAGAIN) && !zmq_msg_size(&client_id))
		{
			//printf("zmq_msg_size=%ld\n",zmq_msg_size(&client_id));
			zmq_msg_init (&release);
			//If something was published, close sockets and exit.
			if (comm_msg_recv(&release, sub, ZMQ_DONTWAIT) > 0)
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

				comm_msg_close(&client_id);
				comm_msg_close(&client_req);
				comm_msg_close(&release);

				pthread_exit(NULL);
			}
			
			//Overwrite errno value.
			errno = 1;

			continue;
		}
		//printf("zmq_eerno=%d %s\n",zmq_errno(), zmq_strerror(1));
		//Save the request to be served
		comm_msg_recv(&client_req, socket, 0);

		//Determine if more messages are comming.
		if ((comm_getsockopt(socket, ZMQ_RCVMORE, &more, &more_size)) == -1)
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
		//printf("stat_worker RECV more=%ld, blocksss=%ld\n",more, block_size_recv);
		//Differentiate between READ and WRITE operations. 
		switch (more)
		{
			//No more messages will arrive to the socket.
			case READ_OP:
			{
				//Specify client to be answered.
				if (comm_send(socket, (int *) zmq_msg_data(&client_id), sizeof(int), ZMQ_SNDMORE) < 0)
				{
					perror("ERRIMSS_WORKER_SENDCLIENTID");
					pthread_exit(NULL);
				}

				switch (block_size_recv)
				{
					case GETDIR:
					{
						//printf("stat_server GETDIR key=%s\n",key.c_str());
						char * buffer;
						int32_t numelems_indir;
						zmq_msg_t msg;
						//Retrieve all elements inside the requested directory.
						pthread_mutex_lock(&tree_mut);
						buffer = GTree_getdir((char *) key.c_str(), &numelems_indir);
						pthread_mutex_unlock(&tree_mut);
						if (buffer == NULL)
						{
							if (comm_send(socket, err_code, strlen(err_code), 0) < 0)
							{
								perror("ERRIMSS_STATWORKER_NODIR");
								pthread_exit(NULL);
							}
							
							break;
						}
						zmq_msg_init_size (&msg,  (numelems_indir*URI_));
						memcpy(zmq_msg_data(&msg), buffer, (numelems_indir*URI_));

						//Send the serialized set of elements within the requested directory.
						if (comm_msg_send(&msg, socket, 0) < 0)
						{
							perror("ERRIMSS_WORKER_SENDBLOCK");
							pthread_exit(NULL);
						}
						free(buffer);

						break;
					}
					case READ_OP:
					{
						//printf("STAT_WORKER READ_OP\n");
						//Check if there was an associated block to the key.
						if (!(map->get(key, &address_, &block_size_rtvd)))
						{
							//Send the error code block.
							if (comm_send(socket, err_code, strlen(err_code), 0) < 0)
							{
								perror("ERRIMSS_WORKER_SENDERR");
								pthread_exit(NULL);
							}
						}
						else
						{
							//dataset_info *dataset = (dataset_info*) address_;
							//printf("[STAT_SERVER] dataset.original=%s\n",dataset->original_name);
							//imss_info * data = (imss_info *) address_;
							//printf("READ_OP SEND data->type=%c\n",data->type);
							//Send the requested block.
							if (comm_send(socket, address_, block_size_rtvd, 0) < 0)
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

						if (comm_send(pub, release_msg, strlen(release_msg), 0) < 0)
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

						if (comm_send(socket, release_msg, strlen(release_msg), 0) < 0)
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

						if (comm_send(socket, release_msg, strlen(release_msg), 0) < 0)
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

						if (comm_send(socket, release_msg, strlen(release_msg), 0) < 0)
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
					comm_recv(socket, buffer, block_size_recv, 0);

					int32_t insert_successful;
					//Include the new record in the tracking structure.
					/*dataset_info * data = (dataset_info *) buffer;
					printf("WRITE_OP key=%s\n",key.c_str());
					printf("WRITE_OP data->type=%c\n",data->type);
					printf("WRITE_OP data->servers=%d\n",data->n_servers);
					printf("WRITE_OP original_name=%s\n",data->original_name);*/
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
							printf("[STAT_WORKER] LOCAL DATASET_UPDATE\n");
							zmq_msg_t data_locations_msg;

							if (zmq_msg_init(&data_locations_msg) != 0)
							{
								perror("ERRIMSS_WORKER_DATALOCATINIT");
								pthread_exit(NULL);
							}

							if (comm_msg_recv(&data_locations_msg, socket, 0) == -1)
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

							if (comm_msg_close(&data_locations_msg) == -1)
							{
								perror("ERRIMSS_WORKER_DATALOCATCLOSE");
								pthread_exit(NULL);
							}

							//Specify client to be answered.
							if (comm_send(socket, (int *) zmq_msg_data(&client_id), sizeof(int), ZMQ_SNDMORE) < 0)
							{
								perror("ERRIMSS_WORKER_DATALOCATANSWER1");
								pthread_exit(NULL);
							}

							//Answer the client with the update.
							char answer[9] = "UPDATED!";
							if (comm_send(socket, answer, 9, 0) < 0)
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
							comm_recv(socket, address_, block_size_rtvd, 0);

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
    //sprintf(endpoint, "%s%d", "inproc://kk-", arguments->port);
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
		comm_msg_recv(&client_id, socket, 0);

		//Check if a timeout was triggered in the previous receive operation.
		if ((errno == EAGAIN) && !zmq_msg_size(&client_id))
		{
			zmq_msg_init (&release);

			//If something was published, close sockets and exit.
			if (comm_msg_recv(&release, sub, ZMQ_DONTWAIT) > 0)
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

				comm_msg_close(&client_id);
				comm_msg_close(&client_req);
				comm_msg_close(&release);

				pthread_exit(NULL);
			}

			//Overwrite errno value.
			errno = 1;

			continue;
		}
		//Save the request to be served.
		comm_msg_recv(&client_req, socket, 0);

		int32_t c_id = *((int32_t *) zmq_msg_data(&client_id));
		//Specify client to answered to.
		if (comm_send(socket, &c_id, sizeof(int32_t), ZMQ_SNDMORE) < 0)
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
			if (comm_send(socket, response_, strlen(response_), 0) < 0)
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
		    if (comm_send(socket, arguments->my_uri, strlen(arguments->my_uri), 0) < 0)
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
	//sprintf(endpoint, "%s%d", "inproc://kk-", arguments->port);
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
		comm_msg_recv(&client_id, socket, 0);

		//Check if a timeout was triggered in the previous receive operation.
		if ((errno == EAGAIN) && !zmq_msg_size(&client_id))
		{
			zmq_msg_init (&release);

			//If something was published, close sockets and exit.
			if (comm_msg_recv(&release, sub, ZMQ_DONTWAIT) > 0)
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

				comm_msg_close(&client_id);
				comm_msg_close(&client_req);
				comm_msg_close(&release);

				pthread_exit(NULL);
			}

			//Overwrite errno value.
			errno = 1;

			continue;
		}
		//Save the request to be served.
		comm_msg_recv(&client_req, socket, 0);

        int32_t c_id = *((int32_t *) zmq_msg_data(&client_id));

        //Specify client to answered to.
        if (comm_send(socket, &c_id, sizeof(int32_t), ZMQ_SNDMORE) < 0)
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
            if (comm_send(socket, response_, strlen(response_), 0) < 0)
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
            if (comm_send(socket, arguments->my_uri, strlen(arguments->my_uri), 0) < 0)
            {
                perror("ERRIMSS_WHOREQUEST");
                pthread_exit(NULL);
            }
        }
	}
	
	pthread_exit(NULL);
}
