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
#include <inttypes.h>

#define GARBAGE_COLLECTOR_PERIOD 120


//Lock dealing when cleaning blocks
pthread_mutex_t mutex_garbage;

//Initial buffer address.
char *   buffer_address;
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

pthread_mutex_t mp = PTHREAD_MUTEX_INITIALIZER;

void * srv_worker (void * th_argv)
{
	ucx_server_ctx_t context;
	ucs_status_t     status;
	int              ret;

	p_argv * arguments = (p_argv *) th_argv;

	context.conn_request =  StsQueue.create();
	status = start_server(arguments->ucp_worker, &context, &context.listener, NULL, arguments->port);

	if (status != UCS_OK) {
		perror("ERRIMSS_STAR_SERVER");
		pthread_exit(NULL);
	}

	for (;;) {
		p_argv * thread_args = (p_argv *) malloc(sizeof(p_argv));
		pthread_t thread;
		pthread_attr_t attr;
		ucp_conn_request_h req;

		while (StsQueue.size(context.conn_request) == 0) {
			ucp_worker_progress(arguments->ucp_worker);
		}

		req = (ucp_conn_request_h)StsQueue.pop(context.conn_request);

		memcpy(thread_args, th_argv, sizeof(p_argv));
		ret = init_worker(arguments->ucp_context, &(thread_args->ucp_data_worker));
		if (ret != 0) {
			perror("ERRIMSS_INIT_WORKER");
			pthread_exit(NULL);
		}

		status = server_create_ep(thread_args->ucp_data_worker, req, &(thread_args->server_ep));
		if (status != UCS_OK) {
			perror("ERRIMSS_SERVER_CREATE_EP");
			pthread_exit(NULL);
		}

		pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);			
		DPRINT("[DATA WORKER] Created thread for new endpoint.\n");
		if (pthread_create(&thread, &attr, srv_worker_thread, (void *) thread_args) == -1)
		{
			//Notify thread error deployment.
			perror("ERRIMSS_SRVWORKER_DEPLOY");
			pthread_exit(NULL);
		}
	} 
	StsQueue.destroy(context.conn_request);
	ucp_listener_destroy(context.listener);
}

//Thread method attending client read-write data requests.
void * srv_worker_thread (void * th_argv)
{   

	ucp_worker_h     ucp_data_worker;
	ucs_status_t     status;
	int              ret;

	//Cast from generic pointer type to p_argv struct type pointer.
	p_argv * arguments = (p_argv *) th_argv;
	//Obtain the current map class element from the set of arguments.
	std::shared_ptr<map_records>  map = arguments->map;
	ucp_data_worker = arguments->ucp_data_worker;


	//Resources specifying if the ZMQ_SNDMORE flag was set in the sender.
	int64_t more;
	size_t more_size = sizeof(more);

	//Code to be sent if the requested to-be-read key does not exist.
	char err_code[] = "$ERRIMSS_NO_KEY_AVAIL$";

	for (;;)
	{  
		uint32_t client_id = 0;
		char req[REQUEST_SIZE];
		char mode[MODE_SIZE];

        DPRINT ("[DATA WORKER] Waiting for new request.\n");
		//Save the identity of the requesting client.
		recv_stream(ucp_data_worker, arguments->server_ep, (char *) &client_id, sizeof(uint32_t ));

		recv_stream(ucp_data_worker, arguments->server_ep, mode, MODE_SIZE);

		if (!strcmp(mode, "GET"))
			more = GET_OP;
		else
			more = SET_OP;

		/*struct timeval start, end;
		  long delta_us;
		  gettimeofday(&start, NULL);*/

		//Save the request to be served.
		recv_stream(ucp_data_worker, arguments->server_ep, req, REQUEST_SIZE);

		DPRINT ("[DATA WORKER] Request - client_id '%" PRIu32 "', mode '%s', req '%s'\n",client_id, mode, req)
			/*struct timeval start2, end2;
			  long delta_us2;
			  gettimeofday(&start2, NULL);*/

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
		char * address_;
		uint64_t block_size_rtvd;

		//Differentiate between READ and WRITE operations. 
		switch (more) 
		{
			//No more messages will arrive to the socket.
			case READ_OP:
				{
					switch (block_size_recv)
					{
						case READ_OP:
							{
								//printf("SRV_WORKER READ_OP\n");

								int ret = map->get(key, &address_, &block_size_rtvd);

								//Check if there was an associated block to the key.
								//if (!(map->get(key, &address_, &block_size_rtvd)))
								if(ret == 0)
								{
									//gettimeofday(&start2, NULL);
									//Send the error code block.
									if (send_dynamic_stream(ucp_data_worker, arguments->server_ep, err_code, STRING) < 0)
									{
										perror("ERRIMSS_WORKER_SENDERR");
										pthread_exit(NULL);
									}

									/*gettimeofday(&end2, NULL);
									  delta_us2 = (long) (end2.tv_usec - start2.tv_usec);
									  printf("\n[SRV_WORKER] [READ] send delta_us=%6.3f\n",(delta_us2/1000.0F));*/
								}
								else
								{
									/*double elapsedTime;
									  gettimeofday(&start2, NULL);*/
									//Send the requested block.

									if (send_stream(ucp_data_worker, arguments->server_ep, address_, block_size_rtvd) < 0)
									{
										perror("ERRIMSS_WORKER_SENDBLOCK");
										pthread_exit(NULL);
									}
									/*gettimeofday(&end2, NULL);
									  elapsedTime = (end2.tv_sec - start2.tv_sec) * 1000.0;      // sec to ms
									  elapsedTime += (end2.tv_usec - start2.tv_usec) / 1000.0;   // us to ms
									  delta_us2 = (long) (end2.tv_usec - start2.tv_usec);
									  printf("[SRV_WORKER] [READ]  NEW send delta_us=%6.3f\n", elapsedTime  );*/
								}
								break;
							}

						case RELEASE:
							{
								ep_flush(arguments->server_ep, ucp_data_worker);
								ep_close(ucp_data_worker, arguments->server_ep, UCP_EP_CLOSE_MODE_FLUSH);
								pthread_exit(NULL);
								break;
							}
						case RENAME_OP:
							{
								std::size_t found = key.find(' ');
								if (found!=std::string::npos){
									std::string old_key = key.substr(0,found);
									std::string new_key = key.substr(found+1,key.length());

									//RENAME MAP
									map->cleaning_specific(new_key);
									int32_t result = map->rename_data_srv_worker(old_key,new_key);
									if(result == 0){
										break;
									}
								}

								char release_msg[] = "RENAME\0";

								if (send_stream(ucp_data_worker, arguments->server_ep, release_msg, RESPONSE_SIZE) < 0)
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
									std::string old_dir = key.substr(0,found);
									std::string rdir_dest = key.substr(found+1,key.length());

									//RENAME MAP
									map->rename_data_dir_srv_worker(old_dir,rdir_dest);
								}

								char release_msg[] = "RENAME\0";
								if (send_stream(ucp_data_worker, arguments->server_ep, release_msg, RESPONSE_SIZE) < 0)
								{
									perror("ERRIMSS_PUBLISH_RENAMEMSG");
									pthread_exit(NULL);
								}
								break;
							}
						case READV://Only 1 server work
							{
								//printf("READV CASE\n");
								std::size_t found = key.find('$');
								std::string path;
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
									char * buf = (char *)malloc(size);

									while(curr_blk <= end_blk){
										std::string element = path;
										element = element + std::to_string(curr_blk);
										//std::cout <<"SERVER READV element:" << element << '\n';
										if (map->get(element, &address_, &block_size_rtvd)==0)
										{//If dont exist 
											//Send the error code block.
											//std::cout <<"SERVER READV NO EXISTE element:" << element << '\n';
											if (send_dynamic_stream(ucp_data_worker, arguments->server_ep, err_code, STRING) < 0)
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
									if (send_stream(ucp_data_worker, arguments->server_ep, buf, size) < 0)
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
								std::string path;
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

									int msg_size = stoi(key.substr(0,found));

									char * msg = (char *) calloc(msg_size,sizeof(char));
									recv_stream(ucp_data_worker, arguments->server_ep,  msg, msg_size);

									key = msg;
									found = key.find('$');
									int amount = stoi(key.substr(0,found));
									int size = amount * blocksize;
									key.erase(0,found+1);

									/*printf("msg=%s\n",key.c_str());
									  printf("msg_size=%d\n",msg_size);
									  printf("*path=%s\n",path.c_str());
									  printf("*blocksize=%d\n",blocksize);
									  printf("*start_offset=%d\n",start_offset);
									  printf("*size=%d\n",size);
									  printf("*amount=%d\n",amount);*/


									char * buf = (char *)malloc(size);

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

										std::string element = path;
										element = element + '$' + std::to_string(curr_blk);
										if (map->get(element, &address_, &block_size_rtvd)==0)
										{//If dont exist 
											//Send the error code block.
											if (send_dynamic_stream(ucp_data_worker, arguments->server_ep, err_code, STRING) < 0)
											{
												perror("ERRIMSS_WORKER_SENDERR");
												pthread_exit(NULL);
											}
										}//If was already stored:

										memcpy(buf + byte_count, address_, blocksize);
										byte_count += blocksize;

									}
									//Send the requested block.
									if (send_stream(ucp_data_worker, arguments->server_ep, buf, byte_count) < 0)
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
								if (send_stream(ucp_data_worker, arguments->server_ep, arguments->my_uri, strlen(arguments->my_uri)) < 0)
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
					//std::cout <<"WRITE_OP key:" << key << '\n';

					struct utsname detect;
					uname(&detect);

					int op;
					std::size_t found = key.find(' ');
					std::size_t found2 = key.find("[OP]=");
					if(found2!=std::string::npos){
						op = stoi(key.substr(found2+5,(found-(found2+5))));
						key.erase(0,found+1);
					}

					//if (found!=std::string::npos){
					if (found!=std::string::npos && found2==std::string::npos){	
						std::string path = key.substr(0,found);
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

						char * buf = (char *)malloc(size);

						//Receive all blocks into the buffer.
						recv_stream(ucp_data_worker, arguments->server_ep,  buf, size);

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
						char *aux = (char *)malloc(IMSS_DATA_BSIZE);
						int count = 0;
						//For the rest of blocks
						while(curr_blk <= end_blk){
							//printf("Nodename    - %s current_block=%d\n", detect.nodename, curr_blk);
							count=count +1;
							//printf("count=%d\n",count);
							pos = path.find('$');
							std::string element = path.substr(0,pos+1);
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
					}else if(found!=std::string::npos && op == 2){
						std::string path;
						std::size_t found = key.find(' ');
						//printf("Nodename	-%s SPLIT WRITEV\n",detect.nodename);

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

						//Receive all blocks into the buffer.
						char * buf = (char *)malloc(size);
						int size_recv = recv_stream(ucp_data_worker, arguments->server_ep, buf, size);
						size_recv = size; // MIRAR
						int32_t insert_successful;

						//printf("Nodename	-%s size_recv=%d\n",detect.nodename,size_recv);
						//printf("Salida buf full=%c\n",buf[100]);

						int32_t byte_count = 0;
						for(int i = 0; i < amount; i++){
							//substract current block
							found = key.find('$');
							int curr_blk = stoi(key.substr(0,found));
							key.erase(0,found+1);

							std::string element = path;
							element = element + '$' + std::to_string(curr_blk);
							//printf("\n element=%s\n",element.c_str());

							if (map->get(element, &address_, &block_size_rtvd)==0){

								//If dont exist 
								char * buffer = (char *)aligned_alloc(1024, blocksize);

								memcpy(buffer, buf + byte_count, blocksize);

								//printf("Salida buffer part=%c\n",buffer[100]);
								insert_successful=map->put(element, buffer, block_size_recv);

								if (insert_successful != 0)
								{
									perror("ERRIMSS_WORKER_MAPPUT");
									//pthread_exit(NULL);
									continue;
								}
							}else{
								//If already exits
								memcpy(address_, buf + byte_count, blocksize);
								//printf("Alreadt exitsSalida buffer part=%c\n",buf[100]);
							}
							byte_count = byte_count + blocksize;
							//printf("Nodename	-%s byte_count=%d\n",detect.nodename,byte_count);
						}
					}
					else{
						//printf("WRITE NORMAL CASE\n");

						//gettimeofday(&start2, NULL);

						int ret = map->get(key, &address_, &block_size_rtvd);

						/*gettimeofday(&end2, NULL);
						  delta_us2 = (long) (end2.tv_usec - start2.tv_usec);
						  printf("\n[SRV_WORKER] [WRITE] map-get delta_us=%6.3f\n",(delta_us2/1000.0F));*/
						//If the record was not already stored, add the block.
						//if (!map->get(key, &address_, &block_size_rtvd))

						if(ret == 0){

							//unsigned char * buffer = (unsigned char *) malloc(block_size_recv);
							char * buffer = (char *)aligned_alloc(1024, block_size_recv);
							//Receive the block into the buffer.
							recv_stream(ucp_data_worker, arguments->server_ep, buffer, block_size_recv);
							struct stat * stats = (struct stat *) buffer;
							int32_t insert_successful;

							//gettimeofday(&start2, NULL);
							//Include the new record in the tracking structure.
							insert_successful=map->put(key, buffer, block_size_recv);
							/*gettimeofday(&end2, NULL);
							  delta_us2 = (long) (end2.tv_usec - start2.tv_usec);
							  printf("\n[SRV_WORKER] [WRITE] map-put delta_us=%6.3f\n",(delta_us2/1000.0F));*/

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
							//gettimeofday(&start2, NULL);
							//Receive the block into the buffer.
							recv_stream(ucp_data_worker, arguments->server_ep, address_, block_size_rtvd);
							/*gettimeofday(&end2, NULL);
							  delta_us2 = (long) (end2.tv_usec - start2.tv_usec);
							  printf("\n[SRV_WORKER] [WRITE] recv delta_us=%6.3f\n",(delta_us2/1000.0F));*/

							//pthread_mutex_unlock(&region_locks[lock]);
						}
						break;
					}
				}
					default:
				break;
				}
				if (client_id == CLOSE_EP) {
					ep_flush(arguments->server_ep, ucp_data_worker);
					ep_close(ucp_data_worker, arguments->server_ep, UCP_EP_CLOSE_MODE_FLUSH);
					pthread_exit(NULL);
				}
		}   

		DPRINT("[STAT WORKER] Terminated stat thread\n");
		ep_flush(arguments->server_ep, ucp_data_worker);
		ep_close(ucp_data_worker, arguments->server_ep, UCP_EP_CLOSE_MODE_FLUSH);
		pthread_exit(NULL);
	}

	//Thread method searching and cleaning nodes with st_nlink=0
	void *
		garbage_collector (void * th_argv)
		{
			//Obtain the current map class element from the set of arguments.
			map_records * map = (map_records *)th_argv;

			for (;;) {
				//Gnodetraverse_garbage_collector(map);//Future
				sleep(GARBAGE_COLLECTOR_PERIOD);
				pthread_mutex_lock(&mutex_garbage);
				map->cleaning();
				pthread_mutex_unlock(&mutex_garbage);
			}
			pthread_exit(NULL);
		}


	void * stat_worker (void * th_argv)
	{
		ucx_server_ctx_t context;
		ucs_status_t     status;
		int              ret;

		p_argv * arguments = (p_argv *) th_argv;

		context.conn_request =  StsQueue.create(); 
		status = start_server(arguments->ucp_worker, &context, &context.listener, NULL, arguments->port);
		if (status != UCS_OK) {
			perror("ERRIMSS_STAR_SERVER");
			pthread_exit(NULL);
		}

		for (;;) {
			p_argv * thread_args = (p_argv *) malloc(sizeof(p_argv));
			pthread_t thread;
			pthread_attr_t attr;
			ucp_conn_request_h req;

			while (StsQueue.size(context.conn_request) == 0) {
				ucp_worker_progress(arguments->ucp_worker);
			}

			req = (ucp_conn_request_h)StsQueue.pop(context.conn_request);


			memcpy(thread_args, th_argv, sizeof(p_argv));
			ret = init_worker(arguments->ucp_context, &(thread_args->ucp_data_worker));
			if (ret != 0) {
				perror("ERRIMSS_INIT_WORKER");
				pthread_exit(NULL);
			}

			status = server_create_ep(thread_args->ucp_data_worker, req, &(thread_args->server_ep));
			if (status != UCS_OK) {
				perror("ERRIMSS_SERVER_CREATE_EP");
				pthread_exit(NULL);
			}

			pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
			if (pthread_create(&thread, &attr, stat_worker_thread, (void *) thread_args) == -1)
			{
				//Notify thread error deployment.
				perror("ERRIMSS_SRVWORKER_DEPLOY");
				pthread_exit(NULL);
			}
		}
		StsQueue.destroy(context.conn_request);
		ucp_listener_destroy(context.listener);
	}

	//Thread method attending client read-write metadata requests.
	void * stat_worker_thread (void * th_argv)
	{   
		ucp_worker_h     ucp_data_worker;
		ucs_status_t     status;
		int              ret;

		//Cast from generic pointer type to p_argv struct type pointer.
		p_argv * arguments = (p_argv *) th_argv;
		//Obtain the current map class element from the set of arguments.
		std::shared_ptr<map_records>  map = arguments->map;

		ucp_data_worker = arguments->ucp_data_worker;

		uint16_t current_offset = 0;

		//Resources specifying if the ZMQ_SNDMORE flag was set in the sender.
		int64_t more;
		size_t more_size = sizeof(more);

		//Code to be sent if the requested to-be-read key does not exist.
		char err_code[] = "$ERRIMSS_NO_KEY_AVAIL$";

		/*struct timeval start, end;
		  long delta_us;*/

		for (;;) { 
			int client_id = 0;
			char req[REQUEST_SIZE];
			char mode[MODE_SIZE];

            DPRINT ("[STAT WORKER] Waiting for new request.\n");
			//Save the identity of the requesting client.
			recv_stream(ucp_data_worker, arguments->server_ep, (char *) &client_id, sizeof(uint32_t));

			/*struct timeval start, end;
			  long delta_us;
			  gettimeofday(&start, NULL);*/

			//Save the request to be served.
			recv_stream(ucp_data_worker, arguments->server_ep, mode, MODE_SIZE);

			if (!strcmp(mode, "GET"))
				more = GET_OP;
			else
				more = SET_OP;

			//Save the request to be served.
			recv_stream(ucp_data_worker, arguments->server_ep, req, REQUEST_SIZE);


			DPRINT ("[STAT WORKER] Request - client_id '%" PRIu32 "', mode '%s', req '%s'\n", client_id, mode, req)
				//Expeted incomming message format: "SIZE_IN_KB KEY"
				int32_t req_size = strlen(req);

			char raw_msg[req_size+1];
			memcpy((void*) raw_msg, req, req_size);
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
			std::string key; 
			key.assign((const char *) uri_);

			//Information associated to the arriving key.
			char * address_;
			uint64_t block_size_rtvd;
			//printf("stat_worker RECV more=%ld, blocksss=%ld\n",more, block_size_recv);
			//Differentiate between READ and WRITE operations. 

			switch (more)
			{
				//No more messages will arrive to the socket.
				case GET_OP:
					{
						switch (block_size_recv)
						{
							case GETDIR:
								{
									//fprintf(stderr,"stat_server GETDIR key=%s\n",key.c_str());
									char * buffer;
									int32_t numelems_indir;
									//Retrieve all elements inside the requested directory.
									pthread_mutex_lock(&tree_mut);
									buffer = GTree_getdir((char *) key.c_str(), &numelems_indir);
									pthread_mutex_unlock(&tree_mut);
									if (buffer == NULL)
									{
										if (send_dynamic_stream(ucp_data_worker, arguments->server_ep, err_code, STRING) < 0)
										{
											perror("ERRIMSS_STATWORKER_NODIR");
											if (client_id == CLOSE_EP) {
												ep_flush(arguments->server_ep, ucp_data_worker);
												ep_close(ucp_data_worker, arguments->server_ep, UCP_EP_CLOSE_MODE_FLUSH);
											}
											pthread_exit(NULL);
										}

										break;
									}

									//Send the serialized set of elements within the requested directory.
									msg_t m;
									m.size = numelems_indir*URI_;
									m.data = buffer;
									char msg [REQUEST_SIZE];
									sprintf(msg,"%ld",m.size);
									if (send_stream(ucp_data_worker, arguments->server_ep, msg, REQUEST_SIZE) < 0)
									{
										perror("ERRIMSS_GETDATA_REQ");
									}
									if (send_dynamic_stream(ucp_data_worker, arguments->server_ep, (char*)&m, MSG) < 0)
									{
										perror("ERRIMSS_WORKER_SENDBLOCK");
										if (client_id == CLOSE_EP) {
											ep_flush(arguments->server_ep, ucp_data_worker);
											ep_close(ucp_data_worker, arguments->server_ep, UCP_EP_CLOSE_MODE_FLUSH);
										}
										pthread_exit(NULL);
									}

									free(buffer);
									break;
								}
							case READ_OP:
								{
									pthread_mutex_lock(&mp);
									//printf("STAT_WORKER READ_OP\n");
									//Check if there was an associated block to the key.
									int err = map->get(key, &address_, &block_size_rtvd);
									DPRINT("[STAT WORKER] map->get (key %s, block_size_rtvd %ld) get res %d\n",key.c_str(),block_size_rtvd, err);

									if(err == 0){
										//Send the error code block.
										if (send_dynamic_stream(ucp_data_worker, arguments->server_ep, err_code, STRING) < 0)
										{
											perror("ERRIMSS_WORKER_SENDERR");
											if (client_id == CLOSE_EP) {
												ep_flush(arguments->server_ep, ucp_data_worker);
												ep_close(ucp_data_worker, arguments->server_ep, UCP_EP_CLOSE_MODE_FLUSH);
											}
											pthread_mutex_unlock(&mp);
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
										msg_t m;
										m.data = address_;
										m.size = block_size_rtvd;
										err = send_dynamic_stream(ucp_data_worker, arguments->server_ep, (char*)&m, MSG);
										if(err < 0){
											perror("ERRIMSS_WORKER_SENDBLOCK");
											if (client_id == CLOSE_EP) {
												ep_flush(arguments->server_ep, ucp_data_worker);
												ep_close(ucp_data_worker, arguments->server_ep, UCP_EP_CLOSE_MODE_FLUSH);
											}
											pthread_mutex_unlock(&mp);
											pthread_exit(NULL);
										}
									}

									pthread_mutex_unlock(&mp);
									break;
								}

							case RELEASE:
								{
									ep_flush(arguments->server_ep, ucp_data_worker);
									ep_close(ucp_data_worker, arguments->server_ep, UCP_EP_CLOSE_MODE_FLUSH);
									pthread_exit(NULL);
									break;
								}
							case DELETE_OP:
								{
									int32_t result = map->delete_metadata_stat_worker(key);
									GTree_delete((char *) key.c_str());

									char release_msg[] = "DELETE\0";

									if (send_stream(ucp_data_worker, arguments->server_ep, release_msg, RESPONSE_SIZE) < 0)
									{
										perror("ERRIMSS_PUBLISH_DELETEMSG");
										if (client_id == CLOSE_EP) {
											ep_flush(arguments->server_ep, ucp_data_worker);
											ep_close(ucp_data_worker, arguments->server_ep, UCP_EP_CLOSE_MODE_FLUSH);
										}
										pthread_exit(NULL);
									}


									break;
								}
							case RENAME_OP:
								{
									std::size_t found = key.find(' ');
									if (found!=std::string::npos){
										std::string old_key = key.substr(0,found);
										std::string new_key = key.substr(found+1,key.length());

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

									if (send_stream(ucp_data_worker, arguments->server_ep, release_msg, RESPONSE_SIZE) < 0)
									{
										perror("ERRIMSS_PUBLISH_RENAMEMSG");
										if (client_id == CLOSE_EP) {
											ep_flush(arguments->server_ep, ucp_data_worker);
											ep_close(ucp_data_worker, arguments->server_ep, UCP_EP_CLOSE_MODE_FLUSH);
										}
										pthread_exit(NULL);
									}
									break;
								}
							case RENAME_DIR_DIR_OP:
								{
									std::size_t found = key.find(' ');
									if (found!=std::string::npos){
										std::string old_dir = key.substr(0,found);
										std::string rdir_dest = key.substr(found+1,key.length());

										//RENAME MAP
										map->rename_metadata_dir_stat_worker(old_dir,rdir_dest);

										//RENAME TREE
										GTree_rename_dir_dir((char *)old_dir.c_str(),(char *)rdir_dest.c_str());
									}

									char release_msg[] = "RENAME\0";

									if (send_stream(ucp_data_worker, arguments->server_ep, release_msg, RESPONSE_SIZE) < 0)
									{
										perror("ERRIMSS_PUBLISH_RENAMEMSG");
										if (client_id == CLOSE_EP) {
											ep_flush(arguments->server_ep, ucp_data_worker);
											ep_close(ucp_data_worker, arguments->server_ep, UCP_EP_CLOSE_MODE_FLUSH);
										}
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
				case SET_OP:
					{
						DPRINT("[STAT WORKER] Creating dataset %s.\n",key.c_str());
						pthread_mutex_lock(&mp);
						//If the record was not already stored, add the block.
						if (!map->get(key, &address_, &block_size_rtvd))
						{
							//Receive the block into the buffer.
							char * buffer = (char *) malloc(block_size_recv);
							DPRINT("[STAT WORKER] Recv dynamic buffer size %ld\n", block_size_recv);
							recv_dynamic_stream(ucp_data_worker, arguments->server_ep, buffer, BUFFER);
							DPRINT("[STAT WORKER] END Recv dynamic \n");

							int32_t insert_successful;
							insert_successful = map->put(key, buffer, block_size_recv);
							DPRINT("[STAT WORKER] map->put (key %s) err %d\n",key.c_str(), insert_successful);

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
								if (client_id == CLOSE_EP) {
									ep_flush(arguments->server_ep, ucp_data_worker);
									ep_close(ucp_data_worker, arguments->server_ep, UCP_EP_CLOSE_MODE_FLUSH);
								}
								pthread_exit(NULL);
							}
							//Update the pointer.
							arguments->pt += block_size_recv;
							DPRINT("[STAT WORKER] Created dataset %s.\n",key.c_str());

						}
						//If was already stored:
						else
						{
							//Follow a certain behavior if the received block was already stored.
							DPRINT("[STAT WORKER] LOCAL DATASET_UPDATE %ld\n", block_size_recv);
							switch (block_size_recv)
							{
								//Update where the blocks of a LOCAL dataset have been stored.
								case LOCAL_DATASET_UPDATE:
									{
										char data_ref[REQUEST_SIZE];
										if (recv_stream(ucp_data_worker, arguments->server_ep, data_ref, REQUEST_SIZE) < 0) 
										{
											perror("ERRIMSS_WORKER_DATALOCATRECV");
											if (client_id == CLOSE_EP) {
												ep_flush(arguments->server_ep, ucp_data_worker);
												ep_close(ucp_data_worker, arguments->server_ep, UCP_EP_CLOSE_MODE_FLUSH);
											}
											pthread_exit(NULL);
										}

										uint32_t data_size = RESPONSE_SIZE; // MIRAR

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


										//Answer the client with the update.
										DPRINT("[STAT_WORKER] Updating existing dataset %s.\n", key.c_str());
										char answer[] = "UPDATED!\0";
										if (send_stream(ucp_data_worker, arguments->server_ep, answer, RESPONSE_SIZE) < 0)
										{
											perror("ERRIMSS_WORKER_DATALOCATANSWER2");
											if (client_id == CLOSE_EP) {
												ep_flush(arguments->server_ep, ucp_data_worker);
												ep_close(ucp_data_worker, arguments->server_ep, UCP_EP_CLOSE_MODE_FLUSH);
											}
											pthread_exit(NULL);
										}

										break;
									}

								default:
									{
										DPRINT("[STAT_WORKER] Updating existing dataset %s.\n", key.c_str());
										//Clear the corresponding memory region.
										char * buffer = (char *) malloc(block_size_recv);
										//Receive the block into the buffer.
										recv_dynamic_stream(ucp_data_worker, arguments->server_ep, buffer, BUFFER);
										free(buffer);
										break;
									}
							}
						}
						pthread_mutex_unlock(&mp);
						break;
					}
				default:
					break;
			}
			if (client_id == CLOSE_EP) {
				ep_flush(arguments->server_ep, ucp_data_worker);
				ep_close(ucp_data_worker, arguments->server_ep, UCP_EP_CLOSE_MODE_FLUSH);
				pthread_exit(NULL);
			} 
		}   
		ep_flush(arguments->server_ep, ucp_data_worker);
		ep_close(ucp_data_worker, arguments->server_ep, UCP_EP_CLOSE_MODE_FLUSH);
		pthread_exit(NULL);
	}

	//Server dispatcher thread method.
	void * srv_attached_dispatcher(void * th_argv)
		{
			//Cast from generic pointer type to p_argv struct type pointer.
			p_argv * arguments = (p_argv *) th_argv;

			ucx_server_ctx_t context;
			ucp_worker_h     ucp_data_worker;
			ucp_am_handler_param_t param;
			ucp_ep_h         server_ep;
			ucs_status_t     status;
			int              ret;

			//Variable specifying the ID that will be granted to the next client.
			uint32_t client_id_ = 0;
			char req[256];

			ret = init_worker(arguments->ucp_context, &ucp_data_worker);
			if (ret != 0) {
				perror("ERRIMSS_INIT_WORKER");
				pthread_exit(NULL);
			}

			/* Initialize the server's context. */
			context.conn_request =  StsQueue.create();
			status = start_server(arguments->ucp_worker, &context, &context.listener, NULL, arguments->port);
			if (status != UCS_OK) {
				perror("ERRIMSS_STAR_SERVER");
				pthread_exit(NULL);
			}


			for (;;)
			{
				ucp_conn_request_h conn_req;
				DPRINT("[DATA DISPATCHER] Waiting for connection requests.\n");

				while (StsQueue.size(context.conn_request) == 0) {
					ucp_worker_progress(arguments->ucp_worker);
				}

				conn_req = (ucp_conn_request_h)StsQueue.pop(context.conn_request);

				status = server_create_ep(ucp_data_worker, conn_req, &server_ep);
				if (status != UCS_OK) {
					perror("ERRIMSS_SERVER_CREATE_EP");
					ep_flush(server_ep, ucp_data_worker);
					ep_close(ucp_data_worker, server_ep, UCP_EP_CLOSE_MODE_FLUSH);
					pthread_exit(NULL);
				}

				//Save the identity of the requesting client.
				recv_stream(ucp_data_worker, server_ep, (char*)&client_id_, sizeof(uint32_t));
				char mode[MODE_SIZE];
				if (recv_stream(ucp_data_worker, server_ep, mode, MODE_SIZE) < 0)
				{
					perror("ERRIMSS_WORKER_SENDCLIENTID");
					ep_flush(server_ep, ucp_data_worker);
					ep_close(ucp_data_worker, server_ep, UCP_EP_CLOSE_MODE_FLUSH);
					pthread_exit(NULL);
				}
				recv_stream(ucp_data_worker, server_ep, req, REQUEST_SIZE);

				uint32_t c_id = client_id_;

				//Check if the client is requesting connection resources.
				if (!strncmp(req, "HELLO!", 6))
				{
					if (strncmp(req, "HELLO!JOIN", 10) != 0)
					{
						//Retrieve the buffer size that will be asigned to the current server process.
						char buff[6];
						sscanf(req, "%s %ld %s", buff, &buffer_KB, att_imss_uri);
						strcpy(arguments->my_uri, att_imss_uri);

						//printf("MU URI: %s\n", att_imss_uri);

						//Notify that the value has been received.
						pthread_mutex_lock(&buff_size_mut);
						copied = 1;
						pthread_cond_signal(&buff_size_cond);
						pthread_mutex_unlock(&buff_size_mut);
					}

					//Message containing the client's communication ID plus its connection port.
					char response_[RESPONSE_SIZE]; 
					memset(response_, '\0', RESPONSE_SIZE);
					//Port that the new client will be forwarded to.
					int32_t port_ = arguments->port + 1 + (client_id_ % THREAD_POOL);
					//Wrap the previous info into the ZMQ message.
					sprintf(response_, "%d%c%d", port_, '-', client_id_++);

					//Send communication specifications.
					if (send_stream(ucp_data_worker, server_ep, response_, RESPONSE_SIZE) < 0)
					{
						perror("ERRIMSS_SRVDISP_SENDBLOCK");
						ep_flush(server_ep, ucp_data_worker);
						ep_close(ucp_data_worker, server_ep, UCP_EP_CLOSE_MODE_FLUSH);
						pthread_exit(NULL);
					}


				    DPRINT("[DATA DISPATCHER] Replied client %s.\n",response_);
					continue;
				}
				//Check if someone is requesting identity resources.
				else if (*((int32_t *) req) == WHO) // MIRAR
				{
					//Provide the uri of this instance.
					if (send_stream(ucp_data_worker, server_ep, arguments->my_uri, RESPONSE_SIZE) < 0) // MIRAR
					{
						perror("ERRIMSS_WHOREQUEST");
						ep_flush(server_ep, ucp_data_worker);
						ep_close(ucp_data_worker, server_ep, UCP_EP_CLOSE_MODE_FLUSH);
						pthread_exit(NULL);
					}
				}
				//context.conn_request = NULL;
				ep_close(ucp_data_worker, server_ep, UCP_EP_CLOSE_MODE_FLUSH);
			}
			pthread_exit(NULL);
		}

	//Metadata dispatcher thread method.
	void * dispatcher(void * th_argv)
		{
			ucx_server_ctx_t context;
			ucp_worker_h     ucp_data_worker;
			ucp_am_handler_param_t param;
			ucs_status_t     status;
			int              ret;

			//Cast from generic pointer type to p_argv struct type pointer.
			p_argv * arguments = (p_argv *) th_argv;

			uint32_t client_id_ = 0;
			char req[REQUEST_SIZE];

			ret = init_worker(arguments->ucp_context, &ucp_data_worker);
			if (ret != 0) {
				perror("ERRIMSS_INIT_WORKER");
				pthread_exit(NULL);
			}

			/* Initialize the server's context. */
			context.conn_request =  StsQueue.create();


			status = start_server(arguments->ucp_worker, &context, &context.listener, NULL, arguments->port);
			if (status != UCS_OK) {
				perror("ERRIMSS_STAR_SERVER");
				pthread_exit(NULL);
			}

			for (;;)
			{
				ucp_ep_h server_ep;
				ucp_conn_request_h conn_req;
				DPRINT("[DISPATCHER] Waiting for connection requests.\n");

				while (StsQueue.size(context.conn_request) == 0) {
					ucp_worker_progress(arguments->ucp_worker);
				}

				conn_req = (ucp_conn_request_h)StsQueue.pop(context.conn_request);

				status = server_create_ep(ucp_data_worker, conn_req, &server_ep);
				if (status != UCS_OK) {
					perror("ERRIMSS_SERVER_CREATE_EP");
					pthread_exit(NULL);
				}

				//Save the identity of the requesting client.
				recv_stream(ucp_data_worker, server_ep, (char *)&client_id_, sizeof(uint32_t));

				char mode[MODE_SIZE];
				recv_stream(ucp_data_worker, server_ep, mode, MODE_SIZE);

				//Save the request to be served.
				recv_stream(ucp_data_worker, server_ep, req, REQUEST_SIZE);

				//Check if the client is requesting connection resources.
				if (!strncmp(req, "HELLO!", 6))
				{
					//Message containing the client's communication ID plus its connection port.
					char response_[RESPONSE_SIZE]; 
					memset(response_, '\0', RESPONSE_SIZE);
					//Port that the new client will be forwarded to.
					int32_t port_ = arguments->port + 1 + (client_id_ % THREAD_POOL);
					//Wrap the previous info into the ZMQ message.

					sprintf(response_, "%d%c%" PRIu32 "", port_, '-', client_id_);


					//Send communication specifications.
					if (send_stream(ucp_data_worker, server_ep, response_, RESPONSE_SIZE) < 0)
					{
						perror("ERRIMSS_STATDISP_SENDBLOCK");
						pthread_exit(NULL);
					}
				    DPRINT("[DISPATCHER] Replied client %s.\n",response_);
				}
				//Check if someone is requesting identity resources.
				else if (*((int32_t *) req) == WHO)
				{
					//Provide the uri of this instance.
					if (send_stream(ucp_data_worker, server_ep, arguments->my_uri, RESPONSE_SIZE) < 0)
					{
						perror("ERRIMSS_WHOREQUEST");
						pthread_exit(NULL);
					}
				    DPRINT("[DISPATCHER] Replied client %s.\n", arguments->my_uri);
				}

				/* Reinitialize the server's context to be used for the next client */
				ep_close(ucp_data_worker, server_ep, UCP_EP_CLOSE_MODE_FLUSH);
			}

			pthread_exit(NULL);
		}
