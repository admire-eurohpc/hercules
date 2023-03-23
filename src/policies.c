#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "crc.h"
#include "policies.h"


//Default session policy: ROUND ROBIN.
int32_t    session_plcy = ROUND_ROBIN_;
//Number of blocks conforming the handled dataset.
int32_t    n_blocks;
//Socket connecting the client to the imss server running in the same node.
int32_t    matching_node_socket;


	/********* LOCAL DATASET MANAGEMENT VARIABLES *********/


//Vector of characters specifying the position of each data element in a LOCAL dataset.
uint16_t * data_locations;
//Number of blocks written by the client in the current session.
uint64_t * num_blocks_written;
//Actual blocks written by the client.
uint32_t * blocks_written;


int32_t  
set_policy (dataset_info * dataset)
{
	//Invalid number of blocks to be sent.
	if (dataset->num_data_elem <= 0)

		return -1;

	//Save the connection to the imss server running in the same node.
	matching_node_socket = dataset->local_conn;

	//Set blocks to be sent.
	n_blocks = dataset->num_data_elem;

	//Set the corresponding policy.
	if (!strcmp(dataset->policy, "RR"))
	{
		session_plcy = 0;
	}
	else if (!strcmp(dataset->policy, "BUCKETS"))
	{
		session_plcy = 1;
	}
	else if (!strcmp(dataset->policy, "HASH"))
	{ 
		session_plcy = 2;
	}
	else if (!strcmp(dataset->policy, "CRC16b"))
	{
		session_plcy = 3;
	}
	else if (!strcmp(dataset->policy, "CRC64b"))
	{	
		session_plcy = 4;
	}
	else if (!strcmp(dataset->policy, "LOCAL"))
	{
		session_plcy = 5;

		//Initialize variables hanlding LOCAL datasets.
		data_locations 		= dataset->data_locations;
		num_blocks_written 	= dataset->num_blocks_written;
		blocks_written 		= dataset->blocks_written;
	}
	else
	{
		
		perror("ERR_SETPLCY_INVLD");
		return -1;
	}

	return 0;
}

//Method retrieving the server that will receive the following message attending a policy.
int32_t
find_server (int32_t 	  n_servers,
	     int32_t 	  n_msg,
	     const char * fname,
	     int32_t 	  op_type)
{
	int32_t next_server = -1;
	slog_debug("[find_server] session_plcy=%ld", session_plcy);
	switch (session_plcy)
	{
		//Follow a round robin policy.
		case ROUND_ROBIN_:
		{
			dataset_info new_dataset;
			//Dataset metadata request.
			int32_t stat_dataset_res = stat_dataset(fname, &new_dataset);
			slog_debug("[find_server] fname=%s, stat_dataset_res=%ld", fname, stat_dataset_res);

			if(stat_dataset_res==0){
				uint16_t crc_ = crc16(fname, strlen(fname));
				//First server that received a block from the current file.
				next_server = crc_ % n_servers;
			}else{
				uint16_t crc_ = crc16(new_dataset.original_name, strlen(new_dataset.original_name));
				next_server = crc_ % n_servers;
			}

			//Next server receiving the following block.
			next_server = (next_server + n_msg) % n_servers;
		}
			break;

		//Follow a bucketbnn distribution.
		case BUCKETS_: 
		{
			uint16_t crc_;
			dataset_info new_dataset;
			//Dataset metadata request.
			int32_t stat_dataset_res = stat_dataset(fname, &new_dataset);
			if(stat_dataset_res==0){
				crc_ = crc16(fname, strlen(fname));
			}else{
				crc_ = crc16(new_dataset.original_name, strlen(new_dataset.original_name));
			}
			
			
			//First server that received a block from the current file.
			// incluir "initial_server" en el dataset.
			uint32_t initial_server = crc_ % n_servers;

			if (n_blocks < n_servers)

				n_blocks = n_servers;

			//Number of servers that will be storing one additional block.
			uint32_t one_more_block = n_blocks % n_servers;

			//Number of blocks that these servers will be storing.
			uint32_t blocks_srv = (n_blocks / n_servers) + 1;

			//The block will be handled by those servers storing one more block.
			if (n_msg < (blocks_srv * one_more_block))
			{
				next_server = n_msg / blocks_srv;

				next_server = (next_server + initial_server) % n_servers;
			}
			//The block will be handled by those storing one block less.
			else
			{
				next_server = (n_msg - (blocks_srv * one_more_block)) / (blocks_srv - 1);

				next_server = (initial_server + one_more_block + next_server) % n_servers;
			}
		}
			break;

		//Follow a hashed distribution.
		case HASHED_:
		{
			dataset_info new_dataset;
			//Dataset metadata request.
			int32_t stat_dataset_res = stat_dataset(fname, &new_dataset);
			if(stat_dataset_res==0){
		
				//Key identifying the current to-be-sent file block.
				char key[strlen(fname) + 64];
				sprintf(key, "%s%c%d", fname, '$', n_msg);

				uint32_t b    	= 378551;
				uint32_t a    	= 63689;
				uint32_t hash 	= 0;
				uint32_t i    	= 0;
				uint32_t length = strlen(key);

				//Create the  hash through the messages's content.
				for (i = 0; i < length; ++i)	
				{
						hash = hash * a + (key[i]);
						a = a * b;
				}

				next_server = hash % n_servers;
			}else{
				//Key identifying the current to-be-sent file block.
				char key[strlen(new_dataset.original_name) + 64];
				sprintf(key, "%s%c%d", new_dataset.original_name, '$', n_msg);

				uint32_t b    	= 378551;
				uint32_t a    	= 63689;
				uint32_t hash 	= 0;
				uint32_t i    	= 0;
				uint32_t length = strlen(key);

				//Create the  hash through the messages's content.
				for (i = 0; i < length; ++i)	
				{
						hash = hash * a + (key[i]);
						a = a * b;
				}

				next_server = hash % n_servers;

			}
		
		}
			break;

		//Following another hashed distribution using Redis's CRC16.
		case CRC16_:
		{
			dataset_info new_dataset;
			//Dataset metadata request.
			int32_t stat_dataset_res = stat_dataset(fname, &new_dataset);
			if(stat_dataset_res==0){

				//Key identifying the current to-be-sent file block.
				char key[strlen(fname) + 64];
				sprintf(key, "%s%c%d", fname, '$', n_msg);
				next_server = crc16(key, strlen(key)) % n_servers;
			}else{
				char key[strlen(new_dataset.original_name) + 64];
				sprintf(key, "%s%c%d", new_dataset.original_name, '$', n_msg);
				next_server = crc16(key, strlen(key)) % n_servers;
			}
		}

			break;

		//Following another hashed distribution using Redis's CRC64.
		case CRC64_:
		{
			dataset_info new_dataset;
			//Dataset metadata request.
			int32_t stat_dataset_res = stat_dataset(fname, &new_dataset);
			if(stat_dataset_res==0){

				//Key identifying the current to-be-sent file block.
				char key[strlen(fname) + 64];
				sprintf(key, "%s%c%d", fname, '$', n_msg);
				next_server = crc64(0, (unsigned char *) key, strlen(key)) % n_servers;
			}else{
				char key[strlen(new_dataset.original_name) + 64];
				sprintf(key, "%s%c%d", new_dataset.original_name, '$', n_msg);
				next_server = crc64(0, (unsigned char *) key, strlen(key)) % n_servers;
			}
		}

			break;

		//Follow a LOCAL distribution.
		case LOCAL_:			//FIXME: improve the following case!
		{
			//Operate in relation to the type of operation.
			switch (op_type)
			{
				//Retrieve the socket connection number associated to the server storing the block.
				case GET:
				{
					if (data_locations[n_msg])

						next_server = (int32_t) (data_locations[n_msg] - 1);

					break;
				}
				//Store the connection number associated to the server storing the data element.
				case SET:
				{
					switch (data_locations[n_msg])
					{
						//If the block was not written yet, write it in the local IMSS server and save the storage event.
						case 0:
						{
							next_server = matching_node_socket;

							data_locations[n_msg] = (uint16_t) (next_server + 1);

							//Save the block number that the client has written.

							blocks_written[(*num_blocks_written)++] = n_msg;

							break;
						}
						//If the block was already stored, retrieve the corresponding server storing it.
						default:
						{
							next_server = (int32_t) (data_locations[n_msg] - 1);

							break;
						}
					}

					break;
				}
			}

			break;
		}

		default:

			break;
	}
	// slog_debug("[find_server] next_server=%ld", next_server);
	return next_server;
}
