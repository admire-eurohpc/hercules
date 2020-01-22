#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include "policies.h"
#include "crc.h"

//Default session policy: ROUND ROBIN.
int32_t session_plcy = ROUND_ROBIN_;
//Number of blocks to be sent.
int32_t n_blocks;
//Socket connecting the client to the imss server running in the same node.
int32_t matching_node_socket;

int32_t set_policy (const char * policy, int32_t blocks, int32_t matching_node_conn)
{
	//Invalid number of blocks to be sent.
	if (blocks <= 0)

		return -1;

	//Save the connection to the imss server running in the same node.
	matching_node_socket = matching_node_conn;

	//Set blocks to be sent.
	n_blocks = blocks;

	//Set the corresponding policy.
	if (!strcmp(policy, "RR"))
	{
		session_plcy = 0;
	}
	else if (!strcmp(policy, "BUCKETS"))
	{
		session_plcy = 1;
	}
	else if (!strcmp(policy, "HASH"))
	{ 
		session_plcy = 2;
	}
	else if (!strcmp(policy, "CRC16b"))
	{
		session_plcy = 3;
	}
	else if (!strcmp(policy, "CRC64b"))
	{
		session_plcy = 4;
	}
	else if (!strcmp(policy, "LOCAL"))
	{
		session_plcy = 5;
	}
	else
	{
		perror("ERR_SETPLCY_INVLD");
		return -1;
	}

	return 0;
}

//Method retrieving the server that will receive the following message attending a policy.
int32_t find_server (int32_t n_servers, uint64_t n_msg, const char * fname)
{
	uint64_t next_server = -1;

	switch (session_plcy)
	{
		//Follow a round robin policy.
		case ROUND_ROBIN_:
		{
			uint16_t crc_ = crc16(fname, strlen(fname));

			//First server that received a block from the current file.
			next_server = crc_ % n_servers;

			//Next server receiving the following block.
			next_server = (next_server + n_msg) % n_servers;
		}
			break;

		//Follow a bucketbnn distribution.
		case BUCKETS_: 
		{
			uint16_t crc_ = crc16(fname, strlen(fname));

			//First server that received a block from the current file.
			uint32_t initial_server = crc_ % n_servers;

			//Number of the server from the first one receiving the current message.
			next_server = (n_msg / (n_blocks / n_servers)) % n_servers;

			//Actual server receiving the next message.
			next_server = (next_server + initial_server) % n_servers;
		}
			break;

		//Follow a hashed distribution.
		case HASHED_:
		{
			//Key identifying the current to-be-sent file block.
			char key[strlen(fname) + 64];
			sprintf(key, "%s%c%lu", fname, '$', n_msg);

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
			break;

		//Following another hashed distribution using Redis's CRC16.
		case CRC16_:
		{
			//Key identifying the current to-be-sent file block.
			char key[strlen(fname) + 64];
			sprintf(key, "%s%c%lu", fname, '$', n_msg);
			next_server = crc16(key, strlen(key)) % n_servers;
		}

			break;

		//Following another hashed distribution using Redis's CRC64.
		case CRC64_:
		{
			//Key identifying the current to-be-sent file block.
			char key[strlen(fname) + 64];
			sprintf(key, "%s%c%lu", fname, '$', n_msg);
			next_server = crc64(0, (unsigned char *) key, strlen(key)) % n_servers;
		}

			break;

		//Follow a locality distribution.
		case LOCAL_:
		{
			next_server = matching_node_socket;
		}

		default:

			break;
	}

	return next_server;
}
