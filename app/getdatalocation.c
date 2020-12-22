#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "imss.h"


//argv[1] = METADATA_PORT_NUM		argv[2] = STAT_IP
//argv[3] = DATASET_NAME		argv[4] = DATA_BLOCK_NUM


int32_t main (int32_t argc, char **argv) 
{
	int32_t  stat_port 	 = atoi(argv[1]);
	char *	 ip 	    	 = argv[2];
	char *   dataset_name 	 = argv[3];
	int32_t  block_number	 = atoi(argv[4]);



	/***********************************************************************************************/
	/********* MINIMUM NUMBER OF STEPS REQUIRED TO PERFORM A get_data_location() OPERATION *********/
	/***********************************************************************************************/
	// All in all, a connection to the metadata server is the unique requirement to perform it.



	// 1. Initialize connections to the metadata server.
	if (stat_init(ip, stat_port, 0) == -1)

		return -1;

	char * location;

	// 2. Perform the actual get_data_location() operation.
	if ((location = get_data_location_host(dataset_name, block_number)) == NULL)

		return -1;

	printf("BLOCK %s$%d LOCATED IN: %s\n", dataset_name, block_number, location);

	// 3. Destroy the communication established with the metadata server.
	if (stat_release() == -1)

		return -1;


	return 0;
}

