#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <mpi.h>
#include <stdio.h>
#include "policies.h"
#include "imss.h"

#define KB 			1024 //KB defined as an integer 32bits
#define CLIENTS_WRITING		4
#define WRITE			0
#define READ			1

//argv[1] = IMSS_HOSTFILE	argv[2] = STAT_IP	argv[3] = STAT_PORT

uint64_t block_size;
uint32_t blocks;
int pid; 
int message_copied = 0;
int n_servers;


int main (int argc, char **argv)
{
	MPI_Init(&argc, &argv);
	//Obtain identifier inside the group.
	MPI_Comm_rank(MPI_COMM_WORLD, &pid);
	//Obtain the current number of processes in the group.
	int world_size; MPI_Comm_size(MPI_COMM_WORLD, &world_size);

	/******************* METADATA SERVER COMMUNICATION *******************/

	if (stat_init(argv[2], (uint16_t) atoi(argv[3])) == -1)

		return -1;

	char imss_uri[] = "imss:/MYIMSS";

	/************************** IMSS DEPLOYMENT **************************/

	if (init_imss(imss_uri, 4, 2, argv[1], 5555) == -1)

		return -1;

	char dataset_uri[] = "imss:/MYIMSS/mydataset";
	char dataset_policy[] = "RR";

	int32_t datasetd;

	/************************** DATASET CREATION *************************/

	if ((datasetd = create_dataset(dataset_uri, dataset_policy, 120, 4)) < 0)

		return -1;

	dataset_info dataset_info_;

	//char fake_dataset[] = "SOMENONEXISTINGDATASET";

	if (stat_dataset(dataset_uri, &dataset_info_) == -1)

		return -1;

	printf("%s\n", dataset_info_.uri_);


	for (int i = 0; i < dataset_info_.num_data_elem; i++)
	{
		/********************* SEND DATA *****************************/

		char fixed = (i%30) + 97;

		unsigned char * buffer = (unsigned char *) malloc(4 * KB);
		memset(buffer, fixed, 4*KB);
		if (set_data(datasetd, i, buffer))

			return -1;

		printf("DATA SENT (%c)\n", *buffer);

		/********************** RECV DATA *****************************/

		unsigned char * incomming = (unsigned char *) malloc(4 * KB);
		int64_t data_received;
		if (get_data(datasetd, i, incomming, &data_received) == -1)

			return -1;

		printf("%d - DATA RECEIVED (%ld B): %s\n", i, data_received, incomming);

		free(incomming);
	}


	if (release_imss(0) == -1)

		return -1;

	if (release_stat() == -1)

		return -1;

	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();	

	return 0;
}
