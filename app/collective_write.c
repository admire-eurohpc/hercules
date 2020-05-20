#include <mpi.h>
#include <chrono>
#include <time.h>
#include <iostream>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include "imss.h"


//argv[1] = METADATA_PORT_NUM		argv[2] = POLICY
//argv[3] = DATASET_SIZE (KB)		argv[4] = BLOCK_SIZE (KB)
//argv[5] = STAT_IP			argv[6] = IMSS_URI
//argv[7] = DATASET_NAME


#define KB		1024

int32_t main (int32_t argc, char **argv) 
{
	int provide;
	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provide);

        int32_t world_size, rank;
	//Obtain identifier inside the group.
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        //Obtain the current number of processes in the group.
	MPI_Comm_size(MPI_COMM_WORLD, &world_size);

	using clk = std::chrono::high_resolution_clock;



	int32_t  stat_port 	 = atoi(argv[1]);
	char *   policy		 = argv[2];
	int32_t  dataset_size	 = atoi(argv[3]);
	int32_t  dtset_blck_size = atoi(argv[4]);
	int32_t  num_blocks	 = (dataset_size/dtset_blck_size);
	char *	 ip 	    	 = argv[5];
	char *   imss_uri	 = argv[6];
	char *   dataset_name 	 = argv[7];

	int32_t  offset = num_blocks/world_size;
	int32_t  init	= offset*rank;
	int32_t  end	= offset*(rank+1);




	if (stat_init(ip, stat_port, rank) == -1)
		return -1;

	int32_t  datasetd;

	/************************************************************************/
	/*************************** COLLECTIVE WRITE ***************************/
	/************************************************************************/

	auto t1 = clk::now();

		if (open_imss(imss_uri) == -1)
			return -1;

		if (!rank)
				if ((datasetd = create_dataset(dataset_name, policy, num_blocks, dtset_blck_size)) < 0)
					return -1;

		MPI_Barrier(MPI_COMM_WORLD);
		if (rank)
				if ((datasetd = open_dataset(dataset_name)) == -1)
					return -1;

		for (int32_t i = init; i < end; i++)
		{
			char fixed = (i%30) + 97;
			unsigned char * buffer = (unsigned char *) malloc(dtset_blck_size * KB);
			memset(buffer, fixed, dtset_blck_size * KB);

			if (set_data(datasetd, i, buffer))
				return -1;
		}
		if (release_dataset(datasetd) == -1)
			return -1;
		if (release_imss(imss_uri) == -1)
			return -1;

	auto t2 = clk::now();
	auto diff = std::chrono::duration_cast<std::chrono::microseconds>(t2-t1);
	double result = (((dataset_size/world_size)/KB)/(diff.count()/1000000.0));

	double *pieces = (double *) calloc(world_size, sizeof(double));
	MPI_Gather(&result, 1, MPI_DOUBLE, pieces, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
	if (rank == 0)
	{
		double total = 0.0;
		for (int i = 0; i < world_size; i++)

			total += pieces[i];

		std::cout << (total) << ",";
	}
	free(pieces);

	MPI_Barrier(MPI_COMM_WORLD);

	if (dataset_size == 4194304)

		sleep(50);

	else
		sleep(15);

	/************************************************************************/
	/*************************** COLLECTIVE READ ****************************/
	/************************************************************************/

	t1 = clk::now();

		if (open_imss(imss_uri) == -1)
			return -1;
		if ((datasetd = open_dataset(dataset_name)) == -1)
			return -1;
		for (int32_t i = init; i < end; i++)
		{
			unsigned char * incomming = (unsigned char *) malloc(dtset_blck_size * KB);
			if (get_data(datasetd, i, incomming) == -1)
				return -1;

			if (incomming[0] == '$')
				return -1;

			free(incomming);
		}
		if (release_dataset(datasetd) == -1)
			return -1;
		if (release_imss(imss_uri) == -1)
			return -1;

	t2 = clk::now();
	diff = std::chrono::duration_cast<std::chrono::microseconds>(t2-t1);

	result = (((dataset_size/world_size)/KB)/(diff.count()/1000000.0));

	pieces = (double *) calloc(world_size, sizeof(double));
	MPI_Gather(&result, 1, MPI_DOUBLE, pieces, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
	if (rank == 0)
	{
		double total = 0.0;
		for (int i = 0; i < world_size; i++)

			total += pieces[i];

		std::cout << (total) << ",";
	}
	free(pieces);

	/************************************************************************/
	/************************** RELEASE RESOURCES ***************************/
	/************************************************************************/

	if (stat_release() == -1)
		return -1;

	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();	

	return 0;
}

