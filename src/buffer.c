#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include <mpi.h>
#include <sys/signal.h>
#include <utility>
#include "th_workers.h"
#include "p_argv.h"
#include "records.hpp"
#include "memalloc.h"

#define KB 		1024

//argv[1] = PORT_NUM	argv[2] = BUFFER_SIZE (GB)

//Pointer to the allocated memory.
unsigned char *pt;

//SIGINT & SIGKILL signal handler.
void exit_server(int32_t)
{
	printf("IN SIG HANDLER\n");
	//Free the memory buffer.
	free(pt);

	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();	

	exit(0);
}

int32_t main (int32_t argc, char **argv) 
{
	MPI_Init(&argc, &argv);

	//Specify SIGINT signal behaviour.
	signal(SIGINT, exit_server);
	//Specify SIGKILL signal behaviour.
	signal(SIGKILL, exit_server);

        int32_t world_size, pid;
	//Obtain identifier inside the group.
        MPI_Comm_rank(MPI_COMM_WORLD, &pid);
        //Obtain the current number of processes in the group.
	MPI_Comm_size(MPI_COMM_WORLD, &world_size);

	//Initial port number.
	int32_t port = atoi(argv[1]);
	//Map tracking saved records.
	map_records map;

	//Size of the buffer involved.
	uint64_t size = (uint64_t) atoi(argv[2])*KB*KB*KB;
	//Check if the requested data is available in the current node.
	int64_t data_reserved = memalloc(size, &pt);

	if (data_reserved == -1)

		return -1;

	//Initialize pool of threads.
	pthread_t threads[(THREAD_POOL+1)];
	//Thread arguments.
	p_argv arguments[(THREAD_POOL+1)];

	//Execute all threads.
	for (int32_t i = 0; i < (THREAD_POOL+1); i++)
	{
		//Add port number to set of thread arguments.
		arguments[i].port = port++;

		//Deploy all dispatcher + service threads.
		if (!i)
		{
			//Deploy a thread distributing incomming clients among all ports.
			if (pthread_create(&threads[i], NULL, dispatcher, (void *) &arguments[i]) == -1)
			{
				//Notify thread error deployment.
				perror("ERRIMSS_BUFF_DISP_DPLY");
				return -1;
			}
		}
		else
		{
			//Add the reference to the map into the set of thread arguments.
			arguments[i].map = &map;
			//Specify the address used by each thread to write inside the buffer.
			arguments[i].pt = (unsigned char *) ((i-1)*(data_reserved/THREAD_POOL) + pt);

			//Throw thread with the corresponding function and arguments.
			if (pthread_create(&threads[i], NULL, worker, (void *) &arguments[i]) == -1)
			{
				//Notify thread error deployment.
				perror("ERRIMSS_BUFF_THREAD_DPLY");
				return -1;
			}
		}
	}

	//Wait for threads to finish.
	for (int32_t i = 0; i < (THREAD_POOL+1); i++)
	{
		if (pthread_join(threads[i], NULL) != 0)
		{
			//Notify thread join error.
			perror("ERRIMSS_BUFF_JOIN");
			return -1;
		}
	}

	//TODO: add persistent storage.

	free(pt);

	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();	

	return 0;
}

