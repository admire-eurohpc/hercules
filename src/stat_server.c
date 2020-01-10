#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include <mpi.h>
#include <sys/signal.h>
#include <string.h>
#include "stat.h"
#include "p_argv.h"
#include "imss_data.h"
#include "memalloc.h"
#include "th_workers.h"


//argv[1] = PORT_NUM	argv[2] = BUFFER_SIZE (GB)	argv[3] = METADATA_FILE


//Pointer to the allocated memory.
unsigned char *pt;
//Map tracking saved records.
map_records map;
//File path specifying the metadata file.
char * metadata_path;

//SIGINT & SIGKILL signal handler.
void exit_server(int32_t foo)
{
	//Save the current metadata information into a file.
	if (metadata_write(metadata_path, &map, pt) == -1)

		exit(-1);

	free(metadata_path);
	free(pt);

	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();	

	exit(0);
}

int32_t main (int32_t argc, char **argv) 
{
	//Specify SIGINT signal behaviour.
	signal(SIGINT, exit_server);
	//Specify SIGKILL signal behaviour.
	signal(SIGKILL, exit_server);

	MPI_Init(&argc, &argv);

	//Initial port number.
	int32_t port = atoi(argv[1]);
	//Size of the buffer involved.
	uint64_t buff_size = (uint64_t) atoi(argv[2])*KB*KB*KB;
	//Check if the requested data is available in the current node.
	int64_t data_reserved = memalloc(buff_size, &pt);

	if (data_reserved == -1)

		return -1;

	//Address pointing to the end of the last metadata record.
	unsigned char * offset = pt;

	//Read the metadata file if it was provided.
	metadata_path = (char *) malloc(strlen(argv[3]) * sizeof(char) + 1);
	metadata_path[strlen(metadata_path)-1] = '\0';
	strcpy(metadata_path, argv[3]);

	//Metadata bytes written into the buffer.
	uint64_t bytes_written;
	
	if ((offset = metadata_read(metadata_path, &map, pt, &bytes_written)) == NULL)

		return -1;

	//Obtain the remaining free amount of data reserved to the buffer after the metadata read operation.
	data_reserved -= bytes_written;

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
				perror("ERRIMSS_MTDT_DISP_DPLY");
				return -1;
			}
		}
		else
		{
			//Add the reference to the map into the set of thread arguments.
			arguments[i].map = &map;
			//Specify the address used by each thread to write inside the buffer.
			arguments[i].pt = (unsigned char *) ((i-1)*(data_reserved/THREAD_POOL) + offset);

			//Throw thread with the corresponding function and arguments.
			if (pthread_create(&threads[i], NULL, worker, (void *) &arguments[i]) == -1)
			{
				//Notify thread error deployment.
				perror("ERRIMSS_MTDT_THREAD_DPLY");
				return -1;
			}
		}
	}

	//Wait for the threads to conclude.
	for (int32_t i = 0; i < (THREAD_POOL+1); i++)
	{
		if (pthread_join(threads[i], NULL) != 0)
		{
			//Notify thread join error.
			perror("ERRIMSS_MTDT_THREAD_JOIN");
			return -1;
		}
	}

	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();	

	return 0;
}

