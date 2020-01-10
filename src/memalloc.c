#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "memalloc.h"


// Method allocating a certain memory region to a buffer process.
//int64_t memalloc (int64_t req_mem, int32_t block_size, unsigned char ** reference)
int64_t memalloc (int64_t req_mem, unsigned char ** reference)
{
	// Available memory in the system.
	int64_t mem_avail;
	// Process status.
	int status;
	// Pipe communicating both processes.
	int pp[2];
	
	if (pipe(pp) == -1)
	{
		perror("ERR_MEMALLOC_PIPE");
		return -1;
	}

	pid_t pid_ = fork();

	// Check if the current process is the parent one.
	if (pid_ > 0)
	{
		// Redirect the standard input to the pipe.
		close(0);
		dup(pp[0]);
		close(pp[0]);
		close(pp[1]);
		// Wait for the child process.
		wait(&status);
		// Read AWK command's result.
		scanf("%ld", &mem_avail);
	}
	// Check if the current process is the child one.
	else if (!pid_)
	{
		// Redirect the standard output to the pipe.
		close(1);
		dup(pp[1]);
		close(pp[0]);
		close(pp[1]);

		// Execute the AWK command retrieving the remaining free memory space.
		if (execl("/usr/bin/awk", "awk", "/MemFree/ { printf $2 }", "/proc/meminfo", NULL) == -1)
		{
			perror("ERR_MEMALLOC_EXECL");
			return -1;
		}
	}
	else
	{
		perror("ERR_MEMALLOC_FORK");
		return -1;
	}

	// Multiply available memory by 1024 as information in /proc/meminfo is in KB.
	mem_avail *= 1024;

	// Check if the remaining free memory is enough to handle the requested.
	if (mem_avail >= req_mem)
	{
		// Reserve the requested memory amount.
		*reference = (unsigned char *) malloc(req_mem * sizeof(char));
	}
	else
	{
		//FIXME: set a policy when the memory requested is bigger than the available.

		// Assign half of the remaining free available memory.
		req_mem = mem_avail * 0.5;

		//req_mem -= (req_mem % block_size);

		*reference = (unsigned char *) malloc(req_mem * sizeof(char));
	}

	return req_mem;
}
