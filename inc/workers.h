#ifndef WORKER_H
#define WORKER_H

#include "records.hpp"

#define READ_OP			0
#define WRITE_OP		1

#define THREAD_POOL		4

#define LOCAL_DATASET_UPDATE	0

#define KB			1024


//Set of arguments passed to each server thread.
typedef struct {

	//Pointer to the corresponding type storing key-address couples.
	map_records * map = NULL;
	//Pointer to the corresponding buffer region assigned to a thread. 
	unsigned char * pt;
	//Integer specifying the port that a certain thread will listen to.
	uint16_t port;

} p_argv;


//Thread method attending client data requests.
void * worker (void * th_argv);

//Dispatcher thread method distributing clients among a pool threads.
void * dispatcher (void * th_argv);

#endif

