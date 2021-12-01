#ifndef WORKER_H
#define WORKER_H

#include "imss.h"
#include "records.hpp"

#define READ_OP			0
#define RELEASE			2
#define WHO			    3
#define WRITE_OP		1

#define GETDIR			1

#define THREAD_POOL		3

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
    //URI assigned to the current IMSS instance.
    char my_uri[URI_];

} p_argv;


//Thread method attending client data requests.
void * srv_worker (void * th_argv);

//Thread method attending client metadata requests.
void * stat_worker (void * th_argv);

//Dispatcher thread method distributing clients among the pool server threads.
void * srv_attached_dispatcher (void * th_argv);

//Dispatcher thread method distributing clients among the pool of metadata server threads.
void * dispatcher (void * th_argv);

#endif
