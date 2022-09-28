#ifndef WORKER_H
#define WORKER_H

#include "imss.h"
#include "records.hpp"
#include <memory>

#define READ_OP			  0
#define GETDIR			  1
#define RELEASE			  2
#define WHO			      3
#define DELETE_OP		  4
#define RENAME_OP		  5
#define RENAME_DIR_DIR_OP 6

#define WRITE_OP		  1
#define WRITEV 			  7
#define READV 			  8
#define SPLIT_READV 	  9
#define SPLIT_WRITEV	 10


#define THREAD_POOL		1

#define GET_OP 0
#define SET_OP 1

#define LOCAL_DATASET_UPDATE	0

#define KB			1024
#define GB 			1073741824


//Set of arguments passed to each server thread.
typedef struct {
	//Pointer to the corresponding type storing key-address couples.
	std::shared_ptr<map_records> map = NULL;
	//Pointer to the corresponding buffer region assigned to a thread. 
	char *        pt;
	//Integer specifying the port that a certain thread will listen to.
	uint16_t      port;
    //URI assigned to the current IMSS instance.
    char          my_uri[URI_];
	int64_t       total_size;
	ucp_context_h ucp_context;
    ucp_worker_h  ucp_worker;
	ucp_worker_h  ucp_data_worker;
	ucp_ep_h      server_ep;
} p_argv;


//Thread method attending client data requests.
void * srv_worker (void * th_argv);
void * srv_worker_slave (void * th_argv);

//Thread method searching and cleaning nodes with st_nlink=0
void * garbage_collector (void * th_argv);

//Thread method attending client metadata requests.
void * stat_worker (void * th_argv);
void * stat_worker_slave (void * th_argv);

//Dispatcher thread method distributing clients among the pool server threads.
void * srv_attached_dispatcher (void * th_argv);

//Dispatcher thread method distributing clients among the pool of metadata server threads.
void * dispatcher (void * th_argv);

#endif
