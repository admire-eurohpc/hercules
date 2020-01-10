#ifndef ARG_STRUCT_H
#define ARG_STRUCT_H

#include "records.hpp"

#define KB		1024


//Set of arguments passed to each server thread.
typedef struct {

	//Pointer to the corresponding type storing key-address couples.
	map_records * map = NULL;
	//Pointer to the corresponding buffer region assigned to a thread. 
	unsigned char * pt;
	//Integer specifying the port that a certain thread will listen to.
	uint16_t port;

} p_argv;

#endif
