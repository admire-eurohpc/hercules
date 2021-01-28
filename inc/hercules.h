#ifndef HERCULES_INMEMORY
#define HERCULES_INMEMORY

#define KB 	1024

#include "imss.h"


/* Method initializing an instance of the HERCULES in-memory storage system.

	RECEIVES:	rank 		   - Integer identifying the current application process among the application itself. 
					     WARNING: one rank variable must take the '0' value as it will be in charge of the metadata server deployment.
			backend_strg_size  - Storage size in KILOBYTES assigned to each Hercules instance.
			server_port_num    - Port that a future IMSS deployment will be binding to.
			metadata_port_num  - Port that the IMSS metadata server will be binding to.
			metadata_buff_size - Storage size in KILOBYTES assigned to the IMSS metadata server.
			metadata_file      - File that will be accessed in order to retrieve previous metadata structures (dataset and IMSS structures).

	RETURNS: 	 0 - Successful deployment.
			-1 - In case of error.
*/
int32_t hercules_init(int32_t rank, uint64_t backend_strg_size, uint16_t server_port_num, uint16_t metadata_port_num, uint64_t metadata_buff_size, char * metadata_file);


/* Method releasing an instance of the HERCULES in-memory storage system.

	RECEIVES:	rank - Process identifier provided to the previous 'hercules_init' method.

	RETURNS: 	 0 - Release operation performed successfully.
			-1 - In case of error.
*/
int32_t hercules_release(int32_t rank);


#endif
