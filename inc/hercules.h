#ifndef HERCULES_INMEMORY
#define HERCULES_INMEMORY

#define KB 	1024

#include "imss.h"


/* Method initializing an instance of the HERCULES in-memory storage system.

	RECEIVES:	rank 		       - Integer identifying the current application process among the application itself. 
                backend_strg_size  - Storage size in KILOBYTES assigned to each Hercules instance.
                server_port_num    - Port that a future IMSS deployment will be binding to.
                deploy_metadata    - Flag determining if a metadata server shall be deployed in the current process.
                metadata_port_num  - Port that the IMSS metadata server will be binding to.
                metadata_buff_size - Storage size in KILOBYTES assigned to the IMSS metadata server.
                metadata_file      - File that will be accessed in order to retrieve previous metadata structures (dataset and IMSS structures).

	RETURNS: 	 0 - Successful deployment.
                -1 - In case of error.
*/
int32_t hercules_init(uint32_t rank, uint64_t backend_strg_size, uint16_t server_port_num, int32_t deploy_metadata, uint16_t metadata_port_num, uint64_t metadata_buff_size, char * metadata_file);


/* Method releasing an instance of the HERCULES in-memory storage system.

	RETURNS: 	 0 - Release operation performed successfully.
                -1 - In case of error.
*/
int32_t hercules_release();


#endif
