#ifndef POLICIES_
#define POLICIES_

#define ROUND_ROBIN_		0
#define BUCKETS_		1
#define HASHED_			2
#define CRC16_			3
#define CRC64_			4
#define LOCAL_ 			5

#define GET			6
#define SET			7

#include "imss.h"

//Method specifying the policy.
int32_t set_policy  (dataset_info * dataset);

/**
 * @brief Method retrieving the server that will receive the following message attending a policy.
 * @return next server number (positive integer, >= 0) to send the operation according to the policy, on error -1 is returned, 
*/
int32_t find_server (int32_t n_servers, int32_t n_msg, const char * fname, int32_t op_type);

#endif
