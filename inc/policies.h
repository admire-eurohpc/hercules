#ifndef POLICIES_
#define POLICIES_

#define ROUND_ROBIN_		0
#define BUCKETS_		1
#define HASHED_			2
#define CRC16_			3
#define CRC64_			4
#define LOCAL_ 			5

//Method specifying the policy.
int32_t set_policy (const char * policy, int32_t blocks, int32_t matching_node_conn);

//Method retrieving the server that will receive the following message attending a policy.
int32_t find_server (int32_t n_servers, uint64_t n_msg, const char * fname);

#endif
