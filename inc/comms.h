#ifndef COMMS
#define COMMS

#define IMSS_INFO	0
#define DATASET_INFO	1

//Free function provided to zmq_msg_init_data in order to free the buffer once sent.
void free_msg (void * data, void * hint);

//Method sending a data structure with dynamic memory allocation fields.
int32_t send_dynamic_struct(void * socket, void * data_struct, int32_t data_type);

//Method retrieving a serialized dynamic data structure.
int32_t recv_dynamic_struct(void * socket, void * data_struct, int32_t data_type);

#endif

