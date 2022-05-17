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

//Method zmq_comm_msg_recv printing error if there is
int comm_msg_recv (zmq_msg_t *msg, void *socket, int flags);

//Method zmq_comm_recv printing error if there is
int comm_recv (void *socket, void *buf, size_t len, int flags);

//Method zmq_comm_msg_send printing error if there is
int comm_msg_send (zmq_msg_t *msg, void *socket, int flags);

//Method zmq_comm_send printing error if there is
int comm_send (void *socket, void *buf, size_t len, int flags);

int comm_setsockopt (void *socket, int option_name, const void *option_value, size_t option_len);

int comm_bind (void *socket, const char *endpoint);

int comm_getsockopt (void *socket, int option_name, void *option_value, size_t *option_len);

int comm_msg_close (zmq_msg_t *msg);

int comm_close (void *socket);

int comm_msg_init (zmq_msg_t *msg);

int comm_connect (void *socket, const char *endpoint);

int comm_ctx_set (void *context, int option_name, int option_value);
#endif
