#ifndef COMMS
#define COMMS

#define IMSS_INFO	 0
#define DATASET_INFO 1
#define STRING       2
#define BUFFER       4
#define MSG          5

#define REQUEST_SIZE    256
#define RESPONSE_SIZE   1024
#define MODE_SIZE        4
#define BUFFER_SIZE     4*1024*1024

#define CLOSE_EP       9999999


#include "ucp_util.h"
#include "queue.h"
#include <arpa/inet.h> /* inet_addr */

#include <ucp/api/ucp.h>

#define IP_STRING_LEN          50
#define PORT_STRING_LEN        8

typedef enum {
    CLIENT_SERVER_SEND_RECV_STREAM  = UCS_BIT(0),
    CLIENT_SERVER_SEND_RECV_TAG     = UCS_BIT(1),
    CLIENT_SERVER_SEND_RECV_AM      = UCS_BIT(2),
    CLIENT_SERVER_SEND_RECV_DEFAULT = CLIENT_SERVER_SEND_RECV_STREAM
} send_recv_type_t;


/**
 * Server's application context to be used in the user's connection request
 * callback.
 * It holds the server's listener and the handle to an incoming connection request.
 */
typedef struct ucx_server_ctx {
	int num_conn;
	StsHeader *conn_request;
    ucp_listener_h              listener;

} ucx_server_ctx_t;


/**
 * Stream request context. Holds a value to indicate whether or not the
 * request is completed.
 */
typedef struct test_req {
    int complete;
} test_req_t;


/**
 * Descriptor of the data received with AM API.
 */
static struct {
    volatile int complete;
    int          is_rndv;
    void         *desc;
    void         *recv_buf;
} am_data_desc = {0, 0, NULL, NULL};

typedef struct msg {
     size_t size;
     char * data;
} msg_t;


int init_worker(ucp_context_h ucp_context, ucp_worker_h *ucp_worker);
int init_context(ucp_context_h *ucp_context, ucp_worker_h *ucp_worker, send_recv_type_t send_recv_type);
ucs_status_t start_client(ucp_worker_h ucp_worker, const char *address_str, int port, ucp_ep_h *client_ep);
void set_sock_addr(const char *address_str, struct sockaddr_storage *saddr, int server_port);
int request_finalize(ucp_worker_h ucp_worker, test_req_t *request, test_req_t *ctx);
size_t send_stream(ucp_worker_h ucp_worker, ucp_ep_h ep, const char * msg, size_t msg_length);
size_t recv_stream(ucp_worker_h ucp_worker, ucp_ep_h ep, char * msg, size_t msg_length);
ucs_status_t request_wait(ucp_worker_h ucp_worker, void *request, test_req_t *ctx);
void stream_recv_cb(void *request, ucs_status_t status, size_t length, void *user_data);
void am_recv_cb(void *request, ucs_status_t status, size_t length, void *user_data);
void send_cb(void *request, ucs_status_t status, void *user_data);
void err_cb(void *arg, ucp_ep_h ep, ucs_status_t status);
void common_cb(void *user_data, const char *type_str);
void server_conn_handle_cb(ucp_conn_request_h conn_request, void *arg);
char* sockaddr_get_ip_str(const struct sockaddr_storage *sock_addr, char *ip_str, size_t max_size);
char* sockaddr_get_port_str(const struct sockaddr_storage *sock_addr,char *port_str, size_t max_size);
ucs_status_t start_server(ucp_worker_h ucp_worker, ucx_server_ctx_t *context, ucp_listener_h *listener_p, const char *address_str, int port);
ucs_status_t server_create_ep(ucp_worker_h data_worker, ucp_conn_request_h conn_request, ucp_ep_h *server_ep);

//Method sending a data structure with dynamic memory allocation fields.
int32_t send_dynamic_stream(ucp_worker_h ucp_worker, ucp_ep_h ep, void * data_struct, int32_t data_type);

//Method retrieving a serialized dynamic data structure.
int32_t recv_dynamic_stream(ucp_worker_h ucp_worker, ucp_ep_h ep, void * data_struct, int32_t data_type);

#endif
