#ifndef COMMS
#define COMMS

#define IMSS_INFO	 0
#define DATASET_INFO 1
#define STRING       2
#define BUFFER       4
#define MSG          5

#define REQUEST_SIZE    1024
#define RESPONSE_SIZE   1024
#define MODE_SIZE        4
#define BUFFER_SIZE     4*1024*1024

#define CLOSE_EP       9999999


#include "queue.h"
#include <arpa/inet.h> /* inet_addr */
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <netdb.h>

#include <ucp/api/ucp.h>

// to manage logs.
#include "slog.h"

extern int32_t  IMSS_DEBUG;

#define IP_STRING_LEN          50
#define PORT_STRING_LEN        8

static const char *addr_msg_str = "UCX address message";
static const char *data_msg_str = "UCX data message";


#define AM_ID 0

/**
* Macro to measure the time spend by function_to_call.
* char*::print_comment: comment to be concatenated to the elapsed time.
*/

typedef struct am_data_desc {
    volatile int complete;
    int          is_rndv;
    void         *desc;
    void         *recv_buf;
} am_data_desc_t;

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
 * Used in ep_close() to finalize write requests.
 * Structure used to store requests that need to be finalized.
 */
typedef struct ucx_async {
    test_req_t *    request;
    test_req_t      ctx;
    char *          tmp_msg;
} ucx_async_t;


typedef struct msg {
     size_t size;
     char * data;
} msg_t;

typedef struct msg_addr
{
	uint64_t data_len;
} msg_addr_t;



struct ucx_context {
	int             completed;
};

#define CHKERR_ACTION(_cond, _msg, _action) \
    do { \
        if (_cond) { \
            fprintf(stderr, "Failed to %s\n", _msg); \
            _action; \
        } \
    } while (0)


#define CHKERR_JUMP(_cond, _msg, _label) \
    CHKERR_ACTION(_cond, _msg, goto _label)


#define CHKERR_JUMP_RETVAL(_cond, _msg, _label, _retval) \
    do { \
        if (_cond) { \
            fprintf(stderr, "Failed to %s, return value %d\n", _msg, _retval); \
            goto _label; \
        } \
    } while (0)



int init_worker(ucp_context_h ucp_context, ucp_worker_h *ucp_worker);
int init_context(ucp_context_h *ucp_context,  ucp_config_t *config, ucp_worker_h *ucp_worker, send_recv_type_t send_recv_type);
ucs_status_t start_client(ucp_worker_h ucp_worker, const char *address_str, int port, ucp_ep_h *client_ep);
void set_sock_addr(const char *address_str, struct sockaddr_storage *saddr, int server_port);
int request_finalize(ucp_worker_h ucp_worker, test_req_t *request, test_req_t *ctx);
size_t send_stream(ucp_worker_h ucp_worker, ucp_ep_h ep, const char * msg, size_t msg_length);
size_t send_istream(ucp_worker_h ucp_worker, ucp_ep_h ep, const char * msg, size_t msg_length);
size_t recv_stream(ucp_worker_h ucp_worker, ucp_ep_h ep, char * msg, size_t msg_length);
ucs_status_t request_wait(ucp_worker_h ucp_worker, void *request, test_req_t *ctx);
void stream_recv_cb(void *request, ucs_status_t status, size_t length, void *user_data);
void am_recv_cb(void *request, ucs_status_t status, size_t length, void *user_data);
void send_handler(void *request, ucs_status_t status, void *ctx);
void recv_handler(void *request, ucs_status_t status, ucp_tag_recv_info_t *info);
void send_cb(void *request, ucs_status_t status, void *user_data);
void isend_cb(void *request, ucs_status_t status, void *user_data);
void err_cb_client(void *arg, ucp_ep_h ep, ucs_status_t status);
void err_cb_server(void *arg, ucp_ep_h ep, ucs_status_t status);
void common_cb(void *user_data, const char *type_str);
void server_conn_handle_cb(ucp_conn_request_h conn_request, void *arg);
char* sockaddr_get_ip_str(const struct sockaddr_storage *sock_addr, char *ip_str, size_t max_size);
char* sockaddr_get_port_str(const struct sockaddr_storage *sock_addr,char *port_str, size_t max_size);
ucs_status_t start_server(ucp_worker_h ucp_worker, ucx_server_ctx_t *context, ucp_listener_h *listener_p, const char *address_str, int port);
ucs_status_t server_create_ep(ucp_worker_h data_worker, ucp_conn_request_h conn_request, ucp_ep_h *server_ep);
void ep_close(ucp_worker_h ucp_worker, ucp_ep_h ep, uint64_t flags);
ucs_status_t ep_flush(ucp_ep_h ep, ucp_worker_h worker);
ucs_status_t client_create_ep(ucp_worker_h worker, ucp_ep_h *ep, ucp_address_t *peer_addr);


//Method sending a data structure with dynamic memory allocation fields.
int32_t send_dynamic_stream(ucp_worker_h ucp_worker, ucp_ep_h ep, void * data_struct, int32_t data_type);

//Method retrieving a serialized dynamic data structure.
int32_t recv_dynamic_stream(ucp_worker_h ucp_worker, ucp_ep_h ep, void * data_struct, int32_t data_type);

int connect_common(const char *server, uint16_t server_port, sa_family_t af);

ucs_status_t ucx_wait(ucp_worker_h ucp_worker, struct ucx_context *request, const char *op_str, const char *data_str);

size_t send_stream_addr(ucp_worker_h ucp_worker, ucp_ep_h ep, ucp_address_t *addr, size_t addr_len);

void request_init(void *request);

ucs_status_t ucp_am_data_cb(void *arg, const void *header, size_t header_length, void *data, size_t length, const ucp_am_recv_param_t *param);

void flush_cb(void *request, ucs_status_t status);

#endif
