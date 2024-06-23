#include <string.h>
#include <stdlib.h>
#include "imss.h"
#include "queue.h"
#include "comms.h"
#include "map_ep.hpp"
#include <errno.h>
#include <netdb.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <inttypes.h>

static sa_family_t ai_family = AF_INET;

/* asynchronous writes stuff */
extern void *map_ep;	  // map_ep used for async write
// extern int32_t is_client; // used to make sure the server doesn't do map_ep stuff
pthread_mutex_t map_ep_mutex;
pthread_mutex_t lock_ucx_comm = PTHREAD_MUTEX_INITIALIZER;

void *send_buffer;
void *recv_buffer;

ucs_status_t ucp_mem_alloc(ucp_context_h ucp_context, size_t length, void **address_p)
{
	ucp_mem_map_params_t params;
	ucp_mem_attr_t attr;
	ucs_status_t status;
	ucp_mem_h memh_p;

	params.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS |
						UCP_MEM_MAP_PARAM_FIELD_LENGTH |
						UCP_MEM_MAP_PARAM_FIELD_FLAGS |
						UCP_MEM_MAP_PARAM_FIELD_MEMORY_TYPE;
	params.address = NULL;
	params.memory_type = UCS_MEMORY_TYPE_HOST;
	params.length = length;
	params.flags = UCP_MEM_MAP_ALLOCATE;
	params.flags |= UCP_MEM_MAP_NONBLOCK;

	status = ucp_mem_map(ucp_context, &params, &memh_p);
	if (status != UCS_OK)
	{
		return status;
	}

	attr.field_mask = UCP_MEM_ATTR_FIELD_ADDRESS;
	status = ucp_mem_query(memh_p, &attr);
	if (status != UCS_OK)
	{
		ucp_mem_unmap(ucp_context, memh_p);
		return status;
	}

	*address_p = attr.address;
	return UCS_OK;
}

/**
 * Create a ucp worker on the given ucp context.
 */
int init_worker(ucp_context_h ucp_context, ucp_worker_h *ucp_worker)
{
	ucp_worker_params_t worker_params;
	ucs_status_t status;
	int ret = 0;

	memset(&worker_params, 0, sizeof(worker_params));

	worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
	worker_params.thread_mode = UCS_THREAD_MODE_MULTI;
	// worker_params.thread_mode = UCS_THREAD_MODE_SERIALIZED;

	status = ucp_worker_create(ucp_context, &worker_params, ucp_worker);
	if (status != UCS_OK)
	{
		// fprintf(stderr, "failed to ucp_worker_create (%s)", ucs_status_string(status));
		slog_error("failed to ucp_worker_create (%s)", ucs_status_string(status));
		perror("ERRIMSS_WORKER_INIT");
		ret = -1;
	}

	// slog_debug("[COMM] Inicializated worker result: %d", ret);
	return ret;
}

/**
 * Initialize the UCP context and worker.
 */
int init_context(ucp_context_h *ucp_context, ucp_config_t *config, ucp_worker_h *ucp_worker, send_recv_type_t send_recv_type)
{
	/* UCP objects */
	ucp_params_t ucp_params;
	ucs_status_t status;
	int ret = 0;

	// status = ucp_config_read(NULL, NULL, &config);
	// ucp_config_print(config, stdout, NULL, UCS_CONFIG_PRINT_CONFIG);

	slog_info("Before memset");
	memset(&ucp_params, 0, sizeof(ucp_params));
	slog_info("After memset");

	/* UCP initialization */
	slog_info("Before ucp_config_read");
	status = ucp_config_read(NULL, NULL, &config);
	slog_info("After ucp_config_read, status=%s", ucs_status_string(status));
	// ucp_config_print(config, stdout, NULL, UCS_CONFIG_PRINT_CONFIG);

	ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES |
							UCP_PARAM_FIELD_REQUEST_SIZE |
							UCP_PARAM_FIELD_REQUEST_INIT |
							UCP_PARAM_FIELD_NAME;
	ucp_params.features = UCP_FEATURE_TAG;
	ucp_params.features |= UCP_FEATURE_WAKEUP;
	ucp_params.request_size = sizeof(struct ucx_context);
	ucp_params.request_init = request_init;
	ucp_params.name = "hercules";
	ucp_params.mt_workers_shared = UCS_THREAD_MODE_SERIALIZED;
	// ucp_params.mt_workers_shared = UCS_THREAD_MODE_SINGLE;
	slog_info("Before ucp_init");
	status = ucp_init(&ucp_params, config, ucp_context);
	// if(errno != 0) {
	// 	fprintf(stderr, "Error, errno=%d:%s", errno, strerror(errno));
	// }
	slog_info("After ucp_init, status=%s", ucs_status_string(status));
	// slog_info("Before ucp_config_release");
	ucp_config_release(config);
	// slog_info("After ucp_config_release");

	// ucp_context_print_info(*ucp_context,stderr);
	if (status != UCS_OK)
	{
		slog_error("failed to ucp_init (%s)", ucs_status_string(status));
		slog_error("HERCULES_INIT_CONTEXT_UCP_INIT");
		ret = -1;
		goto err;
	}
	// slog_info("Before init worker");
	ret = init_worker(*ucp_context, ucp_worker);
	// slog_info("After init worker");
	if (ret != 0)
	{
		goto err_cleanup;
	}

	// slog_info("Before ucp_mem_alloc for send buffer");
	ucp_mem_alloc(*ucp_context, 4 * 1024 * 1024, (void **)&send_buffer);
	// slog_info("After ucp_mem_alloc for send buffer");
	ucp_mem_alloc(*ucp_context, 4 * 1024 * 1024, (void **)&recv_buffer);
	// slog_info("After ucp_mem_alloc for recv buffer");

	// slog_debug("[COMM] Inicializated context result: %d", ret);
	return ret;

err_cleanup:
	ucp_cleanup(*ucp_context);
err:
	return ret;
}

/***
 * @brief send data to the endpoint specified in "ep".
 * @return number of bytes sent on success, on error, 0 is returned.
 */
size_t send_data(ucp_worker_h ucp_worker, ucp_ep_h ep, const void *msg, size_t msg_len, uint64_t from)
{
	ucs_status_t status;
	struct ucx_context *request;
	ucp_request_param_t send_param;
	send_req_t ctx;

	// char req[2048];
	ctx.buffer = (void *)msg;
	// ctx.buffer = (char *)msg;
	// ctx.buffer = (char *)malloc(msg_len);
	ctx.complete = 0;
	// memcpy (ctx.buffer, msg, msg_len);
	// memcpy (send_buffer, msg, msg_len);
	//	ctx.buffer= bb;

	send_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
							  UCP_OP_ATTR_FIELD_USER_DATA;
	send_param.cb.send = send_handler_data;
	send_param.datatype    = ucp_dt_make_contig(1);
	send_param.memory_type  = UCS_MEMORY_TYPE_HOST;
	send_param.user_data = &ctx;

	clock_t t;
	t = clock();
	request = (struct ucx_context *)ucp_tag_send_nbx(ep, ctx.buffer, msg_len, from, &send_param);
	// request = (struct ucx_context *)ucp_tag_send_sync_nbx(ep, ctx.buffer, msg_len, from, &send_param);
	status = ucx_wait(ucp_worker, request, "send", "data");
	t = clock() - t;
	double time_taken = ((double)t) / CLOCKS_PER_SEC; // in seconds
	// fprintf(stderr,"********** send data %lu time = %lf\n", msg_len, time_taken);

	if (UCS_PTR_IS_ERR(request))
	{
		// slog_fatal("[COMM] Error sending to endpoint.");
		slog_fatal("HERCULES_ERR_SEND_DATA");
		perror("HERCULES_ERR_SEND_DATA");
		return 0;
	}

	return msg_len;
}

/**
 * @brief Send a request to an endpoint specified by "ep".
 * @return Number of bytes sent on success, on error 0 is returned.
 */
size_t send_req(ucp_worker_h ucp_worker, ucp_ep_h ep, ucp_address_t *addr, size_t addr_len, char *req)
{

	ucs_status_t status;
	struct ucx_context *request;
	size_t msg_len;
	ucp_request_param_t send_param;
	send_req_t ctx;

	msg_req_t *msg;

	msg_len = sizeof(uint64_t) + REQUEST_SIZE + addr_len;
	// slog_info("[COMM][send_req] msg_len=%ld", msg_len);
	// slog_info("[COMM][send_req] msg_len=%ld, before malloc", msg_len);
	msg = (msg_req_t *)malloc(msg_len);
	// slog_info("[COMM][send_req] msg_len=%lu", msg_len);
	//	msg = (msg_req_t *)send_buffer;

	msg->addr_len = addr_len; // imprimir la long de adress_len.
	memcpy(msg->request, req, REQUEST_SIZE);
	memcpy(msg + 1, addr, addr_len);

	ctx.complete = 0;
	ctx.buffer = (char *)msg;

	send_param.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE |
							  UCP_OP_ATTR_FIELD_CALLBACK |
							  UCP_OP_ATTR_FLAG_NO_IMM_CMPL |
							  UCP_OP_ATTR_FIELD_USER_DATA;

	send_param.datatype = ucp_dt_make_contig(1);
	send_param.cb.send = send_handler_req;
	// send_param.memory_type  = UCS_MEMORY_TYPE_HOST;
	send_param.user_data = &ctx;

	// slog_info("[COMM][send_req] before ucp_tag_send_nbx");
	request = (struct ucx_context *)ucp_tag_send_nbx(ep, msg, msg_len, tag_req, &send_param);
	// request = (struct ucx_context *)ucp_tag_send_sync_nbx(ep, msg, msg_len, tag_req, &send_param);
	// slog_info("[COMM][send_req] after ucp_tag_send_nbx");
	// slog_info("[COMM][send_req] before ucx_wait");
	status = ucx_wait(ucp_worker, request, "send", req);
	// slog_info("[COMM][send_req] after ucx_wait");

	if (status != UCS_OK)
	{
		// slog_error("Connection error\n");
		free(msg);
		//     // goto err_ep;
		// 	ep_close(ucp_worker, ep, UCP_EP_CLOSE_FLAG_FORCE);
		slog_fatal("[COMM][send_req] Connection error, request=%s", req);
		// return -1;
		return 0;
	}

	free(msg);
	// slog_info("[COMM][send_req] errno=%d:%s", errno, strerror(errno));
	return msg_len;
}

size_t get_recv_data_length(ucp_worker_h ucp_worker, uint64_t dest)
{
	// pthread_mutex_lock(&lock_ucx_comm);

	ucp_tag_recv_info_t info_tag;
	ucp_tag_message_h msg_tag;
	// async = 1;
	do
	{
		/* Progressing before probe to update the state */
		ucp_worker_progress(ucp_worker);
		/* Probing incoming events in non-block mode */
		msg_tag = ucp_tag_probe_nb(ucp_worker, dest, tag_mask, 0, &info_tag);
	} while (msg_tag == NULL);

	// pthread_mutex_unlock(&lock_ucx_comm);

	return info_tag.length;
}

/**
 * @brief Fill "msg" with the message received from the server. "msg" must be allocated.
 * @return number of bytes received, on error 0 is returned.
 */
size_t recv_data(ucp_worker_h ucp_worker, ucp_ep_h ep, void *msg, size_t msg_length, uint64_t dest, int async)
{
	// pthread_mutex_lock(&lock_ucx_comm);

	// slog_debug("Init recv_data");
	// ucp_tag_recv_info_t info_tag;
	// ucp_tag_message_h msg_tag;
	ucp_request_param_t recv_param;
	struct ucx_context *request;
	ucs_status_t status;

	async = 1;
	// clock_t t;

	// slog_debug("[COMM] Waiting message  as  %" PRIu64 ".", dest)
	// do
	// {
	// 	ucp_worker_progress(ucp_worker);
	// 	msg_tag = ucp_tag_probe_nb(ucp_worker, dest, tag_mask, 0, &info_tag);
	// } while (msg_tag == NULL);

	// msg = (void *)malloc(info_tag.length);

	/*
	   for (;;) {
	   msg_tag = ucp_tag_probe_nb(ucp_worker, tag_data, tag_mask, 0, &info_tag);
	   if (msg_tag != NULL) {
	   break;
	   } else if (ucp_worker_progress(ucp_worker)) {
	   continue;
	   }
	   status = ucp_worker_wait(ucp_worker);

	   }
	 */
	recv_param.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE |
							  UCP_OP_ATTR_FIELD_CALLBACK |
							  UCP_OP_ATTR_FLAG_NO_IMM_CMPL |
							  UCP_OP_ATTR_FIELD_USER_DATA;

	recv_param.datatype = ucp_dt_make_contig(1);
	recv_param.cb.recv = recv_handler;

	slog_debug("[COMM] Probe tag (%lu bytes)", msg_length);
	//	t = clock();
	clock_t t;
	t = clock();
	if (async)
	{
		request = (struct ucx_context *)ucp_tag_recv_nbx(ucp_worker, msg, msg_length, dest, tag_mask, &recv_param);
	}
	else
	{
		request = (struct ucx_context *)ucp_tag_recv_nbx(ucp_worker, recv_buffer, msg_length, dest, tag_mask, &recv_param);
		memcpy(msg, recv_buffer, msg_length);
	}

	// if (errno != 0)
	// {
	// 	slog_debug("[COMM] Msg in error: %s, length=%d, errno=%d:%s", msg, msg_length, errno, strerror(errno));
	// }

	// sleep(1);

	
	status = ucx_wait(ucp_worker, request, "recv", "data");

	t = clock() - t;
	double time_taken = ((double)t) / CLOCKS_PER_SEC; // in seconds
	// fprintf(stderr,"********** recv data = %lf\n", time_taken);

	// slog_debug("[COMM] status=%s.", ucs_status_string(status));
	// slog_debug("--- %s\n", msg);

	// t = clock() -t;
	//	double time_taken = ((double)t) / CLOCKS_PER_SEC; // in seconds
	//               slog_info("[srv_worker_helper] recv_nbx time %f s", time_taken);

	// slog_debug("[COMM] Recv tag (%ld bytes).", msg_length);
	// fprintf(stderr, "[COMM] Recv tag (%ld bytes).\n", msg_length);
	// pthread_mutex_unlock(&lock_ucx_comm);

	if (status != UCS_OK)
	{
		slog_error("[COMM] HERCULES_RECV_DATA_ERR, msg_length=%lu", msg_length);
		// pthread_mutex_unlock(&lock_ucx_comm);
		// return -1;
		return 0;
	}

	// pthread_mutex_unlock(&lock_ucx_comm);

	return msg_length;
}

/**
 * @brief
 * Malloc and fill "msg" with the message received from the server. "msg" is free in case of error
 * or when the length of the message received is 0. In other case "msg" must be free by the calling function.
 * @param ucp_worker
 * @param ep
 * @param msg
 * @param msg_length
 * @param dest
 * @param async
 * @return number of bytes received, on error 0 is returned.
 */
size_t recv_data_opt(ucp_worker_h ucp_worker, ucp_ep_h ep, void **msg, size_t msg_length, uint64_t dest, int async)
{

	// slog_debug("Init recv_data");
	ucp_tag_recv_info_t info_tag;
	ucp_tag_message_h msg_tag;
	ucp_request_param_t recv_param;
	struct ucx_context *request;
	ucs_status_t status;

	async = 1;
	// clock_t t;

	slog_debug("[COMM] Waiting message  as  %" PRIu64 ".", dest);
	pthread_mutex_lock(&lock_ucx_comm);
	do
	{
		ucp_worker_progress(ucp_worker);
		msg_tag = ucp_tag_probe_nb(ucp_worker, dest, tag_mask, 0, &info_tag);
	} while (msg_tag == NULL);
	msg_length = info_tag.length;
	slog_debug("[COMM] Probe tag (%lu bytes)", msg_length);
	if (msg_length <= 0)
	{
		pthread_mutex_unlock(&lock_ucx_comm);
		perror("ERROR_GETTING_MESSAGE_LENGTH");
		slog_error("ERROR_GETTING_MESSAGE_LENGTH");
		return 0;
	}
	if (*msg == NULL)
	{
		slog_live("Allocating memory=%lu", msg_length);
		*msg = (void *)malloc(msg_length);
	}

	/*
	   for (;;) {
	   msg_tag = ucp_tag_probe_nb(ucp_worker, tag_data, tag_mask, 0, &info_tag);
	   if (msg_tag != NULL) {
	   break;
	   } else if (ucp_worker_progress(ucp_worker)) {
	   continue;
	   }
	   status = ucp_worker_wait(ucp_worker);

	   }
	 */
	recv_param.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE |
							  UCP_OP_ATTR_FIELD_CALLBACK |
							  UCP_OP_ATTR_FLAG_NO_IMM_CMPL |
							  UCP_OP_ATTR_FIELD_USER_DATA;

	recv_param.datatype = ucp_dt_make_contig(1);
	recv_param.cb.recv = recv_handler;

	//	t = clock();
	if (async)
	{
		request = (struct ucx_context *)ucp_tag_recv_nbx(ucp_worker, *msg, msg_length, dest, tag_mask, &recv_param);
	}
	else
	{
		request = (struct ucx_context *)ucp_tag_recv_nbx(ucp_worker, recv_buffer, msg_length, dest, tag_mask, &recv_param);
		memcpy(*msg, recv_buffer, msg_length);
	}

	// if (errno != 0)
	// {
	// 	slog_debug("[COMM] Msg in error: %s, length=%d, errno=%d:%s", msg, msg_length, errno, strerror(errno));
	// }

	// sleep(1);
	status = ucx_wait(ucp_worker, request, "recv", "data");
	// slog_debug("[COMM] status=%s.", ucs_status_string(status));
	// slog_debug("--- %s\n", msg);

	// t = clock() -t;
	//	double time_taken = ((double)t) / CLOCKS_PER_SEC; // in seconds
	//               slog_info("[srv_worker_helper] recv_nbx time %f s", time_taken);

	// slog_debug("[COMM] Recv tag (%ld bytes).", msg_length);
	// fprintf(stderr, "[COMM] Recv tag (%ld bytes).\n", msg_length);
	// pthread_mutex_unlock(&lock_ucx_comm);

	if (status != UCS_OK)
	{
		slog_error("[COMM] HERCULES_RECV_DATA_ERR, msg_length=%lu", msg_length);
		free(*msg);
		pthread_mutex_unlock(&lock_ucx_comm);
		// return -1;
		return 0;
	}

	pthread_mutex_unlock(&lock_ucx_comm);
	return msg_length;
}

/**
 * Progress the request until it completes.
 */
ucs_status_t request_wait(ucp_worker_h ucp_worker, void *request, send_req_t *ctx)
{
	ucs_status_t status;

	/* if operation was completed immediately */
	if (request == NULL)
	{
		return UCS_OK;
	}

	if (UCS_PTR_IS_ERR(request))
	{
		return UCS_PTR_STATUS(request);
	}

	while (ctx->complete == 0)
	{
		ucp_worker_progress(ucp_worker);
	}
	status = ucp_request_check_status(request);

	ucp_request_free(request);

	return status;
}

static void request_init(void *request)
{
	struct ucx_context *contex = (struct ucx_context *)request;

	contex->completed = 0;
}

void send_handler_req(void *request, ucs_status_t status, void *ctx)
{
	struct ucx_context *context = (struct ucx_context *)request;
	send_req_t *data = (send_req_t *)ctx;

	context->completed = 1;

	// slog_info("[COMM] send_handler req");
	// slog_info("[COMM][send_handler_req][0x%x] send handler called with status %d (%s)\n", (unsigned int)pthread_self(), status, ucs_status_string(status));
	// ucp_request_free(request);
}

void send_handler_data(void *request, ucs_status_t status, void *ctx)
{
	struct ucx_context *context = (struct ucx_context *)request;
	context->completed = 1;

	send_req_t *data = (send_req_t *)ctx;
	// free(data->buffer);
	// ucp_request_free(request);

	// slog_info("[COMM] send_handler data");
	// ucp_request_free(request);
}

void recv_handler(void *request, ucs_status_t status,
				  const ucp_tag_recv_info_t *info, void *user_data)
{
	struct ucx_context *context = (struct ucx_context *)request;
	//	slog_info("[COMM] recv_handler");
	context->completed = 1;
	//	ucp_request_free(request);
}
/**
 * The callback on the sending side, which is invoked after finishing sending
 * the message.
 */
void send_cb(void *request, ucs_status_t status, void *user_data)
{
	common_cb(user_data, "send_cb");
}

/**
 * Error handling callback.
 */
void err_cb_client(void *arg, ucp_ep_h ep, ucs_status_t status)
{
	int *server_status = (int *) arg;
	*server_status = 0;
	// ucs_status_t *arg_status = (ucs_status_t *)arg;
	// if (status != UCS_ERR_CONNECTION_RESET && status != UCS_ERR_ENDPOINT_TIMEOUT)
	// {
	// }
	// slog_error("[COMM] Client error handling callback was invoked with status %d (%s)", status, ucs_status_string(status));
	// fprintf(stderr, "client error handling callback was invoked with status %d (%s)", status, ucs_status_string(status));
	slog_error("failure handler called with status %d (%s)\n", status, ucs_status_string(status));
	fprintf(stderr,"failure handler called with status %d (%s)\n", status, ucs_status_string(status));
	// *arg_status = status;

}

void err_cb_server(void *arg, ucp_ep_h ep, ucs_status_t status)
{

	uint64_t worker_uid = (uint64_t)arg;
	// struct worker_info *worker_info = (struct worker_info *)arg;

	if (status != UCS_ERR_CONNECTION_RESET && status != UCS_ERR_ENDPOINT_TIMEOUT)
	{
		// fprintf(stderr, "\t [COMM]['%" PRIu64 "'] Server error handling callback was invoked with status %d (%s)\n", worker_uid, status, ucs_status_string(status));
	}
	slog_error("[COMM]['%" PRIu64 "'] server error handling callback was invoked with status %d (%s)", worker_uid, status, ucs_status_string(status));
}

void common_cb(void *user_data, const char *type_str)
{
	send_req_t *ctx;

	if (user_data == NULL)
	{
		fprintf(stderr, "user_data passed to %s mustn't be NULL", type_str);
		return;
	}

	ctx = (send_req_t *)user_data;
	ctx->complete = 1;
	if (ctx->buffer)
		free(ctx->buffer);
}

void flush_cb(void *request, ucs_status_t status)
{
	slog_info("flush finished");
}

int request_finalize(ucp_worker_h ucp_worker, send_req_t *request, send_req_t *ctx)
{
	int ret = 0;
	ucs_status_t status;

	status = request_wait(ucp_worker, request, ctx);
	if (status != UCS_OK)
	{
		fprintf(stderr, "unable to complete UCX message (%s)", ucs_status_string(status));
		ret = -1;
		// goto release_iov;
	}

	// release_iov:
	return ret;
}

ucs_status_t server_create_ep(ucp_worker_h data_worker,
							  ucp_conn_request_h conn_request,
							  ucp_ep_h *server_ep)
{
	ucp_ep_params_t ep_params;
	ucs_status_t status;

	/* Server creates an ep to the client on the data worker.
	 * This is not the worker the listener was created on.
	 * The client side should have initiated the connection, leading
	 * to this ep's creation */
	ep_params.field_mask = UCP_EP_PARAM_FIELD_ERR_HANDLER | UCP_EP_PARAM_FIELD_CONN_REQUEST;
	ep_params.conn_request = conn_request;
	ep_params.err_handler.cb = err_cb_server;
	ep_params.err_mode = UCP_ERR_HANDLING_MODE_PEER;
	ep_params.err_handler.arg = NULL;

	status = ucp_ep_create(data_worker, &ep_params, server_ep);
	if (status != UCS_OK)
	{
		fprintf(stderr, "\t[COMM] Failed to create an endpoint on the server: (%s)", ucs_status_string(status));
	}

	slog_debug("[COMM] Created server endpoint");
	return status;
}

ucs_status_t client_create_ep_data(ucp_worker_h worker, ucp_ep_h *ep, ucp_address_t *peer_addr, int *server_status)
{
	ucp_ep_params_t ep_params;
	ucs_status_t status;
	ucs_status_t ep_status = UCS_OK;

	/* Server creates an ep to the client on the data worker.
	 * This is not the worker the listener was created on.
	 * The client side should have initiated the connection, leading
	 * to this ep's creation */

	ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS |
						   UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE |
						   UCP_EP_PARAM_FIELD_ERR_HANDLER |
						   UCP_EP_PARAM_FIELD_USER_DATA;
	ep_params.address = peer_addr;
	ep_params.err_mode = UCP_ERR_HANDLING_MODE_PEER;
	ep_params.err_handler.cb = err_cb_client;
	// ep_params.err_handler.arg = NULL;
	ep_params.err_handler.arg = &server_status;
	ep_params.user_data = &ep_status;

	// ucp_worker_print_info(worker, stderr);
	status = ucp_ep_create(worker, &ep_params, ep);
	if (status != UCS_OK)
	{
		fprintf(stderr, "failed to create an endpoint on the server: (%s)", ucs_status_string(status));
	}

	slog_debug("[COMM] Created client endpoint");
	return status;
}

ucs_status_t client_create_ep_metadata(ucp_worker_h worker, ucp_ep_h *ep, ucp_address_t *peer_addr)
{
	ucp_ep_params_t ep_params;
	ucs_status_t status;
	ucs_status_t ep_status = UCS_OK;

	/* Server creates an ep to the client on the data worker.
	 * This is not the worker the listener was created on.
	 * The client side should have initiated the connection, leading
	 * to this ep's creation */

	ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS |
						   UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE |
						   UCP_EP_PARAM_FIELD_ERR_HANDLER |
						   UCP_EP_PARAM_FIELD_USER_DATA;
	ep_params.address = peer_addr;
	ep_params.err_mode = UCP_ERR_HANDLING_MODE_PEER;
	ep_params.err_handler.cb = err_cb_client;
	ep_params.err_handler.arg = NULL;
	ep_params.user_data = &ep_status;

	// ucp_worker_print_info(worker, stderr);
	status = ucp_ep_create(worker, &ep_params, ep);
	if (status != UCS_OK)
	{
		fprintf(stderr, "failed to create an endpoint on the server: (%s)", ucs_status_string(status));
	}

	slog_debug("[COMM] Created client endpoint");
	return status;
}

// Method sending a data structure with dynamic memory allocation fields.
int32_t send_dynamic_stream(ucp_worker_h ucp_worker, ucp_ep_h ep, void *data_struct, int32_t data_type, uint64_t from)
{
	// Buffer containing the structures' information.
	char *info_buffer;
	// Buffer size.
	size_t msg_size;

	slog_debug("[COMM] send_dynamic start, data_type=%d", data_type);
	// Formalize the information to be sent.
	switch (data_type)
	{
	case IMSS_INFO:
	{
		imss_info *struct_ = (imss_info *)data_struct;

		// Calculate the total size of the buffer storing the structure.
		// ips + status list + arr num active storages list.
		msg_size = sizeof(imss_info) + (LINE_LENGTH * struct_->num_storages) + (sizeof(int) * struct_->num_storages) + (sizeof(int) * struct_->num_storages);

		// Reserve the corresponding amount of memory for the previous buffer.
		info_buffer = (char *)malloc(msg_size * sizeof(char));

		// Control variables dealing with incomming memory management actions.
		char *offset_pt = info_buffer;

		// Copy the actual structure to the buffer.
		memcpy(offset_pt, struct_, sizeof(imss_info));

		offset_pt += sizeof(imss_info);

		// Copy the remaining dynamic fields into the buffer.
		for (int32_t i = 0; i < struct_->num_storages; i++)
		{
			memcpy(offset_pt, struct_->ips[i], LINE_LENGTH);
			// slog_debug("pointer address = %p", &offset_pt);
			offset_pt += LINE_LENGTH;
		}

		memcpy(offset_pt, struct_->status, sizeof(int) * struct_->num_storages);

		offset_pt += (sizeof(int) * struct_->num_storages);
		memcpy(offset_pt, struct_->arr_num_active_storages, sizeof(int) * struct_->num_storages);

		// slog_debug("pointer address = %p", &offset_pt);

		break;
	}

	case DATASET_INFO:
	{
		dataset_info *struct_ = (dataset_info *)data_struct;

		// Calculate the total size of the buffer storing the structure.
		msg_size = sizeof(dataset_info);

		// If the dataset is a LOCAL one, the list of position characters must be added.
		if (!strcmp(struct_->policy, "LOCAL"))
			msg_size += (struct_->num_data_elem * sizeof(uint16_t));

		// Reserve the corresponding amount of memory for the previous buffer.
		info_buffer = (char *)malloc(msg_size * sizeof(char));

		// Serialize the provided message into the buffer.
		char *offset_pt = info_buffer;

		// Copy the actual structure to the buffer.
		memcpy(info_buffer, struct_, msg_size);

		// Copy the remaining 'data_locations' field if the dataset is a LOCAL one.
		if (!strcmp(struct_->policy, "LOCAL"))
		{
			offset_pt += sizeof(dataset_info);
			memcpy(offset_pt, struct_->data_locations, (struct_->num_data_elem * sizeof(uint16_t)));
		}
		slog_debug("[COMM] Prepared DATASET_INFO for sending.");
		break;
	}
	case STRING:
	{
		msg_size = strlen((char *)data_struct) + 1;
		info_buffer = (char *)data_struct;
		slog_debug("[COMM] \t\t string=%s ", (char *)data_struct);
		break;
	}
	case MSG:
	{
		msg_t *msg = (msg_t *)data_struct;
		msg_size = msg->size;
		info_buffer = (char *)msg->data;
		slog_debug("[COMM] \t\t msg size=%ld ", msg_size);
	}
	}

	// if (send_data(ucp_worker, ep, info_buffer, msg_size, from) < 0)
	msg_size = send_data(ucp_worker, ep, info_buffer, msg_size, from);
	if (msg_size <= 0)
	{
		slog_error("HERCULES_ERR_SENDDYNAMSTRUCT");
		perror("HERCULES_ERR_SENDDYNAMSTRUCT");
		return -1;
	}

	slog_debug("[COMM] send_dynamic end %lu ", msg_size);
	return msg_size;
}

/**
 * @brief  Method retrieving a serialized dynamic data structure.
 * @return bytes of the message received or -1 on error.
 */
int32_t recv_dynamic_stream(ucp_worker_h ucp_worker, ucp_ep_h ep, void *data_struct, int32_t data_type, uint64_t dest, size_t length)
{
	// size_t length = -1;
	// char result[BUFFER_SIZE];
	char *result = NULL; //= (char*)malloc(1024*130);
	// if (length < 0)
	// {
	// 	// get the length of the message to be received.
	// 	length = get_recv_data_length(ucp_worker, dest);
	// 	if (length == 0)
	// 	{
	// 		perror("HERCULES_ERR_GET_RECV_DATA_LENGTH");
	// 		return -1;
	// 	}
	// }

	// reserve memory to the buffer to store the message.
	result = (char *)malloc(sizeof(char) * length);

	slog_info("[COMM] recv_dynamic_stream start, data_type=%d", data_type);
	// receive the message from the backend.
	size_t ret = recv_data(ucp_worker, ep, result, length, dest, 0);
	// length = recv_data(ucp_worker, ep, result, length, dest, 0);
	if (ret == 0)
	{
		slog_error("HERCULES_RECV_DATA_DYNAMIC_STREAM");
		perror("HERCULES_RECV_DATA_DYNAMIC_STREAM");
		free(result);
		return -1;
	}

	char *msg_data = result;
	// Formalize the received information.
	switch (data_type)
	{
	case IMSS_INFO:
	{
		slog_info(" \t\t receiving IMSS_INFO %lu", length);
		imss_info *struct_ = (imss_info *)data_struct;

		// Copy the actual structure into the one provided through reference.
		memcpy(struct_, msg_data, sizeof(imss_info));

		slog_info(" \t\t msg_data=%s", msg_data);

		if (!strncmp("$ERRIMSS_NO_KEY_AVAIL$", struct_->uri_, 22))
		{
			slog_error("[COMM] recv_dynamic_stream end  with error, length=%lu", length);
			// return length;
			free(result);
			return -1;
		}

		msg_data += sizeof(imss_info);
		
		// Copy the dynamic fields into the structure.
		struct_->ips = (char **)malloc(struct_->num_storages * sizeof(char *));

		for (int32_t i = 0; i < struct_->num_storages; i++)
		{
			struct_->ips[i] = (char *)malloc(LINE_LENGTH * sizeof(char));
			memcpy(struct_->ips[i], msg_data, LINE_LENGTH);
			// slog_debug("pointer address = %p", &msg_data);
			msg_data += LINE_LENGTH;
		}

		struct_->status = (int *)malloc(struct_->num_storages * sizeof(int));
		memcpy(struct_->status, msg_data, struct_->num_storages * sizeof(int));

		msg_data += (struct_->num_storages * sizeof(int));
		struct_->arr_num_active_storages = (int *)malloc(struct_->num_storages * sizeof(int));
		memcpy(struct_->arr_num_active_storages, msg_data, struct_->num_storages * sizeof(int));


		// msg_data += struct_->num_storages * sizeof(int);

		// memcpy(struct_->num_active_storages, msg_data, sizeof(int));


		// slog_debug("pointer address = %p", &msg_data);

		break;
	}

	case DATASET_INFO:
	{
		if (!strncmp("$ERRIMSS_NO_KEY_AVAIL$", msg_data, 22))
		{
			slog_error("[COMM] recv_dynamic_stream end  with error, err code 22");
			// return 22;
			free(result);
			return -1;
		}
		slog_info(" \t\t DATASET_INFO %lu", length);
		dataset_info *struct_ = (dataset_info *)data_struct;

		// Copy the actual structure into the one provided through reference.
		memcpy(struct_, msg_data, sizeof(dataset_info));

		// If the size of the message received was bigger than sizeof(dataset_info), something more came with it.

		/*if (zmq_msg_size(&msg_struct) > sizeof(dataset_info)) MIRAR
		  {
		  msg_data += sizeof(dataset_info);

		//Copy the remaining 'data_locations' field into the structure.
		struct_->data_locations = (uint16_t *) malloc(struct_->num_data_elem * sizeof(uint16_t));
		memcpy(struct_->data_locations, msg_data, (struct_->num_data_elem * sizeof(uint16_t)));
		}*/
		break;
	}
	case STRING:
	case BUFFER:
	{
		// if (data_struct == NULL)
		// {
		// 	data_struct = (char *)malloc(length);
		// }
		slog_info(" \t\t receiving STRING or BUFFER %ld", length);
		if (!strncmp("$ERRIMSS_NO_KEY_AVAIL$", msg_data, 22))
		{
			slog_error("[COMM] recv_dynamic_stream end with error %lu, msg_data=%s", length, msg_data);
			free(result);
			// return length;
			return -1;
		}
		memcpy(data_struct, result, length);
		break;
	}
	}
	slog_info("[COMM] recv_dynamic_stream end %lu", length);
	// free(result);
	return length;
}

/**
 * @brief  Method retrieving a serialized dynamic data structure.
 *
 * @param ucp_worker
 * @param ep
 * @param data_struct
 * @param data_type
 * @param dest
 * @param length
 * @return bytes of the message received or -1 on error.
 */
int32_t recv_dynamic_stream_opt(ucp_worker_h ucp_worker, ucp_ep_h ep, void **data_struct, int32_t data_type, uint64_t dest, size_t length)
{
	// size_t length = -1;
	// char result[BUFFER_SIZE];
	void *result = NULL; //= (char*)malloc(1024*130);
	// ucp_tag_recv_info_t info_tag;
	// ucp_tag_message_h msg_tag;
	// async = 1;
	// do
	// {
	// 	ucp_worker_progress(ucp_worker);
	// 	msg_tag = ucp_tag_probe_nb(ucp_worker, dest, tag_mask, 0, &info_tag);
	// } while (msg_tag == NULL);
	// length = info_tag.length

	// reserve memory to the buffer to store the message.
	// result = (char *)malloc(sizeof(char) * length);

	slog_info("[COMM] recv_dynamic_stream start, data_type=%d", data_type);
	// receive the message from the backend.
	// size_t ret = recv_data_opt(ucp_worker, ep, &result, length, dest, 0);
	length = recv_data_opt(ucp_worker, ep, &result, length, dest, 0);
	if (length == 0)
	{
		slog_error("HERCULES_RECV_DATA_DYNAMIC_STREAM");
		perror("HERCULES_RECV_DATA_DYNAMIC_STREAM");
		// free(result);
		return -1;
	}

	if (*data_struct == NULL)
		*data_struct = (void *)malloc(length * sizeof(imss_info));

	char *msg_data = (char *)result;
	// Formalize the received information.
	switch (data_type)
	{
	case IMSS_INFO:
	{
		slog_info(" \t\t receiving IMSS_INFO %lu", length);
		imss_info *struct_ = (imss_info *)*data_struct;

		// Copy the actual structure into the one provided through reference.
		memcpy(struct_, msg_data, sizeof(imss_info));

		slog_info(" \t\t msg_data=%s", msg_data);

		if (!strncmp("$ERRIMSS_NO_KEY_AVAIL$", struct_->uri_, 22))
		{
			slog_error("[COMM] recv_dynamic_stream end  with error, length=%lu", length);
			// return length;
			free(result);
			free(*data_struct);
			return -1;
		}

		msg_data += sizeof(imss_info);

		// Copy the dynamic fields into the structure.
		slog_live("struct_->num_storages=%d", struct_->num_storages);
		struct_->ips = (char **)malloc(struct_->num_storages * sizeof(char *));

		for (int32_t i = 0; i < struct_->num_storages; i++)
		{
			struct_->ips[i] = (char *)malloc(LINE_LENGTH * sizeof(char));
			memcpy(struct_->ips[i], msg_data, LINE_LENGTH);
			msg_data += LINE_LENGTH;
		}

		break;
	}

	case DATASET_INFO:
	{
		if (!strncmp("$ERRIMSS_NO_KEY_AVAIL$", msg_data, 22))
		{
			slog_error("[COMM] recv_dynamic_stream end  with error, err code 22");
			// return 22;
			free(result);
			free(*data_struct);
			return -1;
		}
		slog_info(" \t\t DATASET_INFO %lu", length);
		dataset_info *struct_ = (dataset_info *)*data_struct;

		// Copy the actual structure into the one provided through reference.
		memcpy(struct_, msg_data, sizeof(dataset_info));

		// If the size of the message received was bigger than sizeof(dataset_info), something more came with it.

		/*if (zmq_msg_size(&msg_struct) > sizeof(dataset_info)) MIRAR
		  {
		  msg_data += sizeof(dataset_info);

		//Copy the remaining 'data_locations' field into the structure.
		struct_->data_locations = (uint16_t *) malloc(struct_->num_data_elem * sizeof(uint16_t));
		memcpy(struct_->data_locations, msg_data, (struct_->num_data_elem * sizeof(uint16_t)));
		}*/
		break;
	}
	case STRING:
	case BUFFER:
	{
		// if (*data_struct == NULL)
		// {
		// 	*data_struct = (char *)malloc(length);
		// }
		slog_info(" \t\t receiving STRING or BUFFER %ld", length);
		if (!strncmp("$ERRIMSS_NO_KEY_AVAIL$", msg_data, 22))
		{
			slog_error("[COMM] recv_dynamic_stream end with error %lu, msg_data=%s", length, msg_data);
			free(result);
			free(*data_struct);
			// return length;
			return -1;
		}
		memcpy(*data_struct, result, length);
		break;
	}
	}
	slog_info("[COMM] recv_dynamic_stream end %lu", length);
	// free(result);
	return length;
}

// void ep_close(ucp_worker_h ucp_worker, ucp_ep_h ep, uint64_t flags)
// {
// 	ucp_request_param_t param;
// 	ucs_status_t status;
// 	void *close_req;

// 	param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
// 	param.flags = flags;
// 	close_req = ucp_ep_close_nbx(ep, &param);
// 	if (UCS_PTR_IS_PTR(close_req))
// 	{
// 		do
// 		{
// 			ucp_worker_progress(ucp_worker);
// 			status = ucp_request_check_status(close_req);
// 		} while (status == UCS_INPROGRESS);
// 		ucp_request_free(close_req);
// 	}
// 	else
// 	{
// 		status = UCS_PTR_STATUS(close_req);
// 	}

// 	if (status != UCS_OK)
// 	{
// 		fprintf(stderr, "[COMM] failed to close ep %p", (void *)ep);
// 		slog_error("[COMM] failed to close ep %p", (void *)ep);
// 	}

// 	slog_debug("[COMM] Closed endpoint");
// }

void empty_function(void *request, ucs_status_t status)
{
}

/**
 * Close UCP endpoint.
 *
 * @param [in]  worker  Handle to the worker that the endpoint is associated
 *                      with.
 * @param [in]  ep      Handle to the endpoint to close.
 * @param [in]  flags   Close UCP endpoint mode. Please see
 *                      @a ucp_ep_close_flags_t for details.
 */
void ep_close(ucp_worker_h ucp_worker, ucp_ep_h ep, uint64_t flags)
{
	ucp_request_param_t param;
	ucs_status_t status;
	void *close_req;

	param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
	param.flags = flags;
	close_req = ucp_ep_close_nbx(ep, &param);
	if (UCS_PTR_IS_PTR(close_req))
	{
		do
		{
			ucp_worker_progress(ucp_worker);
			status = ucp_request_check_status(close_req);
		} while (status == UCS_INPROGRESS);
		ucp_request_free(close_req);
	}
	else
	{
		status = UCS_PTR_STATUS(close_req);
	}

	if (status != UCS_OK)
	{
		slog_error("Failed to close ep %p", (void *)ep);
		fprintf(stderr, "failed to close ep %p\n", (void *)ep);
	}
}

ucs_status_t ep_flush(ucp_ep_h ep, ucp_worker_h worker)
{
	void *request;
	StsHeader *req_queue;
	ucx_async_t *async;

	slog_debug("[COMM] Flushed endpoint started.");
	request = ucp_ep_flush_nb(ep, 0, empty_function);
	if (request == NULL)
	{
		return UCS_OK;
	}
	else if (UCS_PTR_IS_ERR(request))
	{
		return UCS_PTR_STATUS(request);
	}
	else
	{
		ucs_status_t status;
		slog_debug("[COMM] Flush waiting for completion.");
		do
		{
			ucp_worker_progress(worker);
			status = ucp_request_check_status(request);
		} while (status == UCS_INPROGRESS);
		ucp_request_free(request);
		slog_debug("[COMM] Flushed endpoint.");
		return status;
	}
	slog_debug("[COMM] Flushed endpoint.");
}

ucs_status_t flush_ep(ucp_worker_h worker, ucp_ep_h ep)
{
	ucp_request_param_t param;
	void *request;

	param.op_attr_mask = 0;
	request = ucp_ep_flush_nbx(ep, &param);
	if (request == NULL)
	{
		return UCS_OK;
	}
	else if (UCS_PTR_IS_ERR(request))
	{
		return UCS_PTR_STATUS(request);
	}
	else
	{
		ucs_status_t status;
		do
		{
			ucp_worker_progress(worker);
			status = ucp_request_check_status(request);
		} while (status == UCS_INPROGRESS);
		ucp_request_free(request);
		return status;
	}
}

int connect_common(const char *server, uint64_t server_port, sa_family_t af)
{
	int sockfd = -1;
	int listenfd = -1;
	int optval = 1;
	char service[8];
	struct addrinfo hints, *res, *t;
	int ret;

	snprintf(service, sizeof(service), "%lu", server_port);
	memset(&hints, 0, sizeof(hints));
	hints.ai_flags = (server == NULL) ? AI_PASSIVE : 0;
	hints.ai_family = af;
	hints.ai_socktype = SOCK_STREAM;

	ret = getaddrinfo(server, service, &hints, &res);
	CHKERR_JUMP(ret < 0, "getaddrinfo() failed", out);

	for (t = res; t != NULL; t = t->ai_next)
	{
		sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
		if (sockfd < 0)
		{
			char err_msg[128] = {"\0"};
			sprintf(err_msg, "HERCULES_ERR_SOCKET_%s", strerror(errno));
			perror(err_msg);
			continue;
		}

		if (server != NULL)
		{
			if (connect(sockfd, t->ai_addr, t->ai_addrlen) == 0)
			{
				break;
			}
		}
		else
		{
			ret = setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &optval,
							 sizeof(optval));
			CHKERR_JUMP(ret < 0, "server setsockopt()", err_close_sockfd);

			if (bind(sockfd, t->ai_addr, t->ai_addrlen) == 0)
			{
				ret = listen(sockfd, 0);
				CHKERR_JUMP(ret < 0, "listen server", err_close_sockfd);

				/* Accept next connection */
				listenfd = sockfd;
				sockfd = accept(listenfd, NULL, NULL);
				close(listenfd);
				break;
			}
		}

		close(sockfd);
		sockfd = -1;
	}

	CHKERR_ACTION(sockfd < 0,
				  (server) ? "open client socket" : "open server socket",
				  (void)sockfd /* no action */);

out_free_res:
	freeaddrinfo(res);
out:
	return sockfd;
err_close_sockfd:
	close(sockfd);
	sockfd = -1;
	goto out_free_res;
}

ucs_status_t ucx_wait(ucp_worker_h ucp_worker, struct ucx_context *request, const char *op_str, const char *data_str)
{
	ucs_status_t status;

	if (UCS_PTR_IS_ERR(request))
	{
		status = UCS_PTR_STATUS(request);
	}
	else if (UCS_PTR_IS_PTR(request))
	{
		while (!request->completed)
		{
			// fprintf(stderr,"Waiting for completed\n");
			ucp_worker_progress(ucp_worker);
		}

		request->completed = 0;
		status = ucp_request_check_status(request);
		// fprintf(stderr,"Final status = %s\n", ucs_status_string(status));
		ucp_request_free(request);
	}
	else
	{
		status = UCS_OK;
	}

	if (status != UCS_OK)
	{
		fprintf(stderr, "unable to %s %s (%s)\n", op_str, data_str,
				ucs_status_string(status));
		slog_error("[COMM][ucx_wait] unable to %s %s (%s)", op_str, data_str, ucs_status_string(status));
	}

	return status;
}

ucs_status_t worker_flush(ucp_worker_h worker)
{
	ucp_worker_fence(worker);
	ucp_worker_flush_nb(worker, 0, flush_cb);
	return UCS_OK;
}
