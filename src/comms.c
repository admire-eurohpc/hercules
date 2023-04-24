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
extern int32_t is_client; // used to make sure the server doesn't do map_ep stuff
pthread_mutex_t map_ep_mutex;

char *send_buffer;
char *recv_buffer;

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

	status = ucp_worker_create(ucp_context, &worker_params, ucp_worker);
	if (status != UCS_OK)
	{
		fprintf(stderr, "failed to ucp_worker_create (%s)", ucs_status_string(status));
		perror("ERRIMSS_WORKER_INIT");
		ret = -1;
	}

	slog_debug("[COMM] Inicializated worker result: %d", ret);
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

	/* UCP initialization */
	memset(&ucp_params, 0, sizeof(ucp_params));

	/* UCP initialization */
	ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES |
							UCP_PARAM_FIELD_REQUEST_SIZE |
							UCP_PARAM_FIELD_REQUEST_INIT |
							UCP_PARAM_FIELD_NAME;

	ucp_params.features = UCP_FEATURE_TAG;
	ucp_params.request_size = sizeof(struct ucx_context);
	ucp_params.request_init = request_init;
	ucp_params.name = "hercules";
	status = ucp_config_read(NULL, NULL, &config);
	status = ucp_init(&ucp_params, config, ucp_context);

	ucp_config_release(config);

	// ucp_context_print_info(*ucp_context,stderr);
	if (status != UCS_OK)
	{
		fprintf(stderr, "failed to ucp_init (%s)", ucs_status_string(status));
		ret = -1;
		goto err;
	}

	ret = init_worker(*ucp_context, ucp_worker);
	if (ret != 0)
	{
		goto err_cleanup;
	}

	ucp_mem_alloc(*ucp_context, 4 * 1024 * 1024, (void **)&send_buffer);
	ucp_mem_alloc(*ucp_context, 4 * 1024 * 1024, (void **)&recv_buffer);

	slog_debug("[COMM] Inicializated context result: %d", ret);
	return ret;

err_cleanup:
	ucp_cleanup(*ucp_context);
err:
	return ret;
}

size_t send_data(ucp_worker_h ucp_worker, ucp_ep_h ep, const char *msg, size_t msg_len, uint64_t from)
{
	ucs_status_t status;
	struct ucx_context *request;
	ucp_request_param_t send_param;
	send_req_t ctx;

	// char req[2048];

	ctx.buffer = (char *)msg;
	// ctx.buffer = (char *)malloc(msg_len);
	ctx.complete = 0;
	// memcpy (ctx.buffer, msg, msg_len);
	// memcpy (send_buffer, msg, msg_len);
	//	ctx.buffer= bb;

	send_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
							  UCP_OP_ATTR_FIELD_USER_DATA;
	send_param.cb.send = send_handler_data;
	// send_param.datatype    = ucp_dt_make_contig(1);
	// send_param.memory_type  = UCS_MEMORY_TYPE_HOST;
	send_param.user_data = &ctx;

	request = (struct ucx_context *)ucp_tag_send_nbx(ep, ctx.buffer, msg_len, from, &send_param);
	//	status = ucx_wait(ucp_worker, request, "send",  "data");

	/*	if  (UCS_PTR_IS_ERR(request)) {
			slog_fatal("[COMM] Error sending to endpoint.");
			return 0;
		}
	*/
	return msg_len;
}

size_t send_req(ucp_worker_h ucp_worker, ucp_ep_h ep, ucp_address_t *addr, size_t addr_len, char *req)
{
	ucs_status_t status;
	struct ucx_context *request;
	size_t msg_len;
	ucp_request_param_t send_param;
	send_req_t ctx;

	msg_req_t *msg;

	msg_len = sizeof(uint64_t) + REQUEST_SIZE + addr_len;
	msg = (msg_req_t *)malloc(msg_len);
	//	msg = (msg_req_t *)send_buffer;

	msg->addr_len = addr_len; // imprimir la long de adress_len.
	memcpy(msg->request, req, REQUEST_SIZE);
	memcpy(msg + 1, addr, addr_len);

	ctx.complete = 0;

	ctx.buffer = (char *)msg;

	send_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
							  UCP_OP_ATTR_FIELD_USER_DATA;

	// send_param.datatype = ucp_dt_make_contig(1);
	send_param.cb.send = send_handler_req;
	// send_param.memory_type  = UCS_MEMORY_TYPE_HOST;
	send_param.user_data = &ctx;

	request = (struct ucx_context *)ucp_tag_send_nbx(ep, msg, msg_len, tag_req, &send_param);
	status = ucx_wait(ucp_worker, request, "send", req);
	free(msg);
	return msg_len;
}

size_t recv_data(ucp_worker_h ucp_worker, ucp_ep_h ep, char *msg, uint64_t dest, int async)
{
	ucp_tag_recv_info_t info_tag;
	ucp_tag_message_h msg_tag;
	ucp_request_param_t recv_param;
	struct ucx_context *request;
	ucs_status_t status;

	async = 1;
	clock_t t;

	// slog_debug("[COMM] Waiting message  as  %" PRIu64 ".", dest)
	do
	{
		ucp_worker_progress(ucp_worker);
		msg_tag = ucp_tag_probe_nb(ucp_worker, dest, tag_mask, 0, &info_tag);
	} while (msg_tag == NULL);

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

	// slog_debug("[COMM] Probe tag (%ld bytes).", info_tag.length);
	//	t = clock();
	if (async)
	{
		request = (struct ucx_context *)TIMING(ucp_tag_recv_nbx(ucp_worker, msg, info_tag.length, dest, tag_mask, &recv_param), "[imss_read]ucp_tag_recv_nbx", ucs_status_ptr_t);
	}
	else
	{
		request = (struct ucx_context *)ucp_tag_recv_nbx(ucp_worker, recv_buffer, info_tag.length, dest, tag_mask, &recv_param);
		memcpy(msg, recv_buffer, info_tag.length);
	}

	// sleep(1);
	status = TIMING(ucx_wait(ucp_worker, request, "recv", "data"), "[imss_read]ucx_wait", ucs_status_t);
	// slog_debug("[COMM] status=%s.", ucs_status_string(status));
	// slog_debug("--- %s\n", msg);

	// t = clock() -t;
	//	double time_taken = ((double)t) / CLOCKS_PER_SEC; // in seconds
	//               slog_info("[srv_worker_helper] recv_nbx time %f s", time_taken);

	// slog_debug("[COMM] Recv tag (%ld bytes).", info_tag.length);
	// fprintf(stderr, "[COMM] Recv tag (%ld bytes).\n", info_tag.length);
	return info_tag.length;
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

void request_init(void *request)
{
	struct ucx_context *contex = (struct ucx_context *)request;
	contex->completed = 0;
}

void send_handler_req(void *request, ucs_status_t status, void *ctx)
{
	struct ucx_context *context = (struct ucx_context *)request;
	context->completed = 1;

	send_req_t *data = (send_req_t *)ctx;
	slog_info("[COMM] send_handler req");
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
	if (status != UCS_ERR_CONNECTION_RESET && status != UCS_ERR_ENDPOINT_TIMEOUT)
		fprintf(stderr, "client error handling callback was invoked with status %d (%s)", status, ucs_status_string(status));
	slog_debug("[COMM] Client error handling callback was invoked with status %d (%s)", status, ucs_status_string(status));
}

void err_cb_server(void *arg, ucp_ep_h ep, ucs_status_t status)
{
	if (status != UCS_ERR_CONNECTION_RESET && status != UCS_ERR_ENDPOINT_TIMEOUT)
		printf("server error handling callback was invoked with status %d (%s)", status, ucs_status_string(status));
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
	ep_params.err_mode = UCP_ERR_HANDLING_MODE_NONE;
	ep_params.err_handler.arg = NULL;

	status = ucp_ep_create(data_worker, &ep_params, server_ep);
	if (status != UCS_OK)
	{
		fprintf(stderr, "failed to create an endpoint on the server: (%s)", ucs_status_string(status));
	}

	slog_debug("[COMM] Created server endpoint");
	return status;
}

ucs_status_t client_create_ep(ucp_worker_h worker, ucp_ep_h *ep, ucp_address_t *peer_addr)
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
	ep_params.err_mode = UCP_ERR_HANDLING_MODE_NONE;
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

	slog_debug("[COMM] send_dynamic start ");
	// Formalize the information to be sent.
	switch (data_type)
	{
	case IMSS_INFO:
	{
		imss_info *struct_ = (imss_info *)data_struct;

		// Calculate the total size of the buffer storing the structure.
		msg_size = sizeof(imss_info) + (LINE_LENGTH * struct_->num_storages);

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
			offset_pt += LINE_LENGTH;
		}

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
		info_buffer = msg->data;
		slog_debug("[COMM] \t\t msg size=%ld ", msg_size = msg->size);
	}
	}

	if (send_data(ucp_worker, ep, info_buffer, msg_size, from) < 0)
	{
		perror("ERRIMSS_SENDDYNAMSTRUCT");
		return -1;
	}
	// TODO free info_buffer
	slog_debug("[COMM] send_dynamic  end %ld ", msg_size);
	return msg_size;
}

// Method retrieving a serialized dynamic data structure.
int32_t recv_dynamic_stream(ucp_worker_h ucp_worker, ucp_ep_h ep, void *data_struct, int32_t data_type, uint64_t dest)
{
	size_t length;
	char result[BUFFER_SIZE];

	slog_debug("[COMM] recv_dynamic_stream start ");
	length = recv_data(ucp_worker, ep, result, dest, 0);

	if (length == 0)
	{
		perror("ERRIMSS_RECVDYNAMSTRUCT_RECV");
		return -1;
	}

	char *msg_data = result;
	// Formalize the received information.
	switch (data_type)
	{
	case IMSS_INFO:
	{
		slog_debug(" \t\t receiving IMSS_INFO %ld", length);
		imss_info *struct_ = (imss_info *)data_struct;

		// Copy the actual structure into the one provided through reference.
		memcpy(struct_, msg_data, sizeof(imss_info));

		slog_debug(" \t\t msg_data=%s", msg_data);

		if (!strncmp("$ERRIMSS_NO_KEY_AVAIL$", struct_->uri_, 22))
		{
			slog_debug("[COMM] recv_dynamic_stream end %ld", length);
			return length;
		}

		msg_data += sizeof(imss_info);

		// Copy the dynamic fields into the structure.

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
			slog_debug("[COMM] recv_dynamic_stream end 22");
			return 22;
		}
		slog_debug(" \t\t DATASET_INFO %ld", length);
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

		slog_debug(" \t\t receiving STRING or BUFFER %ld", length);
		if (!strncmp("$ERRIMSS_NO_KEY_AVAIL$", msg_data, 22))
		{
			slog_debug("[COMM] recv_dynamic_stream end %ld", length);
			return length;
		}
		memcpy(data_struct, result, length);
		break;
	}
	}
	slog_debug("[COMM] recv_dynamic_stream end %ld", length);
	return length;
}

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
		fprintf(stderr, "failed to close ep %p", (void *)ep);
	}

	slog_debug("[COMM] Closed endpoint");
}

void empty_function(void *request, ucs_status_t status)
{
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
			ucp_worker_progress(ucp_worker);
		}

		request->completed = 0;
		status = ucp_request_check_status(request);
		ucp_request_free(request);
	}
	else
	{
		status = UCS_OK;
	}

	if (status != UCS_OK)
	{
		fprintf(stderr, "unable to %s %s (%s)", op_str, data_str,
				ucs_status_string(status));
	}

	return status;
}

ucs_status_t worker_flush(ucp_worker_h worker)
{
	ucp_worker_fence(worker);
	ucp_worker_flush_nb(worker, 0, flush_cb);
	return UCS_OK;
}
