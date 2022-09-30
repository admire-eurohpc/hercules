#include <string.h>
#include <stdlib.h>
#include "imss.h"
#include "queue.h"
#include "comms.h"
#include "map_ep.hpp"
#include <errno.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

static sa_family_t ai_family   = AF_INET;

// TODO
extern StsHeader *req_queue; //not sure if this is necessary
extern void *map_ep; //map_ep used for async write
extern int32_t is_client; //used to make sure the server doesn't do map_ep stuff


/**
 * Create a ucp worker on the given ucp context.
 */
int init_worker(ucp_context_h ucp_context, ucp_worker_h *ucp_worker)
{
	ucp_worker_params_t worker_params;
	ucs_status_t status;
	int ret = 0;

	memset(&worker_params, 0, sizeof(worker_params));

	worker_params.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
	worker_params.thread_mode = UCS_THREAD_MODE_MULTI;

	status = ucp_worker_create(ucp_context, &worker_params, ucp_worker);
	if (status != UCS_OK) {
		fprintf(stderr, "failed to ucp_worker_create (%s)\n", ucs_status_string(status));
		ret = -1;
	}

	return ret;
}

/**
 * Initialize the UCP context and worker.
 */
int init_context(ucp_context_h *ucp_context, ucp_worker_h *ucp_worker,
		send_recv_type_t send_recv_type)
{
	/* UCP objects */
	ucp_params_t ucp_params;
	ucs_status_t status;
	ucp_config_t *config;
	int ret = 0;


	//status = ucp_config_read(NULL, NULL, &config);
	//ucp_config_print(config, stdout, NULL, UCS_CONFIG_PRINT_CONFIG);

	/* UCP initialization */
	memset(&ucp_params, 0, sizeof(ucp_params));

	/* UCP initialization */
	ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES;

	if (send_recv_type == CLIENT_SERVER_SEND_RECV_STREAM) {
		ucp_params.features = UCP_FEATURE_STREAM;
	} else if (send_recv_type == CLIENT_SERVER_SEND_RECV_TAG) {
		ucp_params.features = UCP_FEATURE_TAG;
	} else {
		ucp_params.features = UCP_FEATURE_AM;
	}

	status = ucp_init(&ucp_params, NULL, ucp_context);
	if (status != UCS_OK) {
		fprintf(stderr, "failed to ucp_init (%s)\n", ucs_status_string(status));
		ret = -1;
		goto err;
	}

	ret = init_worker(*ucp_context, ucp_worker);
	if (ret != 0) {
		goto err_cleanup;
	}

	return ret;

err_cleanup:
	ucp_cleanup(*ucp_context);
err:
	return ret;
}

ucs_status_t start_client(ucp_worker_h ucp_worker, const char *address_str, int port, ucp_ep_h *client_ep)
{
	ucp_ep_params_t ep_params;
	struct sockaddr_storage connect_addr;
	ucs_status_t status;

	set_sock_addr(address_str, &connect_addr, port);

	/*
	 * Endpoint field mask bits:
	 * UCP_EP_PARAM_FIELD_FLAGS             - Use the value of the 'flags' field.
	 * UCP_EP_PARAM_FIELD_SOCK_ADDR         - Use a remote sockaddr to connect
	 *                                        to the remote peer.
	 * UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE - Error handling mode - this flag
	 *                                        is temporarily required since the
	 *                                        endpoint will be closed with
	 *                                        UCP_EP_CLOSE_MODE_FORCE which
	 *                                        requires this mode.
	 *                                        Once UCP_EP_CLOSE_MODE_FORCE is
	 *                                        removed, the error handling mode
	 *                                        will be removed.
	 */
	ep_params.field_mask       = UCP_EP_PARAM_FIELD_FLAGS       |
		UCP_EP_PARAM_FIELD_SOCK_ADDR   |
		UCP_EP_PARAM_FIELD_ERR_HANDLER |
		UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE;
	ep_params.err_mode         = UCP_ERR_HANDLING_MODE_PEER;
	ep_params.err_handler.cb   = err_cb_client;
	ep_params.err_handler.arg  = NULL;
	ep_params.flags            = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
	ep_params.sockaddr.addr    = (struct sockaddr*)&connect_addr;
	ep_params.sockaddr.addrlen = sizeof(connect_addr);

	status = ucp_ep_create(ucp_worker, &ep_params, client_ep);
	if (status != UCS_OK) {
		fprintf(stderr, "failed to connect to %s (%s)\n", address_str,
				ucs_status_string(status));
	}

	// TODO
	if (is_client) {
		StsHeader * queue = StsQueue.create();
		/* see if the map_ep exists, otherwise create it */
		if (!map_ep) {
			map_ep = map_ep_create();
			fprintf(stderr, "created map_ep in comms.c\n");

		}
		//add an entry to the map_ep for this ep
		fprintf(stderr, "PUNTERO queue start_client %p \n", queue);
		map_ep_put(map_ep, *client_ep, queue);

		//see if map_ep_put works at all
		fprintf(stderr, "PUNTERO *client_ep start_client %p \n", *client_ep);
		StsHeader * test_queue;
		int found = map_ep_search(map_ep, *client_ep, &test_queue);
		if (found) {
			fprintf(stderr, "found start_client test_queue\n");
			fprintf(stderr, "PUNTERO test_queue start_client %p \n", test_queue);
			if (test_queue) {
				fprintf(stderr, "test_queue not NULL\n");
			}
		}
	}

	return status;
}

ucs_status_t flush_ep(ucp_worker_h worker, ucp_ep_h ep)
{
	ucp_request_param_t param;
	void *request;

	param.op_attr_mask = 0;
	request            = ucp_ep_flush_nbx(ep, &param);
	if (request == NULL) {
		return UCS_OK;
	} else if (UCS_PTR_IS_ERR(request)) {
		return UCS_PTR_STATUS(request);
	} else {
		ucs_status_t status;
		do {
			ucp_worker_progress(worker);
			status = ucp_request_check_status(request);
		} while (status == UCS_INPROGRESS);
		ucp_request_free(request);
		return status;
	}
}

size_t send_stream(ucp_worker_h ucp_worker, ucp_ep_h ep, const char * msg, size_t msg_length)
{
	//printf("[SEND_STREAM] msg=%s, size=%ld\n",msg,msg_length);
	ucp_request_param_t param;
	test_req_t * request;
	test_req_t ctx;

	ctx.complete       = 0;
	param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
		UCP_OP_ATTR_FIELD_DATATYPE |
		UCP_OP_ATTR_FIELD_USER_DATA;
	param.datatype     = ucp_dt_make_contig(1);
	param.user_data    = &ctx;

	/* Client sends a message to the server using the stream API */
	param.cb.send = send_cb;
	request       = (test_req_t*) ucp_stream_send_nbx(ep, msg, msg_length, &param);

	size_t length = 0;
	request_finalize(ucp_worker, (test_req_t *)request, &ctx); 
	//ucp_stream_recv_request_test(request, &length);

	return msg_length;
}


size_t send_istream(ucp_worker_h ucp_worker, ucp_ep_h ep, const char * msg, size_t msg_length)
{
	//printf("[SEND_STREAM] msg=%s, size=%ld\n",msg,msg_length);
	ucp_request_param_t param;
	ucx_async_t * pending = (ucx_async_t *) malloc(sizeof(ucx_async_t));
	StsHeader *req_queue;

	/* set up ucx_async_t object to push it */
	pending->ctx.complete = 0;

	param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
		UCP_OP_ATTR_FIELD_DATATYPE |
		UCP_OP_ATTR_FIELD_USER_DATA;
	param.datatype     = ucp_dt_make_contig(1);
	param.user_data    = pending;

	pending->tmp_msg = (char *) malloc(msg_length);
	memcpy(pending->tmp_msg, msg, msg_length);
	/* Client sends a message to the server using the stream API */
	param.cb.send = isend_cb;
	pending->request       = (test_req_t*) ucp_stream_send_nbx(ep, pending->tmp_msg, msg_length, &param);


	/* find this ep's queue in the map_ep */
	// TODO
	int found = map_ep_search(map_ep, ep, &req_queue);
	if (!found) {
		req_queue = StsQueue.create();
        map_ep_put(map_ep, ep, req_queue);
	} 

	StsQueue.push(req_queue, pending);
	return msg_length;
}


size_t recv_stream(ucp_worker_h ucp_worker, ucp_ep_h ep, char * msg, size_t msg_length)
{

	ucp_request_param_t param;
	test_req_t * request;
	test_req_t ctx;

	ctx.complete       = 0;
	param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
		UCP_OP_ATTR_FIELD_DATATYPE |
		UCP_OP_ATTR_FIELD_USER_DATA;
	param.datatype     = ucp_dt_make_contig(1);
	param.user_data    = &ctx;
	/* Server receives a message from the client using the stream API */
	param.op_attr_mask  |= UCP_OP_ATTR_FIELD_FLAGS;
	param.flags          = UCP_STREAM_RECV_FLAG_WAITALL;
	param.cb.recv_stream = stream_recv_cb;
	request              = (test_req_t*) ucp_stream_recv_nbx(ep, msg, msg_length, &msg_length, &param);

	request_finalize(ucp_worker, request, &ctx);
	//printf("[RECV_STREAM] msg=%s, size=%ld\n",msg,msg_length);
	size_t length = 0;
	//ucp_stream_recv_request_test(&request, &length);
	return msg_length;
}


void set_sock_addr(const char *address_str, struct sockaddr_storage *saddr, int server_port)
{
	struct sockaddr_in *sa_in;
	struct sockaddr_in6 *sa_in6;


	/* The server will listen on INADDR_ANY */
	memset(saddr, 0, sizeof(*saddr));

	switch (ai_family) {
		case AF_INET:
			sa_in = (struct sockaddr_in*)saddr;
			if (address_str != NULL) {
				struct hostent *host_entry;
				char *ip;

				host_entry = gethostbyname(address_str);
				ip = inet_ntoa(*((struct in_addr*) host_entry->h_addr_list[0])); 
				int err = inet_pton(AF_INET, ip, &sa_in->sin_addr);
			} else {
				sa_in->sin_addr.s_addr = INADDR_ANY;
			}
			sa_in->sin_family = AF_INET;
			sa_in->sin_port   = htons(server_port);
			break;
		case AF_INET6:
			sa_in6 = (struct sockaddr_in6*)saddr;
			if (address_str != NULL) {
				inet_pton(AF_INET6, address_str, &sa_in6->sin6_addr);
			} else {
				sa_in6->sin6_addr = in6addr_any;
			}
			sa_in6->sin6_family = AF_INET6;
			sa_in6->sin6_port   = htons(server_port);
			break;
		default:
			fprintf(stderr, "Invalid address family");
			break;
	}

}

/**
 * Progress the request until it completes.
 */
ucs_status_t request_wait(ucp_worker_h ucp_worker, void *request, test_req_t *ctx)
{
	ucs_status_t status;

	/* if operation was completed immediately */
	if (request == NULL) {
		return UCS_OK;
	}

	if (UCS_PTR_IS_ERR(request)) {
		return UCS_PTR_STATUS(request);
	}

	while (ctx->complete == 0) {
		ucp_worker_progress(ucp_worker);
	}
	status = ucp_request_check_status(request);

	ucp_request_free(request);

	return status;
}

void stream_recv_cb(void *request, ucs_status_t status, size_t length,
		void *user_data)
{
	common_cb(user_data, "stream_recv_cb");
}

/**
 * The callback on the receiving side, which is invoked upon receiving the
 * active message.
 */
void am_recv_cb(void *request, ucs_status_t status, size_t length,
		void *user_data)
{
	common_cb(user_data, "am_recv_cb");
}

/**
 * The callback on the sending side, which is invoked after finishing sending
 * the message.
 */
void send_cb(void *request, ucs_status_t status, void *user_data)
{
	common_cb(user_data, "send_cb");
}

void isend_cb(void *request, ucs_status_t status, void *user_data)
{
	ucx_async_t * pending;

	pending = (ucx_async_t *) user_data;

	free(pending->tmp_msg);
	pending->ctx.complete = 1;
}

/**
 * Error handling callback.
 */
void err_cb_client(void *arg, ucp_ep_h ep, ucs_status_t status)
{
	//	if (status != UCS_ERR_CONNECTION_RESET && status != UCS_ERR_ENDPOINT_TIMEOUT)
	printf("client error handling callback was invoked with status %d (%s)\n", status, ucs_status_string(status));
}

void err_cb_server(void *arg, ucp_ep_h ep, ucs_status_t status)
{
	if (status != UCS_ERR_CONNECTION_RESET && status != UCS_ERR_ENDPOINT_TIMEOUT)
		printf("server error handling callback was invoked with status %d (%s)\n",  status, ucs_status_string(status));
}

void common_cb(void *user_data, const char *type_str)
{
	test_req_t *ctx;

	if (user_data == NULL) {
		fprintf(stderr, "user_data passed to %s mustn't be NULL\n", type_str);
		return;
	}

	ctx           = (test_req *)user_data;
	ctx->complete = 1;
}

int request_finalize(ucp_worker_h ucp_worker, test_req_t *request, test_req_t *ctx)
{
	int ret = 0;
	ucs_status_t status;

	status = request_wait(ucp_worker, request, ctx);
	if (status != UCS_OK) {
		fprintf(stderr, "unable to complete UCX message (%s)\n", ucs_status_string(status));
		ret = -1;
		goto release_iov;
	}

release_iov:
	return ret;
}

ucs_status_t start_server(ucp_worker_h ucp_worker, ucx_server_ctx_t *context, ucp_listener_h *listener_p, const char *address_str, int port)
{
	struct sockaddr_storage listen_addr;
	ucp_listener_params_t params;
	ucp_listener_attr_t attr;
	ucs_status_t status;

	set_sock_addr(address_str, &listen_addr, port);

	params.field_mask         = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR |
		UCP_LISTENER_PARAM_FIELD_CONN_HANDLER;
	params.sockaddr.addr      = (const struct sockaddr*)&listen_addr;
	params.sockaddr.addrlen   = sizeof(listen_addr);
	params.conn_handler.cb    = server_conn_handle_cb;
	params.conn_handler.arg   = context;

	/* Create a listener on the server side to listen on the given address.*/
	status = ucp_listener_create(ucp_worker, &params, listener_p);
	if (status != UCS_OK) {
		fprintf(stderr, "failed to listen (%s)\n", ucs_status_string(status));
		goto out;
	}

	/* Query the created listener to get the port it is listening on. */
	attr.field_mask = UCP_LISTENER_ATTR_FIELD_SOCKADDR;
	status = ucp_listener_query(*listener_p, &attr);
	if (status != UCS_OK) {
		fprintf(stderr, "failed to query the listener (%s)\n",
				ucs_status_string(status));
		ucp_listener_destroy(*listener_p);
		goto out;
	}

out:
	return status;
}


/**
 * The callback on the server side which is invoked upon receiving a connection
 * request from the client.
 */
void server_conn_handle_cb(ucp_conn_request_h conn_request, void *arg)
{
	ucx_server_ctx_t *context = (ucx_server_ctx_t *)arg;
	ucp_conn_request_attr_t attr;
	char ip_str[IP_STRING_LEN];
	char port_str[PORT_STRING_LEN];
	ucs_status_t status;

	attr.field_mask = UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR;
	status = ucp_conn_request_query(conn_request, &attr);
	if (status == UCS_OK) {
		/*printf("Server received a connection request from client at address %s:%s\n",
		  sockaddr_get_ip_str(&attr.client_address, ip_str, sizeof(ip_str)),
		  sockaddr_get_port_str(&attr.client_address, port_str, sizeof(port_str)));*/
	} else if (status != UCS_ERR_UNSUPPORTED) {
		fprintf(stderr, "failed to query the connection request (%s)\n",
				ucs_status_string(status));
	}

	StsQueue.push(context->conn_request, conn_request);
}

char* sockaddr_get_ip_str(const struct sockaddr_storage *sock_addr,
		char *ip_str, size_t max_size)
{
	struct sockaddr_in  addr_in;
	struct sockaddr_in6 addr_in6;

	switch (sock_addr->ss_family) {
		case AF_INET:
			memcpy(&addr_in, sock_addr, sizeof(struct sockaddr_in));
			inet_ntop(AF_INET, &addr_in.sin_addr, ip_str, max_size);
			return ip_str;
		case AF_INET6:
			memcpy(&addr_in6, sock_addr, sizeof(struct sockaddr_in6));
			inet_ntop(AF_INET6, &addr_in6.sin6_addr, ip_str, max_size);
			return ip_str;
		default:
			return NULL;
	}
}

char* sockaddr_get_port_str(const struct sockaddr_storage *sock_addr,
		char *port_str, size_t max_size)
{
	struct sockaddr_in  addr_in;
	struct sockaddr_in6 addr_in6;

	switch (sock_addr->ss_family) {
		case AF_INET:
			memcpy(&addr_in, sock_addr, sizeof(struct sockaddr_in));
			snprintf(port_str, max_size, "%d", ntohs(addr_in.sin_port));
			return port_str;
		case AF_INET6:
			memcpy(&addr_in6, sock_addr, sizeof(struct sockaddr_in6));
			snprintf(port_str, max_size, "%d", ntohs(addr_in6.sin6_port));
			return port_str;
		default:
			return NULL;
	}
}


ucs_status_t server_create_ep(ucp_worker_h data_worker,
		ucp_conn_request_h conn_request,
		ucp_ep_h *server_ep)
{
	ucp_ep_params_t ep_params;
	ucs_status_t    status;

	/* Server creates an ep to the client on the data worker.
	 * This is not the worker the listener was created on.
	 * The client side should have initiated the connection, leading
	 * to this ep's creation */
	ep_params.field_mask      = UCP_EP_PARAM_FIELD_ERR_HANDLER |
		UCP_EP_PARAM_FIELD_CONN_REQUEST;
	ep_params.conn_request    = conn_request;
	ep_params.err_handler.cb  = err_cb_server;
	ep_params.err_mode = UCP_ERR_HANDLING_MODE_PEER;
	ep_params.err_handler.arg = NULL;

	status = ucp_ep_create(data_worker, &ep_params, server_ep);
	if (status != UCS_OK) {
		fprintf(stderr, "failed to create an endpoint on the server: (%s)\n",
				ucs_status_string(status));
	}

	return status;
}



//Method sending a data structure with dynamic memory allocation fields.
int32_t send_dynamic_stream(ucp_worker_h ucp_worker, ucp_ep_h ep,
		void *  data_struct,
		int32_t data_type)
{
	//Buffer containing the structures' information.
	char * info_buffer;
	//Buffer size.
	size_t msg_size;

	DPRINT( "[COMM] send_dynamic_stream start \n"); 
	//Formalize the information to be sent.
	switch (data_type)
	{
		case IMSS_INFO:
			{
				imss_info * struct_ = (imss_info *) data_struct;

				//Calculate the total size of the buffer storing the structure.
				msg_size = sizeof(imss_info) + (LINE_LENGTH * struct_->num_storages);

				//Reserve the corresponding amount of memory for the previous buffer.
				info_buffer = (char *) malloc(msg_size * sizeof(char));

				//Control variables dealing with incomming memory management actions.
				char * offset_pt = info_buffer;

				//Copy the actual structure to the buffer.
				memcpy(offset_pt, struct_, sizeof(imss_info));

				offset_pt += sizeof(imss_info);

				//Copy the remaining dynamic fields into the buffer.
				for (int32_t i = 0; i < struct_->num_storages; i++)
				{
					memcpy(offset_pt, struct_->ips[i], LINE_LENGTH);
					offset_pt += LINE_LENGTH;
				}

				break;
			}

		case DATASET_INFO:
			{
				dataset_info * struct_ = (dataset_info *) data_struct;

				//Calculate the total size of the buffer storing the structure.
				msg_size = sizeof(dataset_info);

				//If the dataset is a LOCAL one, the list of position characters must be added.
				if (!strcmp(struct_->policy, "LOCAL"))
					msg_size += (struct_->num_data_elem * sizeof(uint16_t));

				//Reserve the corresponding amount of memory for the previous buffer.
				info_buffer = (char *) malloc(msg_size * sizeof(char));

				//Serialize the provided message into the buffer.

				char * offset_pt = info_buffer;

				//Copy the actual structure to the buffer.
				memcpy(offset_pt, struct_, sizeof(dataset_info));

				//Copy the remaining 'data_locations' field if the dataset is a LOCAL one.
				if (!strcmp(struct_->policy, "LOCAL"))
				{
					offset_pt += sizeof(dataset_info);
					memcpy(offset_pt, struct_->data_locations, (struct_->num_data_elem * sizeof(uint16_t)));
				}

				break;
			}
		case STRING:
			{
				msg_size = strlen((char*) data_struct) + 1;
				info_buffer = (char *)data_struct;
				break;
			}
		case MSG:
			{
				msg_t * msg = (msg_t *) data_struct;
				msg_size = msg->size;
				info_buffer = msg->data;
			}
	}

	//Send the buffer.
	if (send_stream(ucp_worker, ep, (char*)&msg_size, sizeof(size_t)) < 0) {
		perror("ERRIMSS_SENDDYNAMSTRUCT");
		return -1;
	}

	DPRINT("[COMM] send_dynamic_stream length %ld \n", msg_size); 
	if (send_stream(ucp_worker, ep, info_buffer, msg_size) < 0) {
		perror("ERRIMSS_SENDDYNAMSTRUCT");
		return -1;
	}
	// TODO free info_buffer
	DPRINT( "[COMM] send_dynamic_stream content %ld \n", msg_size); 
	DPRINT( "[COMM] send_dynamic_stream end %ld \n", msg_size); 
	return msg_size;
}

//Method retrieving a serialized dynamic data structure.
int32_t recv_dynamic_stream(ucp_worker_h ucp_worker, ucp_ep_h ep,
		void *  data_struct,
		int32_t data_type)
{
	size_t length;
	char result[BUFFER_SIZE];

	DPRINT( "[COMM] recv_dynamic_stream start \n"); 
	if (recv_stream(ucp_worker, ep, (char*)&length, sizeof(size_t)) < 0)
	{
		perror("ERRIMSS_RECVDYNAMSTRUCT_RECV");
		return -1;
	}

	DPRINT( "[COMM] recv_dynamic_stream length %ld \n", length); 
	if (recv_stream(ucp_worker, ep, result, length) < 0) 
	{
		perror("ERRIMSS_RECVDYNAMSTRUCT_RECV");
		return -1;
	}

	DPRINT( "[COMM] recv_dynamic_stream content %ld \n",length); 
	char * msg_data = result;

	//Formalize the received information.
	switch (data_type)
	{
		case IMSS_INFO:
			{

				DPRINT( "[COMM] Recv: receiving IMSS_INFO %ld\n",length);
				imss_info * struct_ = (imss_info *) data_struct;

				//Copy the actual structure into the one provided through reference.
				memcpy(struct_, msg_data, sizeof(imss_info));

				if (!strncmp("$ERRIMSS_NO_KEY_AVAIL$", struct_->uri_, 22))
				{
					DPRINT("[COMM] recv_dynamic_stream end %ld\n", length); 
					return length;
				}

				msg_data += sizeof(imss_info);

				//Copy the dynamic fields into the structure.

				struct_->ips = (char **) malloc(struct_->num_storages * sizeof(char *));

				for (int32_t i = 0; i < struct_->num_storages; i++)
				{
					struct_->ips[i] = (char *) malloc(LINE_LENGTH * sizeof(char));
					memcpy(struct_->ips[i], msg_data, LINE_LENGTH);
					msg_data += LINE_LENGTH;
				}

				break;
			}

		case DATASET_INFO:
			{
				DPRINT("Recv: receiving DATASET_INFO %ld\n", length);
				dataset_info * struct_ = (dataset_info *) data_struct;

				//Copy the actual structure into the one provided through reference.
				memcpy(struct_, msg_data, sizeof(dataset_info));

				if (!strncmp("$ERRIMSS_NO_KEY_AVAIL$", struct_->uri_, 22))
				{
					DPRINT("[COMM] recv_dynamic_stream end %ld\n", length); 
					return length;
				}

				//If the size of the message received was bigger than sizeof(dataset_info), something more came with it.

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

				DPRINT("Recv: receiving STRING or BUFFER %ld\n", length);
				if (!strncmp("$ERRIMSS_NO_KEY_AVAIL$", msg_data, 22))
				{
					DPRINT("[COMM] recv_dynamic_stream end %ld\n", length); 
					return length;
				}
				memcpy(data_struct, result, length);
				break;
			}
	}
	DPRINT("[COMM] recv_dynamic_stream end %ld\n", length); 
	return length;
}

void ep_close(ucp_worker_h ucp_worker, ucp_ep_h ep, uint64_t flags)
{
	ucp_request_param_t param;
	ucs_status_t status;
	void *close_req;
	StsHeader *req_queue;
	ucx_async_t *async;

	param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
	param.flags        = flags;

	if (is_client) {
		//look for this ep's queue in the map
		int found = map_ep_search(map_ep, ep, &req_queue);
		if (found) {
				while (StsQueue.size(req_queue) > 0) {
					async = (ucx_async_t *) StsQueue.pop(req_queue);
					request_finalize(ucp_worker, async->request, &(async->ctx));
					free(async);
				}
			map_ep_erase(map_ep, ep);
		}
	}

	close_req = ucp_ep_close_nbx(ep, &param);
	if (UCS_PTR_IS_PTR(close_req)) {
		do {
			ucp_worker_progress(ucp_worker);
			status = ucp_request_check_status(close_req);
		} while (status == UCS_INPROGRESS);
		ucp_request_free(close_req);
	} else {
		status = UCS_PTR_STATUS(close_req);
	}

	if (status != UCS_OK) {
		fprintf(stderr, "failed to close ep %p\n", (void*)ep);
	}
	DPRINT("[COMM] Closed endpoint\n");
}

void empty_function(void *request, ucs_status_t status)
{
	DPRINT("[COMM] Flushed endpoint\n");
}


ucs_status_t ep_flush(ucp_ep_h ep, ucp_worker_h worker)
{
	void *request;
	StsHeader *req_queue;
	ucx_async_t *async;

	DPRINT( "[COMM] Flushing endpoint\n");
	request = ucp_ep_flush_nb(ep, 0, empty_function);
	if (request == NULL) {
		return UCS_OK;
	} else if (UCS_PTR_IS_ERR(request)) {
		return UCS_PTR_STATUS(request);
	} else {
		ucs_status_t status;
		do {
			ucp_worker_progress(worker);
			status = ucp_request_check_status(request);
		} while (status == UCS_INPROGRESS);
		ucp_request_free(request);
		return status;
	}

	/*if (is_client) {
		fprintf(stderr, "PUNTERO map_ep ep_flush %p \n", map_ep);
		int found = map_ep_search(map_ep, ep, &req_queue);
		if (found) {
			while (StsQueue.size(req_queue) > 0) {
				async = (ucx_async_t *) StsQueue.pop(req_queue);
				request_finalize(worker, async->request, async->ctx);
			}
		}
		StsQueue.destroy(req_queue);
		map_ep_erase(map_ep, ep);
	}*/
}
