#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/sysinfo.h>
#include <signal.h>
#include "imss.h"
#include "workers.h"
#include "directory.h"
#include "records.hpp"
#include "map_server_eps.hpp"
#include <sys/time.h>
// #include <inttypes.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <net/if.h>
#include <arpa/inet.h>

#include <fcntl.h>

// Lock dealing when cleaning blocks
pthread_mutex_t mutex_garbage = PTHREAD_MUTEX_INITIALIZER;

// Initial buffer address.
char *buffer_address;
// Set of locks dealing with the memory buffer access.
pthread_mutex_t *region_locks;
// Segment size (amount of memory assigned to each thread).
uint64_t buffer_segment;

// Memory amount (in GB) assigned to the buffer process.
uint64_t buffer_KB;
// Flag stating that the previous parameter has been received.
int32_t size_received;
// Communication resources in order to retrieve the buffer_GB parameter.
pthread_mutex_t buff_size_mut;
pthread_cond_t buff_size_cond;
int32_t copied;

uint64_t BLOCK_SIZE; // In KB

StsHeader *mem_pool;

// URI of the attached deployment.
char att_imss_uri[URI_];

// Map that stores server side endpoints
void *map_server_eps;

pthread_mutex_t tree_mut = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mp = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t lock_server_network = PTHREAD_MUTEX_INITIALIZER;

ucp_worker_h *ucp_worker_threads;
ucp_address_t **local_addr;
size_t *local_addr_len;
ucx_server_ctx_t context;
ucp_worker_h global_ucp_worker;

extern int IMSS_THREAD_POOL;
int finished = 0;

// const char *TESTX = "imss://lorem_text.txt$1";
// const char *TESTX = "imss://wfc1.dat$1";
// const char *TESTX = "p4x2.save/wfc1.dat";

#define GARBAGE_COLLECTOR_PERIOD 120

int ready(char *tmp_file_path, const char *msg)
{
	char status[25];
	FILE *tmp_file = tmpfile(); // make the file pointer as temporary file.
	tmp_file = fopen(tmp_file_path, "w");
	if (tmp_file == NULL)
	{
		puts("Error in creating temporary file");
		return -1;
	}

	strcpy(status, "STATUS = ");
	strcat(status, msg);

	fwrite(status, strlen(status), 1, tmp_file);

	fclose(tmp_file);

	if (!strncmp(msg, "ERROR", sizeof("ERROR")))
	{
		exit(1);
	}
	return 0;
}

void handle_signal(int signal)
{
	if (signal == SIGUSR1)
	{
		// fprintf(stderr, "Received SIGUSR1\n");
		finished = 1;
		ucs_status_t status;
		status = ucp_worker_signal(global_ucp_worker);
		if (status != UCS_OK)
		{
			fprintf(stderr, "Failed to signal to UCX worker: %s\n", ucs_status_string(status));
		}

		// ucp_listener_destroy(context.listener);
		// pthread_exit(NULL);
		// exit(0);
	}
}

typedef struct
{
	ucp_worker_h worker;
	ucp_ep_h client_ep;
	p_argv arguments;
} thread_arg_t;

void *handle_client(void *arg)
{
	thread_arg_t *thread_arg = (thread_arg_t *)arg;

	ucp_worker_h ucp_worker = thread_arg->worker;
	ucp_ep_h client_ep = thread_arg->client_ep;
	p_argv arguments = thread_arg->arguments;

	ucp_ep_params_t ep_params;
	ucp_address_t *peer_addr;
	size_t peer_addr_len;
	ucs_status_t ep_status = UCS_OK;
	// ucp_ep_h ep;
	ucp_tag_message_h msg_tag;
	ucp_tag_recv_info_t info_tag;
	ucp_request_param_t recv_param;
	ucs_status_t status;

	struct ucx_context *request = NULL;
	ucs_status_ptr_t request_x;
	char *req;
	msg_req_t *msg;
	int ret = 0;
	// fprintf(stderr, "Handling client in thread\n");
	for (;;)
	{
		do
		{
			// fprintf(stderr, "progressing worker\n");
			ucp_worker_progress(ucp_worker);
			/* Probing incoming events in non-block mode */
			// fprintf(stderr, "progressing ucp_tag_probe_nb\n");
			msg_tag = ucp_tag_probe_nb(ucp_worker, tag_req, tag_mask, 1, &info_tag);
		} while (msg_tag == NULL);
		// char recv_buffer[1024];
		// while (1) {
		//     msg_tag = ucp_tag_probe_nb(ucp_worker, tag_req, tag_mask, 1, &info_tag);
		//     if (msg_tag != NULL) {
		//         request_x = ucp_tag_msg_recv_nb(ucp_worker, recv_buffer, info_tag.length, ucp_dt_make_contig(1), msg_tag, NULL);
		//         if (UCS_PTR_IS_ERR(request_x)) {
		//             fprintf(stderr, "Unable to receive UCX message\n");
		//             break;
		//         }
		//         while (ucp_request_check_status(request_x) == UCS_INPROGRESS) {
		//             ucp_worker_progress(ucp_worker);
		//         }
		//         printf("Received message: %s\n", recv_buffer);
		//         ucp_request_free(request_x);
		//     }
		//     ucp_worker_progress(ucp_worker);
		// }
		// exit(0);

		// slog_debug("New req, message length=%ld bytes.", info_tag.length);
		// info_tag.length = 1000;
		// fprintf(stderr, "New req, message length=%ld bytes.\n", info_tag.length);
		msg = (msg_req_t *)malloc(info_tag.length);

		recv_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
								  UCP_OP_ATTR_FIELD_DATATYPE;

		recv_param.datatype = ucp_dt_make_contig(1);
		recv_param.cb.recv = recv_handler;

		// request = (struct ucx_context *)ucp_tag_msg_recv_nbx(ucp_worker, msg, info_tag.length, msg_tag, &recv_param);
		request = (struct ucx_context *)ucp_tag_msg_recv_nbx(ucp_worker, msg, info_tag.length, msg_tag, &recv_param);
		// fprintf(stderr, "Waiting to complete recv\n");
		status = ucx_wait(ucp_worker, request, "receive", "srv_worker");
		peer_addr_len = msg->addr_len;
		peer_addr = (ucp_address *)malloc(peer_addr_len);
		req = msg->request;

		memcpy(peer_addr, msg + 1, peer_addr_len);

		ucp_worker_address_attr_t attr;
		attr.field_mask = UCP_WORKER_ADDRESS_ATTR_FIELD_UID;
		ucp_worker_address_query(peer_addr, &attr);
		slog_debug("Receiving request from %" PRIu64 ".", attr.worker_uid);

		// //  look for this peer_addr in the map and get the ep
		// ret = map_server_eps_search(map_server_eps, attr.worker_uid, &ep);
		// // create ep if it's not in the map
		// if (ret < 0)
		// {
		// 	// ucp_ep_h new_ep;
		// 	ep_params.address = peer_addr;
		// 	ep_params.user_data = &ep_status;
		// 	// struct worker_info *worker_info = (struct worker_info*)malloc(sizeof(struct worker_info));
		// 	// worker_info->worker_uid = attr.worker_uid;
		// 	// worker_info->server_type = 'd';
		// 	// ep_params.err_handler.arg = &worker_info;
		// 	ep_params.err_handler.arg = &attr.worker_uid;

		// 	status = ucp_ep_create(ucp_worker, &ep_params, &ep);
		// 	// add ep to the map
		// 	map_server_eps_put(map_server_eps, attr.worker_uid, ep);
		// }
		// else
		// {
		// 	slog_debug("\t[srv_worker]['%" PRIu64 "] Endpoint already exist'", attr.worker_uid);
		// 	// fprintf(stderr, "\t[d]['%" PRIu64 "] Endpoint already exist'\n", attr.worker_uid);
		// }

		arguments.peer_address = peer_addr;
		// arguments.server_ep = ep;
		arguments.server_ep = client_ep; // FIX = this is provisional until map_servers_eps is implemented.
		arguments.worker_uid = attr.worker_uid;
		arguments.req = req;
		// fprintf(stderr, "Attending request '%s'\n", req);
		switch (arguments.server_type)
		{
		case 'd':
			ret = srv_worker_helper(&arguments);
			break;
		case 'm':
			ret = stat_worker_helper(&arguments);
			break;
		default:
			break;
		}

		// fprintf(stderr, "Request '%s' has been attended\n", req);
		free(peer_addr);
		if (ret == 2)
		{
			break;
		}
	}
	// fprintf(stderr, "Client is finishing the communication\n");
	// ucp_ep_close_nb(client_ep, UCP_EP_CLOSE_MODE_FLUSH);
	ucp_ep_close_nb(client_ep, UCP_EP_CLOSE_MODE_FORCE);
	// fprintf(stderr, "Ending thread\n");

	return NULL;
}

void server_accept_cb(ucp_conn_request_h conn_request, void *arg)
{
	// ucp_worker_h worker = (ucp_worker_h)arg;
	// ucp_context_h ucp_context = (ucp_context_h)arg;
	p_argv *arguments = (p_argv *)arg;

	ucp_ep_h client_ep;
	ucp_ep_params_t ep_params;
	ucs_status_t status;
	ucp_worker_h ucp_data_worker;
	ucp_context_h ucp_context = arguments->ucp_context;

	/* Create a data worker (to be used for data exchange between the server
	 * and the client after the connection between them was established) */
	init_worker_ori(ucp_context, &ucp_data_worker);
	// if (ret != 0)
	// {
	// 	// goto err;
	// 	// exit(NULL);
	// }

	ep_params.field_mask = UCP_EP_PARAM_FIELD_CONN_REQUEST |
						   UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE |
						   UCP_EP_PARAM_FIELD_ERR_HANDLER;
	ep_params.conn_request = conn_request;
	ep_params.err_mode = UCP_ERR_HANDLING_MODE_NONE;
	ep_params.err_handler.cb = err_cb_server;
	ep_params.err_handler.arg = NULL;

	status = ucp_ep_create(ucp_data_worker, &ep_params, &client_ep);
	if (status != UCS_OK)
	{
		fprintf(stderr, "Failed to create endpoint: %s\n", ucs_status_string(status));
		return;
	}

	pthread_t thread;
	thread_arg_t *thread_arg = (thread_arg_t *)malloc(sizeof(thread_arg_t));
	thread_arg->worker = ucp_data_worker;
	thread_arg->client_ep = client_ep;
	arguments->ucp_worker = ucp_data_worker;
	arguments->client_ep = client_ep;

	memcpy(&thread_arg->arguments, arguments, sizeof(p_argv));
	// thread_arg->arguments = arguments;

	pthread_create(&thread, NULL, handle_client, thread_arg);
	// pthread_join(thread, NULL);
	pthread_detach(thread);
}

ucs_status_t start_server(ucp_worker_h ucp_worker, ucp_context_h ucp_context, ucx_server_ctx_t *context, ucp_listener_h *listener_p, const char *address_str, uint64_t server_port, p_argv *arguments)
{
	struct sockaddr_storage listen_addr;
	ucp_listener_params_t params;
	ucp_listener_attr_t attr;
	ucs_status_t status;
	char ip_str[IP_STRING_LEN];
	char port_str[PORT_STRING_LEN];

	set_sock_addr(address_str, &listen_addr, server_port, 1);

	params.field_mask = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR |
						UCP_LISTENER_PARAM_FIELD_CONN_HANDLER;
	params.sockaddr.addr = (const struct sockaddr *)&listen_addr;
	params.sockaddr.addrlen = sizeof(listen_addr);
	params.conn_handler.cb = server_accept_cb;
	// params.conn_handler.arg = ucp_context;
	params.conn_handler.arg = (void *)arguments;

	/* Create a listener on the server side to listen on the given address.*/
	status = ucp_listener_create(ucp_worker, &params, listener_p);
	if (status != UCS_OK)
	{
		fprintf(stderr, "failed to listen (%s)\n", ucs_status_string(status));
		goto out;
	}

	/* Query the created listener to get the port it is listening on. */
	attr.field_mask = UCP_LISTENER_ATTR_FIELD_SOCKADDR;
	status = ucp_listener_query(*listener_p, &attr);
	if (status != UCS_OK)
	{
		fprintf(stderr, "failed to query the listener (%s)\n",
				ucs_status_string(status));
		ucp_listener_destroy(*listener_p);
		goto out;
	}

	fprintf(stderr, "server is listening on IP %s port %s\n",
			sockaddr_get_ip_str(&attr.sockaddr, ip_str, IP_STRING_LEN),
			sockaddr_get_port_str(&attr.sockaddr, port_str, PORT_STRING_LEN));

	fprintf(stderr, "Waiting for connection...\n");

out:
	return status;
}

// Thread method attending client read-write data requests.
void *srv_worker(void *th_argv)
{
	// ucp_ep_params_t ep_params;

	// ucp_am_handler_param_t param;
	// ucs_status_t status;

	// int ret = 0;
	p_argv *arguments = (p_argv *)th_argv;

	/* UCP objects */
	ucp_context_h ucp_context;
	// ucp_worker_h ucp_worker; // = arguments->ucp_worker;

	init_context_ori(&ucp_context, &global_ucp_worker, CLIENT_SERVER_SEND_RECV_TAG);

	// ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS |
	// 					   UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE |
	// 					   UCP_EP_PARAM_FIELD_ERR_HANDLER |
	// 					   UCP_EP_PARAM_FIELD_USER_DATA;
	// ep_params.err_mode = UCP_ERR_HANDLING_MODE_PEER;
	// ep_params.err_handler.cb = err_cb_server;
	// // ep_params.err_handler.arg = NULL;

	map_server_eps = map_server_eps_create();

	BLOCK_SIZE = arguments->blocksize * 1024;

	ucx_server_ctx_t context;
	ucp_worker_h ucp_data_worker;
	ucp_ep_h server_ep;
	ucs_status_t status;
	ucp_address_t *peer_addr;
	size_t peer_addr_len;
	ucp_request_param_t recv_param;
	void *request = NULL;
	// void *msg;
	// struct msg *msg = NULL;
	msg_req_t *msg;
	int ret;

	// /* Create a data worker (to be used for data exchange between the server
	//  * and the client after the connection between them was established) */
	// ret = init_worker_ori(ucp_context, &ucp_data_worker);
	// if (ret != 0)
	// {
	// 	// goto err;
	// 	// exit(NULL);
	// }

	/* Initialize the server's context. */
	context.conn_request = NULL;

	// char server_add[] = "192.168.201.162";
	// FIX: add dynamic address.
	// char server_add[] = "broadwell-001";
	char *server_add = NULL;
	arguments->ucp_context = ucp_context;
	start_server(global_ucp_worker, ucp_context, &context, &context.listener, server_add, arguments->port + 1, arguments);

	/* Server is always up listening */
	// while (1)
	// {
	/* Wait for the server to receive a connection request from the client.
	 * If there are multiple clients for which the server's connection request
	 * callback is invoked, i.e. several clients are trying to connect in
	 * parallel, the server will handle only the first one and reject the rest */
	// while (context.conn_request == NULL)
	//  {
	//  	ucp_worker_progress(ucp_worker);
	//  }

	// Register signal handler
	signal(SIGUSR1, handle_signal);

	while (!finished)
	{
		status = ucp_worker_wait(global_ucp_worker);
		if (status == UCS_ERR_BUSY)
		{
			// Work was already handled, continue the loop
			continue;
		}
		else if (status != UCS_OK)
		{
			fprintf(stderr, "Failed to wait on UCX worker: %s\n", ucs_status_string(status));
			break;
		}
		ucp_worker_progress(global_ucp_worker);
	}

	ucp_listener_destroy(context.listener);

	pthread_exit(NULL);
}

// Thread method searching and cleaning nodes with st_nlink=0
void *garbage_collector(void *th_argv)
{
	// Obtain the current map class element from the set of arguments.
	map_records *map = (map_records *)th_argv;

	for (;;)
	{
		// Gnodetraverse_garbage_collector(map);//Future
		sleep(GARBAGE_COLLECTOR_PERIOD);
		pthread_mutex_lock(&mutex_garbage);
		map->cleaning();
		pthread_mutex_unlock(&mutex_garbage);
	}
	pthread_exit(NULL);
}

// Thread method attending client read-write metadata requests.
void *stat_worker(void *th_argv)
{
	p_argv *arguments = (p_argv *)th_argv;

	/* UCP objects */
	ucp_context_h ucp_context;
	// ucp_worker_h ucp_worker; // = arguments->ucp_worker;

	init_context_ori(&ucp_context, &global_ucp_worker, CLIENT_SERVER_SEND_RECV_TAG);

	ucp_worker_h ucp_data_worker;
	ucp_ep_h server_ep;
	ucs_status_t status;
	ucp_address_t *peer_addr;
	ucp_am_handler_param_t param;
	size_t peer_addr_len;
	void *request = NULL;
	// void *msg;
	// struct msg *msg = NULL;
	msg_req_t *msg;
	int ret = 0;

	/* Initialize the server's context. */
	context.conn_request = NULL;
	char *server_add = NULL;
	arguments->ucp_context = ucp_context;
	start_server(global_ucp_worker, ucp_context, &context, &context.listener, server_add, arguments->port + 1, arguments);

	// p_argv *arguments = (p_argv *)th_argv;
	map_server_eps = map_server_eps_create();

	// Register signal handler
	signal(SIGUSR1, handle_signal);

	while (!finished)
	{
		status = ucp_worker_wait(global_ucp_worker);
		if (status == UCS_ERR_BUSY)
		{
			// Work was already handled, continue the loop
			continue;
		}
		else if (status != UCS_OK)
		{
			fprintf(stderr, "Failed to wait on UCX worker: %s\n", ucs_status_string(status));
			break;
		}
		ucp_worker_progress(global_ucp_worker);
	}

	ucp_listener_destroy(context.listener);
	pthread_exit(NULL);
}

// int stat_worker_helper(p_argv *arguments, char *req)
int stat_worker_helper(void *th_argv)
{
	// ucs_status_t status;
	int ret;

	p_argv *arguments = (p_argv *)th_argv;
	const char *req = arguments->req;

	// Obtain the current map class element from the set of arguments.
	std::shared_ptr<map_records> map = arguments->map;

	uint16_t current_offset = 0;

	// Resources specifying if the ZMQ_SNDMORE flag was set in the sender.
	int64_t more;
	size_t more_size = sizeof(more);

	// Code to be sent if the requested to-be-read key does not exist.
	char err_code[] = "$ERRIMSS_NO_KEY_AVAIL$";

	/*struct timeval start, end;
	  long delta_us;*/

	int operation = 0;
	char mode[MODE_SIZE];

	// slog_debug("[STAT WORKER] Waiting for new request.");
	// Save the request to be served.
	// recv_data(arguments->ucp_worker, arguments->server_ep, req);
	// slog_info("[STAT WORKER] Request - '%s'", req);
	sscanf(req, "%" PRIu32 " %s", &operation, mode);

	const char *req_content = strstr(req, mode);
	req_content += 4;

	if (!strcmp(mode, "GET"))
		more = GET_OP;
	else
		more = SET_OP;

	// Expeted incomming message format: "SIZE_IN_KB KEY"
	int32_t req_size = strlen(req_content);

	char raw_msg[req_size + 1];
	memcpy((void *)raw_msg, req_content, req_size);
	raw_msg[req_size] = '\0';

	// printf("*********worker_metadata raw_msg %s",raw_msg);
	slog_info("[workers][stat_worker_helper] request received=%s", req);

	// Reference to the client request.
	char number[16];
	sscanf(raw_msg, "%s", number);
	int32_t number_length = (int32_t)strlen(number);
	// Elements conforming the request.
	char *uri_ = raw_msg + number_length + 1;
	uint64_t block_size_recv = (uint64_t)atoi(number);

	slog_info("[workers][stat_worker_helper] operation=%d, number=%s, number_length=%d, uri=%s, block_size_recv=%ld", operation, number, number_length, uri_, block_size_recv);

	// Create an std::string in order to be managed by the map structure.
	std::string key;
	key.assign((const char *)uri_);

	// Information associated to the arriving key.
	void *address_;
	uint64_t block_size_rtvd;
	dataset_info *dataset;

	// printf("stat_worker RECV more=%ld, blocksss=%ld",more, block_size_recv);
	// Differentiate between READ and WRITE operations.
	switch (more)
	{
	// No more messages will arrive to the socket.
	case GET_OP:
	{
		switch (block_size_recv)
		{
		case GETDIR:
		{
			// slog_fatal("stat_server GETDIR key=%s",key.c_str());
			char *buffer;
			int32_t numelems_indir;
			// Retrieve all elements inside the requested directory.
			pthread_mutex_lock(&tree_mut);
			slog_info("[workers][stat_worker_helper] Calling GTree_getdir, key=%s", key.c_str());
			buffer = GTree_getdir((char *)key.c_str(), &numelems_indir);
			slog_info("[workers][stat_worker_helper] Ending GTree_getdir, key=%s, numelems_indir=%d", key.c_str(), numelems_indir);
			pthread_mutex_unlock(&tree_mut);
			if (buffer == NULL)
			{
				if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid) < 0)
				{
					perror("HERCULES_ERR_STATWORKER_NODIR");
					return -1;
				}
				break;
			}

			// Send the serialized set of elements within the requested directory.
			msg_t m;
			m.size = numelems_indir * URI_;
			m.data = buffer;

			slog_info("[workers][stat_worker_helper] MSG, numelems_indir=%ld, size=%ld", numelems_indir, m.size);

			if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, (char *)&m, MSG, arguments->worker_uid) < 0)
			{
				perror("ERRIMSS_WORKER_SENDBLOCK");
				return -1;
			}

			slog_debug("[workers][stat_worker_helper] buffer=%s", buffer);

			// char *curr = buffer;
			// char *item = (char *)malloc(URI_ * sizeof(char));
			// for (int32_t i = 0; i < numelems_indir; i++)
			// {
			// 	memcpy(item, curr, URI_);
			// 	//(*items)[i] = elements;
			// 	slog_debug("[IMSS][get_dir] item %d: %s", i, item);
			// 	curr += URI_;
			// }
			// free(item);
			free(buffer);
			break;
		}
		case READ_OP:
		{
			// printf("STAT_WORKER READ_OP");
			// Check if there was an associated block to the key.
			int err = map->get(key, &address_, &block_size_rtvd);
			slog_debug("[STAT WORKER] map->get (key %s, block_size_rtvd %ld) get res %d", key.c_str(), block_size_rtvd, err);

			if (err == 0)
			{
				// Send the error code block.
				if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid) < 0)
				{
					perror("ERRIMSS_WORKER_SENDERR");
					return -1;
				}
			}
			else
			{
				// dataset_info *dataset = (dataset_info*) address_;
				// printf("[STAT_SERVER] dataset.original=%s",dataset->original_name);
				// imss_info * data = (imss_info *) address_;
				// printf("READ_OP SEND data->type=%c",data->type);
				// Send the requested block.

				dataset = (dataset_info *)address_;
				slog_debug("Before dataset->n_open=%d", dataset->n_open);
				// Checks if the clients wants to open the file.
				switch (operation)
				{
				case 1: // file opened.
					dataset->n_open += 1;
					slog_debug("File opened");
					break;

				default:
					break;
				}

				slog_debug("After dataset->n_open=%d", dataset->n_open);

				msg_t m;
				m.data = address_;
				m.size = block_size_rtvd;
				err = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, (char *)&m, MSG, arguments->worker_uid);
				if (err < 0)
				{
					perror("ERRIMSS_WORKER_SENDBLOCK");
					return -1;
				}
			}
			break;
		}
		case RELEASE:
		{
			slog_debug("[stat_worker_thread][READ_OP][RELEASE] Deleting endpoint with %" PRIu64 "", arguments->worker_uid);
			// sleep(10);
			map_server_eps_erase(map_server_eps, arguments->worker_uid, arguments->ucp_worker);
			return 2;
			// ucp_destroy(arguments->ucp_context);
			slog_debug("[stat_worker_thread][READ_OP][RELEASE] Endpoints deleted ");
			break;
			// return 0;
		}
		case DELETE_OP:
		{
			slog_debug("DELETE_OP");
			int err = map->get(key, &address_, &block_size_rtvd);
			slog_debug("[STAT WORKER] map->get (key %s, block_size_rtvd %ld) get res %d", key.c_str(), block_size_rtvd, err);
			if (err == 0)
			{
				// Send the error code block.
				if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid) < 0)
				{
					perror("ERRIMSS_WORKER_SENDERR");
					return -1;
				}
			}
			else
			{
				dataset = (dataset_info *)address_;

				// Checks if the clients wants to unlink the file.
				switch (operation)
				{
				case 4: // unlink.
					strncpy(dataset->status, "dest", strlen("dest"));
					slog_debug("Dataset mark as dest");
					break;
				default:
					break;
				}

				slog_debug("dataset->n_open=%d, dataset->status=%s", dataset->n_open, dataset->status);
				char release_msg[10];	  //= "DELETE\0";
				if (dataset->n_open == 0) // if no more process has the file opened.
				{
					int32_t result = map->delete_metadata_stat_worker(key);
					slog_debug("[stat_worker_thread][READ_OP][DELETE_OP] delete_metadata_stat_worker=%d", result);
					GTree_delete((char *)key.c_str());
					strncpy(release_msg, "DELETE\0", strlen("DELETE\0") + 1);
				}
				else
				{
					strncpy(release_msg, "NODELETE\0", strlen("NODELETE") + 1);
				}

				// char release_msg[] = "DELETE\0";
				slog_debug("release_msg=%s", release_msg);
				if (send_data(arguments->ucp_worker, arguments->server_ep, release_msg, strlen(release_msg) + 1, arguments->worker_uid) == 0)
				{
					perror("ERR_HERCULES_PUBLISH_DELETEMSG");
					slog_error("ERR_HERCULES_PUBLISH_DELETEMSG");
					return -1;
				}
			}
			break;
		}
		case RENAME_OP:
		{
			std::size_t found = key.find(' ');
			if (found != std::string::npos)
			{
				std::string old_key = key.substr(0, found);
				std::string new_key = key.substr(found + 1, key.length());

				slog_debug("[stat_worker_helper][RENAME] old_key=%s, new_key=%s\n", old_key.c_str(), new_key.c_str());

				// RENAME MAP
				int32_t result = map->rename_metadata_stat_worker(old_key, new_key);
				slog_live("rename metadata stat worker=%d", result);
				if (result == 0)
				{
					// printf("0 elements rename from stat_worker");
					slog_warn("[stat_worker_helper][RENAME] 0 elements rename from stat_worker");
					break;
				}

				// RENAME TREE
				int ret = GTree_rename((char *)old_key.c_str(), (char *)new_key.c_str());
				slog_debug("[stat_worker_helper][RENAME] GTree_rename=%d", ret);
			}

			char release_msg[] = "RENAME\0";

			if (send_data(arguments->ucp_worker, arguments->server_ep, release_msg, strlen(release_msg) + 1, arguments->worker_uid) == 0)
			{
				perror("ERR_HERCULES_PUBLISH_RENAMEMSG");
				perror("ERR_HERCULES_PUBLISH_RENAMEMSG");
				return -1;
			}
			break;
		}
		case RENAME_DIR_DIR_OP:
		{
			std::size_t found = key.find(' ');
			if (found != std::string::npos)
			{
				std::string old_dir = key.substr(0, found);
				std::string rdir_dest = key.substr(found + 1, key.length());

				// RENAME MAP
				map->rename_metadata_dir_stat_worker(old_dir, rdir_dest);

				// RENAME TREE
				GTree_rename_dir_dir((char *)old_dir.c_str(), (char *)rdir_dest.c_str());
			}

			char release_msg[] = "RENAME\0";

			if (send_data(arguments->ucp_worker, arguments->server_ep, release_msg, strlen(release_msg) + 1, arguments->worker_uid) == 0)
			{
				perror("ERR_HERCULES_PUBLISH_RENAMEMSG");
				slog_error("ERR_HERCULES_PUBLISH_RENAMEMSG");
				return -1;
			}
			break;
		}
		case CLOSE_OP:
		{
			slog_debug("CLOSE_OP");
			int err = map->get(key, &address_, &block_size_rtvd);
			slog_debug("[STAT WORKER] map->get (key %s, block_size_rtvd %ld) get res %d", key.c_str(), block_size_rtvd, err);
			if (err == 0)
			{
				// Send the error code block.
				if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid) < 0)
				{
					perror("ERRIMSS_WORKER_SENDERR");
					return -1;
				}
			}
			else
			{
				dataset = (dataset_info *)address_;
				// Checks if the clients wants to open the file.
				// switch (operation)
				// {
				// case 1: // file opened.
				slog_debug("Closing file, dataset->n_open=%d", dataset->n_open);
				if (dataset->n_open > 0)
				{
					dataset->n_open -= 1;
				}

				// slog_debug("File closed");
				// break;
				// default:
				// 	break;
				// }
				slog_debug("After dataset->n_open=%d, status=%s", dataset->n_open, dataset->status);
				char release_msg[10]; //= "DELETE\0";
									  // if file status is marked as "dest", it is delete after close.
				if (!strncmp(dataset->status, "dest", strlen("dest")) && dataset->n_open == 0)
				{
					slog_debug("Deleting %s", key.c_str());
					int32_t result = map->delete_metadata_stat_worker(key);
					slog_debug("[stat_worker_thread][READ_OP][DELETE_OP] delete_metadata_stat_worker=%d", result);
					GTree_delete((char *)key.c_str());
					strcpy(release_msg, "DELETE\0");
				}
				else
				{
					strcpy(release_msg, "CLOSE\0");
				}
				// int32_t result = map->delete_metadata_stat_worker(key);
				// slog_debug("[stat_worker_thread][READ_OP][DELETE_OP] delete_metadata_stat_worker=%d", result);
				// GTree_delete((char *)key.c_str());
				// char release_msg[] = "CLOSE\0";
				// if (send_data(arguments->ucp_worker, arguments->server_ep, release_msg, RESPONSE_SIZE, arguments->worker_uid) < 0)
				if (send_data(arguments->ucp_worker, arguments->server_ep, release_msg, strlen(release_msg) + 1, arguments->worker_uid) == 0)
				{
					perror("ERR_HERCULES_PUBLISH_DELETEMSG");
					slog_error("ERR_HERCULES_PUBLISH_DELETEMSG");
					return -1;
				}
			}
			break;
		}
		case OPEN_OP:
		{
			slog_debug("OPEN_OP");
			int err = map->get(key, &address_, &block_size_rtvd);
			slog_debug("[STAT WORKER] map->get (key %s, block_size_rtvd %ld) get res %d", key.c_str(), block_size_rtvd, err);
			if (err == 0)
			{
				// Send the error code block.
				if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid) < 0)
				{
					perror("ERRIMSS_WORKER_SENDERR");
					return -1;
				}
			}
			else
			{
				dataset = (dataset_info *)address_;
				// Checks if the clients wants to open the file.
				// switch (operation)
				// {
				// case 1: // file opened.
				slog_debug("Before dataset->n_open=%d", dataset->n_open);
				dataset->n_open += 1;
				slog_debug("File closed");
				// break;
				// default:
				// 	break;
				// }
				slog_debug("After dataset->n_open=%d, status=%s", dataset->n_open, dataset->status);
				char release_msg[10]; //= "DELETE\0";
				strncpy(release_msg, "OPEN", strlen("OPEN") + 1);
				// int32_t result = map->delete_metadata_stat_worker(key);
				// slog_debug("[stat_worker_thread][READ_OP][DELETE_OP] delete_metadata_stat_worker=%d", result);
				// GTree_delete((char *)key.c_str());
				// char release_msg[] = "CLOSE\0";
				if (send_data(arguments->ucp_worker, arguments->server_ep, release_msg, strlen(release_msg) + 1, arguments->worker_uid) == 0)
				{
					perror("ERR_HERCULES_PUBLISH_DELETEMSG");
					slog_error("ERR_HERCULES_PUBLISH_DELETEMSG");
					return -1;
				}
			}
			break;
		}
		default:
			break;
		}
		break;
	}
		// More messages will arrive to the socket.
	case SET_OP:
	{
		slog_debug("SET_OP");
		slog_debug("[STAT WORKER] Creating dataset %s.", key.c_str());
		// pthread_mutex_lock(&mp);
		// If the record was not already stored, add the block.
		if (!map->get(key, &address_, &block_size_rtvd))
		{
			// pthread_mutex_unlock(&mp);
			slog_debug("[STAT WORKER] Adding new block %p", &address_);

			slog_debug("[STAT WORKER] Recv dynamic buffer size %ld", block_size_recv);
			// Get the length of the message to be received.
			size_t length = 0;
			int32_t ret = -1;
			length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
			if (length == 0)
			{
				perror("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_SET_OP");
				slog_error("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_SET_OP");
				return -1;
			}
			// Receive the block into the buffer.
			void *buffer = malloc(block_size_recv);
			ret = recv_dynamic_stream(arguments->ucp_worker, arguments->server_ep, buffer, BUFFER, arguments->worker_uid, length);
			// length = recv_dynamic_stream_opt(arguments->ucp_worker, arguments->server_ep, &buffer, BUFFER, arguments->worker_uid, length);
			slog_debug("[STAT WORKER] END Recv dynamic");

			if (ret < 0)
			{
				perror("HERCULES_ERR_STAT_WORKER_HELPER_RECV_DYNAMIC_STREAM");
				slog_error("HERCULES_ERR_STAT_WORKER_HELPER_RECV_DYNAMIC_STREAM");
				free(buffer);
				return -1;
			}

			int32_t insert_successful = -1;
			insert_successful = map->put(key, buffer, block_size_recv);
			slog_debug("[STAT WORKER] map->put (key %s) err %d", key.c_str(), insert_successful);

			if (insert_successful != 0)
			{
				slog_error("HERCULES_ERR_METADATA_WORKER_MAPPUT_SET_OP");
				perror("HERCULES_ERR_METADATA_WORKER_MAPPUT_SET_OP");
				return -1;
			}

			// Insert the received uri into the directory tree.
			pthread_mutex_lock(&tree_mut);
			// slog_debug("[STAT WORKER] Inserting %s into directory tree", key.c_str());
			insert_successful = GTree_insert((char *)key.c_str());
			pthread_mutex_unlock(&tree_mut);

			if (insert_successful == -1)
			{
				slog_error("HERCULES_ERR_METADATA_WORKER_GTREEINSERT_SET_OP");
				perror("HERCULES_ERR_METADATA_WORKER_GTREEINSERT_SET_OP");
				// perror("ERRIMSS_STATWORKER_GTREEINSERT");
				return -1;
			}

			// copy the block into disk.

			// Update the pointer.
			arguments->pt += block_size_recv;
			slog_debug("[STAT WORKER] Dataset %s has been created.", key.c_str());
		}
		// If was already stored:
		else
		{
			// pthread_mutex_unlock(&mp);
			// Follow a certain behavior if the received block was already stored.
			slog_debug("[STAT WORKER] LOCAL DATASET_UPDATE %ld", block_size_recv);
			switch (block_size_recv)
			{
			// Update where the blocks of a LOCAL dataset have been stored.
			case LOCAL_DATASET_UPDATE:
			{
				size_t msg_length = 0;
				msg_length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
				if (msg_length == 0)
				{
					perror("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_SET_OP");
					slog_error("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_SET_OP");
					// perror("ERRIMSS_METADATA_LOCAL_DATASET_UPDATE_INVALID_MSG_LENGTH");
					return -1;
				}
				// // void data_ref[msg_length];
				void *data_ref = malloc(msg_length);
				// void *data_ref;
				// char data_ref[REQUEST_SIZE];
				msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, data_ref, msg_length, arguments->worker_uid, 0);
				if (msg_length == 0)
				{
					perror("HERCULES_ERR_METADATA_WORKER_RECV_DATA_SET_OP");
					slog_error("HERCULES_ERR_METADATA_WORKER_RECV_DATA_SET_OP");
					free(data_ref);
					// perror("ERRIMSS_METADATA_LOCAL_DATASET_UPDATE_RECV_DATA");
					return -1;
				}

				uint32_t data_size = RESPONSE_SIZE; // MIRAR

				// Value to be written in certain positions of the vector.
				uint16_t *update_value = (uint16_t *)(data_size + (char *)data_ref - 8);
				// Positions to be updated.
				uint32_t *update_positions = (uint32_t *)data_ref;

				// Set of positions that are going to be updated (those are just under the concerned dataset but not pointed by it).
				uint16_t *data_locations = (uint16_t *)((char *)address_ + sizeof(dataset_info));

				// Number of positions to be updated.
				int32_t num_pos_toupdate = (data_size / sizeof(uint32_t)) - 2;

				// Perform the update operation.
				for (int32_t i = 0; i < num_pos_toupdate; i++)
					data_locations[update_positions[i]] = *update_value;

				// Answer the client with the update.
				slog_debug("[STAT_WORKER] Updating existing dataset %s.", key.c_str());
				char answer[] = "UPDATED!\0";
				if (send_data(arguments->ucp_worker, arguments->server_ep, answer, strlen(answer) + 1, arguments->worker_uid) == 0)
				{
					slog_error("HERCULES_ERR_METADATA_WORKER_SEND_DATA_SET_OP");
					perror("HERCULES_ERR_METADATA_WORKER_SEND_DATA_SET_OP");
					// perror("ERRIMSS_WORKER_DATALOCATANSWER2");
					free(data_ref);
					return -1;
				}
				free(data_ref);

				break;
			}

			default:
			{
				slog_debug("[STAT_WORKER] Updating existing dataset %s.", key.c_str());
				// Get the length of the message to be received.
				size_t length = 0;
				int32_t ret = -1;
				length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
				if (length == 0)
				{
					slog_error("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_UPDATE_DATASET");
					perror("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_UPDATE_DATASET");
					return -1;
				}
				// Clear the corresponding memory region.
				void *buffer = (void *)malloc(length);
				// void *buffer = NULL;
				// Receive the block into the buffer.
				ret = recv_dynamic_stream(arguments->ucp_worker, arguments->server_ep, buffer, BUFFER, arguments->worker_uid, length);
				// ret = recv_dynamic_stream_opt(arguments->ucp_worker, arguments->server_ep, &buffer, BUFFER, arguments->worker_uid, length);
				if (ret < 0)
				{
					perror("ERR_HERCULES_UPDATING_EXISTING_DATASET");
					slog_error("ERR_HERCULES_UPDATING_EXISTING_DATASET");
					free(buffer);
					return -1;
				}
				free(buffer);

				slog_debug("[STAT_WORKER] End Updating existing dataset %s.", key.c_str());
				break;
			}
			}
		}
		break;
	}
	default:
		break;
	}

	slog_debug("[srv_worker_thread] Terminated meta helper\n");

	return 0;
}

// Metadata dispatcher thread method.
void *dispatcher(void *th_argv)
{
	pthread_exit(NULL);

	// Cast from generic pointer type to p_argv struct type pointer.
	p_argv *arguments = (p_argv *)th_argv;

	uint32_t client_id_ = 0;
	char req[REQUEST_SIZE];
	struct sockaddr_in server_addr;
	socklen_t addrlen = sizeof(server_addr);
	int ret;
	int server_fd = -1;
	int listenfd = -1;
	int optval = 1;
	// char service[8];
	char *tmp_file_path = arguments->tmp_file_path;
	int client = 0;

	// snprintf(service, sizeof(service), "%ld", arguments->port);
	// Get a socket file descriptor.
	server_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (server_fd < 0)
	{
		perror("ERR_HERCULES_DISPATCHER_SOCKET");
		ready(tmp_file_path, "ERROR");
		pthread_exit(NULL);
	}

	// To reuse the address and port.
	ret = setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
	if (ret < 0)
	{
		perror("ERR_HERCULES_DISPATCHER_SET_SOCKET_OPT");
		ready(tmp_file_path, "ERROR");
		pthread_exit(NULL);
	}

	// Obtenemos la dirección del servidor
	bzero((char *)&server_addr, sizeof(server_addr));
	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr = INADDR_ANY;
	server_addr.sin_port = htons(arguments->port);

	// Asociamos el socket a la dirección del servidor
	if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
	{
		perror("ERR_HERCULES_DISPATCHER_BIND");
		ready(tmp_file_path, "ERROR");
		pthread_exit(NULL);
	}

	// Prepare to accept connections.
	ret = listen(server_fd, 3);
	if (ret < 0)
	{
		perror("ERR_HERCULES_DISPATCHER_LISTEN");
		ready(tmp_file_path, "ERROR");
		pthread_exit(NULL);
	}

	/* Accept next connection */
	// listenfd = sockfd;
	int new_socket = -1;
	while (1)
	{
		// ucs_status_t status;
		char mode[MODE_SIZE];

		slog_debug("[DISPATCHER] Waiting for connection requests.");
		// fprintf(stderr, "[DISPATCHER] Waiting for connection requests.\n");
		new_socket = accept(server_fd, (struct sockaddr *)&server_addr, &addrlen);
		if (new_socket < 0)
		{
			slog_error("ERR_HERCULES_DISPATCHER_ACCEPT");
		}
		ret = recv(new_socket, req, REQUEST_SIZE, MSG_WAITALL);
		if (ret < 0)
		{
			slog_error("ERR_HERCULES_DISPATCHER_RECV");
		}

		sscanf(req, "%" PRIu32 " %s", &client_id_, mode);

		char *req_content = strstr(req, mode);
		req_content += 4;

		slog_debug("[DISPATCHER] req=%s, req_content=%s, IMSS_THREAD_POOL=%d", req, req_content, IMSS_THREAD_POOL);

		// Check if the client is requesting connection resources.
		if (!strncmp(req_content, "HELLO!", 6))
		{
			// send the worker address lenght.
			ret = send(new_socket, &local_addr_len[(client % IMSS_THREAD_POOL)], sizeof(local_addr_len[(client % IMSS_THREAD_POOL)]), 0);
			if (ret == -1)
			{
				slog_error("ERR_HERCULES_DISPATCHER_SEND1");
			}
			// send the worker address.
			ret = send(new_socket, local_addr[(client % IMSS_THREAD_POOL)], local_addr_len[(client % IMSS_THREAD_POOL)], 0);
			if (ret == -1)
			{
				slog_error("ERR_HERCULES_DISPATCHER_SEND2");
			}
			client++;
			slog_debug("[DISPATCHER] Replied client.");
		}
		else if (!strncmp(req_content, "MAIN!", 5))
		{
			ret = send(new_socket, &local_addr_len[0], sizeof(local_addr_len[0]), 0);
			ret = send(new_socket, local_addr[0], local_addr_len[0], 0);
		}
		// Check if someone is requesting identity resources.
		else if (*((int32_t *)req) == WHO)
		{
			ret = send(new_socket, &local_addr_len[client], sizeof(local_addr_len[client]), 0);
			ret = send(new_socket, local_addr[client], local_addr_len[client], 0);
			slog_debug("[DISPATCHER] Replied client %s.", arguments->my_uri);
		}

		// MIRAR ucp_worker_release_address(ucp_worker_threads[client_id_ % IMSS_THREAD_POOL], local_addr);
		close(new_socket);
	}
	close(server_fd);

	pthread_exit(NULL);
}

// Server dispatcher thread method.
// int srv_worker_helper(p_argv *arguments, const char *req)
int srv_worker_helper(void *th_argv)
{
	// slog_init("workers", SLOG_INFO, 1, 0, 1, 1, 1);
	// std::unique_lock<std::mutex> lock(*mut2);
	p_argv *arguments = (p_argv *)th_argv;
	const char *req = arguments->req;

	// ucs_status_t status;
	int ret = -1;

	// Cast from generic pointer type to p_argv struct type pointer.

	// Obtain the current map class element from the set of arguments.
	std::shared_ptr<map_records> map = arguments->map;

	// Resources specifying if the ZMQ_SNDMORE flag was set in the sender.
	int64_t more;
	size_t more_size = sizeof(more);

	// Code to be sent if the requested to-be-read key does not exist.
	char err_code[] = "$ERRIMSS_NO_KEY_AVAIL$";

	char mode[MODE_SIZE];

	slog_debug("[srv_worker_thread] Waiting for new request.");
	// Save the request to be served.
	// TIMING(ret = recv_data(arguments->ucp_worker, arguments->server_ep, req), "[srv_worker_thread] Save the request to be served");
	slog_debug("[srv_worker_thread] request to be served %s", req);

	// slog_info("********** %d",ret);

	// Elements conforming the request.
	uint32_t block_size_recv, block_offset;
	char uri_[URI_];
	size_t to_read = 0;

	sscanf(req, "%s %" PRIu32 " %" PRIu32 " %s %lu", mode, &block_size_recv, &block_offset, uri_, &to_read);

	if (!strcmp(mode, "GET"))
		more = GET_OP;
	else
		more = SET_OP;

	slog_debug("[srv_worker_thread] Request - mode '%s', block_size_recv '%" PRIu32 "', block_offset '%" PRIu32 "', uri_ '%s', more %ld", mode, block_size_recv, block_offset, uri_, more);

	// Create an std::string in order to be managed by the map structure.
	std::string key;
	key.assign((const char *)uri_);

	// Information associated to the arriving key.
	void *address_;
	uint64_t block_size_rtvd;

	// Differentiate between READ and WRITE operations.
	switch (more)
	{
	// No more messages will arrive to the socket.
	case READ_OP:
	{
		switch (block_size_recv)
		{
		case READ_OP:
		{
			int32_t ret = map->get(key, &address_, &block_size_rtvd);
			// Check if there was an associated block to the key.
			if (ret == 0)
			{
				// Send the error code block.
				// pthread_mutex_lock(&lock_server_network);
				ret = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid);
				if (ret < 0)
				{
					// pthread_mutex_unlock(&lock_server_network);
					slog_error("ERR_HERCULES_WORKER_SEND_READ_OP");
					perror("ERR_HERCULES_WORKER_SEND_READ_OP");
					return -1;
				}
				// pthread_mutex_unlock(&lock_server_network);
			}
			else
			{
				// Send the requested block.
				// ret = TIMING(send_data(arguments->ucp_worker, arguments->server_ep, address_, block_size_rtvd, arguments->worker_uid), "[srv_worker_thread][READ_OP][READ_OP] Send the requested block");
				if (to_read <= 0)
				{
					to_read = block_size_rtvd;
				}
				struct stat *stats;
				stats = (struct stat *)address_;
				slog_debug("[srv_worker_thread][READ_OP][READ_OP] Send the requested block with key=%s, block_offset=%ld, block_size_rtvd=%ld kb, to_read=%ld kb, stat->st_nlink=%lu", key.c_str(), block_offset, block_size_rtvd / 1024, to_read / 1024, stats->st_nlink);
				size_t ret_send_data = 0;

				// pthread_mutex_lock(&lock_server_network);
				ret_send_data = send_data(arguments->ucp_worker, arguments->client_ep, (char *)address_ + block_offset, to_read, arguments->worker_uid);
				slog_debug("[srv_worker_thread][READ_OP][READ_OP] send_data, ret_send_data=%lu", ret_send_data);
				if (ret_send_data == 0)
				{
					// pthread_mutex_unlock(&lock_server_network);
					slog_error("ERR_HERCULES_WORKER_SENDBLOCK");
					perror("ERR_HERCULES_WORKER_SENDBLOCK");
					return -1;
				}
				// pthread_mutex_unlock(&lock_server_network);
			}
			break;
		}
		case RELEASE:
		{
			// map_server_eps_erase(map_server_eps, arguments->worker_uid, arguments->ucp_worker); // commented for testing purposes.
			slog_debug("[srv_worker_thread][READ_OP][RELEASE]");
			char release_msg[] = "RELEASE\0";
			// pthread_mutex_lock(&lock_server_network);
			ret = TIMING(send_data(arguments->ucp_worker, arguments->server_ep, release_msg, strlen(release_msg) + 1, arguments->worker_uid), "[srv_worker_thread][READ_OP][RENAME_OP] Send release", int);
			if (ret == 0)
			{
				// pthread_mutex_unlock(&lock_server_network);
				perror("ERR_HERCULES_SRV_SEND_DATA_RELEASE");
				slog_error("ERR_HERCULES_SRV_SEND_DATA_RELEASE");
				return -1;
			}
			// pthread_mutex_unlock(&lock_server_network);
			return 2;
			break;
		}
		case DELETE_OP:
		{
			slog_debug("DELETE_OP");
			slog_debug("Cleaning %s", key.c_str());
			map->cleaning_specific(key);
			char release_msg[] = "DELETE\0";
			ret = send_data(arguments->ucp_worker, arguments->server_ep, release_msg, strlen(release_msg) + 1, arguments->worker_uid);
			if (ret == 0)
			{
				perror("ERR_HERCULES_PUBLISH_DELETEOP");
				slog_error("ERR_HERCULES_PUBLISH_DELETEOP");
				return -1;
			}
			break;
		}
		case RENAME_OP:
		{
			std::size_t found = key.find(',');
			slog_debug("[srv_worker_thread][RENAME_OP], key=%s, found=%d", key.c_str(), found);
			if (found != std::string::npos)
			{
				slog_debug("[srv_worker_thread][RENAME_OP], found != npos");
				std::string old_key = key.substr(0, found);
				std::string new_key = key.substr(found + 1, key.length());
				slog_debug("[srv_worker_thread][RENAME_OP], old_key=%s, new_key=%s", old_key.c_str(), new_key.c_str());
				// RENAME MAP
				map->cleaning_specific(new_key);
				int32_t result = map->rename_data_srv_worker(old_key, new_key);
				if (result == 0)
				{
					break;
				}
			}
			else
			{
				slog_debug("[srv_worker_thread][RENAME_OP], found == npos");
			}

			char release_msg[] = "RENAME\0";
			ret = TIMING(send_data(arguments->ucp_worker, arguments->server_ep, release_msg, strlen(release_msg) + 1, arguments->worker_uid), "[srv_worker_thread][READ_OP][RENAME_OP] Send rename", int);
			if (ret == 0)
			{
				perror("ERR_HERCULES_PUBLISH_RENAMEMSG");
				slog_error("ERR_HERCULES_PUBLISH_RENAMEMSG");
				return -1;
			}
			break;
		}
		case RENAME_DIR_DIR_OP:
		{
			// printf("SRV_WORKER RENAME_DIR_DIR_OP");
			std::size_t found = key.find(' ');
			if (found != std::string::npos)
			{
				std::string old_dir = key.substr(0, found);
				std::string rdir_dest = key.substr(found + 1, key.length());

				// RENAME MAP
				map->rename_data_dir_srv_worker(old_dir, rdir_dest);
			}

			char release_msg[] = "RENAME\0";
			ret = TIMING(send_data(arguments->ucp_worker, arguments->server_ep, release_msg, strlen(release_msg) + 1, arguments->worker_uid), "[srv_worker_thread][READ_OP][RENAME_DIR_DIR_OP] Send rename", int);
			if (ret == 0)
			{
				perror("ERR_HERCULES_PUBLISH_RENAMEMSG");
				slog_error("ERR_HERCULES_PUBLISH_RENAMEMSG");
				return 1;
			}
			break;
		}
		case READV: // Only 1 server work
		{
			// printf("READV CASE");
			std::size_t found = key.find('$');
			std::string path;
			if (found != std::string::npos)
			{
				path = key.substr(0, found + 1);
				// std::cout <<"path:" << path << '';
				key.erase(0, found + 1);
				std::size_t found = key.find(' ');
				int curr_blk = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				found = key.find(' ');
				int end_blk = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				found = key.find(' ');
				int blocksize = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				found = key.find(' ');
				int start_offset = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				found = key.find(' ');
				int64_t size = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				// Needed variables
				size_t byte_count = 0;
				int first = 0;
				int ds = 0;
				int64_t to_copy = 0;
				uint32_t filled = 0;
				size_t to_read = 0;

				int pos = path.find('$');
				std::string first_element = path.substr(0, pos + 1);
				first_element = first_element + std::to_string(0);
				// printf("first_element=%s",first_element.c_str());
				map->get(first_element, &address_, &block_size_rtvd);
				struct stat *stats = (struct stat *)address_;
				void *buf = (void *)malloc(size);

				while (curr_blk <= end_blk)
				{
					std::string element = path;
					element = element + std::to_string(curr_blk);
					// std::cout <<"SERVER READV element:" << element << '';
					if (map->get(element, &address_, &block_size_rtvd) == 0)
					{ // If dont exist
					  // Send the error code block.
					  // std::cout <<"SERVER READV NO EXISTE element:" << element << '';
						ret = TIMING(send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid), "[srv_worker_thread][READ_OP][READV] send_dynamic_stream", int);
						if (ret < 0)
						{
							perror("ERRIMSS_WORKER_SENDERR");
							return -1;
						}
					} // If was already stored:
					else
					{
						// First block case
						if (first == 0)
						{
							if (size < stats->st_size - start_offset)
							{
								// to_read = size;
								to_read = blocksize * KB - start_offset;
							}
							else
							{
								if (stats->st_size < blocksize * KB)
								{
									to_read = stats->st_size - start_offset;
								}
								else
								{
									to_read = blocksize * KB - start_offset;
								}
							}
							// Check if offset is bigger than filled, return 0 because is EOF case
							if (start_offset > stats->st_size)
								return 0;
							memcpy(buf, (char *)address_ + start_offset, to_read);
							byte_count += to_read;
							++first;

							// Middle block case
						}
						else if (curr_blk != end_blk)
						{
							memcpy((char *)buf + byte_count, address_, blocksize * KB);
							byte_count += blocksize * KB;
							// End block case
						}
						else
						{

							// Read the minimum between end_offset and filled (read_ = min(end_offset, filled))
							int64_t pending = size - byte_count;
							memcpy((char *)buf + byte_count, address_, pending);
							byte_count += pending;
						}
					}
					++curr_blk;
				}
				ret = TIMING(send_data(arguments->ucp_worker, arguments->server_ep, buf, size, arguments->worker_uid), "[srv_worker_thread][READ_OP][READV] send", int);
				// Send the requested block.
				if (ret == 0)
				{
					perror("ERR_HERCULES_WORKER_SENDBLOCK");
					slog_error("ERR_HERCULES_WORKER_SENDBLOCK");
					return -1;
				}
			}
			break;
		}
		case SPLIT_READV:
		{
			// printf("SPLIT_READV CASE");
			slog_debug("key=%s", key.c_str());
			std::size_t found = key.find(' ');
			std::string path;
			if (found != std::string::npos)
			{
				path = key.substr(0, found);
				key.erase(0, found + 1);

				found = key.find(' ');
				int blocksize = stoi(key.substr(0, found)) * KB;
				key.erase(0, found + 1);

				found = key.find(' ');
				int start_offset = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				found = key.find(' ');
				int stats_size = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				size_t msg_size = stoi(key.substr(0, found));

				// char *msg = (char *)calloc(msg_size, sizeof(char));
				size_t msg_length = 0;
				msg_length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
				if (msg_length == 0)
				{
					perror("ERRIMSS_DATA_WORKER_INVALID_MSG_LENGTH");
					slog_error("ERRIMSS_DATA_WORKER_INVALID_MSG_LENGTH");
					return -1;
				}
				void *msg = malloc(msg_length);

				msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, msg, msg_length, arguments->worker_uid, 0);
				// msg_length = recv_data_opt(arguments->ucp_worker, arguments->server_ep, &msg, msg_length, arguments->worker_uid, 0);
				// Send the requested block.
				if (msg_length == 0)
				{
					perror("ERRIMSS_DATA_WORKER_RECV_DATA");
					slog_error("ERRIMSS_DATA_WORKER_RECV_DATA");
					free(msg);
					return -1;
				}

				key = (char *)msg;
				found = key.find('$');
				int amount = stoi(key.substr(0, found));
				int size = amount * blocksize;
				key.erase(0, found + 1);

				slog_debug("msg=%s", key.c_str());
				slog_debug("msg_size=%lu", msg_size);
				slog_debug("*path=%s", path.c_str());
				slog_debug("*blocksize=%d", blocksize);
				slog_debug("*start_offset=%d", start_offset);
				slog_debug("*size=%d", size);
				slog_debug("*amount=%d", amount);

				char *buf = (char *)malloc(size);
				// Needed variables
				size_t byte_count = 0;
				int first = 0;
				int ds = 0;
				int64_t to_copy = 0;
				uint32_t filled = 0;
				size_t to_read = 0;
				int curr_blk = 0;

				for (int i = 0; i < amount; i++)
				{
					// substract current block
					found = key.find('$');
					int curr_blk = stoi(key.substr(0, found));
					key.erase(0, found + 1);

					std::string element = path;
					element = element + '$' + std::to_string(curr_blk);
					if (map->get(element, &address_, &block_size_rtvd) == 0)
					{ // If dont exist
					  // Send the error code block.
						if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid) < 0)
						{
							free(msg);
							return -1;
							pthread_exit(NULL);
						}
					} // If was already stored:

					memcpy(buf + byte_count, address_, blocksize);
					byte_count += blocksize;
				}
				// Send the requested block.
				ret = TIMING(send_data(arguments->ucp_worker, arguments->server_ep, buf, byte_count, arguments->worker_uid), "[srv_worker_thread][READ_OP][READV] send buf", int);
				if (ret == 0)
				{
					free(msg);
					perror("ERR_HERCULES_WORKER_SENDBLOCK");
					slog_error("ERR_HERCULES_WORKER_SENDBLOCK");
					return -1;
				}
			}
			break;
		}
		case WHO:
		{
			// Provide the uri of this instance.
			ret = TIMING(send_data(arguments->ucp_worker, arguments->server_ep, arguments->my_uri, strlen(arguments->my_uri) + 1, arguments->worker_uid), ("[srv_worker_thread][READ_OP][WHO] send uri: %s", arguments->my_uri), int);
			if (ret == 0)
			{
				perror("ERR_HERCULES_WHOREQUEST");
				slog_error("ERR_HERCULES_WHOREQUEST");
				return -1;
			}
			break;
		}
		default:
			break;
		}
		break;
	}
	// More messages will arrive to the socket.
	case WRITE_OP:
	{
		int op;
		std::size_t found = key.find(' ');
		std::size_t found2 = key.find("[OP]=");
		slog_debug("[srv_worker_thread][WRITE_OP] found=%d, found2=%d", found, found2);
		if (found2 != std::string::npos)
		{
			slog_debug("[srv_worker_thread][WRITE_OP] Entra en found2");
			op = stoi(key.substr(found2 + 5, (found - (found2 + 5))));
			key.erase(0, found + 1);
		}

		if (found != std::string::npos && found2 == std::string::npos)
		{
			std::string path = key.substr(0, found);
			key.erase(0, found + 1);
			// std::cout <<"path:" << key << '';

			std::size_t found = key.find(' ');
			int curr_blk = stoi(key.substr(0, found));
			key.erase(0, found + 1);

			found = key.find(' ');
			int end_blk = stoi(key.substr(0, found));
			key.erase(0, found + 1);

			found = key.find(' ');
			int start_offset = stoi(key.substr(0, found));
			key.erase(0, found + 1);

			found = key.find(' ');
			int end_offset = stoi(key.substr(0, found));
			key.erase(0, found + 1);

			found = key.find(' ');
			int IMSS_DATA_BSIZE = stoi(key.substr(0, found));
			key.erase(0, found + 1);

			int size = stoi(key);

			size_t msg_length = 0;
			msg_length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
			if (msg_length == 0)
			{
				slog_error("ERRIMSS_DATA_WORKER_WORKER_OP_1_INVALID_MSG_LENGTH");
				perror("ERRIMSS_DATA_WORKER_WORKER_OP_1_INVALID_MSG_LENGTH");
				return -1;
			}

			char *buf = (char *)malloc(msg_length * sizeof(char));
			// Receive all blocks into the buffer.
			msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, (char *)buf, msg_length, arguments->worker_uid, 0);
			// msg_length = recv_data_opt(arguments->ucp_worker, arguments->server_ep, (void **)&buf, msg_length, arguments->worker_uid, 0);
			if (msg_length == 0)
			{
				perror("ERRIMSS_DATA_WORKER_WORKER_OP_1_RECV_DATA");
				slog_error("ERRIMSS_DATA_WORKER_WORKER_OP_1_RECV_DATA");
				free(buf);
				return -1;
			}

			// printf("WRITEV-buffer=%s",buf);
			int pos = path.find('$');
			std::string first_element = path.substr(0, pos + 1);
			first_element = first_element + "0";
			map->get(first_element, &address_, &block_size_rtvd);
			// imss_info * data = (imss_info *) address_;
			// printf("READ_OP SEND data->type=%c",data->type);
			struct stat *stats = (struct stat *)address_;

			// Needed variables
			size_t byte_count = 0;
			int first = 0;
			int ds = 0;
			int64_t to_copy = 0;
			uint32_t filled = 0;
			void *aux = (void *)malloc(IMSS_DATA_BSIZE);
			int count = 0;
			// For the rest of blocks
			while (curr_blk <= end_blk)
			{
				// printf("Nodename    - %s current_block=%d", detect.nodename, curr_blk);
				count = count + 1;
				// printf("count=%d",count);
				pos = path.find('$');
				std::string element = path.substr(0, pos + 1);
				element = element + std::to_string(curr_blk);
				// std::cout <<"element:" << element << '';

				// First fragmented block
				if (first == 0 && start_offset && stats->st_size != 0)
				{
					// Get previous block
					map->get(element, &aux, &block_size_rtvd); // path por curr_block
															   // Bytes to write are the minimum between the size parameter and the remaining space in the block (BLOCKSIZE-start_offset)
					to_copy = (size < IMSS_DATA_BSIZE - start_offset) ? size : IMSS_DATA_BSIZE - start_offset;

					memcpy((char *)aux + start_offset, buf + byte_count, to_copy);
				}
				// Last Block
				else if (curr_blk == end_blk)
				{
					if (end_offset != 0)
					{
						to_copy = end_offset;
					}
					else
					{
						to_copy = IMSS_DATA_BSIZE;
					}
					// Only if last block has contents
					if (curr_blk <= stats->st_blocks && start_offset)
					{
						map->get(element, &aux, &block_size_rtvd); // path por curr_block
					}
					else
					{
						memset(aux, 0, IMSS_DATA_BSIZE);
					}
					if (byte_count == size)
					{
						to_copy = 0;
					}
					// printf("curr_block=%d, end_block=%d, byte_count=%d",curr_blk, end_blk, byte_count);
					memcpy(aux, buf + byte_count, to_copy);
				}
				// middle block
				else
				{
					to_copy = IMSS_DATA_BSIZE;
					memcpy(aux, buf + byte_count, to_copy);
				}

				// Write and update variables
				if (!map->get(element, &address_, &block_size_rtvd))
				{
					map->put(element, aux, block_size_rtvd);

					// printf("Nodename    - %s after put", detect.nodename);
				}
				else
				{
					memcpy(address_, aux, block_size_rtvd);
				}
				// printf("currblock=%d, byte_count=%d",curr_blk, byte_count);
				byte_count += to_copy;
				++curr_blk;
				++first;
			}
			int16_t off = (end_blk * IMSS_DATA_BSIZE) - 1 - size;
			if (size + off > stats->st_size)
			{
				stats->st_size = size + off;
				stats->st_blocks = curr_blk - 1;
			}

			free(buf);
		}
		else if (found != std::string::npos && op == 2)
		{
			std::string path;
			std::size_t found = key.find(' ');
			// printf("Nodename	-%s SPLIT WRITEV",detect.nodename);

			path = key.substr(0, found);
			key.erase(0, found + 1);

			found = key.find(' ');
			int blocksize = stoi(key.substr(0, found)) * KB;
			key.erase(0, found + 1);

			found = key.find(' ');
			int start_offset = stoi(key.substr(0, found));
			key.erase(0, found + 1);

			found = key.find(' ');
			int stats_size = stoi(key.substr(0, found));
			key.erase(0, found + 1);

			found = key.find('$');
			int amount = stoi(key.substr(0, found));
			int size = amount * blocksize;
			key.erase(0, found + 1);

			slog_debug("amount=%d", amount);
			slog_debug("path=%s", path.c_str());
			slog_debug("blocksize=%d", blocksize);
			slog_debug("start_offset=%d", start_offset);
			slog_debug("size=%d", size);
			slog_debug("rest=%s", key.c_str());

			// Receive all blocks into the buffer.
			size_t msg_length = 0;
			msg_length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
			if (msg_length == 0)
			{
				perror("ERRIMSS_WORKER_DATA_WRITE_OP_2_INVALID_MSG_LENGTH");
				slog_error("ERRIMSS_WORKER_DATA_WRITE_OP_2_INVALID_MSG_LENGTH");
				return -1;
			}

			void *buf = malloc(msg_length);

			msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, buf, msg_length, arguments->worker_uid, 0);
			// msg_length = recv_data_opt(arguments->ucp_worker, arguments->server_ep, &buf, msg_length, arguments->worker_uid, 0);
			if (msg_length == 0)
			{
				perror("ERRIMSS_WORKER_DATA_WRITE_OP_2_RECV_DATA");
				slog_error("ERRIMSS_WORKER_DATA_WRITE_OP_2_RECV_DATA");
				free(buf);
				return -1;
			}

			// size_recv = size; // MIRAR
			int32_t insert_successful;

			// printf("Nodename	-%s size_recv=%d",detect.nodename,size_recv);
			// printf("Salida buf full=%c",buf[100]);

			int32_t byte_count = 0;
			for (int i = 0; i < amount; i++)
			{
				// substract current block
				found = key.find('$');
				int curr_blk = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				std::string element = path;
				element = element + '$' + std::to_string(curr_blk);
				// printf(" element=%s",element.c_str());

				if (map->get(element, &address_, &block_size_rtvd) == 0)
				{
					// If don't exist
					char *buffer = (char *)aligned_alloc(1024, blocksize);
					memcpy(buffer, (char *)buf + byte_count, blocksize);
					// printf("Salida buffer part=%c",buffer[100]);
					insert_successful = map->put(element, buffer, block_size_recv);
					if (insert_successful != 0)
					{
						perror("ERRIMSS_WORKER_MAPPUT");
						return -1;
					}
				}
				else
				{
					// If already exits
					memcpy(address_, (char *)buf + byte_count, blocksize);
					// printf("Alreadt exitsSalida buffer part=%c",buf[100]);
				}
				byte_count = byte_count + blocksize;
				// printf("Nodename	-%s byte_count=%d",detect.nodename,byte_count);
			}
			free(buf);
		}
		else
		{
			slog_debug("[srv_worker_thread][WRITE_OP] WRITE NORMAL CASE. Size %ld, offset=%ld", block_size_recv, block_offset);
			// search for the block to know if it was previously stored.
			int ret = map->get(key, &address_, &block_size_rtvd);

			// if the block was not already stored:
			if (ret == 0)
			{
				slog_debug("[srv_worker_thread][WRITE_OP] NO key find %s", key.c_str());
				clock_t tp;
				tp = clock();
				// Get an allocated block pointer.
				void *buffer = (void *)StsQueue.pop(mem_pool);
				tp = clock() - tp;
				double time_taken2 = ((double)tp) / CLOCKS_PER_SEC; // in seconds
				// slog_info("[srv_worker_helper] pop time %f s", time_taken2);
				//  Receive the block into the buffer.
				if (buffer == NULL)
					buffer = (void *)calloc(BLOCK_SIZE, sizeof(char));
				clock_t tr;
				size_t msg_length = 0;
				slog_debug("[srv_worker_thread][WRITE_OP] get_recv_data_length, %s", key.c_str());
				// pthread_mutex_lock(&lock_server_network);
				msg_length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
				if (msg_length == 0)
				{
					// pthread_mutex_unlock(&lock_server_network);
					perror("ERR_HERCULES_DATA_WORKER_WRITE_NEW_BLOCK_INVALID_MSG_LENGTH");
					slog_error("ERR_HERCULES_DATA_WORKER_WRITE_NEW_BLOCK_INVALID_MSG_LENGTH");
					return -1;
				}

				// void *aux_buf = (char *)buffer + block_offset;
				slog_debug("[srv_worker_thread][WRITE_OP] recv_data, %s", key.c_str());
				msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, (char *)buffer + block_offset, msg_length, arguments->worker_uid, 1);
				if (msg_length == 0)
				{
					// pthread_mutex_unlock(&lock_server_network);
					perror("ERR_HERCULES_DATA_WORKER_WRITE_NEW_BLOCK_RECV_DATA");
					slog_error("ERR_HERCULES_DATA_WORKER_WRITE_NEW_BLOCK_RECV_DATA");
					return -1;
				}
				// pthread_mutex_unlock(&lock_server_network);
				// sleep(5);
				// struct stat *stats = (struct stat *)buffer;
				int32_t insert_successful;

				// Include the new record in the tracking structure.
				tr = clock();
				// fprintf(stderr,"[srv_worker_thread][WRITE_OP] ****[PUT]********* key=%s\n",  key.c_str());
				slog_debug("[srv_worker_thread][WRITE_OP] ****[PUT, block_size_recv=%ld, BLOCK_SIZE=%lu, msg_length=%lu]********* key=%s", block_size_recv, BLOCK_SIZE, msg_length, key.c_str());
				// TODO: should this be block_size_recv or a different size? block_size_recv might not be the full block size
				// insert_successful = map->put(key, buffer, block_size_recv);
				insert_successful = map->put(key, buffer, BLOCK_SIZE);

				slog_debug("[srv_worker_thread][WRITE_OP] insert_successful %d key=%s", insert_successful, key.c_str());
				tr = clock() - tr;
				double time_taken = ((double)tr) / CLOCKS_PER_SEC; // in seconds

				// Include the new record in the tracking structure.
				if (insert_successful != 0)
				{
					perror("ERR_HERCULES_WORKER_MAPPUT");
					slog_error("ERR_HERCULES_WORKER_MAPPUT");
					return -1;
				}

				// Update the pointer.
				arguments->pt += block_size_recv;
			}
			// if the block was already stored:
			else
			{
				slog_debug("[srv_worker_thread][WRITE_OP] Key find %s", key.c_str());
				// Receive the block into the buffer.
				std::size_t found = key.find("$0");
				if (found != std::string::npos)
				{ // block 0.
					slog_debug("[srv_worker_thread][WRITE_OP] Updating block $0 (%d)", block_size_rtvd);
					struct stat *old, *latest;
					// TODO: make sure this works.
					// pthread_mutex_lock(&lock_server_network);
					size_t msg_length = 0;
					msg_length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
					if (msg_length == 0)
					{
						// pthread_mutex_unlock(&lock_server_network);
						perror("ERR_HERCULES_DATA_WORKER_WRITE_BLOCK_0_INVALID_MSG_LENGTH");
						slog_error("ERR_HERCULES_DATA_WORKER_WRITE_BLOCK_0_INVALID_MSG_LENGTH");
						return -1;
					}
					slog_live("msg_length=%lu", msg_length);
					// void *buffer = malloc(block_size_recv);
					void *buffer = malloc(msg_length);
					msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, buffer, msg_length, arguments->worker_uid, 0);
					if (msg_length == 0)
					{
						// pthread_mutex_unlock(&lock_server_network);
						perror("ERR_HERCULES_DATA_WORKER_WRITE_BLOCK_0_RECV_DATA");
						slog_error("ERR_HERCULES_DATA_WORKER_WRITE_BLOCK_0_RECV_DATA");
						free(buffer);
						return -1;
					}
					// pthread_mutex_unlock(&lock_server_network);

					old = (struct stat *)address_;
					latest = (struct stat *)buffer;
					slog_debug("[srv_worker_thread] File size new %ld old %ld", latest->st_size, old->st_size);
					latest->st_size = std::max(latest->st_size, old->st_size);
					// slog_debug("[srv_worker_thread] buffer->st_size: %ld, block_offset=%ld", latest->st_size, block_offset);
					slog_debug("[srv_worker_thread] buffer->st_size: %ld, block_offset=%ld, old->st_nlink: %ld, new->st_nlink: %ld", latest->st_size, block_offset, old->st_nlink, latest->st_nlink);

					// TODO: make sure this works
					// memcpy((char *)address_ + block_offset, buffer, block_size_recv);
					memcpy((char *)address_ + block_offset, buffer, msg_length);
					// TODO: should we update this block's size in the map?
					free(buffer);
				}
				else
				{ // non block 0.
					slog_debug("[srv_worker_thread][WRITE_OP] Updated non 0 existing block, key.c_str(): %s", key.c_str());
					// pthread_mutex_lock(&lock_server_network);
					size_t msg_length = 0;
					msg_length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
					if (msg_length == 0)
					{
						// pthread_mutex_unlock(&lock_server_network);
						slog_error("ERR_HERCULES_DATA_WORKER_WRITE_NON_BLOCK_0_INVALID_MSG_LENGTH");
						perror("ERR_HERCULES_DATA_WORKER_WRITE_NON_BLOCK_0_INVALID_MSG_LENGTH");
						return -1;
					}

					msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, (char *)address_ + block_offset, msg_length, arguments->worker_uid, 1);
					if (msg_length == 0)
					{
						// pthread_mutex_unlock(&lock_server_network);
						slog_error("ERR_HERCULES_DATA_WORKER_WRITE_NON_BLOCK_0_RECV_DATA");
						perror("ERR_HERCULES_DATA_WORKER_WRITE_NON_BLOCK_0_RECV_DATA");
						return -1;
					}
					// pthread_mutex_unlock(&lock_server_network);
				}
			}
			break;
		}
	}
	default:
		break;
	}

	slog_debug("[srv_worker_thread] Terminated data helper");
	return 0;
}
