#include <glib.h>
#include <netdb.h>
#include <errno.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include "crc.h"
#include "imss.h"
#include "comms.h"
#include "map_ep.hpp"
#include "workers.h"
#include "policies.h"
#include <sys/time.h>
#include <sys/utsname.h>
#include <time.h>
#include <inttypes.h>

/**********************************************************************************/
/******************************** GLOBAL VARIABLES ********************************/
/**********************************************************************************/

uint32_t process_rank; // Process identifier within the deployment.

uint32_t n_stat_servers; // Number of metadata servers available.
uint32_t *stat_ids;
void *stat_mon; // Metadata monitoring socket.

uint32_t NUM_DATA_SERVERS;

int32_t __thread current_dataset;	// Dataset whose policy has been set last.
dataset_info __thread curr_dataset; // Currently managed dataset.
imss __thread curr_imss;

GArray *imssd;			// Set of IMSS metadata and connection structures currently used.
GArray *free_imssd;		// Set of free entries within the 'imssd' vector.
int32_t imssd_pos;		// Next position within the vextor were a new IMSS will be inserted.
int32_t imssd_max_size; // Maximum number of elements that could be introduced into the imss array.

GArray *datasetd;		   // Set of dataset metadata structures.
GArray *free_datasetd;	   // Set of free entries within the 'datasetd' vector.
int32_t datasetd_pos;	   // Next position within the vextor were a new dataset will be inserted.
int32_t datasetd_max_size; // Maximum number of elements that could be introduced into the dataset array.

int32_t tried_conn = 0; // Flag specifying if a connection has been attempted.

char client_node[512];	 // Node name where the client is running.
int32_t len_client_node; // Length of the previous node name.
char client_ip[16];		 // IP number of the node where the client is taking execution.

dataset_info empty_dataset;
imss empty_imss;

int32_t found_in; // Variable storing the position where a certain structure was stored in a certain vector.

extern uint16_t connection_port; // FIXME

char att_deployment[URI_];

int32_t IMSS_DEBUG = 0;
int32_t IMSS_WRITE_ASYNC = 1;

/* UCP objects */
ucp_context_h ucp_context_client;
ucp_worker_h ucp_worker_meta;
ucp_worker_h ucp_worker_data;

void *map_ep;		   // map_ep used for async write
int32_t is_client = 1; // also used for async write

static ucp_address_t *local_addr_meta;
static size_t local_addr_len_meta;

static ucp_address_t *local_addr_data;
static size_t local_addr_len_data;

uint64_t local_meta_uid;
uint64_t local_data_uid;

ucp_address_t **stat_addr;
ucp_ep_h *stat_eps;

extern int IMSS_THREAD_POOL;

pthread_mutex_t lock_gtree = PTHREAD_MUTEX_INITIALIZER;
// To synchronize network operations.
pthread_mutex_t lock_network = PTHREAD_MUTEX_INITIALIZER;

int32_t imss_comm_cleanup()
{
	// ep_close(ucp_worker_meta, stat_eps[0], 0);
	ucp_worker_release_address(ucp_worker_meta, local_addr_meta);
	ucp_worker_release_address(ucp_worker_data, local_addr_data);

	// Following lines were comment.
	ucp_worker_destroy(ucp_worker_meta);
	ucp_worker_destroy(ucp_worker_data);

	ucp_cleanup(ucp_context_client);

	return 0;
}

// Method inserting an element into a certain control GArray vector.
int32_t
GInsert(int32_t *pos,
		int32_t *max,
		char *item,
		GArray *garray_insert,
		GArray *garray_free)
{
	// Position where the element will be inserted.
	int32_t inserted_pos = -1;

	*pos = -1;

	// Updating the position where the following item will be inserted within a GArray.
	if (garray_free->len)
	{
		// Retrieve a free position from the existing wholes within the vector.
		*pos = g_array_index(garray_free, int32_t, 0);
		g_array_remove_index(garray_free, 0);
	}

	if (*pos == -1)
	{
		// Append an element into the corresponding array if there was no space left.
		g_array_append_val(garray_insert, *item);
		inserted_pos = ++(*max) - 1;
	}
	else
	{
		// Insert an element in a certain position within the provided garray.
		if (*pos < garray_insert->len)
			g_array_remove_index(garray_insert, *pos);

		g_array_insert_val(garray_insert, *pos, *item);
		inserted_pos = *pos;
	}

	slog_debug("[IMSS][GTree] inserted_pos=%d", inserted_pos);

	return inserted_pos;
}

// Method inserting an element into a certain control GArray vector.
int32_t
Get_fd(int32_t *pos,
	   int32_t *max,
	   GArray *garray_insert,
	   GArray *garray_free)
{
	// Position where the element will be inserted.
	int32_t inserted_pos = -1;

	*pos = -1;

	// Updating the position where the following item will be inserted within a GArray.
	if (garray_free->len)
	{
		// Retrieve a free position from the existing wholes within the vector.
		*pos = g_array_index(garray_free, int32_t, 0);
		// g_array_remove_index(garray_free, 0);
	}

	if (*pos == -1)
	{
		// Append an element into the corresponding array if there was no space left.
		// g_array_append_val(garray_insert, *item);
		inserted_pos = ++(*max) - 1;
	}
	else
	{
		// Insert an element in a certain position within the provided garray.
		if (*pos < garray_insert->len)
			// g_array_remove_index(garray_insert, *pos);

			// g_array_insert_val(garray_insert, *pos, *item);
			inserted_pos = *pos;
	}

	return inserted_pos;
}

// Check the existance within the session of the IMSS that the dataset is to be created in.
int32_t imss_check(char *dataset_uri)
{
	imss imss_;
	slog_live("[imss_check] imssd->len=%d", imssd->len);
	// Traverse the whole set of IMSS structures in order to find the one.
	for (int32_t i = 0; i < imssd->len; i++)
	{
		imss_ = g_array_index(imssd, imss, i);

		int32_t imss_uri_len = strlen(imss_.info.uri_);
		// int32_t dataset_uri_len = strlen(dataset_uri);
		// int32_t length = MAX(dataset_uri_len, imss_uri_len);
		slog_live("[imss_check] data_uri=%s, imss_.info.uri_=%s, imss_uri_len=%d", dataset_uri, imss_.info.uri_, imss_uri_len);
		// if ((imss_uri_len > 0) && !strncmp(dataset_uri, imss_.info.uri_, length))
		if ((imss_uri_len > 0) && !strncmp(dataset_uri, imss_.info.uri_, imss_uri_len))
			return i;
	}
	return -1;
}

// Method searching for a certain IMSS in the vector.
int32_t find_imss(char *imss_uri, imss *imss_)
{
	// Search for a certain IMSS within the vector.
	slog_live("imssd->len=%d", imssd->len);
	for (int32_t i = 0; i < imssd->len; i++)
	{
		*imss_ = g_array_index(imssd, imss, i);
		slog_live("imss_uri=%s, imss_->info.uri_=%s", imss_uri, imss_->info.uri_);
		if (!strncmp(imss_uri, imss_->info.uri_, URI_))
			return i;
	}
	return -1;
}

// Method deleting a certains IMSS in the vector
int32_t
delete_imss(char *imss_uri,
			imss *imss_)
{
	int32_t pos = find_imss(imss_uri, imss_);
	if (pos != -1)
	{
		g_array_remove_index(imssd, pos);
		return 0;
	}
	else
	{
		return -1;
	}
}

/**********************************************************************************/
/********************* METADATA SERVICE MANAGEMENT FUNCTIONS  *********************/
/**********************************************************************************/

// Method creating a communication channel with the IMSS metadata server. Besides, the stat_imss method initializes a set of elements that will be used through the session.
int32_t stat_init(char *stat_hostfile,
				  uint64_t port,
				  int32_t num_stat_servers,
				  uint32_t rank)
{
	slog_debug("[IMSS] Calling stat_init.");

	// Number of metadata servers to connect to.
	n_stat_servers = num_stat_servers;
	// Initialize memory required to deal with metadata sockets.
	stat_addr = (ucp_address_t **)malloc(n_stat_servers * sizeof(ucp_address_t *));
	// Dataset whose policy was set last.
	current_dataset = -1;
	// Rank of the current process.
	process_rank = rank;
	// Next position within the imss vector to be occupied.
	imssd_pos = 0;
	// Next position within the dataset vector to be occupied.
	datasetd_pos = 0;
	// Current size of the imss array.
	imssd_max_size = ELEMENTS;
	// Current size of the dataset array.
	datasetd_max_size = ELEMENTS;
	int ret = 0;
	ucs_status_t status;

	/* Initialize the UCX required objects */
	ret = init_context(&ucp_context_client, NULL, &ucp_worker_meta, CLIENT_SERVER_SEND_RECV_TAG);
	if (ret != 0)
	{
		slog_error("HERCULES_ERR_INIT_CONTEXT");
		perror("HERCULES_ERR_INIT_CONTEXT");
		return -1;
	}

	ret = init_worker(ucp_context_client, &ucp_worker_data);
	if (ret != 0)
	{
		slog_error("HERCULES_ERR_INIT_WORKER");
		perror("HERCULES_ERR_INIT_WORKER");
		return -1;
	}

	// /* UCP objects */
	// ucp_context_h ucp_context;
	// ucp_worker_h ucp_worker;
	// init_context_ori(&ucp_context, &ucp_worker, CLIENT_SERVER_SEND_RECV_TAG);

	// char server_address[] = "192.168.201.156";
	// char server_address[] = "broadwell-001";
	// run_client(ucp_worker, server_address, CLIENT_SERVER_SEND_RECV_TAG);
	// ucp_worker_destroy(ucp_worker);
	// ucp_cleanup(ucp_context);

	memset(&empty_dataset, 0, sizeof(dataset_info));
	memset(&empty_imss, 0, sizeof(imss));
	memset(&att_deployment, 0, URI_);

	// Initialize the set of GArrays dealing with the underlying set of structures.
	if ((imssd = g_array_sized_new(FALSE, FALSE, sizeof(imss), ELEMENTS)) == NULL)
	{
		perror("ERRIMSS_STATINIT_GARRAYIMSS");
		return -1;
	}
	if ((free_imssd = g_array_sized_new(FALSE, FALSE, sizeof(int32_t), ELEMENTS)) == NULL)
	{
		perror("ERRIMSS_STATINIT_GARRAYIMSSREG");
		return -1;
	}

	if ((datasetd = g_array_sized_new(FALSE, FALSE, sizeof(dataset_info), ELEMENTS)) == NULL)
	{
		perror("ERRIMSS_STATINIT_GARRAYDATASET");
		return -1;
	}

	if ((free_datasetd = g_array_sized_new(FALSE, FALSE, sizeof(int32_t), ELEMENTS)) == NULL)
	{
		perror("ERRIMSS_STATINIT_GARRAYDATASETREG");
		return -1;
	}

	// Fill the free positions arrays.
	for (int32_t i = 0; i < ELEMENTS; i++)
	{
		g_array_insert_val(free_imssd, i, i);
		g_array_insert_val(free_datasetd, i, i);
	}

	// Retrieve the hostname where the current process is running.
	if (gethostname(client_node, 512) == -1)
	{
		perror("ERRIMSS_GETHOSTNAME");
		return -1;
	}
	len_client_node = strlen(client_node);

	struct hostent *host_entry;
	if ((host_entry = gethostbyname(client_node)) == NULL)
	{
		perror("ERRIMSS_GETHOSTBYNAME");
		return -1;
	}

	strcpy(client_ip, inet_ntoa(*((struct in_addr *)host_entry->h_addr_list[0])));

	// FILE entity managing the IMSS metadata hostfile.
	FILE *stat_nodes;
	// Number of characters successfully read from the line.
	int n_chars;

	// Open the file containing the IMSS metadata server nodes.
	if ((stat_nodes = fopen(stat_hostfile, "r+")) == NULL)
	{
		char error_msg[500];
		sprintf(error_msg, "ERRHERCULES_OPEN_STATFILE: %s", stat_hostfile);
		perror(error_msg);
		return -1;
	}

	stat_ids = (uint32_t *)malloc(n_stat_servers * sizeof(uint32_t));
	stat_eps = (ucp_ep_h *)malloc(n_stat_servers * sizeof(ucp_ep_h));

	char *stat_node = (char *)malloc(LINE_LENGTH);
	// Connect to all servers.
	char request[REQUEST_SIZE];

	status = ucp_worker_get_address(ucp_worker_meta, &local_addr_meta, &local_addr_len_meta);
	ucp_worker_address_attr_t attr;
	attr.field_mask = UCP_WORKER_ADDRESS_ATTR_FIELD_UID;
	ucp_worker_address_query(local_addr_meta, &attr);
	local_meta_uid = attr.worker_uid;

	for (int i = 0; i < n_stat_servers; i++)
	{
		ucs_status_t status;
		size_t l_size = LINE_LENGTH;
		int oob_sock;
		size_t addr_len;

		// Save IMSS metadata deployment.
		n_chars = getline(&stat_node, &l_size, stat_nodes);

		// Erase the new line character ('') from the string.
		if (stat_node[n_chars - 1] == '\n')
		{
			stat_node[n_chars - 1] = '\0';
		}

		// slog_debug("[IMSS] stat_client=%s", stat_node);
		slog_debug("[IMSS][stat_int] i=%d, stat_node=%s, port=%ld, rank=%" PRIu32 "", i, stat_node, port, rank);
		slog_debug("[IMSS][stat_int] Contacting stat dispatcher at %s:%ld", stat_node, port);
		// fprintf(stderr,"[IMSS][stat_int] Contacting stat dispatcher at %s:%ld\n", stat_node, port);

		oob_sock = connect_common(stat_node, port, AF_INET);
		if (oob_sock < 0)
		{
			char err_msg[100];
			sprintf(err_msg, "ERR_HERCULES_CONNECT-%s:%ld", stat_node, port);
			perror(err_msg);
			return -1;
		}

		sprintf(request, "%" PRIu32 " GET HELLO!", rank);
		slog_debug("[IMSS][stat_int] request=%s", request);
		if (send(oob_sock, request, REQUEST_SIZE, 0) < 0)
		{
			char err_msg[100];
			sprintf(err_msg, "ERR_HERCULES_STAT_HELLO_2-%s:%ld", stat_node, port);
			perror(err_msg);
			return -1;
		}

		ret = recv(oob_sock, &addr_len, sizeof(addr_len), MSG_WAITALL);
		stat_addr[i] = (ucp_address *)malloc(addr_len);
		ret = recv(oob_sock, stat_addr[i], addr_len, MSG_WAITALL);
		// slog_debug("[IMSS][stat_init] stat_addr=%s, len=%d", stat_addr[i], addr_len);
		close(oob_sock);

		client_create_ep(ucp_worker_meta, &stat_eps[i], stat_addr[i]);
		slog_debug("[IMSS][stat_init] created ep with %s:%ld", stat_node, port);
	}
	// Close the file.
	if (fclose(stat_nodes) != 0)
	{
		perror("ERR_CLOSE_STATFILE");
		return -1;
	}

	return 0;
}

// Method disabling the communication chucp_address_t *peer_addrannel with the metadata server. Besides, the current method releases session-related elements previously initialized.
int32_t stat_release()
{
	slog_live("[IMSS] Calling stat_release.");
	// Release the underlying set of vectors.

	/*	G_ARRAY_FREE:

		"Returns the element data if free_segment is FALSE, otherwise NULL.
		The element data should be freed using g_free()."

		Yet, comparing the result to != NULL or == NULL will 'fail'.

		The "g_array_free(GArray *, FALSE)" function will be returning the content
		iself if something was stored or NULL otherwise.
	 */

	g_array_free(imssd, FALSE);
	g_array_free(free_imssd, FALSE);
	g_array_free(datasetd, FALSE);
	g_array_free(free_datasetd, FALSE);

	pthread_mutex_lock(&lock_network);

	// Disconnect from all metadata servers.
	for (int i = 0; i < n_stat_servers; i++)
	{
		ucp_ep_h ep = stat_eps[i];
		char release_msg[REQUEST_SIZE];
		sprintf(release_msg, "%" PRIu32 " GET 2 RELEASE", process_rank);
		// slog_live("Request - %s", release_msg);
		if (send_req(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta, release_msg) == 0)
		{
			pthread_mutex_unlock(&lock_network);
			slog_error("HERCULES_ERR_RLSHERCULES_SENDADDR");
			perror("HERCULES_ERR_RLSHERCULES_SENDADDR");
			return -1;
		}

		close_ucx_endpoint(ucp_worker_meta, ep);

		// ep_flush(ep, ucp_worker_meta);
		// //  ucp_ep_destroy(ep);
		// ucp_context_destroy(ucp_worker_meta);
		// ep_close(ucp_worker_meta, ep, 0);
		free(stat_addr[i]);
	}

	// Optionally, flush the worker
	ucs_status_t status = ucp_worker_flush(ucp_worker_meta);
	if (status != UCS_OK)
	{
		fprintf(stderr, "Failed to flush worker: %s\n", ucs_status_string(status));
	}
	// Destroy the worker
	ucp_worker_destroy(ucp_worker_meta);

	// ucp_worker_destroy(ucp_worker_meta);
	free(stat_eps);
	pthread_mutex_unlock(&lock_network);
	// ucp_cleanup(ucp_context_client);
	return 0;
}

// Method discovering which metadata server is in charge of a certain URI.
uint32_t discover_stat_srv(const char *_uri)
{
	// Calculate a crc from the provided URI.
	uint64_t crc_ = crc64(0, (const unsigned char *)_uri, strlen(_uri));

	// Return the metadata server within the set that shall deal with the former entity.
	return crc_ % n_stat_servers;
}

// FIXME: fix implementation for multiple servers.
// Method retrieving the whole set of elements contained by a specific URI.
uint32_t get_dir(char *requested_uri, char **buffer, char ***items)
{
	int ret = 0;
	// Discover the metadata server that shall deal with the former URI.
	uint32_t m_srv = discover_stat_srv(requested_uri);
	ucp_ep_h ep = stat_eps[m_srv];

	pthread_mutex_lock(&lock_network);
	// GETDIR request.
	char getdir_req[REQUEST_SIZE];
	sprintf(getdir_req, "%" PRIu32 " GET %d %s", stat_ids[m_srv], GETDIR, requested_uri);
	slog_debug("[IMSS][get_dir] Request - %s", getdir_req);
	if (send_req(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta, getdir_req) == 0)
	{
		pthread_mutex_unlock(&lock_network);
		slog_error("HERCULES_ERR_GET_DIR_SEND_REQ");
		perror("HERCULES_ERR_GET_DIR_SEND_REQ");
		return -1;
	}

	// char elements[43264];
	// Get the length of the message to be received.
	size_t length = 0;
	length = get_recv_data_length(ucp_worker_meta, local_meta_uid);
	if (length == 0)
	{
		pthread_mutex_unlock(&lock_network);
		perror("HERCULES_ERR_GET_DIR_GET_RECV_DATA_LENGTH");
		slog_error("HERCULES_ERR_GET_DIR_GET_RECV_DATA_LENGTH");
		return -1;
	}
	void *elements = malloc(length);
	//  Retrieve the set of elements within the requested uri.
	ret = recv_dynamic_stream(ucp_worker_meta, ep, elements, BUFFER, local_meta_uid, length);
	// ret = recv_dynamic_stream_opt(ucp_worker_meta, ep, &elements, BUFFER, local_meta_uid, length);
	if (ret < 0)
	{
		pthread_mutex_unlock(&lock_network);
		perror("HERCULES_ERR_GET_DIR_RECV_STREAM");
		slog_error("HERCULES_ERR_GET_DIR_RECV_STREAM");
		// fprintf(stderr, "HERCULES_ERR_GET_DIR_RECV_STREAM , getdir_req = %s, ret=%d, length=%lu\n", getdir_req, ret, length);
		free(elements);
		return -1;
	}

	if (!strncmp("$ERRIMSS_NO_KEY_AVAIL$", (const char *)elements, 22))
	{
		pthread_mutex_unlock(&lock_network);
		slog_error("HERCULES_ERR_GET_DIR_NODIR");
		free(elements);
		return -1;
	}

	uint32_t elements_size = ret;

	// slog_debug("[IMSS][get_dir] elements_size=%lu - ret = %d", elements_size, ret);

	//*buffer = (char *) malloc(sizeof(char)*elements_size);
	// memcpy(*buffer, elements, elements_size);
	// elements = *buffer;

	uint32_t num_elements = elements_size / URI_;
	// slog_debug("[IMSS][get_dir] num_elements=%lu", num_elements);
	// *items = (char **)malloc(sizeof(char *) * (num_elements));
	*items = (char **)calloc(num_elements, sizeof(char *));
	// slog_debug("[IMSS][get_dir] malloc items");

	// Identify each element within the buffer provided.
	char *curr = (char *)elements;
	for (int32_t i = 0; i < num_elements; i++)
	{
		// slog_debug("[IMSS][get_dir] item %d -- calloc", i);
		(*items)[i] = (char *)calloc(URI_, 1);
		// slog_debug("[IMSS][get_dir] item %d -- memcpy", i);
		memcpy((*items)[i], curr, URI_);
		//(*items)[i] = elements;
		// slog_debug("[IMSS][get_dir] item %d -- curr+=URI", i);
		curr += URI_;
		// slog_debug("[IMSS][get_dir] item %d: %s", i, (*items)[i]);
	}

	// free(elements);
	slog_debug("[IMSS][get_dir] Ending, num_elements=%d", num_elements);
	pthread_mutex_unlock(&lock_network);
	return num_elements;
}

/**********************************************************************************/
/***************** IN-MEMORY STORAGE SYSTEM MANAGEMENT FUNCTIONS  *****************/
/**********************************************************************************/

// Method initializing an IMSS deployment.
// int32_t init_imss(char *imss_uri,
// 				  char *hostfile,
// 				  char *meta_hostfile,
// 				  int32_t n_servers,
// 				  uint16_t conn_port,
// 				  uint64_t buff_size,
// 				  uint32_t deployment,
// 				  char *binary_path,
// 				  uint16_t metadata_port)
// {
// 	int ret = 0;
// 	imss_info aux_imss;
// 	ucp_config_t *config;
// 	ucs_status_t status;

// 	if (getenv("IMSS_DEBUG") != NULL)
// 	{
// 		IMSS_DEBUG = 1;
// 	}

// 	// Check if the new IMSS uri has been already assigned.
// 	int32_t existing_imss = stat_imss(imss_uri, &aux_imss);
// 	if (existing_imss)
// 	{
// 		slog_fatal("ERRIMSS_INITIMSS_ALREADYEXISTS");
// 		return -1;
// 	}
// 	// Once it has been notified that no other IMSS instance had the same URI, the deployment will be performed in case of a DETACHED instance.
// 	if (deployment == DETACHED)
// 	{
// 		if (!binary_path)
// 		{
// 			slog_fatal("ERRIMSS_INITIMSS_NOBINARY");
// 			return -1;
// 		}

// 		char *command = (char *)calloc(2048, sizeof(char));

// 		FILE *in_file = fopen(meta_hostfile, "r");
// 		if (!in_file)
// 		{
// 			perror("fopen");
// 			return 0;
// 		}
// 		struct stat sb;
// 		if (stat(meta_hostfile, &sb) == -1)
// 		{
// 			perror("stat");
// 			return 0;
// 		}

// 		char *textRead = (char *)malloc(sb.st_size);
// 		int i = 1;

// 		while (fscanf(in_file, "%[^\n] ", textRead) != EOF)
// 		{
// 			// printf("Line %d is  %s",i, textRead);
// 			// sprintf(command, "mpirun -np %d -hostfile %s %s %s %d %lu %s %d %d %s %d &", n_servers, hostfile, binary_path, imss_uri, conn_port, buff_size, textRead,metadata_port, n_servers ,hostfile, IMSS_THREAD_POOL);
// 			i++;
// 		}
// 		fclose(in_file);
// 		// Original
// 		// sprintf(command, "mpirun -np %d -f %s %s %s %d %lu foo %d %d %s &", n_servers, hostfile, binary_path, imss_uri, conn_port, buff_size, 0, n_servers, "");
// 		// sprintf(command, "mpirun -np %d -hostfile %s %s %s %d %lu foo %d %d %s &", n_servers, hostfile, binary_path, imss_uri, conn_port, buff_size, 0, n_servers, "");

// 		// Imss server(data)
// 		// mpirun -np $num_servers -f $imss_hostfile $server_binary $imss_uri $imss_port_number $imss_buffer_size $metadata_server_address $metadata_server_port $num_servers $imss_hostfile $io_threads &
// 		//["imss://", "5555", "1048576000", "compute-6-2", "5569", "1", "./hostfile", "1"]
// 		// sprintf(command, "mpirun -np %d -hostfile %s %s %s %d %lu %s %d %d %s %d &", n_servers, hostfile, binary_path, imss_uri, conn_port, buff_size, meta_hostfile, metadata_port, n_servers ,hostfile, IMSS_THREAD_POOL);
// 		sprintf(command, "mpirun.mpich -np %d -f %s %s %s %d %lu %s %d %d %s %d &", 2, hostfile, binary_path, imss_uri, conn_port, buff_size, textRead, metadata_port, 2, hostfile, IMSS_THREAD_POOL);

// 		sprintf(command, "mpirun -np %d -hostfile %s %s %s %d %lu %s %d %d", n_servers, hostfile, binary_path, imss_uri, conn_port, buff_size, "compute-6-2", 5569, n_servers);

// 		printf("command=%s", command);
// 		// Perform the deployment (FROM LINUX MAN PAGES: "system() returns after the command has been completed").
// 		if (system(command) == -1)
// 		{
// 			perror("ERRIMSS_INITIMSS_DEPLOY");
// 			return -1;
// 		}
// 		free(command);
// 	}
// 	// IMSS creation.

// 	imss new_imss;
// 	strcpy(new_imss.info.uri_, imss_uri);
// 	new_imss.info.num_storages = n_servers;
// 	new_imss.info.conn_port = conn_port;
// 	new_imss.info.type = 'I';

// 	if (deployment == ATTACHED)
// 		new_imss.info.conn_port = connection_port;

// 	new_imss.info.ips = (char **)malloc(n_servers * sizeof(char *));

// 	// Resources required to connect to the corresponding IMSS.
// 	new_imss.conns.peer_addr = (ucp_address_t **)malloc(n_servers * sizeof(ucp_address_t *));
// 	new_imss.conns.eps = (ucp_ep_h *)malloc(n_servers * sizeof(ucp_ep_h));

// 	// FILE entity managing the IMSS hostfile.
// 	FILE *svr_nodes;
// 	// Open the file containing the IMSS server nodes.
// 	if ((svr_nodes = fopen(hostfile, "r+")) == NULL)
// 	{
// 		perror("ERRIMSS_OPEN_FILE");
// 		return -1;
// 	}
// 	// Number of characters successfully read from the line.
// 	int n_chars;

// 	new_imss.conns.matching_server = -1;

// 	new_imss.conns.id = (uint32_t *)malloc(n_servers * sizeof(uint32_t));

// 	status = ucp_worker_get_address(ucp_worker_data, &local_addr_data, &local_addr_len_data);
// 	ucp_worker_address_attr_t attr;
// 	attr.field_mask = UCP_WORKER_ADDRESS_ATTR_FIELD_UID;
// 	ucp_worker_address_query(local_addr_data, &attr);
// 	local_meta_uid = attr.worker_uid;

// 	// Connect to all servers.
// 	for (int i = 0; i < n_servers; i++)
// 	{
// 		ucs_status_t status;
// 		int oob_sock;
// 		ucp_address_t *peer_addr;
// 		size_t addr_len;
// 		ucp_ep_params_t ep_params;
// 		ucs_status_t ep_status = UCS_OK;

// 		// Allocate resources in the metadata structure so as to store the current IMSS's IP.
// 		(new_imss.info.ips)[i] = (char *)malloc(LINE_LENGTH);
// 		size_t l_size = LINE_LENGTH;
// 		// Save IMSS metadata deployment.
// 		n_chars = getline(&((new_imss.info.ips)[i]), &l_size, svr_nodes);

// 		// Erase the new line character ('') from the string.
// 		((new_imss.info.ips)[i])[n_chars - 1] = '\0';

// 		// Save the current socket value when the IMSS ip matches the clients' one.
// 		if (!strncmp((new_imss.info.ips)[i], client_node, len_client_node) || !strncmp((new_imss.info.ips)[i], client_ip, strlen(new_imss.info.ips[i])))
// 		{
// 			new_imss.conns.matching_server = i;
// 			strcpy(att_deployment, imss_uri);
// 		}
// 		// Create the connection to the IMSS server dispatcher thread.
// 		slog_debug("[IMSS] imss_init: Contacting dispatcher at %s:%d", (new_imss.info.ips)[i], new_imss.info.conn_port);

// 		process_rank = CLOSE_EP;

// 		oob_sock = connect_common((new_imss.info.ips)[i], new_imss.info.conn_port, AF_INET);

// 		char request[REQUEST_SIZE];
// 		sprintf(request, "%" PRIu32 " GET HELLO! %ld %s", process_rank, buff_size, imss_uri);

// 		if (send(oob_sock, request, REQUEST_SIZE, 0) < 0)
// 		{
// 			perror("ERRIMSS_STAT_HELLO_0");
// 			return -1;
// 		}

// 		ret = recv(oob_sock, &addr_len, sizeof(addr_len), MSG_WAITALL);
// 		new_imss.conns.peer_addr[i] = (ucp_address *)malloc(addr_len);
// 		ret = recv(oob_sock, new_imss.conns.peer_addr[i], addr_len, MSG_WAITALL);
// 		close(oob_sock);

// 		client_create_ep(ucp_worker_data, &new_imss.conns.eps[i], curr_imss.conns.peer_addr[i]);
// 	}
// 	// Close the file.
// 	if (fclose(svr_nodes) != 0)
// 	{
// 		perror("ERR_CLOSE_FILE");
// 		return -1;
// 	}

// 	// Discover the metadata server that shall deal with the new IMSS instance.
// 	uint32_t m_srv = discover_stat_srv(new_imss.info.uri_);
// 	ucp_ep_h ep = stat_eps[m_srv];

// 	// Send the created structure to the metadata server.
// 	char key_plus_size[REQUEST_SIZE];
// 	sprintf(key_plus_size, "%" PRIu32 " SET %lu %s", stat_ids[m_srv], (sizeof(imss_info) + new_imss.info.num_storages * LINE_LENGTH), new_imss.info.uri_);

// 	if (send_req(ucp_worker_meta, ep, local_addr_data, local_addr_len_data, key_plus_size) == 0)
// 	{
// 		perror("ERRIMSS_RLSIMSS_SENDADDR");
// 		return -1;
// 	}

// 	// Send the new IMSS metadata structure to the metadata server entity.
// 	if (send_dynamic_stream(ucp_worker_meta, ep, (void *)&new_imss.info, IMSS_INFO, local_meta_uid) < 0)
// 		return -1;

// 	// Add the created struture into the underlying IMSS vector.
// 	GInsert(&imssd_pos, &imssd_max_size, (char *)&new_imss, imssd, free_imssd);

// 	return 0;
// }

// Method initializing the required resources to make use of an existing IMSS.
int32_t open_imss(char *imss_uri)
{
	// New IMSS structure storing the entity to be created.
	imss new_imss;
	// ucp_config_t *config;
	ucs_status_t status;

	int32_t not_initialized = 0;

	slog_live("[IMSS][open_imss] starting function, imss_uri=%s", imss_uri);
	// Retrieve the actual information from the metadata server.
	int32_t imss_existance = stat_imss(imss_uri, &new_imss.info);
	slog_live("[IMSS][open_imss] imss_uri=%s, imss_existance=%d", imss_uri, imss_existance);
	// Check if the requested IMSS did not exist or was already stored in the local vector.
	switch (imss_existance)
	{
	case 0:
	{
		slog_fatal("HERCULES_ERR_OPEN_IMSS_NOT_EXISTS");
		return -1;
	}
	case 2:
	{
		imss check_imss = g_array_index(imssd, imss, found_in);

		if (check_imss.conns.matching_server != -2)
			return -2;

		for (int32_t i = 0; i < check_imss.info.num_storages; i++)
			free(check_imss.info.ips[i]);

		free(check_imss.info.ips);
		not_initialized = 1;
		break;
	}
	case -1:
	{
		return -1;
	}
	}
	slog_debug("[IMSS][open_imss] new_imss.info.num_storages=%ld", new_imss.info.num_storages);

	new_imss.conns.peer_addr = (ucp_address_t **)malloc(new_imss.info.num_storages * sizeof(ucp_address_t *));
	new_imss.conns.eps = (ucp_ep_h *)malloc(new_imss.info.num_storages * sizeof(ucp_ep_h));
	new_imss.conns.id = (uint32_t *)malloc(new_imss.info.num_storages * sizeof(uint32_t));
	new_imss.conns.matching_server = -1;

	status = ucp_worker_get_address(ucp_worker_data, &local_addr_data, &local_addr_len_data);
	ucp_worker_address_attr_t attr;
	attr.field_mask = UCP_WORKER_ADDRESS_ATTR_FIELD_UID;
	ucp_worker_address_query(local_addr_data, &attr);
	local_data_uid = attr.worker_uid;

	NUM_DATA_SERVERS = new_imss.info.num_storages;
	// Connect to the requested IMSS.
	for (int32_t i = 0; i < new_imss.info.num_storages; i++)
	{
		int oob_sock;
		size_t addr_len;
		int ret = 0;

		fprintf(stderr, "Connecting to %s:%d\n", new_imss.info.ips[i], new_imss.info.conn_port);
		// Open a socket communication with a data server dispatcher.
		oob_sock = connect_common(new_imss.info.ips[i], new_imss.info.conn_port, AF_INET);
		if (oob_sock < 0)
		{
			char err_msg[128];
			sprintf(err_msg, "HERCULES_ERR_CONNECT_COMMON - i=%d - %s:%d", i, new_imss.info.ips[i], new_imss.info.conn_port);
			slog_error("%s", err_msg);
			perror(err_msg);
			return -1;
		}

		char request[REQUEST_SIZE];
		sprintf(request, "%" PRIu32 " GET %s", process_rank, "HELLO!JOIN");
		slog_live("[open_imss] ip_address=%s:%d", new_imss.info.ips[i], new_imss.info.conn_port);
		// Request for an UCX worker address.
		if (send(oob_sock, request, REQUEST_SIZE, 0) < 0)
		{
			perror("HERCULES_ERR_STAT_HELLO_1");
			return -1;
		}
		// Get the worker address lenght.
		ret = recv(oob_sock, &addr_len, sizeof(addr_len), MSG_WAITALL);
		if (ret < 0)
		{
			perror("HERCULES_ERR_RECV_1_HELLO");
			close(oob_sock);
			return -1;
		}
		new_imss.conns.peer_addr[i] = (ucp_address *)malloc(addr_len);
		// Get the worker address.
		ret = recv(oob_sock, new_imss.conns.peer_addr[i], addr_len, MSG_WAITALL);
		if (ret < 0)
		{
			perror("HERCULES_ERR_RECV_2_HELLO");
			close(oob_sock);
			return -1;
		}
		// Close the socket fd.
		if (ret < close(oob_sock))
		{
			perror("HERCULES_ERR_CLOSE_SOCKET");
			return -1;
		}

		new_imss.conns.id[i] = i;

		// Save the current socket value when the IMSS ip matches the clients' one.
		if (!strncmp((new_imss.info.ips)[i], client_node, len_client_node) || !strncmp((new_imss.info.ips)[i], client_ip, strlen(new_imss.info.ips[i])))
		{
			new_imss.conns.matching_server = i;
			strcpy(att_deployment, imss_uri);
		}

		// ucs_status_t status;
		// int ret = -1;
		// char server_addr[] = "broadwell-010";
		status = start_client(ucp_worker_data, new_imss.info.ips[i], new_imss.info.conn_port + 1, &new_imss.conns.eps[i]);
		if (status != UCS_OK)
		{
			fprintf(stderr, "failed to start client (%s)\n", ucs_status_string(status));
			ret = -1;
			// goto out;
			return ret;
		}

		// client_create_ep(ucp_worker_data, &new_imss.conns.eps[i], new_imss.conns.peer_addr[i]);
		slog_debug("[IMSS] open_imss: Created endpoint with %s", (new_imss.info.ips)[i]);
	}

	// /* UCP objects */
	// ucp_context_h ucp_context;
	// ucp_worker_h ucp_worker;
	// init_context_ori(&ucp_context, &ucp_worker, CLIENT_SERVER_SEND_RECV_TAG);

	// char server_address[] = "broadwell-001";
	// run_client(ucp_worker, server_address, new_imss.info.conn_port + 1, CLIENT_SERVER_SEND_RECV_TAG);
	// ucp_worker_destroy(ucp_worker);
	// ucp_cleanup(ucp_context);

	// exit(1);

	// If the struct was found within the vector but uninitialized, once updated, store it in the same position.
	if (not_initialized)
	{
		g_array_remove_index(imssd, found_in);
		g_array_insert_val(imssd, found_in, new_imss);

		return 0;
	}

	// Add the created struture into the underlying IMSSs.
	GInsert(&imssd_pos, &imssd_max_size, (char *)&new_imss, imssd, free_imssd);
	return 0;
}

// Function to flush and close the endpoint
void close_ucx_endpoint(ucp_worker_h worker, ucp_ep_h ep)
{
	// Flush the endpoint to ensure all outstanding operations are completed
	ucs_status_t status = ucp_ep_flush(ep);
	if (status != UCS_OK)
	{
		fprintf(stderr, "Failed to flush endpoint: %s\n", ucs_status_string(status));
	}

	// Destroy the endpoint
	ucp_ep_destroy(ep);

	// Optionally, progress the worker
	// ucp_worker_progress(worker);
}

void ucx_cleanup()
{
	ucp_worker_destroy(ucp_worker_data);
}

// Method releasing client-side and/or server-side resources related to a certain IMSS instance.
int32_t release_imss(char *imss_uri, uint32_t release_op)
{
	// Search for the requested IMSS.
	imss imss_;
	int32_t imss_position;
	if ((imss_position = find_imss(imss_uri, &imss_)) == -1)
	{
		perror("HERCULES_ERR_RLSHERCULES_NOTFOUND");
		slog_fatal("HERCULES_ERR_RLSHERCULES_NOTFOUND");
		return -1;
	}

	// Release the set of connections to the corresponding IMSS.
	pthread_mutex_lock(&lock_network);
	slog_live("imss_position=%d, num_storages=%d", imss_position, imss_.info.num_storages);
	// sleep(1);
	for (int32_t i = 0; i < imss_.info.num_storages; i++)
	{
		// Request IMSS instance closure per server if the instance is a DETACHED one and the corresponding argumet was provided.
		if (release_op == CLOSE_DETACHED)
		{
			// char release_msg[REQUEST_SIZE];
			ucp_ep_h ep;

			ep = imss_.conns.eps[i];

			// sprintf(release_msg, "GET 2 0 RELEASE");
			char release_msg[] = "GET 2 0 RELEASE\0";
			slog_live("release_msg=%s to server %d", release_msg, i);
			if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, release_msg) == 0)
			{
				pthread_mutex_unlock(&lock_network);
				perror("HERCULES_ERR_RLSHERCULES_SENDADDR");
				slog_error("HERCULES_ERR_RLSHERCULES_SENDADDR");
				return -1;
			}

			size_t msg_length = 0;
			msg_length = get_recv_data_length(ucp_worker_data, local_data_uid);
			if (msg_length < 0)
			{
				pthread_mutex_unlock(&lock_network);
				perror("ERR_HERCULES_RLSHERCULES_INVALID_MSG_LENGTH");
				slog_error("ERR_HERCULES_RLSHERCULES_INVALID_MSG_LENGTH");
				return -1;
			}

			// char result[msg_length];
			void *result = malloc(msg_length);
			msg_length = recv_data(ucp_worker_data, ep, result, msg_length, local_data_uid, 0);
			if (msg_length == 0)
			{
				pthread_mutex_unlock(&lock_network);
				perror("ERR_HERCULES_RLSHERCULES_RECV_DATA");
				slog_error("ERR_HERCULES_RLSHERCULES_RECV_DATA");
				free(result);
				return -1;
			}

			slog_debug("[delete_dataset] result=%s, msg_length=%d", (const char *)result, msg_length);
			if (!strncmp((const char *)result, "RELEASE", strlen("RELEASE")))
			{
				// free the message received from the metadata server.
				free(result);
				pthread_mutex_unlock(&lock_network);
				return 1;
			}
			// free the message received from the metadata server.
			free(result);
			pthread_mutex_unlock(&lock_network);
			// slog_live("closing endpoint %d", i);
			// close_ucx_endpoint(ucp_worker_data, ep);

			// ep_close(ucp_worker_data, ep, 0);
			//  // ep_close(ucp_worker_data, ep, UCP_EP_CLOSE_MODE_FLUSH);
			// ep_flush(ep, ucp_worker_data);

			// ucp_ep_destroy(ep);
			// ucp_context_destroy(ucp_worker_data);
		}
		// ep_flush(ep, ucp_worker_data);
		// ep_flush(imss_.conns.eps_[i], ucp_worker_data);
		free(imss_.info.ips[i]);
	}
	slog_live("flusing data worker");
	// Optionally, flush the worker
	ucs_status_t status = ucp_worker_flush(ucp_worker_data);
	if (status != UCS_OK)
	{
		fprintf(stderr, "Failed to flush worker: %s\n", ucs_status_string(status));
	}
	slog_live("destroying data worker");
	// Destroy the worker
	ucp_worker_destroy(ucp_worker_data);

	free(imss_.info.ips);
	free(curr_imss.conns.eps);

	g_array_remove_index(imssd, imss_position);
	g_array_insert_val(imssd, imss_position, empty_imss);
	// Add the released position to the set of free positions.
	g_array_append_val(free_imssd, imss_position);

	if (!memcmp(att_deployment, imss_uri, URI_))
		memset(att_deployment, '\0', URI_);

	pthread_mutex_unlock(&lock_network);

	return 0;
}

// Method retrieving information related to a certain IMSS instance.
int32_t stat_imss(char *imss_uri, imss_info *imss_info_)
{
	// Check for the IMSS info structure in the local vector.
	int32_t imss_found_in;
	imss searched_imss;
	int ret = 0;
	ucp_ep_h ep;

	// slog_debug("[IMSS][stat_imss] imss_uri=%s", imss_uri);
	if ((imss_found_in = find_imss(imss_uri, &searched_imss)) != -1)
	{
		slog_live("[IMSS][stat_imss] imss_found_in=%d", imss_found_in);
		memcpy(imss_info_, &searched_imss.info, sizeof(imss_info));
		imss_info_->ips = (char **)malloc((imss_info_->num_storages) * sizeof(char *));
		for (int32_t i = 0; i < imss_info_->num_storages; i++)
		{
			imss_info_->ips[i] = (char *)malloc(LINE_LENGTH * sizeof(char));
			strcpy(imss_info_->ips[i], searched_imss.info.ips[i]);
		}

		return 2;
	}

	// Formated imss uri to be sent to the metadata server.
	char formated_uri[REQUEST_SIZE];

	// Discover the metadata server that handles the IMSS instance.
	uint32_t m_srv = discover_stat_srv(imss_uri);

	ep = stat_eps[m_srv];

	pthread_mutex_lock(&lock_network);
	// Send the request.
	sprintf(formated_uri, "%" PRIu32 " GET 0 %s", stat_ids[m_srv], imss_uri);
	slog_info("[IMSS][stat_imss] Request - '%s'", formated_uri);
	if (send_req(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta, formated_uri) == 0)
	{
		pthread_mutex_unlock(&lock_network);
		perror("HERCULES_ERR_STAT_IMSS_SEND_REQ");
		slog_error("HERCULES_ERR_STAT_IMSS_SEND_REQ");
		// perror("ERRIMSS_RLSIMSS_SENDADDR");
		return -1;
	}

	// Get the length of the message to be received.
	size_t length = 0;
	length = get_recv_data_length(ucp_worker_meta, local_meta_uid);
	if (length == 0)
	{
		pthread_mutex_unlock(&lock_network);
		perror("HERCULES_ERR_STAT_IMSS_GET_RECV_DATA_LENGTH");
		slog_error("HERCULES_ERR_STAT_IMSS_GET_RECV_DATA_LENGTH");
		return -1;
	}
	void *request = malloc(length);
	ret = recv_dynamic_stream(ucp_worker_meta, ep, request, IMSS_INFO, local_meta_uid, length);
	// ret = recv_dynamic_stream_opt(ucp_worker_meta, ep, &request, IMSS_INFO, local_meta_uid, length);
	slog_debug("[IMSS][stat_imss] End, ret=%d", ret);
	// if (ret < sizeof(imss_info))
	if (ret == -1)
	{
		pthread_mutex_unlock(&lock_network);
		free(request);
		return 0;
	}
	// imss_info_ = (imss_info *)request;
	memcpy(imss_info_, request, sizeof(imss_info));
	free(request);
	pthread_mutex_unlock(&lock_network);
	return 1;
}

// Method providing the URI of the attached IMSS instance.
char *get_deployed()
{
	if (att_deployment[0] != '\0')
	{
		char *att_dep_uri = (char *)malloc(URI_ * sizeof(char));
		strcpy(att_dep_uri, att_deployment);
		return att_dep_uri;
	}

	return NULL;
}

// Method providing the URI of the instance deployed in some endpoint.
char *get_deployed(char *endpoint)
{
	return endpoint;
}

/**********************************************************************************/
/************************** DATASET MANAGEMENT FUNCTIONS **************************/
/**********************************************************************************/

// Method creating a dataset and the environment enabling READ or WRITE operations over it.
int32_t create_dataset(char *dataset_uri,
					   char *policy,
					   int32_t num_data_elem,
					   int32_t data_elem_size,
					   int32_t repl_factor,
					   int32_t n_servers,
					   char *link,
					   int opened)
{
	// int err = 0;
	int ret = 0;
	ucp_ep_h ep;

	curr_imss = g_array_index(imssd, imss, curr_dataset.imss_d);

	slog_live("[IMSS] dataset_create: starting, imss_d=%d, num_storages=%d", curr_dataset.imss_d, curr_imss.info.num_storages);

	if ((dataset_uri == NULL) || (policy == NULL) || !num_data_elem || !data_elem_size)
	{
		slog_fatal("HERCULES_ERR_CRTDATASET_WRONGARG");
		return -EINVAL;
	}
	if ((repl_factor < NONE) || (repl_factor > TRM))
	{
		slog_fatal("HERCULES_ERR_CRTDATASET_BADREPLFACTOR");
		return -EINVAL;
	}

	int32_t associated_imss_indx;
	// Check if the IMSS storing the dataset exists within the clients session.
	slog_live("[IMSS] Before imss_check  %s ", dataset_uri);
	if ((associated_imss_indx = imss_check(dataset_uri)) == -1)
	{
		slog_fatal("[IMSS] ERRIMSS_OPENDATA_IMSSNOTFOUND, associated_imss_indx=%d", associated_imss_indx);
		return -ENOENT;
	}
	// else
	// {
	imss associated_imss;
	slog_live("[IMSS] After imss_check, associated_imss_indx=%d", associated_imss_indx);
	associated_imss = g_array_index(imssd, imss, associated_imss_indx);
	// }

	dataset_info new_dataset;

	// Dataset metadata request. To know if the dataset already exists.
	if (stat_dataset(dataset_uri, &new_dataset, opened))
	{ // The dataset exists.
		slog_live("[IMSS] ERRIMSS_CREATEDATASET_ALREADYEXISTS, associated_imss.conns.matching_server=%d", associated_imss.conns.matching_server);
		new_dataset.imss_d = associated_imss_indx;
		new_dataset.local_conn = associated_imss.conns.matching_server;

		// Initialize dataset fields monitoring the dataset itself if it is a LOCAL one.
		if (!strcmp(new_dataset.policy, "LOCAL"))
		{
			// Current number of blocks written by the client.
			new_dataset.num_blocks_written = (uint64_t *)malloc(1 * sizeof(uint64_t));
			*(new_dataset.num_blocks_written) = 0;
			new_dataset.blocks_written = (uint32_t *)calloc(new_dataset.num_data_elem, sizeof(uint32_t));
		}

		// if (opened)
		// { // the dataset is created by an 'open' syscall.
		// 	new_dataset.n_open += 1;
		// }

		// Add the created struture into the underlying IMSSs.
		// return (GInsert(&datasetd_pos, &datasetd_max_size, (char *)&new_dataset, datasetd, free_datasetd));
		GInsert(&datasetd_pos, &datasetd_max_size, (char *)&new_dataset, datasetd, free_datasetd);

		return -EEXIST;
	}
	// The dataset does not exist, we create a new one.
	// Save the associated metadata of the current dataset.
	strcpy(new_dataset.uri_, dataset_uri);
	strcpy(new_dataset.policy, policy);
	new_dataset.num_data_elem = num_data_elem;
	new_dataset.data_entity_size = data_elem_size * 1024; // dataset in kilobytes
	// new_dataset.imss_d = curr_dataset.imss_d;			  // associated_imss_indx;
	new_dataset.imss_d = associated_imss_indx;
	new_dataset.local_conn = associated_imss.conns.matching_server;
	new_dataset.size = 0;

	if (opened == 1)
	{ // the dataset is created by an 'open' syscall.
		new_dataset.n_open = 1;
	}
	else
	{ // the dataset is created by another syscall.
		new_dataset.n_open = 0;
	}
	// mark the file as attach. It helps to delete the file in case of a delete and close.
	strncpy(new_dataset.status, "attach", strlen("attach"));

	if (link != NO_LINK)
	{
		strcpy(new_dataset.link, link);
		slog_live("[IMSS][create_dataset] link=%s, new_dataset=%s", link, new_dataset.link);
		new_dataset.is_link = 1;
	}
	else
	{
		slog_live("[IMSS][create_dataset] link=%s", link);
		new_dataset.is_link = 0;
	}

	slog_live("[IMSS][create_dataset] curr_imss.info.num_storages=%d, n_servers=%d", curr_imss.info.num_storages, n_servers);
	// if (n_servers > 0 && n_servers <= curr_imss.info.num_storages)
	{
		new_dataset.n_servers = n_servers;
	}
	// else
	// {
	// 	new_dataset.n_servers = curr_imss.info.num_storages;
	// }

	new_dataset.repl_factor = (repl_factor < new_dataset.n_servers) ? (repl_factor) : (new_dataset.n_servers);

	// new_dataset.initial_node = 0;

	//*****NEXT LINE NEED FOR DIFERENT POLICIES TO WORK IN DISTRIBUTED*****//
	strcpy(new_dataset.original_name, dataset_uri);
	//*****BEFORE LINE NEED FOR DIFERENT POLICIES TO WORK IN DISTRIBUTED*****//

	// Size of the message to be sent.
	uint64_t msg_size = sizeof(dataset_info);

	// Reserve memory so as to store the position of each data element if the dataset is a LOCAL one.
	if (!strcmp(new_dataset.policy, "LOCAL"))
	{
		uint32_t info_size = new_dataset.num_data_elem * sizeof(uint16_t);

		/*new_dataset.data_locations = (uint16_t *) malloc(info_size);
		  memset(new_dataset.data_locations, 0, info_size);*/
		new_dataset.data_locations = (uint16_t *)calloc(info_size, sizeof(uint16_t));

		// Specify that the created dataset is a LOCAL one.
		new_dataset.type = 'L';

		// Add additional bytes that will be sent.
		msg_size += info_size;
	}
	else
		new_dataset.type = 'D';

	slog_live("[IMSS][create_dataset] new_dataset.type=%c, local_conn=%d", new_dataset.type, associated_imss.conns.matching_server);

	// Discover the metadata server that handle the new dataset.
	uint32_t m_srv = discover_stat_srv(new_dataset.uri_);

	char formated_uri[REQUEST_SIZE];
	// sprintf(formated_uri, "%" PRIu32 " SET %lu %s", stat_ids[m_srv], msg_size, new_dataset.uri_);
	sprintf(formated_uri, "%" PRIu32 " SET %lu %s", opened, msg_size, new_dataset.uri_);
	slog_info("[IMSS][create_dataset] Request - '%s'", formated_uri);

	pthread_mutex_lock(&lock_network);
	ep = stat_eps[m_srv];
	if (send_req(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta, formated_uri) == 0)
	{
		pthread_mutex_unlock(&lock_network);
		perror("HERCULES_ERR_CREATEDATASET_SENDREQ");
		slog_error("HERCULES_ERR_CREATEDATASET_SENDREQ");
		return -1;
	}

	// slog_live("[IMSS][create_dataset] dataset_create: sending dataset_info");
	// Send the new dataset metadata structure to the metadata server entity.
	if (send_dynamic_stream(ucp_worker_meta, ep, (void *)&new_dataset, DATASET_INFO, local_meta_uid) < 0)
	{
		pthread_mutex_unlock(&lock_network);
		slog_error("HERCULES_ERR_CREATEDATASET_SENDSTREAM");
		perror("HERCULES_ERR_CREATEDATASET_SENDSTREAM");
		return -1;
	}

	// slog_live("[IMSS] dataset_create: sent dataset_info");
	// Initialize dataset fields monitoring the dataset itself if it is a LOCAL one.
	if (!strcmp(new_dataset.policy, "LOCAL"))
	{
		// Current number of blocks written by the client.
		new_dataset.num_blocks_written = (uint64_t *)malloc(1 * sizeof(uint64_t));
		*(new_dataset.num_blocks_written) = 0;
		// Specific blocks written by the client.

		/*new_dataset.blocks_written = (uint32_t *) malloc(new_dataset.num_data_elem*sizeof(uint32_t));
		  memset(new_dataset.blocks_written, 0, new_dataset.num_data_elem*sizeof(uint32_t));*/

		new_dataset.blocks_written = (uint32_t *)calloc(new_dataset.num_data_elem, sizeof(uint32_t));
	}

	//	//Set the specified policy.
	//	if (set_policy(&new_dataset) == -1)
	//	{
	//		perror("ERRIMSS_DATASET_SETPLCY");
	//		return -1;
	//	}

	// Add the created struture into the underlying IMSSs.
	ret = GInsert(&datasetd_pos, &datasetd_max_size, (char *)&new_dataset, datasetd, free_datasetd);
	slog_live("[IMSS] dataset_create: GIsinsert %d", ret);
	pthread_mutex_unlock(&lock_network);
	return ret;
}

// Method creating the required resources in order to READ and WRITE an existing dataset.
int32_t open_dataset(char *dataset_uri, int opened)
{
	int32_t associated_imss_indx;
	// printf("OPEN DATASET INSIDE dataset_uri=%s",dataset_uri);
	// Check if the IMSS storing the dataset exists within the clients session.
	if ((associated_imss_indx = imss_check(dataset_uri)) == -1)
	{
		slog_fatal("HERCULES_ERR_OPEN_DATASET_NOT_FOUND");
		// return -1;
	}

	slog_live("[IMSS][open_dataset] associated_imss_indx=%d", associated_imss_indx);

	imss associated_imss;
	associated_imss = g_array_index(imssd, imss, associated_imss_indx);

	dataset_info new_dataset;
	// Dataset metadata request.
	int32_t stat_dataset_res = stat_dataset(dataset_uri, &new_dataset, opened);
	if (stat_dataset_res == 0)
	{
		slog_warn("HERCULES_ERR_OPEN_DATASET_NOT_EXIST_1: %s, dataset does not exist", dataset_uri);
		return -1;
	}

	if (new_dataset.is_link != 0)
	{
		slog_debug("[IMSS][open_dataset] is_link=%d, link name=%s", new_dataset.is_link, new_dataset.link);
		stat_dataset_res = stat_dataset(new_dataset.link, &new_dataset, 0);
		// check if it comes from Hercules
		if (strncmp(new_dataset.link, "imss://", strlen("imss://")))
		{
			// return open(new_dataset.link);

			// map_put(map, imss_path, file_desc, stats, aux);
			slog_debug("[IMSS][open_dataset] No Hercules link %s", new_dataset.link);

			// dataset_uri = new_dataset.link;
			strcpy(dataset_uri, new_dataset.link);
			return -2;
		}
	}
	else
	{
		slog_debug("[IMSS][open_dataset] is_link=%d, stat_dataset_res=%d, new_dataset.local_conn=%d", new_dataset.is_link, stat_dataset_res, new_dataset.local_conn);
	}
	int32_t not_initialized = 0;

	// Check if the requested dataset did not exist or was already stored in the local vector.
	switch (stat_dataset_res)
	{
	case 0:
	{
		// slog_fatal("HERCULES_ERR_OPENDATASET_NOTEXISTS: %s", dataset_uri);
		slog_warn("HERCULES_ERR_OPEN_DATASET_NOT_EXIST_2: %s, dataset does not exist", dataset_uri);
		return -1;
	}
	case 2:
	{
		if (new_dataset.local_conn != -2)
		{
			slog_fatal("HERCULES_ERR_OPEN_DATASET_ALREADY_STORED");
			slog_live("new_dataset.local_conn=%d", new_dataset.local_conn);
			// fprintf(stderr, "HERCULES_ERR_OPEN_DATASET_ALREADY_STORED\n");
			return -EEXIST;
			// return -1;
			// return 0;
		}
		not_initialized = 1;

		break;
	}
	case -1:
	{
		return -1;
	}
	}

	// Assign the associated IMSS descriptor to the new dataset structure.
	new_dataset.imss_d = curr_dataset.imss_d; // associated_imss_indx;
	new_dataset.local_conn = associated_imss.conns.matching_server;

	// Initialize dataset fields monitoring the dataset itself if it is a LOCAL one.
	if (!strcmp(new_dataset.policy, "LOCAL"))
	{
		// Current number of blocks written by the client.
		new_dataset.num_blocks_written = (uint64_t *)malloc(1 * sizeof(uint64_t));
		*(new_dataset.num_blocks_written) = 0;
		// Specific blocks written by the client.
		/*new_dataset.blocks_written = (uint32_t *) malloc(new_dataset.num_data_elem*sizeof(uint32_t));
		  memset(new_dataset.blocks_written, '\0', new_dataset.num_data_elem*sizeof(uint32_t));*/

		new_dataset.blocks_written = (uint32_t *)calloc(new_dataset.num_data_elem, sizeof(uint32_t));
	}

	//	//Set the specified policy.
	//	if (set_policy(&new_dataset) == -1)
	//	{
	//		perror("ERRIMSS_DATASET_SETPLCY");
	//		return -1;
	//	}

	// If the struct was found within the vector but uninitialized, once updated, store it in the same position.
	if (not_initialized)
	{
		slog_live("[IMSS][open_dataset] not_initialized=%d, found_in=%d", not_initialized, found_in);
		g_array_remove_index(datasetd, found_in);
		g_array_insert_val(datasetd, found_in, new_dataset);

		return found_in;
	}

	// Add the created struture into the underlying IMSSs.
	// pthread_mutex_lock(&lock_gtree);
	int32_t ret = GInsert(&datasetd_pos, &datasetd_max_size, (char *)&new_dataset, datasetd, free_datasetd);
	// pthread_mutex_unlock(&lock_gtree);
	return ret;
}

// Method releasing the set of resources required to deal with a dataset.
int32_t release_dataset(int32_t dataset_id)
{
	ucp_ep_h ep;
	// Check if the provided descriptor corresponds to a position within the vector.
	if ((dataset_id < 0) || (dataset_id >= datasetd_max_size))
	{
		slog_fatal("ERRIMSS_RELDATASET_BADDESCRIPTOR");
		return -1;
	}

	// Dataset to be released.
	dataset_info release_dataset = g_array_index(datasetd, dataset_info, dataset_id);

	// If the dataset is a LOCAL one, the position of the data elements must be updated.
	if (!strcmp(release_dataset.policy, "LOCAL"))
	{
		pthread_mutex_lock(&lock_network);
		// Discover the metadata server that handles the dataset.
		uint32_t m_srv = discover_stat_srv(release_dataset.uri_);

		ep = stat_eps[m_srv];

		// Formated dataset uri to be sent to the metadata server.
		char formated_uri[REQUEST_SIZE];
		sprintf(formated_uri, "%" PRIu32 " SET 0 %s", stat_ids[m_srv], release_dataset.uri_);
		slog_debug("[IMSS][release_dataset] formated_uri='%s'", formated_uri);

		if (send_req(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta, formated_uri) == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERRIMSS_RLSIMSS_SENDADDR");
			slog_error("ERRIMSS_RLSIMSS_SENDADDR");
			return -1;
		}

		// Format update message to be sent to the metadata server.
		uint64_t blocks_written_size = *(release_dataset.num_blocks_written) * sizeof(uint32_t);
		uint64_t update_msg_size = 8 + blocks_written_size;

		/*char update_msg[update_msg_size];
		  memset(update_msg, '\0', update_msg_size);*/
		char *update_msg = (char *)calloc(update_msg_size, sizeof(char));

		uint16_t update_value = (release_dataset.local_conn + 1);
		memcpy(update_msg, release_dataset.blocks_written, blocks_written_size);
		memcpy((update_msg + blocks_written_size), &update_value, sizeof(uint16_t));

		// Send the list of servers storing the data elements.
		char mode2[] = "SET";
		if (send_data(ucp_worker_meta, ep, mode2, MODE_SIZE, local_meta_uid) == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERR_HERCULES_RELEASE_DATASET_SEND_DATA");
			slog_error("ERR_HERCULES_RELEASE_DATASET_SEND_DATA");
			return -1;
		}

		// if (send_data_addr(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta) < 0)
		//{
		//	perror("ERRIMSS_RLSIMSS_SENDADDR");
		//	return -1;
		// }

		if (send_data(ucp_worker_meta, ep, update_msg, REQUEST_SIZE, local_meta_uid) == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERR_HERCULES_RELEASE_DATASET_SEND_DATA");
			slog_error("ERR_HERCULES_RELEASE_DATASET_SEND_DATA");
			return -1;
		}

		size_t msg_length = 0;
		// char update_result[RESPONSE_SIZE];
		msg_length = get_recv_data_length(ucp_worker_meta, local_meta_uid);
		if (msg_length == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERRIMSS_REL_DATASET_INVALID_MSG_LENGTH");
			slog_error("ERRIMSS_REL_DATASET_INVALID_MSG_LENGTH");
			return -1;
		}

		// char update_result[msg_length];
		void *update_result = malloc(msg_length);
		// msg_length = recv_data_opt(ucp_worker_meta, ep, &update_result, msg_length, local_meta_uid, 0);
		msg_length = recv_data(ucp_worker_meta, ep, update_result, msg_length, local_meta_uid, 0);
		if (msg_length == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERRIMSS_REL_DATASET_RECV_UPDATERES");
			slog_error("ERRIMSS_REL_DATASET_RECV_UPDATERES");
			free(update_result);
			return -1;
		}

		if (strcmp((const char *)update_result, "UPDATED!"))
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERRIMSS_RELDATASET_UPDATE");
			// Free the message received from the metadata server.
			free(update_result);
			return -1;
		}

		// Free the data locations vector.
		free(release_dataset.data_locations);
		// Free the monitoring vector.
		free(release_dataset.blocks_written);
		free(release_dataset.num_blocks_written);
		free(update_msg);
		// Free the message received from the metadata server.
		free(update_result);
		pthread_mutex_unlock(&lock_network);
	}

	g_array_remove_index(datasetd, dataset_id);
	g_array_insert_val(datasetd, dataset_id, empty_dataset);
	// Add the index to the set of free positions within the dataset vector.
	g_array_append_val(free_datasetd, dataset_id);

	return 0;
}

/**
 * @brief Set a dataset as closed on the metadata server.
 * @return 1 if the file was correctly closed,
 * 0 if the file was deleted (e.g., file has been removed using unlink),
 * on error -1 is returned.
 */
int32_t close_dataset(const char *dataset_uri, int fd)
{
	pthread_mutex_lock(&lock_network);
	ucp_ep_h ep;
	// size_t ret = 0;
	size_t msg_length = 0;
	// Formated dataset uri to be sent to the metadata server.
	char formated_uri[REQUEST_SIZE];

	// Discover the metadata server that handles the dataset.
	uint32_t m_srv = discover_stat_srv((char *)dataset_uri);

	ep = stat_eps[m_srv];

	sprintf(formated_uri, "%" PRIu32 " GET 7 %s", stat_ids[m_srv], dataset_uri); // delete

	slog_debug("[IMSS][close_dataset] formated_uri='%s'", formated_uri);
	// Send the request set the current dataset as closed on the metadata server.
	msg_length = send_req(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta, formated_uri);
	slog_debug("[IMSS][close_dataset] after send_req, ret=%lu", msg_length);
	if (msg_length == 0)
	{
		pthread_mutex_unlock(&lock_network);
		slog_error("HERCULES_ERR_SEND_REQ_CLOSE_DATASET");
		perror("HERCULES_ERR_SEND_REQ_CLOSE_DATASET");
		return -1;
	}

	// slog_debug("[IMSS][close_dataset] before get_recv_data_length");
	msg_length = get_recv_data_length(ucp_worker_meta, local_meta_uid);
	slog_debug("[IMSS][close_dataset] get_recv_data_length, msg_length=%lu", msg_length);
	if (msg_length == 0)
	{
		pthread_mutex_unlock(&lock_network);
		perror("HERCULES_ERR_MSG_LENGTH_CLOSE_DATASET");
		slog_error("HERCULES_ERR_MSG_LENGTH_CLOSE_DATASET");
		return -1;
	}

	// char result[msg_length];
	// slog_debug("[delete_dataset] before malloc, msg_length=%ld", msg_length);
	void *result = malloc(msg_length);
	// void *result = NULL;
	// slog_debug("[close_dataset] after malloc, msg_length=%lu", msg_length);
	// slog_debug("[delete_dataset] before recv_data");
	msg_length = recv_data(ucp_worker_meta, ep, result, msg_length, local_meta_uid, 0);
	// msg_length = recv_data_opt(ucp_worker_meta, ep, &result, msg_length, local_meta_uid, 0);
	slog_debug("[close_dataset] after recv_data, msg_length=%lu", msg_length);
	if (msg_length == 0)
	{
		pthread_mutex_unlock(&lock_network);
		perror("HERCULES_ERR_RECV_DATA_CLOSE_DATASET");
		slog_error("HERCULES_ERR_RECV_DATA_CLOSE_DATASET");
		free(result);
		return -1;
	}

	slog_debug("[close_dataset] result=%s, msg_length=%d", result, msg_length);
	// if the file descriptor was the last reference to a file which has been removed using
	// unlink, the file is deleted.
	if (!strncmp((const char *)result, "DELETE", strlen("DELETE")))
	{
		pthread_mutex_unlock(&lock_network);
		delete_dataset_srv_worker(dataset_uri, fd, 0);
		free(result);
		return 0;
	}
	free(result);
	pthread_mutex_unlock(&lock_network);
	return 1;
}

// Method deleting a dataset.
int32_t delete_dataset(const char *dataset_uri, int32_t dataset_id)
{
	pthread_mutex_lock(&lock_network);
	ucp_ep_h ep;
	// Formated dataset uri to be sent to the metadata server.
	char formated_uri[REQUEST_SIZE];

	// Discover the metadata server that handles the dataset.
	uint32_t m_srv = discover_stat_srv((char *)dataset_uri);

	ep = stat_eps[m_srv];

	// sprintf(formated_uri, "%" PRIu32 " GET 4 %s", stat_ids[m_srv], dataset_uri); // delete
	sprintf(formated_uri, "%d GET 4 %s", 4, dataset_uri); // delete

	slog_debug("[IMSS][delete_dataset] formated_uri='%s'", formated_uri);

	// Send the request.
	if (send_req(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta, formated_uri) == 0)
	{
		pthread_mutex_unlock(&lock_network);
		perror("ERRIMSS_RLSIMSS_SENDADDR");
		slog_error("ERRIMSS_RLSIMSS_SENDADDR");
		return -1;
	}

	// char result[RESPONSE_SIZE];
	// if (recv_data(ucp_worker_meta, ep, result, local_meta_uid, 0) < 0)
	// {
	// 	perror("ERRIMSS_RECVDYNAMSTRUCT_RECV");
	// 	return -1;
	// }
	// slog_debug("[IMSS][delete_dataset] response=%s", result);

	size_t msg_length = 0;
	msg_length = get_recv_data_length(ucp_worker_meta, local_meta_uid);
	if (msg_length < 0)
	{
		pthread_mutex_unlock(&lock_network);
		perror("ERRIMSS_DEL_DATASET_INVALID_MSG_LENGTH");
		slog_error("ERRIMSS_DEL_DATASET_INVALID_MSG_LENGTH");
		return -1;
	}

	// char result[msg_length];
	void *result = malloc(msg_length);
	msg_length = recv_data(ucp_worker_meta, ep, result, msg_length, local_meta_uid, 0);
	// msg_length = recv_data_opt(ucp_worker_meta, ep, result, msg_length, local_meta_uid, 0);
	if (msg_length == 0)
	{
		pthread_mutex_unlock(&lock_network);
		perror("ERR_HERCULES_DEL_DATASET_RECV_DATA");
		slog_error("ERR_HERCULES_DEL_DATASET_RECV_DATA");
		free(result);
		return -1;
	}

	slog_debug("[delete_dataset] result=%s, msg_length=%d", (const char *)result, msg_length);
	if (!strncmp((const char *)result, "NODELETE", strlen("NODELETE")))
	{
		// free the message received from the metadata server.
		free(result);
		pthread_mutex_unlock(&lock_network);
		return 1;
	}
	// free the message received from the metadata server.
	free(result);
	pthread_mutex_unlock(&lock_network);

	// deletes the dataset from the data server.
	delete_dataset_srv_worker(dataset_uri, dataset_id, 0);

	return 0;
}

int32_t rename_dataset_metadata_dir_dir(char *old_dir, char *rdir_dest)
{
	ucp_ep_h ep;
	/*********RENAME GARRAY DATASET*******/
	dataset_info dataset_info_;

	for (int32_t i = 0; i < datasetd->len; i++)
	{
		dataset_info_ = g_array_index(datasetd, dataset_info, i);

		if (strstr(dataset_info_.uri_, old_dir) != NULL)
		{
			char *path = dataset_info_.uri_;

			size_t len = strlen(old_dir);
			if (len > 0)
			{
				char *p = path;
				while ((p = strstr(p, old_dir)) != NULL)
				{
					memmove(p, p + len, strlen(p + len) + 1);
				}
			}
			// char * new_path = (char *) malloc(strlen(rdir_dest) + 1);
			char *new_path = (char *)malloc(256);
			strcpy(new_path, rdir_dest);
			strcat(new_path, "/");
			strcat(new_path, path);

			strcpy(dataset_info_.uri_, new_path);
			g_array_remove_index(datasetd, i);
			g_array_insert_val(datasetd, i, dataset_info_);
		}
	}
	pthread_mutex_lock(&lock_network);
	/*********RENAME METADATA*******/
	// Formated dataset uri to be sent to the metadata server.
	char formated_uri[REQUEST_SIZE];

	// Discover the metadata server that handles the dataset.
	uint32_t m_srv = discover_stat_srv((char *)old_dir);

	ep = stat_eps[m_srv];

	// Send the request.
	sprintf(formated_uri, "%" PRIu32 " GET 6 %s %s", stat_ids[m_srv], old_dir, rdir_dest);
	if (send_req(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta, formated_uri) == 0)
	{
		pthread_mutex_unlock(&lock_network);
		perror("ERRIMSS_RLSIMSS_SENDADDR");
		return -1;
	}

	// char result[RESPONSE_SIZE];
	// if (recv_data(ucp_worker_meta, ep, result, local_meta_uid, 0) < 0)
	// {
	// 	perror("ERRIMSS_RECVDYNAMSTRUCT_RECV");
	// 	return -1;
	// }

	size_t msg_length = 0;
	msg_length = get_recv_data_length(ucp_worker_meta, local_meta_uid);
	if (msg_length == 0)
	{
		pthread_mutex_unlock(&lock_network);
		slog_error("ERRIMSS_REN_DATASET_METADATA_DIR_DIR_INVALID_MSG_LENGTH");
		perror("ERRIMSS_REN_DATASET_METADATA_DIR_DIR_INVALID_MSG_LENGTH");
		return -1;
	}

	// char result[msg_length];
	// char *result = (char *)malloc(msg_length * sizeof(char));
	void *result = malloc(msg_length);
	msg_length = recv_data(ucp_worker_meta, ep, result, msg_length, local_meta_uid, 0);
	// msg_length = recv_data_opt(ucp_worker_meta, ep, result, msg_length, local_meta_uid, 0);
	if (msg_length == 0)
	{
		pthread_mutex_unlock(&lock_network);
		perror("ERRIMSS_REN_DATASET_METADATA_DIR_DIR_RECV_DATA");
		slog_error("ERRIMSS_REN_DATASET_METADATA_DIR_DIR_RECV_DATA");
		free(result);
		return -1;
	}
	free(result);
	pthread_mutex_unlock(&lock_network);
	return 0;
}

int32_t
rename_dataset_metadata(char *old_dataset_uri, char *new_dataset_uri)
{
	ucp_ep_h ep;
	/*********RENAME GARRAY DATASET*******/
	dataset_info dataset_info_;

	for (int32_t i = 0; i < datasetd->len; i++)
	{
		dataset_info_ = g_array_index(datasetd, dataset_info, i);
		if (!strcmp(old_dataset_uri, dataset_info_.uri_))
		{
			strcpy(dataset_info_.uri_, new_dataset_uri);
			g_array_remove_index(datasetd, i);
			g_array_insert_val(datasetd, i, dataset_info_);
		}
	}

	/*********RENAME METADATA*******/
	pthread_mutex_lock(&lock_network);
	// Formated dataset uri to be sent to the metadata server.
	char formated_uri[REQUEST_SIZE];

	// Discover the metadata server that handles the dataset.
	uint32_t m_srv = discover_stat_srv((char *)old_dataset_uri);

	ep = stat_eps[m_srv];

	// Send the request.
	sprintf(formated_uri, "%" PRIu32 " GET 5 %s %s", stat_ids[m_srv], old_dataset_uri, new_dataset_uri);
	slog_live("[rename_dataset_metadata] Request - %s", formated_uri);
	// fprintf(stderr, "Request - %s\n", formated_uri);
	if (send_req(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta, formated_uri) == 0)
	{
		pthread_mutex_unlock(&lock_network);
		perror("ERR_HERCULES_RENAME_DATASET_METADATA_SEND_REQ");
		slog_error("ERR_HERCULES_RENAME_DATASET_METADATA_SEND_REQ");
		return -1;
	}

	// char result[RESPONSE_SIZE];
	// if (recv_data(ucp_worker_meta, ep, result, local_meta_uid, 0) < 0)
	// {
	// 	perror("ERRIMSS_RECVDYNAMSTRUCT_RECV");
	// 	return -1;
	// }

	size_t msg_length = 0;
	msg_length = get_recv_data_length(ucp_worker_meta, local_meta_uid);
	slog_live("[rename_dataset_metadata]  get_recv_data_length, msg_length=%lu", msg_length);
	if (msg_length == 0)
	{
		pthread_mutex_unlock(&lock_network);
		perror("ERRIMSS_REN_DATASET_INVALID_MSG_LENGTH");
		slog_error("ERRIMSS_REN_DATASET_INVALID_MSG_LENGTH");
		return -1;
	}

	// char result[msg_length];
	// char *result = (char *)malloc(msg_length * sizeof(char));
	void *result = malloc(msg_length);
	// msg_length = recv_data_opt(ucp_worker_meta, ep, &result, msg_length, local_meta_uid, 0);
	msg_length = recv_data(ucp_worker_meta, ep, result, msg_length, local_meta_uid, 0);
	if (msg_length == 0)
	{
		pthread_mutex_unlock(&lock_network);
		perror("ERR_HERCULES_RENAME_DATASET_METADATA_RECV_DATA");
		slog_error("ERR_HERCULES_RENAME_DATASET_METADATA_RECV_DATA");
		free(result);
		return -1;
	}

	free(result);
	pthread_mutex_unlock(&lock_network);
	return 0;
}

// Send a query to the metadata server to tell it a client is opening a local dataset.
int32_t open_local_dataset(const char *dataset_uri, int opened)
{
	pthread_mutex_lock(&lock_network);
	int ret = 0;
	ucp_ep_h ep;

	slog_live("[IMSS][open_local_dataset] opened=%d", opened);
	// Formated dataset uri to be sent to the metadata server.
	char formated_uri[REQUEST_SIZE];

	// Discover the metadata server that handles the dataset.
	uint32_t m_srv = discover_stat_srv((char *)dataset_uri);

	ep = stat_eps[m_srv];

	// sprintf(formated_uri, "%" PRIu32 " GET 0 %s", stat_ids[m_srv], dataset_uri);
	sprintf(formated_uri, "%" PRIu32 " GET 8 %s", opened, dataset_uri);
	slog_live("[IMSS][open_local_dataset] Metadata Request - %s", formated_uri);
	// Send the request.
	size_t msg_len = 0;
	msg_len = send_req(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta, formated_uri);
	if (msg_len == 0)
	{
		pthread_mutex_unlock(&lock_network);
		perror("HERCULES_ERR_STAT_DATASET_SEND_REQ");
		slog_error("HERCULES_ERR_STAT_DATASET_SEND_REQ");
		return -1;
	}

	size_t msg_length = 0;
	// slog_live("[open_local_dataset] before get_recv_data_length");
	msg_length = get_recv_data_length(ucp_worker_meta, local_meta_uid);
	// slog_live("[open_local_dataset] after get_recv_data_length, msg_length=%ld", msg_length);
	if (msg_length == 0)
	{
		pthread_mutex_unlock(&lock_network);
		perror("HERCULES_ERR__OPEN_LOCAL_DATASET_INVALID_MSG_LENGTH");
		slog_error("HERCULES_ERR__OPEN_LOCAL_DATASET_INVALID_MSG_LENGTH");
		return -1;
	}

	// char result[msg_length];
	// slog_live("[open_local_dataset] before malloc, errno=%d:%s", errno, strerror(errno));
	// char *result = (char *)malloc(msg_length * sizeof(char));
	void *result = malloc(msg_length);
	// slog_live("[open_local_dataset] after malloc, errno=%d:%s", errno, strerror(errno));
	msg_length = recv_data(ucp_worker_meta, ep, result, msg_length, local_meta_uid, 0);
	// msg_length = recv_data_opt(ucp_worker_meta, ep, &result, msg_length, local_meta_uid, 0);
	if (msg_length == 0)
	{
		pthread_mutex_unlock(&lock_network);
		slog_error("HERCULES_ERR__OPEN_LOCAL_DATASET_RECV_DATA");
		perror("HERCULES_ERR__OPEN_LOCAL_DATASET_RECV_DATA");
		free(result);
		return -1;
	}

	slog_debug("[open_local_dataset] result=%s, msg_length=%lu", result, msg_length);
	if (!strncmp((const char *)result, "OPEN", strlen("OPEN")))
	{
		pthread_mutex_unlock(&lock_network);
		slog_debug("[open_local_dataset] dataset opened = %s", dataset_uri);
		free(result);
		return 1;
	}

	free(result);
	pthread_mutex_unlock(&lock_network);
	return 0;
}

// Method retrieving information related to a certain dataset.
int32_t stat_dataset(const char *dataset_uri, dataset_info *dataset_info_, int opened)
{
	int ret = 0;
	ucp_ep_h ep;

	slog_live("[IMSS][stat_dataset] opened=%d, datasetd->len=%d", opened, datasetd->len);

	// Search for the requested dataset in the local vector.
	for (int32_t i = 0; i < datasetd->len; i++)
	{
		*dataset_info_ = g_array_index(datasetd, dataset_info, i);
		if (!strcmp(dataset_uri, dataset_info_->uri_))
		{
			slog_live("[IMSS][stat_dataset] dataset_uri=%s, dataset_info_->uri_=%s", dataset_uri, dataset_info_->uri_);
			return 2;
		}
	}

	pthread_mutex_lock(&lock_network);
	// Formated dataset uri to be sent to the metadata server.
	char formated_uri[REQUEST_SIZE];

	// Discover the metadata server that handles the dataset.
	uint32_t m_srv = discover_stat_srv((char *)dataset_uri);

	ep = stat_eps[m_srv];
	// sprintf(formated_uri, "%" PRIu32 " GET 0 %s", stat_ids[m_srv], dataset_uri);
	sprintf(formated_uri, "%" PRIu32 " GET 0 %s", opened, dataset_uri);
	slog_live("Request - %s", formated_uri);
	// Send the request.
	size_t msg_len = 0;
	msg_len = send_req(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta, formated_uri);
	// slog_live("Request has been sent - %s", formated_uri);
	if (msg_len == 0)
	{
		pthread_mutex_unlock(&lock_network);
		perror("HERCULES_ERR_STAT_DATASET_SEND_REQ");
		slog_error("HERCULES_ERR_STAT_DATASET_SEND_REQ");
		return -1;
	}

	// Get the length of the message to be received.
	msg_len = get_recv_data_length(ucp_worker_meta, local_meta_uid);
	if (msg_len == 0)
	{
		pthread_mutex_unlock(&lock_network);
		perror("HERCULES_ERR_STAT_DATASET_GET_RECV_DATA_LENGTH");
		slog_error("HERCULES_ERR_STAT_DATASET_GET_RECV_DATA_LENGTH");
		return -1;
	}

	// Receive the associated structure.
	// void *data = NULL;
	void *data = malloc(msg_len);
	// ret = recv_dynamic_stream(ucp_worker_meta, ep, (void **)&dataset_info_, DATASET_INFO, local_meta_uid, msg_len);
	ret = recv_dynamic_stream(ucp_worker_meta, ep, data, DATASET_INFO, local_meta_uid, msg_len);
	// ret = recv_dynamic_stream_opt(ucp_worker_meta, ep, &data, DATASET_INFO, local_meta_uid, msg_len);
	//  if (ret < sizeof(dataset_info))
	if (ret < 0)
	{
		pthread_mutex_unlock(&lock_network);
		slog_warn("[IMSS][stat_dataset] dataset does not exist, req=%s.", formated_uri);
		free(data);
		return 0;
	}
	pthread_mutex_unlock(&lock_network);
	memcpy(dataset_info_, data, sizeof(dataset_info));
	free(data);
	return 1;
}

/*
//Method retrieving a whole dataset parallelizing the procedure.
unsigned char * get_dataset(char * dataset_uri, uint64_t * buff_length);
{}

//Method storing a whole dataset parallelizing the procedure.
int32_t set_dataset(char * dataset_uri, unsigned char * buffer, uint64_t offset)
{}
 */

/**********************************************************************************/
/************************ DATA OBJECT MANAGEMENT FUNCTIONS ************************/
/**********************************************************************************/

// Method retrieving the location of a specific data object.
int32_t get_data_location(int32_t dataset_id, int32_t data_id, int32_t op_type)
{
	// If the current dataset policy was not established yet.
	slog_debug("[IMSS] dataset_id=%d, current_dataset=%d", dataset_id, current_dataset);
	// fprintf(stderr,"[get_data_location] current_dataset=%d, dataset_id=%d\n", current_dataset, dataset_id);

	// if (current_dataset != dataset_id) // TO FIX: if a thread does not met the condition, their curr_datset could have the value of the previous thread. curr_dataset is global.
	{
		// Retrieve the corresponding dataset_info structure and the associated IMSS.
		curr_dataset = g_array_index(datasetd, dataset_info, dataset_id);
		// curr_imss = g_array_index(imssd, imss, curr_dataset.imss_d);
		curr_imss = g_array_index(imssd, imss, curr_dataset.imss_d);
		slog_debug("[IMSS][get_data_location] curr_dataset.uri=%s, curr_dataset.imss_d=%d, curr_dataset.repl_factor=%d, dataset_id=%d, curr_dataset.policy=%s, curr_dataset.n_servers=%d", curr_dataset.uri_, curr_dataset.imss_d, curr_dataset.repl_factor, dataset_id, curr_dataset.policy, curr_dataset.n_servers);

		// Set the corresponding.
		if (set_policy(&curr_dataset) == -1)
		{
			return -1;
		}

		current_dataset = dataset_id;
	}

	int32_t server = -1;
	slog_live("[IMSS] curr_imss.uri=%s, curr_imss.info.num_storages=%d, curr_dataset.n_servers=%d", curr_dataset.uri_, curr_imss.info.num_storages, curr_dataset.n_servers);
	// Search for the server that is supposed to have the specified data element.
	// slog_debug("[get_data_location] curr_dataset.uri_=%s", curr_dataset.uri_);
	// if ((server = find_server(curr_dataset.n_servers, data_id, curr_dataset.uri_, op_type)) < 0)
	if ((server = find_server(curr_imss.info.num_storages, data_id, curr_dataset.uri_, op_type)) < 0)
	{
		perror("HERCULES_ERR_FIND_SERVER");
		slog_fatal("HERCULES_ERR_FIND_SERVER");
		return -1;
	}
	slog_debug("[IMSS][get_data_location] next_server=%d", server);

	return server;
}

// Method renaming a dir_dir
int32_t rename_dataset_srv_worker_dir_dir(char *old_dir, char *rdir_dest,
										  int32_t dataset_id, int32_t data_id)
{
	int32_t n_server;
	// Server containing the corresponding data to be retrieved.
	if ((n_server = get_data_location(dataset_id, data_id, GET)) == -1)

		return -1;

	// Servers that the data block is going to be requested to.
	int32_t repl_servers[curr_dataset.repl_factor];

	int32_t curr_imss_storages = curr_imss.info.num_storages;

	// Retrieve the corresponding connections to the previous servers.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		// Server storing the current data block.
		uint32_t n_server_ = (n_server + i * (curr_imss_storages / curr_dataset.repl_factor)) % curr_imss_storages;
		repl_servers[i] = n_server_;

		// Check if the current connection is the local one (if there is).
		if (repl_servers[i] == curr_dataset.local_conn)
		{
			// Move the local connection to the first one to be requested.
			int32_t aux_conn = repl_servers[0];
			repl_servers[0] = repl_servers[i];
			repl_servers[i] = aux_conn;
		}
	}
	pthread_mutex_lock(&lock_network);
	char key_[REQUEST_SIZE];
	// Key related to the requested data element.

	// Request the concerned block to the involved servers.
	// for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	for (int32_t i = 0; i < curr_imss.info.num_storages; i++)
	{
		ucp_ep_h ep = curr_imss.conns.eps[i];

		sprintf(key_, "GET 6 0 %s %s", old_dir, rdir_dest);
		// if (comm_send(curr_imss.conns.eps_[repl_servers[i]], key, key_length, 0) != key_length)
		if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERRIMSS_RLSIMSS_SENDADDR");
			return -1;
		}

		// char result[RESPONSE_SIZE];
		// if (recv_data(ucp_worker_data, ep, result, local_data_uid, 0) < 0)
		// {
		// 	perror("ERRIMSS_RECVDYNAMSTRUCT_RECV");
		// 	return -1;
		// }

		size_t msg_length = 0;
		msg_length = get_recv_data_length(ucp_worker_data, local_data_uid);
		if (msg_length == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERRIMSS_REN_DATASET_DATA_DIR_DIR_INVALID_MSG_LENGTH");
			slog_error("ERRIMSS_REN_DATASET_DATA_DIR_DIR_INVALID_MSG_LENGTH");
			return -1;
		}

		// // char result[msg_length];
		// char *result = (char *)malloc(msg_length * sizeof(char));
		void *result = malloc(msg_length);
		msg_length = recv_data(ucp_worker_data, ep, result, msg_length, local_data_uid, 0);
		// msg_length = recv_data_opt(ucp_worker_data, ep, &result, msg_length, local_data_uid, 0);
		if (msg_length == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERRIMSS_REN_DATASET_DATA_DIR_DIR_RECV_DATA");
			slog_error("ERRIMSS_REN_DATASET_DATA_DIR_DIR_RECV_DATA");
			free(result);
			return -1;
		}

		free(result);
		// Important to update
		// strcpy(curr_dataset.uri_,new_dataset_uri);
	}

	pthread_mutex_unlock(&lock_network);
	return 0;
}

// Method renaming a dataset.
int32_t rename_dataset_srv_worker(char *old_dataset_uri, char *new_dataset_uri,
								  int32_t dataset_id, int32_t data_id)
{
	int32_t n_server;
	// Server containing the corresponding data to be retrieved.
	if ((n_server = get_data_location(dataset_id, data_id, GET)) == -1)
		return -1;

	// Servers that the data block is going to be requested to.
	int32_t repl_servers[curr_dataset.repl_factor];
	int32_t curr_imss_storages = curr_imss.info.num_storages;

	// Retrieve the corresponding connections to the previous servers.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		// Server storing the current data block.
		uint32_t n_server_ = (n_server + i * (curr_imss_storages / curr_dataset.repl_factor)) % curr_imss_storages;
		repl_servers[i] = n_server_;

		// Check if the current connection is the local one (if there is).
		if (repl_servers[i] == curr_dataset.local_conn)
		{
			// Move the local connection to the first one to be requested.
			int32_t aux_conn = repl_servers[0];
			repl_servers[0] = repl_servers[i];
			repl_servers[i] = aux_conn;
		}
	}

	pthread_mutex_lock(&lock_network);
	char key_[REQUEST_SIZE];

	// Request the concerned block to the involved servers.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		ucp_ep_h ep = curr_imss.conns.eps[repl_servers[i]];

		// Key related to the requested data element.
		sprintf(key_, "GET 5 0 %s,%s", old_dataset_uri, new_dataset_uri);
		slog_debug("Request - %s", key_);
		// fprintf(stderr, "Request - %s\n", key_);
		// printf("BLOCK %d ASKED TO %d SERVER with key: %s (%d)", data_id, repl_servers[i], key, key_length);
		if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERR_HERCULES_RENAME_DATASET_SRV_WORKER_SEND_REQ");
			slog_error("ERR_HERCULES_RENAME_DATASET_SRV_WORKER_SEND_REQ");
			return -1;
		}

		// char result[RESPONSE_SIZE];
		// if (recv_data(ucp_worker_data, ep, result, local_data_uid, 0) < 0)
		// {
		// 	perror("ERRIMSS_RECVDYNAMSTRUCT_RECV");
		// 	return -1;
		// }

		size_t msg_length = 0;
		msg_length = get_recv_data_length(ucp_worker_data, local_data_uid);
		slog_live("[rename_dataset_srv_worker] get_recv_data_length, msg_length=%lu", msg_length);
		if (msg_length == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERRIMSS_REN_DATASET_DATA_INVALID_MSG_LENGTH");
			slog_error("ERRIMSS_REN_DATASET_DATA_INVALID_MSG_LENGTH");
			return -1;
		}

		// char result[msg_length];
		// slog_live("[rename_dataset_srv_worker] before malloc");
		// char *result = (char *)malloc(msg_length * sizeof(char));
		void *result = malloc(msg_length);
		// slog_live("[rename_dataset_srv_worker] after malloc");
		msg_length = recv_data(ucp_worker_data, ep, result, msg_length, local_data_uid, 0);
		// msg_length = recv_data_opt(ucp_worker_data, ep, &result, msg_length, local_data_uid, 0);
		slog_live("[rename_dataset_srv_worker] after recv_data, msg_length=%lu", msg_length);
		free(result);
		if (msg_length == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERR_HERCULES_RENAME_DATASET_SRV_WORKER_RECV_DATA");
			slog_error("ERR_HERCULES_RENAME_DATASET_SRV_WORKER_RECV_DATA");
			return -1;
		}

		// Important to update
		strcpy(curr_dataset.uri_, new_dataset_uri);
	}

	pthread_mutex_unlock(&lock_network);
	return 0;
}

// Method to delete the dataset data.
int32_t delete_dataset_srv_worker(const char *dataset_uri, int32_t dataset_id, int32_t data_id)
{
	int32_t n_server;
	// Server containing the corresponding data to be retrieved.
	if ((n_server = get_data_location(dataset_id, data_id, GET)) == -1)
		return -1;

	// Servers that the data block is going to be requested to.
	int32_t repl_servers[curr_dataset.repl_factor];
	int32_t curr_imss_storages = curr_imss.info.num_storages;

	// Retrieve the corresponding connections to the previous servers.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		// Server storing the current data block.
		uint32_t n_server_ = (n_server + i * (curr_imss_storages / curr_dataset.repl_factor)) % curr_imss_storages;
		repl_servers[i] = n_server_;

		// Check if the current connection is the local one (if there is).
		if (repl_servers[i] == curr_dataset.local_conn)
		{
			// Move the local connection to the first one to be requested.
			int32_t aux_conn = repl_servers[0];
			repl_servers[0] = repl_servers[i];
			repl_servers[i] = aux_conn;
		}
	}

	pthread_mutex_lock(&lock_network);
	char key_[REQUEST_SIZE];

	// Request the concerned block to the involved servers.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		ucp_ep_h ep = curr_imss.conns.eps[repl_servers[i]];

		// Key related to the requested data element.
		sprintf(key_, "GET 4 0 %s", dataset_uri);
		// fprintf(stderr, "Request - %s\n", key_);
		// printf("BLOCK %d ASKED TO %d SERVER with key: %s (%d)", data_id, repl_servers[i], key, key_length);
		slog_debug("Data Request - %s\n", key_);
		if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("HERCULES_ERR_DELETE_DATASET_SENDADDR");
			return -1;
		}

		size_t msg_length = 0;
		msg_length = get_recv_data_length(ucp_worker_data, local_data_uid);
		if (msg_length == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("HERCULES_ERR_DELETE_DATASET_INVALID_MSG_LENGTH");
			slog_error("HERCULES_ERR_DELETE_DATASET_INVALID_MSG_LENGTH");
			return -1;
		}

		// // char result[msg_length];
		// char *result = (char *)malloc(msg_length * sizeof(char));
		void *result = malloc(msg_length);
		msg_length = recv_data(ucp_worker_data, ep, result, msg_length, local_data_uid, 0);
		// msg_length = recv_data_opt(ucp_worker_data, ep, &result, msg_length, local_data_uid, 0);
		free(result);
		if (msg_length == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("HERCULES_ERR_DELETE_DATASET_RECV_DATA");
			slog_error("HERCULES_ERR_DELETE_DATASET_RECV_DATA");
			return -1;
		}
	}

	pthread_mutex_unlock(&lock_network);
	return 0;
}

// Method storing a specific data element.
int32_t writev_multiple(const char *buf, int32_t dataset_id, int64_t data_id,
						int64_t end_blk, int64_t start_offset, int64_t end_offset, int64_t IMSS_DATA_BSIZE, int64_t size)
{

	int32_t n_server;
	// Server containing the corresponding data to be written.
	if ((n_server = get_data_location(dataset_id, data_id, SET)) == -1)
	{
		perror("ERRIMSS_GET_DATA_LOCATION");
		return -1;
	}

	pthread_mutex_lock(&lock_network);
	char key_[REQUEST_SIZE];
	// Key related to the requested data element.

	int32_t curr_imss_storages = curr_imss.info.num_storages;

	// Send the data block to every server implementing redundancy.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{

		// Server receiving the current data block.
		uint32_t n_server_ = (n_server + i * (curr_imss_storages / curr_dataset.repl_factor)) % curr_imss_storages;

		ucp_ep_h ep = curr_imss.conns.eps[n_server_];

		// printf("BLOCK %ld SENT TO %d SERVER with key: %s (%d)", data_id, n_server_, key, key_length);
		sprintf(key_, " SET %d 0 %s$%ld %ld %ld %ld %ld %ld %ld", curr_dataset.data_entity_size, curr_dataset.uri_, data_id, data_id, end_blk, start_offset, end_offset, IMSS_DATA_BSIZE, size);

		if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERR_HERCULES_WRITEV_MULTIPLE_SEND_REQ");
			slog_error("ERR_HERCULES_WRITEV_MULTIPLE_SEND_REQ");
			return -1;
		}

		if (send_data(ucp_worker_data, ep, buf, size, local_data_uid) == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERR_HERCULES_WRITEV_MULTIPLE_SEND_DATA");
			slog_error("ERR_HERCULES_WRITEV_MULTIPLE_SEND_DATA");
			return -1;
		}
	}

	pthread_mutex_unlock(&lock_network);
	return 0;
}

// Method retrieving multiple data
int32_t readv_multiple(int32_t dataset_id,
					   int32_t curr_block,
					   int32_t end_block,
					   char *buffer,
					   uint64_t BLOCKSIZE,
					   int64_t start_offset,
					   int64_t size)
{
	// printf("readv size=%d",size);
	int32_t n_server;
	// Server containing the corresponding data to be retrieved.
	if ((n_server = get_data_location(dataset_id, curr_block, GET)) == -1)

		return -1;

	// Servers that the data block is going to be requested to.
	int32_t repl_servers[curr_dataset.repl_factor];
	int32_t curr_imss_storages = curr_imss.info.num_storages;

	// Retrieve the corresponding connections to the previous servers.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		// Server storing the current data block.
		uint32_t n_server_ = (n_server + i * (curr_imss_storages / curr_dataset.repl_factor)) % curr_imss_storages;
		repl_servers[i] = n_server_;

		// Check if the current connection is the local one (if there is).
		if (repl_servers[i] == curr_dataset.local_conn)
		{
			// Move the local connection to the first one to be requested.
			int32_t aux_conn = repl_servers[0];
			repl_servers[0] = repl_servers[i];
			repl_servers[i] = aux_conn;
		}
	}
	pthread_mutex_lock(&lock_network);
	char key_[REQUEST_SIZE];
	// Key related to the requested data element.

	// Request the concerned block to the involved servers.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		ucp_ep_h ep;

		ep = curr_imss.conns.eps[repl_servers[i]];

		// printf("BLOCK %d ASKED TO %d SERVER with key: %s (%d)", curr_block, repl_servers[i], key, key_length);
		// Send read request message specifying the block URI.
		sprintf(key_, "GET 8 0 %s$%d %d %ld %ld %ld", curr_dataset.uri_, curr_block, end_block, BLOCKSIZE, start_offset, size);

		if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERRIMSS_RLSIMSS_SENDADDR");
			slog_error("ERRIMSS_RLSIMSS_SENDADDR");
			return -1;
		}

		size_t msg_length = 0;
		msg_length = get_recv_data_length(ucp_worker_data, local_data_uid);
		if (msg_length == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERRIMSS_REN_DATASET_DATA_DIR_DIR_INVALID_MSG_LENGTH");
			slog_error("ERRIMSS_REN_DATASET_DATA_DIR_DIR_INVALID_MSG_LENGTH");
			return -1;
		}

		// Receive data related to the previous read request directly into the buffer.
		// To check!
		msg_length = recv_data(ucp_worker_data, ep, (void **)&buffer, msg_length, local_data_uid, 0);
		if (msg_length == 0)
		{
			if (errno != EAGAIN)
			{
				pthread_mutex_unlock(&lock_network);
				perror("ERRIMSS_GETDATA_RECV_DATA");
				return -1;
			}
			else
				break;
		}

		// Check if the requested key was correctly retrieved.
		if (strncmp((const char *)buffer, "$ERRIMSS_NO_KEY_AVAIL$", 22))
		{
			free(buffer);
			pthread_mutex_unlock(&lock_network);
			return 0;
		}
		free(buffer);
	}

	// slog_fatal( "ERRIMSS_GETDATA_UNAVAIL");
	pthread_mutex_unlock(&lock_network);
	return -1;
}

void *split_writev(void *th_argv)
{
	// Cast from generic pointer type to p_argv struct type pointer.
	thread_argv *arguments = (thread_argv *)th_argv;

	int32_t n_server;
	pthread_mutex_lock(&lock_network);
	char key_[REQUEST_SIZE];
	int32_t curr_imss_storages = curr_imss.info.num_storages;

	// Send the data block to every server implementing redundancy.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		ucp_ep_h ep;
		// Server receiving the current data block.
		uint32_t n_server_ = (arguments->n_server + i * (curr_imss_storages / curr_dataset.repl_factor)) % curr_imss_storages;

		ep = curr_imss.conns.eps[n_server_];

		// Key related to the requested data element.
		sprintf(key_, "SET %d 0 [OP]=2 %s %ld %ld %d %s", curr_dataset.data_entity_size,
				arguments->path, arguments->BLKSIZE, arguments->start_offset,
				arguments->stats_size, arguments->msg);

		if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERR_HERCULES_SPLIT_WRITEV_SEND_REQ");
			slog_error("ERR_HERCULES_SPLIT_WRITEV_SEND_REQ");
			pthread_exit(NULL);
		}

		if (send_data(ucp_worker_data, ep, arguments->buffer, arguments->size * arguments->BLKSIZE * KB, local_data_uid) == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERR_HERCULES_SPLIT_WRITEV_SEND_DATA");
			slog_error("ERR_HERCULES_SPLIT_WRITEV_SEND_DATA");
			pthread_exit(NULL);
		}
	}

	pthread_mutex_unlock(&lock_network);
	pthread_exit(NULL);
}

void *split_readv(void *th_argv)
{
	// Cast from generic pointer type to p_argv struct type pointer.
	thread_argv *arguments = (thread_argv *)th_argv;

	// Servers that the data block is going to be requested to.
	int32_t repl_servers[curr_dataset.repl_factor];

	int32_t curr_imss_storages = curr_imss.info.num_storages;

	// Retrieve the corresponding connections to the previous servers.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		// Server storing the current data block.
		uint32_t n_server_ = (arguments->n_server + i * (curr_imss_storages / curr_dataset.repl_factor)) % curr_imss_storages;

		repl_servers[i] = n_server_;

		// Check if the current connection is the local one (if there is).
		if (repl_servers[i] == curr_dataset.local_conn)
		{
			// Move the local connection to the first one to be requested.
			int32_t aux_conn = repl_servers[0];
			repl_servers[0] = repl_servers[i];
			repl_servers[i] = aux_conn;
		}
	}
	//	printf("[CLIENT] [Split_readv]");
	pthread_mutex_lock(&lock_network);
	char key_[REQUEST_SIZE];
	// Key related to the requested data element.
	int msg_length = strlen(arguments->msg) + 1;
	// printf("key=%s",key);
	// Request the concerned block to the involved servers.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		ucp_ep_h ep;
		// Send read request message specifying the block URI.
		// if (comm_send(curr_imss.conns.eps_[repl_servers[i]], key, KEY, 0) < 0)
		// printf("[SPLIT READV] 1-send_data");
		sprintf(key_, "GET 9 0 %s %ld %ld %d %d",
				arguments->path, arguments->BLKSIZE, arguments->start_offset,
				arguments->stats_size, msg_length);

		ep = curr_imss.conns.eps[repl_servers[i]];

		if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERR_HERCULES_SPLIT_READV_SEND_REQ");
			slog_error("ERR_HERCULES_SPLIT_READV_SEND_REQ");
			pthread_exit(NULL);
		}

		// printf("[SPLIT READV] 4-Send_msg");
		if (send_data(ucp_worker_data, ep, arguments->msg, msg_length, local_data_uid) == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERR_HERCULES_SPLIT_READV_SEND_DATA");
			slog_error("ERR_HERCULES_SPLIT_READV_SEND_DATA");
			pthread_exit(NULL);
		}

		slog_debug("[IMSS] Request split_readv: client_id '%" PRIu32 "', mode '%s', key '%s', request '%s'", curr_imss.conns.id[repl_servers[i]], "GET", key_, arguments->msg);

		struct timeval start, end;
		/*long delta_us;
		  gettimeofday(&start, NULL);*/
		// Receive data related to the previous read request directly into the buffer.

		size_t msg_length = 0;
		msg_length = get_recv_data_length(ucp_worker_data, local_data_uid);
		if (msg_length == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERRIMSS_SPLIT_READ_INVALID_MSG_LENGTH");
			slog_error("ERRIMSS_SPLIT_READ_INVALID_MSG_LENGTH");
			pthread_exit(NULL);
		}
		// to check!
		msg_length = recv_data(ucp_worker_data, ep, (void **)&arguments->buffer, msg_length, local_data_uid, 0);
		if (msg_length == 0)
		{
			if (errno != EAGAIN)
			{
				pthread_mutex_unlock(&lock_network);
				perror("ERRIMSS_SPLIT_READ_RECV");
				pthread_exit(NULL);
			}
			else
				break;
		}

		gettimeofday(&end, NULL);
		/*delta_us = (long) (end.tv_usec - start.tv_usec);
		  printf("[CLIENT] [S_SPLIT_READ] recv data delta_us=%6.3f",(delta_us/1000.0F));*/
		// Check if the requested key was correctly retrieved.
		if (strncmp((const char *)arguments->buffer, "$ERRIMSS_NO_KEY_AVAIL$", 22))
		{
			free(arguments->buffer);
		}
	}
	pthread_mutex_unlock(&lock_network);
	pthread_exit(NULL);
}
// Method retrieving multiple data from a specific server
/*int32_t
  split_readv(int32_t n_server,
  char * path,
  char * msg,
  unsigned char * buffer,
  int32_t size,
  uint64_t BLKSIZE,
  int64_t    start_offset,
  int    stats_size)
  {
  printf("n_server=%d msg=%s size=%d", n_server, msg, size);

//Servers that the data block is going to be requested to.
int32_t repl_servers[curr_dataset.repl_factor];

int32_t curr_imss_storages = curr_imss.info.num_storages;

//Retrieve the corresponding connections to the previous servers.
for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
{
//Server storing the current data block.
uint32_t n_server_ = (n_server + i*(curr_imss_storages/curr_dataset.repl_factor)) % curr_imss_storages;

repl_servers[i] = n_server_;

//Check if the current connection is the local one (if there is).
if (repl_servers[i] == curr_dataset.local_conn)
{
//Move the local connection to the first one to be requested.

int32_t aux_conn = repl_servers[0];

repl_servers[0] = repl_servers[i];

repl_servers[i] = aux_conn;
}
}

char key_[KEY];
//Key related to the requested data element.
sprintf(key_, "9 %s %ld %ld %d %s",path, BLKSIZE, start_offset, stats_size, msg);
int key_length = strlen(key_)+1;
char key[key_length];
memcpy((void *) key, (void *) key_, key_length);
key[key_length-1] = '\0';

//Request the concerned block to the involved servers.
for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
{
//printf("BLOCK %d ASKED TO %d SERVER with key: %s (%d)", curr_block, repl_servers[i], key, key_length);

//Send read request message specifying the block URI.
//if (comm_send(curr_imss.conns.eps_[repl_servers[i]], key, KEY, 0) < 0)

if (comm_send(curr_imss.conns.eps_[repl_servers[i]], key, key_length, 0) != key_length)
{
perror("ERRIMSS_GETDATA_REQ");
return -1;
}
//Receive data related to the previous read request directly into the buffer.
if (comm_recv(curr_imss.conns.eps_[repl_servers[i]], buffer, size*BLKSIZE*KB, 0) == -1)
{
if (errno != EAGAIN)
{
perror("ERRIMSS_GETDATA_RECV");
return -1;
}
else
break;
}
//Check if the requested key was correctly retrieved.
if (strncmp((const char *) buffer, "$ERRIMSS_NO_KEY_AVAIL$", 22)){
	return 0;
}

}

//slog_fatal( "ERRIMSS_GETDATA_UNAVAIL");
return -1;
}
*/

int32_t flush_data()
{
	worker_flush(ucp_worker_data);
	return 1;
}

int32_t imss_flush_data()
{
	// Search for the requested IMSS.
	// imss imss_;
	// int32_t imss_position;
	// if ((imss_position = find_imss(imss_uri, &imss_)) == -1)
	// {
	// 	slog_fatal("ERRIMSS_IMSS_TO_FLUSH_NOTFOUND");
	// 	return -1;
	// }

	// Release the set of connections to the corresponding IMSS.
	for (int32_t i = 0; i < curr_imss.info.num_storages; i++)
	{
		ucp_ep_h ep;

		ep = curr_imss.conns.eps[i];

		flush_ep(ucp_worker_data, ep);
	}

	return 1;
}

// Method retrieving a data element associated to a certain dataset.
int32_t get_data(int32_t dataset_id, int32_t data_id, void *buffer)
{
	slog_debug("[IMSS][get_data] dataset_id=%d, data_id=%d", dataset_id, data_id);
	// slog_fatal("Caller name: %pS", __builtin_return_address(0));
	int32_t n_server;

	// Server containing the corresponding data to be retrieved.
	if ((n_server = get_data_location(dataset_id, data_id, GET)) == -1)
	{
		return -1;
	}

	slog_debug("[IMSS][get_data] next_server=%d, curr_dataset.repl_factor=%d, curr_dataset.n_servers=%d, curr_imss.info.num_storages=%d", n_server, curr_dataset.repl_factor, curr_dataset.n_servers, curr_imss.info.num_storages);

	// Servers that the data block is going to be requested to.
	int32_t repl_servers[curr_dataset.repl_factor];
	// int32_t curr_imss_storages = curr_imss.info.num_storages; // (original)
	int32_t curr_imss_storages = curr_dataset.n_servers; // curr_imss.info.num_storages;

	// Retrieve the corresponding connections to the previous servers.
	// slog_debug("[IMSS][get_data] curr_dataset.repl_factor=%d", curr_dataset.repl_factor);
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		// Server storing the current data block.
		uint32_t n_server_ = (n_server + i * (curr_imss_storages / curr_dataset.repl_factor)) % curr_imss_storages;
		// printf("Server storing is=%d",n_server_);
		repl_servers[i] = n_server_;

		// Check if the current connection is the local one (if there is).
		if (repl_servers[i] == curr_dataset.local_conn)
		{
			// Move the local connection to the first one to be requested.
			int32_t aux_conn = repl_servers[0];
			repl_servers[0] = repl_servers[i];
			repl_servers[i] = aux_conn;
		}
	}
	pthread_mutex_lock(&lock_network);
	char key_[REQUEST_SIZE];
	clock_t t;
	double time_taken;
	// Request the concerned block to the involved servers.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		ucp_ep_h ep;
		// t = clock();
		//  Key related to the requested data element.
		sprintf(key_, "GET 0 0 %s$%d", curr_dataset.uri_, data_id);
		slog_debug("[IMSS][get_data] Request - '%s' to server %ld", key_, repl_servers[i]);
		ep = curr_imss.conns.eps[repl_servers[i]];
		// new_imss.info.ips[i], new_imss.info.conn_port
		////////
		// ucp_context_h ucp_context;
		// ucp_worker_h ucp_worker;
		// init_context_ori(&ucp_context, &ucp_worker, CLIENT_SERVER_SEND_RECV_TAG);
		// ucp_ep_h client_ep;
		// ucs_status_t status;
		// int ret = -1;
		// char server_addr[] = "broadwell-010";
		// status = start_client(ucp_worker_data, server_addr, 8501, &ep);
		// if (status != UCS_OK)
		// {
		// 	fprintf(stderr, "failed to start client (%s)\n", ucs_status_string(status));
		// 	ret = -1;
		// 	// goto out;
		// 	return ret;
		// }
		////////

		// if (send_req_data(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) == 0)
		if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) == 0)
		{
			pthread_mutex_unlock(&lock_network);
			slog_error("HERCULES_ERR_GET_DATA_SENDREQDATA");
			perror("HERCULES_ERR_GET_DATA_SENDREQDATA");
			return -1;
		}

		// slog_debug("[IMSS] Request has been sent - '%s' to server %ld", key_, repl_servers[i]);

		/*	gettimeofday(&end, NULL);
			delta_us = (long) (end.tv_usec - start.tv_usec);
			printf("[CLIENT] [GET DATA] send petition delta_us=%6.3f",(delta_us/1000.0F));*/

		// t = clock() - t;
		// time_taken = ((double)t) / CLOCKS_PER_SEC; // in seconds
		// slog_debug("[IMSS][get_data] send_data %f s", time_taken);

		// int size = 0;
		// if (data_id)
		// 	size = curr_dataset.data_entity_size;
		// else
		// 	size = sizeof(struct stat);

		//	gettimeofday(&start, NULL);
		// printf("GET_DATA after send petition to read");
		// Receive data related to the previous read request directly into the buffer.
		// t = clock();
		// slog_info("[IMSS] Before getting length");
		size_t msg_length = 0;
		msg_length = get_recv_data_length(ucp_worker_data, local_data_uid);
		slog_info("[IMSS] Receiving data, msg_length=%lu", msg_length);
		if (msg_length == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERRIMSS_GET_DATA_INVALID_MSG_LENGTH");
			slog_error("ERRIMSS_GET_DATA_INVALID_MSG_LENGTH");
			return -EINVAL;
		}

		// size_t length = 0;
		msg_length = recv_data(ucp_worker_data, ep, buffer, msg_length, local_data_uid, 0);

		// msg_length = recv_data_opt(ucp_worker_data, ep, &buffer, msg_length, local_data_uid, 0);

		slog_info("[IMSS] After recv_data, msg_length=%lu", msg_length);
		if (msg_length == 0)
		{
			if (errno != EAGAIN)
			{
				pthread_mutex_unlock(&lock_network);
				slog_error("HERCULES_ERR_GETDATA_RECV");
				perror("HERCULES_ERR_GETDATA_RECV");
				return -1;
			}
			else
				break;
		}

		// fprintf(stderr,"buffer en recv_data=%s\n", buffer);

		// t = clock() - t;

		// time_taken = ((double)t) / CLOCKS_PER_SEC; // in seconds
		// slog_debug("[IMSS][get_data] RECV_STREAM %f s", time_taken);

		// Check if the requested key was correctly retrieved.
		if (strncmp((const char *)buffer, "$ERRIMSS_NO_KEY_AVAIL$", 22))
		{
			slog_info("OK!, length=%ld", msg_length);
			pthread_mutex_unlock(&lock_network);
			return (int32_t)msg_length;
		}
		else
		{
			// free(buffer);
			slog_error("HERCULES_ERR_NO_KEY_AVAIL");
		}
	}

	pthread_mutex_unlock(&lock_network);
	return -1;
}

// Method retrieving a data element associated to a certain dataset.
size_t get_ndata(int32_t dataset_id, int32_t data_id, void *buffer, ssize_t to_read, off_t offset)
{
	// slog_debug("[IMSS][get_data]");
	// slog_fatal("Caller name: %pS", __builtin_return_address(0));
	int32_t n_server;

	// Server containing the corresponding data to be retrieved.
	if ((n_server = get_data_location(dataset_id, data_id, GET)) == -1)
	{
		return -1;
	}

	// Servers that the data block is going to be requested to.
	int32_t repl_servers[curr_dataset.repl_factor];
	int32_t curr_imss_storages = curr_imss.info.num_storages;

	// Retrieve the corresponding connections to the previous servers.
	// slog_debug("curr_dataset.repl_factor=%d", curr_dataset.repl_factor);
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		// Server storing the current data block.
		uint32_t n_server_ = (n_server + i * (curr_imss_storages / curr_dataset.repl_factor)) % curr_imss_storages;
		// printf("Server storing is=%d",n_server_);
		repl_servers[i] = n_server_;

		// Check if the current connection is the local one (if there is).
		if (repl_servers[i] == curr_dataset.local_conn)
		{
			// Move the local connection to the first one to be requested.
			int32_t aux_conn = repl_servers[0];
			repl_servers[0] = repl_servers[i];
			repl_servers[i] = aux_conn;
		}
	}

	pthread_mutex_lock(&lock_network);
	char key_[REQUEST_SIZE];
	clock_t t;
	double time_taken;
	// Request the concerned block to the involved servers.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		ucp_ep_h ep;
		// t = clock();
		//  Key related to the requested data element.
		//  sprintf(key_, "GET 0 0 %s$%d", curr_dataset.uri_, data_id);
		sprintf(key_, "GET %lu %ld %s$%d %ld", 0l, offset, curr_dataset.uri_, data_id, to_read);
		// sprintf(key_, "GET %lu %ld %s$%d %zd", 0l, offset, curr_dataset.original_name, data_id, to_read);
		// slog_info("[IMSS][get_data] Request - '%s'", key_);
		ep = curr_imss.conns.eps[repl_servers[i]];
		slog_debug("[get_ndata] Sending request %s", key_);
		if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERRIMSS_RLSIMSS_SENDADDR");
			return -1;
		}

		size_t msg_length = 0;
		msg_length = get_recv_data_length(ucp_worker_data, local_data_uid);
		slog_info("[IMSS][get_ndata] Receiving data, msg_length=%lu", msg_length);
		if (msg_length == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("HERCULES_ERR_REN_DATASET_DATA_INVALID_MSG_LENGTH");
			slog_error("HERCULES_ERR_REN_DATASET_DATA_INVALID_MSG_LENGTH");
			return -1;
		}

		// Receive data related to the previous read request directly into the buffer.
		// size_t length = 0;
		// msg_length = recv_data_opt(ucp_worker_data, ep, &buffer, msg_length, local_data_uid, 0);
		msg_length = recv_data(ucp_worker_data, ep, buffer, msg_length, local_data_uid, 0);
		slog_debug("[IMSS][get_ndata] After request data, length=%lu", msg_length);
		if (msg_length == 0)
		{
			if (errno != EAGAIN)
			{
				pthread_mutex_unlock(&lock_network);
				slog_error("HERCULES_ERR_GETDATA_RECV");
				perror("HERCULES_ERR_GETDATA_RECV");
				return -1;
			}
			else
				break;
		}

		// Check if the requested key was correctly retrieved.
		if (strncmp((const char *)buffer, "$ERRIMSS_NO_KEY_AVAIL$", 22))
		{
			// return (int32_t)msg_length;
			pthread_mutex_unlock(&lock_network);
			return msg_length;
		}
		else
		{
			// free(buffer);
			slog_warn("[IMSS][get_data]HERCULES_ERR_NO_KEY_AVAIL");
		}
	}

	pthread_mutex_unlock(&lock_network);
	return 1;
}

// Method retrieving a data element associated to a certain dataset.
size_t get_data_mall(int32_t dataset_id, int32_t data_id, void *buffer, ssize_t to_read, off_t offset, int32_t num_storages)
{
	int32_t n_server;

	// Server containing the corresponding data to be retrieved.
	if ((n_server = get_data_location(dataset_id, data_id, GET)) == -1)
	{
		return 0;
	}

	// Servers that the data block is going to be requested to.
	int32_t repl_servers[curr_dataset.repl_factor];
	int32_t curr_imss_storages = curr_imss.info.num_storages;

	// Retrieve the corresponding connections to the previous servers.
	// slog_debug("curr_dataset.repl_factor=%d", curr_dataset.repl_factor);
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		// Server storing the current data block.
		uint32_t n_server_ = (n_server + i * (curr_imss_storages / curr_dataset.repl_factor)) % curr_imss_storages;
		// printf("Server storing is=%d",n_server_);
		repl_servers[i] = n_server_;

		// Check if the current connection is the local one (if there is).
		if (repl_servers[i] == curr_dataset.local_conn)
		{
			// Move the local connection to the first one to be requested.
			int32_t aux_conn = repl_servers[0];
			repl_servers[0] = repl_servers[i];
			repl_servers[i] = aux_conn;
		}
	}

	pthread_mutex_lock(&lock_network);
	char key_[REQUEST_SIZE];
	clock_t t;
	double time_taken;
	// Request the concerned block to the involved servers.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		ucp_ep_h ep;
		// t = clock();
		//  Key related to the requested data element.
		//  sprintf(key_, "GET 0 0 %s$%d", curr_dataset.uri_, data_id);
		sprintf(key_, "GET %lu %ld %s$%d %ld", 0l, offset, curr_dataset.uri_, data_id, to_read);
		// sprintf(key_, "GET %lu %ld %s$%d %zd", 0l, offset, curr_dataset.original_name, data_id, to_read);
		// slog_info("[IMSS][get_data] Request - '%s'", key_);
		ep = curr_imss.conns.eps[repl_servers[i]];
		slog_debug("[get_ndata] Sending request %s", key_);
		if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERRIMSS_RLSIMSS_SENDADDR");
			return 0;
		}

		size_t msg_length = 0;
		msg_length = get_recv_data_length(ucp_worker_data, local_data_uid);
		slog_info("[IMSS][get_ndata] Receiving data, msg_length=%lu", msg_length);
		if (msg_length == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("HERCULES_ERR_REN_DATASET_DATA_INVALID_MSG_LENGTH");
			slog_error("HERCULES_ERR_REN_DATASET_DATA_INVALID_MSG_LENGTH");
			return 0;
		}

		// Receive data related to the previous read request directly into the buffer.
		// size_t length = 0;
		// msg_length = recv_data_opt(ucp_worker_data, ep, &buffer, msg_length, local_data_uid, 0);
		msg_length = recv_data(ucp_worker_data, ep, buffer, msg_length, local_data_uid, 0);
		slog_debug("[IMSS][get_ndata] After request data, length=%lu", msg_length);
		if (msg_length == 0)
		{
			if (errno != EAGAIN)
			{
				pthread_mutex_unlock(&lock_network);
				slog_error("HERCULES_ERR_GETDATA_RECV");
				perror("HERCULES_ERR_GETDATA_RECV");
				return 0;
			}
			else
				break;
		}

		// Check if the requested key was correctly retrieved.
		if (strncmp((const char *)buffer, "$ERRIMSS_NO_KEY_AVAIL$", 22))
		{
			// return (int32_t)msg_length;
			pthread_mutex_unlock(&lock_network);
			return msg_length;
		}
		else
		{
			// free(buffer);
			slog_warn("[IMSS][get_data]HERCULES_ERR_NO_KEY_AVAIL");
		}
	}

	pthread_mutex_unlock(&lock_network);
	return 1;
}

/**
 * @brief Method storing a specific data element.
 * @return 1 on success, on error -1 is returned.
 */
int32_t set_data(int32_t dataset_id, int32_t data_id, const void *buffer, size_t size, off_t offset)
{
	int32_t n_server;

	// Server containing the corresponding data to be written.
	if ((n_server = get_data_location(dataset_id, data_id, SET)) == -1)
	{
		perror("HERCULES_ERR_GET_DATA_LOCATION");
		slog_error("HERCULES_ERR_GET_DATA_LOCATION");
		return -1;
	}

	pthread_mutex_lock(&lock_network);
	char key_[REQUEST_SIZE];
	int32_t curr_imss_storages = curr_imss.info.num_storages;

	// slog_debug("[IMSS][set_data] get_data_location(dataset_id:%ld, data_id:%ld, SET:%d), n_server:%ld, curr_imss_storages:%ld", dataset_id, data_id, SET, n_server, curr_imss_storages);

	// Send the data block to every server implementing redundancy.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		ucp_ep_h ep;
		// Server receiving the current data block.
		uint32_t n_server_ = (n_server + i * (curr_imss_storages / curr_dataset.repl_factor)) % curr_imss_storages;

		//	gettimeofday(&start, NULL);

		if (data_id == 0)
			size = sizeof(struct stat);
		else if (size == 0)
			size = curr_dataset.data_entity_size;

		sprintf(key_, "SET %lu %ld %s$%d", size, offset, curr_dataset.uri_, data_id);
		slog_info("[IMSS][set_data] BLOCK %d SENT TO %d SERVER with Request: %s (%d)", data_id, n_server_, key_, size);
		ep = curr_imss.conns.eps[n_server_];
		/////////////
		// ucs_status_t status;
		// int ret = -1;
		// char server_addr[] = "broadwell-010";
		// status = start_client(ucp_worker_data, server_addr, 8501, &ep);
		// if (status != UCS_OK)
		// {
		// 	fprintf(stderr, "failed to start client (%s)\n", ucs_status_string(status));
		// 	ret = -1;
		// 	// goto out;
		// 	return ret;
		// }
		/////////////
		// send the request to the data server, indicating we will perform a write operation (SET) to certain data block (data_id)
		// in a dataset (curr_dataset.uri).
		if (send_req_data(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("HERCULES_ERR_SET_REQ_SEND_REQ");
			slog_error("HERCULES_ERR_SET_REQ_SEND_REQ");
			// return -1;
			exit(-1);
		}

		slog_info("[IMSS][set_data]  SENDING DATA TO %d", n_server_);

		// send the data to the data server of the current dataset.
		if (send_data(ucp_worker_data, ep, buffer, size, local_data_uid) == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("HERCULES_ERR_SEND_DATA_SEND_DATA");
			slog_error("HERCULES_ERR_SEND_DATA_SEND_DATA");
			return -1;
		}
		/*	gettimeofday(&end, NULL);
			delta_us = (long) (end.tv_usec - start.tv_usec);
			printf("[CLIENT] [SWRITE SEND_DATA] delta_us=%6.3f",(delta_us/1000.0F));*/

		// slog_debug("[IMSS] Request set_data: client_id '%" PRIu32 "', mode 'SET', key '%s'", curr_imss.conns.id[n_server_], key_);
		// slog_debug("[IMSS][set_data] send_data(curr_imss.conns.id[%ld]:%ld, curr_dataset.data_entity_size:%ld)", n_server_, curr_imss.conns.id[n_server_], curr_dataset.data_entity_size);
	}
	// t = clock() - t;
	// double time_taken = ((double)t) / CLOCKS_PER_SEC; // in seconds

	// slog_info("[IMSS] [SET DATA] sent data %f s", time_taken);
	pthread_mutex_unlock(&lock_network);
	return 1;
}

// Method storing a specific data element.
int32_t set_data_mall(int32_t dataset_id, int32_t data_id, const void *buffer, size_t size, off_t offset, int32_t num_storages)
{
	int32_t n_server;
	clock_t t;
	// size_t (*const send_choose_stream)(ucp_worker_h ucp_worker, ucp_ep_h ep, const char *msg, size_t msg_length) = (IMSS_WRITE_ASYNC == 1) ? send_istream : send_data;

	// slog_debug("[IMSS][set_data]");
	t = clock();

	curr_imss.info.num_storages = num_storages;

	// Server containing the corresponding data to be written.
	if ((n_server = get_data_location(dataset_id, data_id, SET)) == -1)
	{
		perror("ERRIMSS_GET_DATA_LOCATION");
		slog_error("ERRIMSS_GET_DATA_LOCATION");
		return -1;
	}
	pthread_mutex_lock(&lock_network);
	char key_[REQUEST_SIZE];
	int32_t curr_imss_storages = 0;
	curr_imss_storages = curr_imss.info.num_storages;

	// slog_debug("[IMSS][set_data] get_data_location(dataset_id:%ld, data_id:%ld, SET:%d), n_server:%ld, curr_imss_storages:%ld", dataset_id, data_id, SET, n_server, curr_imss_storages);

	// Send the data block to every server implementing redundancy.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		ucp_ep_h ep;
		// Server receiving the current data block.
		uint32_t n_server_ = (n_server + i * (curr_imss_storages / curr_dataset.repl_factor)) % curr_imss_storages;

		// printf("BLOCK %d SENT TO %d SERVER with key: %s (%d)", data_id, n_server_, key, key_length);

		//	gettimeofday(&start, NULL);

		if (data_id == 0)
			size = sizeof(struct stat);
		else if (size == 0)
			size = curr_dataset.data_entity_size;

		sprintf(key_, "SET %lu %ld %s$%d", size, offset, curr_dataset.uri_, data_id);
		slog_info("[IMSS][set_data] Request - '%s'", key_);
		ep = curr_imss.conns.eps[n_server_];

		if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERR_HERCULES_SET_DATA_MALL_SEND_REQ");
			slog_error("ERR_HERCULES_SET_DATA_MALL_SEND_REQ");
			return -1;
		}

		// slog_debug("[IMSS][set_data] send_data(curr_imss.conns.id[%ld]:%ld, key_:%s, REQUEST_SIZE:%d)", n_server_, curr_imss.conns.id[n_server_], key_, REQUEST_SIZE);

		if (send_data(ucp_worker_data, ep, buffer, size, local_data_uid) == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERR_HERCULES_SET_DATA_MALL_SEND_DATA");
			slog_error("ERR_HERCULES_SET_DATA_MALL_SEND_DATA");
			return -1;
		}
		/*	gettimeofday(&end, NULL);
			delta_us = (long) (end.tv_usec - start.tv_usec);
			printf("[CLIENT] [SWRITE SEND_DATA] delta_us=%6.3f",(delta_us/1000.0F));*/

		// slog_debug("[IMSS] Request set_data: client_id '%" PRIu32 "', mode 'SET', key '%s'", curr_imss.conns.id[n_server_], key_);
		// slog_debug("[IMSS][set_data] send_data(curr_imss.conns.id[%ld]:%ld, curr_dataset.data_entity_size:%ld)", n_server_, curr_imss.conns.id[n_server_], curr_dataset.data_entity_size);
	}
	t = clock() - t;
	double time_taken = ((double)t) / CLOCKS_PER_SEC; // in seconds

	// slog_info("[IMSS] [SET DATA] sent data %f s", time_taken);
	pthread_mutex_unlock(&lock_network);
	return 1;
}

// Method storing a specific data element.
int32_t
set_ndata(int32_t dataset_id,
		  int32_t data_id,
		  char *buffer,
		  uint32_t size)
{
	int32_t n_server;
	// Server containing the corresponding data to be written.
	if ((n_server = get_data_location(dataset_id, data_id, SET)) == -1)

		return -1;

	pthread_mutex_lock(&lock_network);
	char key_[REQUEST_SIZE];
	int32_t curr_imss_storages = curr_imss.info.num_storages;

	// Send the data block to every server implementing redundancy.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		ucp_ep_h ep;
		// Server receiving the current data block.
		uint32_t n_server_ = (n_server + i * (curr_imss_storages / curr_dataset.repl_factor)) % curr_imss_storages;

		// printf("BLOCK %d SENT TO %d SERVER with key: %s (%d)", data_id, n_server_, key, key_length);
		// Key related to the requested data element.
		sprintf(key_, "SET 0 %d %s$%d", size, curr_dataset.uri_, data_id);

		ep = curr_imss.conns.eps[n_server_];

		if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERR_HERCULES_SET_NDATA_SEND_REQ");
			slog_error("ERR_HERCULES_SET_NDATA_SEND_REQ");
			return -1;
		}

		// Send read request message specifying the block data.
		if (send_data(ucp_worker_data, ep, buffer, size, local_data_uid) == 0)
		{
			pthread_mutex_unlock(&lock_network);
			perror("ERR_HERCULES_SET_NDATA_SEND_DATA");
			slog_error("ERR_HERCULES_SET_NDATA_SEND_DATA");
			return -1;
		}

		slog_debug("[IMSS] Request set_ndata: client_id '%" PRIu32 "', mode 'SET', key '%s'", curr_imss.conns.id[n_server_], key_);
	}

	pthread_mutex_unlock(&lock_network);
	return 0;
}

// WARNING! This function allocates memory that must be released by the user.

// Method retrieving the location of a specific data object.
char **get_dataloc(const char *dataset,
				   int32_t data_id,
				   int32_t *num_storages)
{
	// Dataset structure of the one requested.
	dataset_info where_dataset;

	// Check which resource was used to retrieve the concerned dataset.
	switch (stat_dataset(dataset, &where_dataset, 0))
	{
	// No dataset was found with the requested name.
	case 0:
	{
		slog_fatal("ERRIMSS_GETDATALOC_DATASETNOTEXISTS");
		return NULL;
	}
		// The dataset was retrieved from the metadata server.
	case 1:
	{
		// The dataset structure will not be stored if it is a LOCAL one as those are dynamically updated.
		if (strcmp(where_dataset.policy, "LOCAL"))
		{
			// Hint specifying that the dataset was retrieved but not initialized.
			where_dataset.local_conn = -2;
			GInsert(&datasetd_pos, &datasetd_max_size, (char *)&where_dataset, datasetd, free_datasetd);
		}

		break;
	}
	}

	int32_t dataset_name_length = strlen(dataset);

	// Position where the first '/' character within the dataset name has been found.
	int32_t end_imss_name;

	// TODO: retrieving the imss uri from the dataset one must be updated.

	for (end_imss_name = dataset_name_length; end_imss_name > 0; end_imss_name--)
	{
		if (dataset[end_imss_name] == '/')

			break;
	}

	// Name of the IMSS entity managing the concerned dataset.
	char imss_name[end_imss_name];

	memcpy(imss_name, dataset, end_imss_name);

	// IMSS structure storing the information related to the concerned IMSS entity.
	imss where_imss;

	int32_t found_imss_in = stat_imss(imss_name, &where_imss.info);

	// Check which resource was used to retrieve the concerned IMSS structure.
	if (found_imss_in)
	{
		// Hint specifying that the IMSS structure was retrieved but not initialized.
		where_imss.conns.matching_server = -2;
		GInsert(&imssd_pos, &imssd_max_size, (char *)&where_imss, imssd, free_imssd);
	}
	else
	{
		slog_fatal("ERRIMSS_GETDATALOC_IMSSNOTEXISTS");
		return NULL;
	}

	// Set the policy corresponding to the retrieved dataset.
	/* if (set_policy(&where_dataset) == -1)
	   {
	   slog_fatal( "ERRIMSS_GETDATALOC_SETPOLICY");
	   return NULL;
	   }
	 */

	current_dataset = -1;

	int32_t server;
	// Find the server storing the corresponding block.
	if ((server = find_server(where_imss.info.num_storages, data_id, where_dataset.uri_, GET)) < 0)
	{
		slog_fatal("ERRIMSS_GETDATALOC_FINDSERVER");
		return NULL;
	}

	*num_storages = where_dataset.repl_factor;

	char **machines = (char **)malloc(*num_storages * sizeof(char *));

	for (int32_t i = 0; i < *num_storages; i++)
	{
		// Server storing the current data block.
		uint32_t n_server_ = (server + i * (where_imss.info.num_storages / where_dataset.repl_factor)) % where_imss.info.num_storages;
		machines[i] = (char *)malloc(strlen(where_imss.info.ips[n_server_]) * sizeof(char));
		strcpy(machines[i], where_imss.info.ips[n_server_]);
		if (found_imss_in == 2)
			free(where_imss.info.ips[i]);
	}

	if (found_imss_in == 2)
		free(where_imss.info.ips);

	if (!strcmp(where_dataset.policy, "LOCAL"))
		free(where_dataset.data_locations);

	return machines;
}

// pthread_mutex_t lock_op = PTHREAD_MUTEX_INITIALIZER;

// Method specifying the type (DATASET or IMSS INSTANCE) of a provided URI.
int32_t get_type(const char *uri)
{
	// pthread_mutex_lock(&lock_op);
	ucp_ep_h ep;
	// Formated uri to be sent to the metadata server.
	pthread_mutex_lock(&lock_network);
	char formated_uri[REQUEST_SIZE];

	// slog_debug("[IMSS][get_type]");
	// Discover the metadata server that handles the entity.
	uint32_t m_srv = discover_stat_srv(uri);
	int32_t ret;

	ep = stat_eps[m_srv];

	// sprintf(formated_uri, "%" PRIu32 " GET 0 %s", stat_ids[m_srv], uri);
	sprintf(formated_uri, "%d GET 0 %s", 0, uri);
	slog_info("[IMSS][get_type] Request - '%s'", formated_uri);
	// printf("get_type=%s",uri);
	// Send the request.
	if (send_req(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta, formated_uri) == 0)
	{
		pthread_mutex_unlock(&lock_network);
		// pthread_mutex_unlock(&lock_op);
		slog_error("HERCULES_ERR_GET_TYPE_SEND_ADDR");
		perror("HERCULES_ERR_GET_TYPE_SEND_ADDR");
		return -1;
	}

	// fprintf(stderr, "[IMSS] Request - '%s'\n", formated_uri);

	// // Search for the requested IMSS.
	// imss imss_;
	// int32_t imss_position;
	// if ((imss_position = find_imss(uri, &imss_)) == -1)
	// {
	// 	slog_fatal("ERRIMSS_GETTYPE_NOTFOUND");
	// 	return -1;
	// }

	imss_info *data;

	// Receive the answer.
	// char result[RESPONSE_SIZE];
	// size_t res_size = sizeof(imss_info) + NUM_DATA_SERVERS * LINE_LENGTH + 1024;

	// char result[res_size];
	void *result = NULL; // = (char *)malloc(sizeof(char) * res_size);
	// Get the length of the message to be received.
	size_t length = 0;
	length = get_recv_data_length(ucp_worker_meta, local_meta_uid);
	if (length == 0)
	{
		pthread_mutex_unlock(&lock_network);
		// pthread_mutex_unlock(&lock_op);
		perror("HERCULES_ERR_GET_TYPE_GET_RECV_DATA_LENGTH");
		slog_error("HERCULES_ERR_GET_TYPE_GET_RECV_DATA_LENGTH");
		return -1;
	}
	// slog_debug("[IMSS][get_type] length=%ld, NUM_DATA_SERVERS=%d", length, NUM_DATA_SERVERS);
	result = malloc(length);

	ret = recv_dynamic_stream(ucp_worker_meta, ep, result, BUFFER, local_meta_uid, length);
	// ret = recv_dynamic_stream_opt(ucp_worker_meta, ep, &result, BUFFER, local_meta_uid, length);
	// if (result)
	// {
	// 	slog_debug("[IMSS][get_type] after recv_dynamic_stream ret=%d, result=%s", ret, result);
	// } else {
	slog_debug("[IMSS][get_type] after recv_dynamic_stream ret=%d", ret);
	// }

	if (ret < 0)
	{
		slog_fatal("HERCULES_ERR_GET_TYPE_RECV_STREAM");
		free(result);
		pthread_mutex_unlock(&lock_network);
		return -1;
	}

	data = (imss_info *)result;
	// memcpy(data, result, sizeof(imss_info));

	// Determine what was retrieved from the metadata server.
	if (data->type == 'I')
	{
		// return 1;
		ret = 1;
	}
	else if (data->type == 'D' || data->type == 'L')
	{
		// return 2;
		ret = 2;
	}
	else
	{
		ret = 0;
	}
	// pthread_mutex_unlock(&lock_op);

	free(result);
	// return 0;
	pthread_mutex_unlock(&lock_network);
	return ret;
}

// Method retriving list of servers to read.
int32_t split_location_servers(int **list_servers, int32_t dataset_id, int32_t curr_blk, int32_t end_blk)
{
	int size = end_blk - curr_blk + 1;
	// printf("size=%d",size);

	for (int i = 0; i < size; i++)
	{
		int32_t n_server;

		// Server containing the corresponding data to be retrieved.
		if ((n_server = get_data_location(dataset_id, curr_blk, GET)) == -1)
		{
			return -1;
		}
		// printf("list_servers[%d][%d]=%d",n_server,i,curr_blk);
		list_servers[n_server][i] = curr_blk;
		curr_blk++;
	}

	return 0;
}
/**********************************************************************************/
/***************************** DATA RELEASE RESOURCES *****************************/
/**********************************************************************************/

// Method releasing an imss_info structure previously provided to the client.
int32_t free_imss(imss_info *imss_info_)
{
	for (int32_t i = 0; i < imss_info_->num_storages; i++)
		free(imss_info_->ips[i]);

	free(imss_info_->ips);

	return 0;
}

// Method releasing a dataset structure previously provided to the client.
int32_t free_dataset(dataset_info *dataset_info_)
{
	if (!strcmp(dataset_info_->policy, "LOCAL"))
	{
		if (dataset_info_->data_locations)
			free(dataset_info_->data_locations);

		if (dataset_info_->num_blocks_written)
			free(dataset_info_->num_blocks_written);

		if (dataset_info_->blocks_written)
			free(dataset_info_->blocks_written);
	}

	return 0;
}
