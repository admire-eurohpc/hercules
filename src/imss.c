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

#include <ucp/api/ucp.h>

/**********************************************************************************/
/******************************** GLOBAL VARIABLES ********************************/
/**********************************************************************************/

uint32_t process_rank; // Process identifier within the deployment.

uint32_t n_stat_servers; // Number of metadata servers available.
uint32_t *stat_ids;
void *stat_mon; // Metadata monitoring socket.

int32_t current_dataset;   // Dataset whose policy has been set last.
dataset_info curr_dataset; // Currently managed dataset.
imss curr_imss;

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

int32_t imss_comm_cleanup()
{
	// ep_close(ucp_worker_meta, stat_eps[0], 0);
	ucp_worker_release_address(ucp_worker_meta, local_addr_meta);
	ucp_worker_release_address(ucp_worker_data, local_addr_data);

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
	// Traverse the whole set of IMSS structures in order to find the one.
	for (int32_t i = 0; i < imssd->len; i++)
	{
		imss_ = g_array_index(imssd, imss, i);

		int32_t imss_uri_len = strlen(imss_.info.uri_);
		if ((imss_uri_len > 0) && !strncmp(dataset_uri, imss_.info.uri_, imss_uri_len))
			return i;
	}
	return -1;
}

// Method searching for a certain IMSS in the vector.
int32_t find_imss(char *imss_uri, imss *imss_)
{
	// Search for a certain IMSS within the vector.
	for (int32_t i = 0; i < imssd->len; i++)
	{
		*imss_ = g_array_index(imssd, imss, i);
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
		perror("ERRIMSS_INIT_CONTEXT");
		return -1;
	}

	ret = init_worker(ucp_context_client, &ucp_worker_data);
	if (ret != 0)
	{
		perror("ERRIMSS_INIT_WORKER");
		return -1;
	}

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
		sprintf(error_msg, "ERRIMSS_OPEN_STATFILE: %s", stat_hostfile);
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

		oob_sock = connect_common(stat_node, port, AF_INET);

		sprintf(request, "%" PRIu32 " GET HELLO!", rank);
		slog_debug("[IMSS][stat_int] request=%s", request);
		if (send(oob_sock, request, REQUEST_SIZE, 0) < 0)
		{
			perror("ERRIMSS_STAT_HELLO_2");
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
	slog_debug("[IMSS] Calling stat_release.");
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

	// Disconnect from all metadata servers.
	for (int i = 0; i < n_stat_servers; i++)
	{
		ucp_ep_h ep = stat_eps[i];
		char release_msg[REQUEST_SIZE];

		sprintf(release_msg, "%" PRIu32 " GET 2 RELEASE", process_rank);

		if (send_req(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta, release_msg) < 0)
		{
			perror("ERRIMSS_RLSIMSS_SENDADDR");
			return -1;
		}

		ucp_ep_destroy(ep);
		// ucp_context_destroy(ucp_worker_meta);
		// ep_close(ucp_worker_meta, ep, 0);
		free(stat_addr[i]);
	}

	// ucp_worker_destroy(ucp_worker_meta);
	free(stat_eps);

	// ucp_cleanup(ucp_context_client);
	return 0;
}

// Method discovering which metadata server is in charge of a certain URI.
uint32_t discover_stat_srv(char *_uri)
{
	// Calculate a crc from the provided URI.
	uint64_t crc_ = crc64(0, (unsigned char *)_uri, strlen(_uri));

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

	// GETDIR request.
	char getdir_req[REQUEST_SIZE];
	sprintf(getdir_req, "%" PRIu32 " GET %d %s", stat_ids[m_srv], GETDIR, requested_uri);
	slog_debug("[IMSS][get_dir] Request - %s", getdir_req);
	if (send_req(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta, getdir_req) < 0)
	{
		perror("ERRIMSS_RLSIMSS_SENDADDR");
		return -1;
	}

	char elements[4096];
	// Retrieve the set of elements within the requested uri.
	ret = recv_dynamic_stream(ucp_worker_meta, ep, elements, BUFFER, local_meta_uid);
	if (ret < 0)
	{
		perror("ERRIMSS_GETDIR_RECV");
		return -1;
	}

	if (!strncmp("$ERRIMSS_NO_KEY_AVAIL$", elements, 22))
	{
		slog_fatal("ERRIMSS_GETDIR_NODIR");
		return -1;
	}

	uint32_t elements_size = ret;

	//*buffer = (char *) malloc(sizeof(char)*elements_size);
	// memcpy(*buffer, elements, elements_size);
	// elements = *buffer;

	uint32_t num_elements = elements_size / URI_;
	*items = (char **)malloc(sizeof(char *) * num_elements);

	// Identify each element within the buffer provided.
	char *curr = elements;
	for (int32_t i = 0; i < num_elements; i++)
	{
		(*items)[i] = (char *)malloc(URI_);
		memcpy((*items)[i], curr, URI_);
		//(*items)[i] = elements;

		curr += URI_;
	}
	return num_elements;
}

/**********************************************************************************/
/***************** IN-MEMORY STORAGE SYSTEM MANAGEMENT FUNCTIONS  *****************/
/**********************************************************************************/

// Method initializing an IMSS deployment.
int32_t init_imss(char *imss_uri,
				  char *hostfile,
				  char *meta_hostfile,
				  int32_t n_servers,
				  uint16_t conn_port,
				  uint64_t buff_size,
				  uint32_t deployment,
				  char *binary_path,
				  uint16_t metadata_port)
{
	int ret = 0;
	imss_info aux_imss;
	ucp_config_t *config;
	ucs_status_t status;

	if (getenv("IMSS_DEBUG") != NULL)
	{
		IMSS_DEBUG = 1;
	}

	// Check if the new IMSS uri has been already assigned.
	int32_t existing_imss = stat_imss(imss_uri, &aux_imss);
	if (existing_imss)
	{
		slog_fatal("ERRIMSS_INITIMSS_ALREADYEXISTS");
		return -1;
	}
	// Once it has been notified that no other IMSS instance had the same URI, the deployment will be performed in case of a DETACHED instance.
	if (deployment == DETACHED)
	{
		if (!binary_path)
		{
			slog_fatal("ERRIMSS_INITIMSS_NOBINARY");
			return -1;
		}

		char *command = (char *)calloc(2048, sizeof(char));

		FILE *in_file = fopen(meta_hostfile, "r");
		if (!in_file)
		{
			perror("fopen");
			return 0;
		}
		struct stat sb;
		if (stat(meta_hostfile, &sb) == -1)
		{
			perror("stat");
			return 0;
		}

		char *textRead = (char *)malloc(sb.st_size);
		int i = 1;

		while (fscanf(in_file, "%[^\n] ", textRead) != EOF)
		{
			// printf("Line %d is  %s",i, textRead);
			// sprintf(command, "mpirun -np %d -hostfile %s %s %s %d %lu %s %d %d %s %d &", n_servers, hostfile, binary_path, imss_uri, conn_port, buff_size, textRead,metadata_port, n_servers ,hostfile, IMSS_THREAD_POOL);
			i++;
		}
		fclose(in_file);
		// Original
		// sprintf(command, "mpirun -np %d -f %s %s %s %d %lu foo %d %d %s &", n_servers, hostfile, binary_path, imss_uri, conn_port, buff_size, 0, n_servers, "");
		// sprintf(command, "mpirun -np %d -hostfile %s %s %s %d %lu foo %d %d %s &", n_servers, hostfile, binary_path, imss_uri, conn_port, buff_size, 0, n_servers, "");

		// Imss server(data)
		// mpirun -np $num_servers -f $imss_hostfile $server_binary $imss_uri $imss_port_number $imss_buffer_size $metadata_server_address $metadata_server_port $num_servers $imss_hostfile $io_threads &
		//["imss://", "5555", "1048576000", "compute-6-2", "5569", "1", "./hostfile", "1"]
		// sprintf(command, "mpirun -np %d -hostfile %s %s %s %d %lu %s %d %d %s %d &", n_servers, hostfile, binary_path, imss_uri, conn_port, buff_size, meta_hostfile, metadata_port, n_servers ,hostfile, IMSS_THREAD_POOL);
		sprintf(command, "mpirun.mpich -np %d -f %s %s %s %d %lu %s %d %d %s %d &", 2, hostfile, binary_path, imss_uri, conn_port, buff_size, textRead, metadata_port, 2, hostfile, IMSS_THREAD_POOL);

		sprintf(command, "mpirun -np %d -hostfile %s %s %s %d %lu %s %d %d", n_servers, hostfile, binary_path, imss_uri, conn_port, buff_size, "compute-6-2", 5569, n_servers);

		printf("command=%s", command);
		// Perform the deployment (FROM LINUX MAN PAGES: "system() returns after the command has been completed").
		if (system(command) == -1)
		{
			perror("ERRIMSS_INITIMSS_DEPLOY");
			return -1;
		}
		free(command);
	}
	// IMSS creation.

	imss new_imss;
	strcpy(new_imss.info.uri_, imss_uri);
	new_imss.info.num_storages = n_servers;
	new_imss.info.conn_port = conn_port;
	new_imss.info.type = 'I';

	if (deployment == ATTACHED)
		new_imss.info.conn_port = connection_port;

	new_imss.info.ips = (char **)malloc(n_servers * sizeof(char *));

	// Resources required to connect to the corresponding IMSS.
	new_imss.conns.peer_addr = (ucp_address_t **)malloc(n_servers * sizeof(ucp_address_t *));
	new_imss.conns.eps = (ucp_ep_h *)malloc(n_servers * sizeof(ucp_ep_h));

	// FILE entity managing the IMSS hostfile.
	FILE *svr_nodes;
	// Open the file containing the IMSS server nodes.
	if ((svr_nodes = fopen(hostfile, "r+")) == NULL)
	{
		perror("ERRIMSS_OPEN_FILE");
		return -1;
	}
	// Number of characters successfully read from the line.
	int n_chars;

	new_imss.conns.matching_server = -1;

	new_imss.conns.id = (uint32_t *)malloc(n_servers * sizeof(uint32_t));

	status = ucp_worker_get_address(ucp_worker_data, &local_addr_data, &local_addr_len_data);
	ucp_worker_address_attr_t attr;
	attr.field_mask = UCP_WORKER_ADDRESS_ATTR_FIELD_UID;
	ucp_worker_address_query(local_addr_data, &attr);
	local_meta_uid = attr.worker_uid;

	// Connect to all servers.
	for (int i = 0; i < n_servers; i++)
	{
		ucs_status_t status;
		int oob_sock;
		ucp_address_t *peer_addr;
		size_t addr_len;
		ucp_ep_params_t ep_params;
		ucs_status_t ep_status = UCS_OK;

		// Allocate resources in the metadata structure so as to store the current IMSS's IP.
		(new_imss.info.ips)[i] = (char *)malloc(LINE_LENGTH);
		size_t l_size = LINE_LENGTH;
		// Save IMSS metadata deployment.
		n_chars = getline(&((new_imss.info.ips)[i]), &l_size, svr_nodes);

		// Erase the new line character ('') from the string.
		((new_imss.info.ips)[i])[n_chars - 1] = '\0';

		// Save the current socket value when the IMSS ip matches the clients' one.
		if (!strncmp((new_imss.info.ips)[i], client_node, len_client_node) || !strncmp((new_imss.info.ips)[i], client_ip, strlen(new_imss.info.ips[i])))
		{
			new_imss.conns.matching_server = i;
			strcpy(att_deployment, imss_uri);
		}
		// Create the connection to the IMSS server dispatcher thread.
		slog_debug("[IMSS] imss_init: Contacting dispatcher at %s:%d", (new_imss.info.ips)[i], new_imss.info.conn_port);

		process_rank = CLOSE_EP;

		oob_sock = connect_common((new_imss.info.ips)[i], new_imss.info.conn_port, AF_INET);

		char request[REQUEST_SIZE];
		sprintf(request, "%" PRIu32 " GET HELLO! %ld %s", process_rank, buff_size, imss_uri);

		if (send(oob_sock, request, REQUEST_SIZE, 0) < 0)
		{
			perror("ERRIMSS_STAT_HELLO_0");
			return -1;
		}

		ret = recv(oob_sock, &addr_len, sizeof(addr_len), MSG_WAITALL);
		new_imss.conns.peer_addr[i] = (ucp_address *)malloc(addr_len);
		ret = recv(oob_sock, new_imss.conns.peer_addr[i], addr_len, MSG_WAITALL);
		close(oob_sock);

		client_create_ep(ucp_worker_data, &new_imss.conns.eps[i], curr_imss.conns.peer_addr[i]);
	}
	// Close the file.
	if (fclose(svr_nodes) != 0)
	{
		perror("ERR_CLOSE_FILE");
		return -1;
	}

	// Discover the metadata server that shall deal with the new IMSS instance.
	uint32_t m_srv = discover_stat_srv(new_imss.info.uri_);
	ucp_ep_h ep = stat_eps[m_srv];

	// Send the created structure to the metadata server.
	char key_plus_size[REQUEST_SIZE];
	sprintf(key_plus_size, "%" PRIu32 " SET %lu %s", stat_ids[m_srv], (sizeof(imss_info) + new_imss.info.num_storages * LINE_LENGTH), new_imss.info.uri_);

	if (send_req(ucp_worker_meta, ep, local_addr_data, local_addr_len_data, key_plus_size) < 0)
	{
		perror("ERRIMSS_RLSIMSS_SENDADDR");
		return -1;
	}

	// Send the new IMSS metadata structure to the metadata server entity.
	if (send_dynamic_stream(ucp_worker_meta, ep, (void *)&new_imss.info, IMSS_INFO, local_meta_uid) < 0)
		return -1;

	// Add the created struture into the underlying IMSS vector.
	GInsert(&imssd_pos, &imssd_max_size, (char *)&new_imss, imssd, free_imssd);

	return 0;
}

// Method initializing the required resources to make use of an existing IMSS.
int32_t open_imss(char *imss_uri)
{
	// New IMSS structure storing the entity to be created.
	imss new_imss;

	ucp_config_t *config;
	ucs_status_t status;

	// if (getenv("IMSS_DEBUG") != NULL)
	// {
	// 	IMSS_DEBUG = 1;
	// }

	// if (IMSS_DEBUG)
	// {
	// 	//		status = ucp_config_read(NULL, NULL, &config);
	// 	//		ucp_config_print(config, stderr, NULL, UCS_CONFIG_PRINT_CONFIG);
	// 	//		ucp_config_release(config);
	// }

	int32_t not_initialized = 0;

	slog_debug("[IMSS][open_imss] starting function, imss_uri=%s", imss_uri);
	// Retrieve the actual information from the metadata server.
	int32_t imss_existance = stat_imss(imss_uri, &new_imss.info);
	// Check if the requested IMSS did not exist or was already stored in the local vector.
	switch (imss_existance)
	{
	case 0:
	{
		slog_fatal("ERRIMSS_OPENIMSS_NOTEXISTS");
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

	new_imss.conns.peer_addr = (ucp_address_t **)malloc(new_imss.info.num_storages * sizeof(ucp_address_t *));
	slog_debug("new_imss.info.num_storages=%ld", new_imss.info.num_storages);
	new_imss.conns.eps = (ucp_ep_h *)malloc(new_imss.info.num_storages * sizeof(ucp_ep_h));
	new_imss.conns.id = (uint32_t *)malloc(new_imss.info.num_storages * sizeof(uint32_t));

	new_imss.conns.matching_server = -1;

	status = ucp_worker_get_address(ucp_worker_data, &local_addr_data, &local_addr_len_data);
	ucp_worker_address_attr_t attr;
	attr.field_mask = UCP_WORKER_ADDRESS_ATTR_FIELD_UID;
	ucp_worker_address_query(local_addr_data, &attr);
	local_data_uid = attr.worker_uid;

	// Connect to the requested IMSS.
	for (int32_t i = 0; i < new_imss.info.num_storages; i++)
	{
		int oob_sock;
		size_t addr_len;
		int ret = 0;

		oob_sock = connect_common(new_imss.info.ips[i], new_imss.info.conn_port, AF_INET);

		char request[REQUEST_SIZE];
		sprintf(request, "%" PRIu32 " GET %s", process_rank, "HELLO!JOIN");

		slog_debug("[open_imss] ip_address=%s:%d", new_imss.info.ips[i], new_imss.info.conn_port);
		// fprintf(stderr, "[open_imss] ip_address=%s:%d\n", new_imss.info.ips[i], new_imss.info.conn_port);

		if (send(oob_sock, request, REQUEST_SIZE, 0) < 0)
		{
			perror("ERRIMSS_STAT_HELLO_1");
			return -1;
		}

		ret = recv(oob_sock, &addr_len, sizeof(addr_len), MSG_WAITALL);
		new_imss.conns.peer_addr[i] = (ucp_address *)malloc(addr_len);
		ret = recv(oob_sock, new_imss.conns.peer_addr[i], addr_len, MSG_WAITALL);
		close(oob_sock);

		new_imss.conns.id[i] = i;

		// Save the current socket value when the IMSS ip matches the clients' one.
		if (!strncmp((new_imss.info.ips)[i], client_node, len_client_node) || !strncmp((new_imss.info.ips)[i], client_ip, strlen(new_imss.info.ips[i])))
		{
			new_imss.conns.matching_server = i;
			strcpy(att_deployment, imss_uri);
		}

		client_create_ep(ucp_worker_data, &new_imss.conns.eps[i], new_imss.conns.peer_addr[i]);
		slog_debug("[IMSS] open_imss: Created endpoint with %s", (new_imss.info.ips)[i]);
	}

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

// Method releasing client-side and/or server-side resources related to a certain IMSS instance.
int32_t release_imss(char *imss_uri, uint32_t release_op)
{
	// Search for the requested IMSS.
	imss imss_;
	int32_t imss_position;
	if ((imss_position = find_imss(imss_uri, &imss_)) == -1)
	{
		slog_fatal("ERRIMSS_RLSIMSS_NOTFOUND");
		return -1;
	}

	// Release the set of connections to the corresponding IMSS.

	for (int32_t i = 0; i < imss_.info.num_storages; i++)
	{
		// Request IMSS instance closure per server if the instance is a DETACHED one and the corresponding argumet was provided.
		if (release_op == CLOSE_DETACHED)
		{
			char release_msg[REQUEST_SIZE];
			ucp_ep_h ep;

			ep = imss_.conns.eps[i];

			sprintf(release_msg, "GET 2 0 RELEASE");

			if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, release_msg) < 0)
			{
				perror("ERRIMSS_RLSIMSS_SENDADDR");
				return -1;
			}

			// ep_close(ucp_worker_data, ep, 0);

			ucp_ep_destroy(ep);
			// ucp_context_destroy(ucp_worker_data);
		}
		// ep_flush(ep, ucp_worker_data);
		// ep_flush(imss_.conns.eps_[i], ucp_worker_data);
		free(imss_.info.ips[i]);
	}

	free(imss_.info.ips);
	free(curr_imss.conns.eps);

	g_array_remove_index(imssd, imss_position);
	g_array_insert_val(imssd, imss_position, empty_imss);
	// Add the released position to the set of free positions.
	g_array_append_val(free_imssd, imss_position);

	if (!memcmp(att_deployment, imss_uri, URI_))
		memset(att_deployment, '\0', URI_);

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

	slog_debug("[IMSS][stat_imss] imss_uri=%s", imss_uri);
	if ((imss_found_in = find_imss(imss_uri, &searched_imss)) != -1)
	{
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

	// Send the request.
	sprintf(formated_uri, "%" PRIu32 " GET 0 %s", stat_ids[m_srv], imss_uri);
	slog_info("[IMSS][stat_imss] Request - '%s'", formated_uri);
	if (send_req(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta, formated_uri) < 0)
	{
		perror("ERRIMSS_RLSIMSS_SENDADDR");
		return -1;
	}

	ret = recv_dynamic_stream(ucp_worker_meta, ep, (char *)imss_info_, IMSS_INFO, local_meta_uid);

	slog_debug("[IMSS][stat_imss] End");
	if (ret < sizeof(imss_info))
		return 0;
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
					   char *link)
{
	int err = 0;
	int ret = 0;
	ucp_ep_h ep;

	slog_debug("[IMSS][create_dataset] dataset_create: starting.");

	curr_imss = g_array_index(imssd, imss, curr_dataset.imss_d);

	if ((dataset_uri == NULL) || (policy == NULL) || !num_data_elem || !data_elem_size)
	{
		slog_fatal("ERRIMSS_CRTDATASET_WRONGARG");
		return -EINVAL;
	}
	if ((repl_factor < NONE) || (repl_factor > TRM))
	{
		slog_fatal("ERRIMSS_CRTDATASET_BADREPLFACTOR");
		return -EINVAL;
	}

	int32_t associated_imss_indx;
	// Check if the IMSS storing the dataset exists within the clients session.
	slog_debug("[IMSS][create_dataset] Before imss_check  %s ", dataset_uri);
	if ((associated_imss_indx = imss_check(dataset_uri)) == -1)
	{
		slog_debug("[IMSS] create_dataset: ERRIMSS_OPENDATA_IMSSNOTFOUND");
		return -ENOENT;
	}

	slog_debug("[IMSS][create_dataset] After imss_check, associated_imss_indx=%ld", associated_imss_indx);
	imss associated_imss;
	associated_imss = g_array_index(imssd, imss, associated_imss_indx);

	dataset_info new_dataset;

	// Dataset metadata request.
	if (stat_dataset(dataset_uri, &new_dataset))
	{
		slog_debug("[IMSS] create_dataset: ERRIMSS_CREATEDATASET_ALREADYEXISTS");
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

		// Add the created struture into the underlying IMSSs.
		return (GInsert(&datasetd_pos, &datasetd_max_size, (char *)&new_dataset, datasetd, free_datasetd));
	}

	// Save the associated metadata of the current dataset.
	strcpy(new_dataset.uri_, dataset_uri);
	strcpy(new_dataset.policy, policy);
	new_dataset.num_data_elem = num_data_elem;
	new_dataset.data_entity_size = data_elem_size * 1024; // dataset in kilobytes
	new_dataset.imss_d = associated_imss_indx;
	new_dataset.local_conn = associated_imss.conns.matching_server;
	new_dataset.size = 0;
	// new_dataset.is_link = 0; // Initially not linked
	if (link != NO_LINK)
	{
		strcpy(new_dataset.link, link);
		slog_debug("[IMSS][create_dataset] link=%s, new_dataset=%s", link, new_dataset.link);
		new_dataset.is_link = 1;
	}
	else
	{
		slog_debug("[IMSS][create_dataset] link=%s", link);
		new_dataset.is_link = 0;
	}

	if (n_servers > 0 && n_servers <= curr_imss.info.num_storages)
	{
		new_dataset.n_servers = n_servers;
	}
	else
	{
		new_dataset.n_servers = curr_imss.info.num_storages;
	}

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

	slog_debug("[IMSS][create_dataset] new_dataset.type=%c", new_dataset.type);

	// Discover the metadata server that handle the new dataset.
	uint32_t m_srv = discover_stat_srv(new_dataset.uri_);

	char formated_uri[REQUEST_SIZE];
	sprintf(formated_uri, "%" PRIu32 " SET %lu %s", stat_ids[m_srv], msg_size, new_dataset.uri_);
	slog_info("[IMSS][create_dataset] Request - '%s'", formated_uri);

	ep = stat_eps[m_srv];

	if (send_req(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta, formated_uri) < 0)
	{
		perror("ERRIMSS_RLSIMSS_SENDADDR");
		return -1;
	}

	slog_debug("[IMSS][create_dataset] dataset_create: sending dataset_info");
	// Send the new dataset metadata structure to the metadata server entity.
	if (send_dynamic_stream(ucp_worker_meta, ep, (void *)&new_dataset, DATASET_INFO, local_meta_uid) < 0)
	{
		perror("ERRIMSS_DATASET_SNDDS");
		return -1;
	}

	slog_debug("[IMSS] dataset_create: sent dataset_info");
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
	err = GInsert(&datasetd_pos, &datasetd_max_size, (char *)&new_dataset, datasetd, free_datasetd);
	slog_debug("[IMSS] dataset_create: GIsinsert %d", err);
	return err;
}

// Method creating the required resources in order to READ and WRITE an existing dataset.
int32_t open_dataset(char *dataset_uri)
{
	int32_t associated_imss_indx;
	// printf("OPEN DATASET INSIDE dataset_uri=%s",dataset_uri);
	// Check if the IMSS storing the dataset exists within the clients session.
	if ((associated_imss_indx = imss_check(dataset_uri)) == -1)
	{
		slog_fatal("ERRIMSS_OPENDATA_IMSSNOTFOUND");
		return -1;
	}

	imss associated_imss;
	associated_imss = g_array_index(imssd, imss, associated_imss_indx);

	dataset_info new_dataset;
	// Dataset metadata request.
	int32_t stat_dataset_res = stat_dataset(dataset_uri, &new_dataset);

	if (new_dataset.is_link != 0)
	{
		slog_debug("[IMSS][open_dataset] is_link=%d, link name=%s", new_dataset.is_link, new_dataset.link);
		stat_dataset_res = stat_dataset(new_dataset.link, &new_dataset);
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
		slog_debug("[IMSS][open_dataset] is_link=%d, stat_dataset_res=%d", new_dataset.is_link, stat_dataset_res);
	}
	int32_t not_initialized = 0;

	// Check if the requested dataset did not exist or was already stored in the local vector.
	switch (stat_dataset_res)
	{
	case 0:
	{
		slog_fatal("ERRIMSS_OPENDATASET_NOTEXISTS: %s", dataset_uri);
		return -1;
	}
	case 2:
	{
		if (new_dataset.local_conn != -2)
		{
			slog_fatal("ERRIMSS_OPENDATASET_ALREADYSTORED");
			return -1;
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
	new_dataset.imss_d = associated_imss_indx;
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
		g_array_remove_index(datasetd, found_in);
		g_array_insert_val(datasetd, found_in, new_dataset);

		return found_in;
	}

	// Add the created struture into the underlying IMSSs.
	return (GInsert(&datasetd_pos, &datasetd_max_size, (char *)&new_dataset, datasetd, free_datasetd));
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
		// Discover the metadata server that handles the dataset.
		uint32_t m_srv = discover_stat_srv(release_dataset.uri_);

		ep = stat_eps[m_srv];

		// Formated dataset uri to be sent to the metadata server.
		char formated_uri[REQUEST_SIZE];
		sprintf(formated_uri, "%" PRIu32 " SET 0 %s", stat_ids[m_srv], release_dataset.uri_);

		if (send_req(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta, formated_uri) < 0)
		{
			perror("ERRIMSS_RLSIMSS_SENDADDR");
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
		if (send_data(ucp_worker_meta, ep, mode2, MODE_SIZE, local_meta_uid) < 0)
		{
			perror("ERRIMSS_STAT_HELLO");
			return -1;
		}

		// if (send_data_addr(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta) < 0)
		//{
		//	perror("ERRIMSS_RLSIMSS_SENDADDR");
		//	return -1;
		// }

		if (send_data(ucp_worker_meta, ep, update_msg, REQUEST_SIZE, local_meta_uid) < 0)
		{
			perror("ERRIMSS_RELDATASET_SENDPOSITIONS");
			return -1;
		}

		char update_result[RESPONSE_SIZE];

		if (recv_data(ucp_worker_meta, ep, update_result, local_meta_uid, 0) < 0)
		{
			perror("ERRIMSS_RELDATASET_RECVUPDATERES");
			return -1;
		}

		if (strcmp(update_result, "UPDATED!"))
		{
			perror("ERRIMSS_RELDATASET_UPDATE");
			return -1;
		}

		// Free the data locations vector.
		free(release_dataset.data_locations);
		// Freem the monitoring vector.
		free(release_dataset.blocks_written);
		free(release_dataset.num_blocks_written);
		free(update_msg);
	}

	g_array_remove_index(datasetd, dataset_id);
	g_array_insert_val(datasetd, dataset_id, empty_dataset);
	// Add the index to the set of free positions within the dataset vector.
	g_array_append_val(free_datasetd, dataset_id);

	return 0;
}

// Method deleting a dataset.
int32_t delete_dataset(const char *dataset_uri)
{
	ucp_ep_h ep;
	// Formated dataset uri to be sent to the metadata server.
	char formated_uri[REQUEST_SIZE];

	// Discover the metadata server that handles the dataset.
	uint32_t m_srv = discover_stat_srv((char *)dataset_uri);

	ep = stat_eps[m_srv];

	sprintf(formated_uri, "%" PRIu32 " GET 4 %s", stat_ids[m_srv], dataset_uri); // delete

	slog_debug("[IMSS][delete_dataset] formated_uri='%s'", formated_uri);

	// Send the request.
	if (send_req(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta, formated_uri) < 0)
	{
		perror("ERRIMSS_RLSIMSS_SENDADDR");
		return -1;
	}

	char result[RESPONSE_SIZE];
	if (recv_data(ucp_worker_meta, ep, result, local_meta_uid, 0) < 0)
	{
		perror("ERRIMSS_RECVDYNAMSTRUCT_RECV");
		return -1;
	}
	slog_debug("[IMSS][delete_dataset] response=%s", result);

	return 1;
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

	/*********RENAME METADATA*******/
	// Formated dataset uri to be sent to the metadata server.
	char formated_uri[REQUEST_SIZE];

	// Discover the metadata server that handles the dataset.
	uint32_t m_srv = discover_stat_srv((char *)old_dir);

	ep = stat_eps[m_srv];

	// Send the request.
	sprintf(formated_uri, "%" PRIu32 " GET 6 %s %s", stat_ids[m_srv], old_dir, rdir_dest);
	if (send_req(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta, formated_uri) < 0)
	{
		perror("ERRIMSS_RLSIMSS_SENDADDR");
		return -1;
	}

	char result[RESPONSE_SIZE];
	if (recv_data(ucp_worker_meta, ep, result, local_meta_uid, 0) < 0)
	{
		perror("ERRIMSS_RECVDYNAMSTRUCT_RECV");
		return -1;
	}
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
	// Formated dataset uri to be sent to the metadata server.
	char formated_uri[REQUEST_SIZE];

	// Discover the metadata server that handles the dataset.
	uint32_t m_srv = discover_stat_srv((char *)old_dataset_uri);

	ep = stat_eps[m_srv];

	// Send the request.
	sprintf(formated_uri, "%" PRIu32 " GET 5 %s %s", stat_ids[m_srv], old_dataset_uri, new_dataset_uri);
	fprintf(stderr, "Request - %s\n", formated_uri);
	if (send_req(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta, formated_uri) < 0)
	{
		perror("ERRIMSS_RLSIMSS_SENDADDR");
		return -1;
	}

	char result[RESPONSE_SIZE];
	if (recv_data(ucp_worker_meta, ep, result, local_meta_uid, 0) < 0)
	{
		perror("ERRIMSS_RECVDYNAMSTRUCT_RECV");
		return -1;
	}

	return 0;
}

// Method retrieving information related to a certain dataset.
int32_t stat_dataset(const char *dataset_uri, dataset_info *dataset_info_)
{
	int ret = 0;
	ucp_ep_h ep;
	// Search for the requested dataset in the local vector.
	for (int32_t i = 0; i < datasetd->len; i++)
	{
		*dataset_info_ = g_array_index(datasetd, dataset_info, i);
		slog_debug("[IMSS][stat_dataset] dataset_uri=%s, dataset_info_->uri_=%s", dataset_uri, dataset_info_->uri_);
		if (!strcmp(dataset_uri, dataset_info_->uri_))
			return 2;
	}

	// Formated dataset uri to be sent to the metadata server.
	char formated_uri[REQUEST_SIZE];

	// Discover the metadata server that handles the dataset.
	uint32_t m_srv = discover_stat_srv((char *)dataset_uri);

	ep = stat_eps[m_srv];

	sprintf(formated_uri, "%" PRIu32 " GET 0 %s", stat_ids[m_srv], dataset_uri);
	slog_debug("[IMSS][stat_dataset] Request - %s", formated_uri);
	// Send the request.
	if (send_req(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta, formated_uri) < 0)
	{
		perror("ERRIMSS_RLSIMSS_SENDADDR");
		return -1;
	}

	// Receive the associated structure.
	ret = recv_dynamic_stream(ucp_worker_meta, ep, dataset_info_, DATASET_INFO, local_meta_uid);

	if (ret < sizeof(dataset_info))
	{
		slog_debug("[IMSS][stat_dataset] stat_dataset: dataset does not exist.");
		// fprintf(stderr,"[IMSS] stat_dataset: dataset does not exist.\n");
		// exit(0);
		return 0;
	}
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
	slog_debug("[get_data_location] current_dataset=%ld, dataset_id=%ld", current_dataset, dataset_id);
	if (current_dataset != dataset_id)
	{
		// Retrieve the corresponding dataset_info structure and the associated IMSS.
		curr_dataset = g_array_index(datasetd, dataset_info, dataset_id);
		curr_imss = g_array_index(imssd, imss, curr_dataset.imss_d);

		// Set the corresponding.
		if (set_policy(&curr_dataset) == -1)
		{
			return -1;
		}

		current_dataset = dataset_id;
	}

	int32_t server;
	// Search for the server that is supposed to have the specified data element.
	// slog_debug("[get_data_location] curr_dataset.uri_=%s", curr_dataset.uri_);
	if ((server = find_server(curr_imss.info.num_storages, data_id, curr_dataset.uri_, op_type)) < 0)
	{
		slog_fatal("ERRIMSS_FIND_SERVER");
		return -1;
	}

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

	char key_[REQUEST_SIZE];
	// Key related to the requested data element.

	// Request the concerned block to the involved servers.
	// for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	for (int32_t i = 0; i < curr_imss.info.num_storages; i++)
	{
		ucp_ep_h ep = curr_imss.conns.eps[i];

		sprintf(key_, "GET 6 0 %s %s", old_dir, rdir_dest);
		// if (comm_send(curr_imss.conns.eps_[repl_servers[i]], key, key_length, 0) != key_length)
		if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) < 0)
		{
			perror("ERRIMSS_RLSIMSS_SENDADDR");
			return -1;
		}

		char result[RESPONSE_SIZE];
		if (recv_data(ucp_worker_data, ep, result, local_data_uid, 0) < 0)
		{
			perror("ERRIMSS_RECVDYNAMSTRUCT_RECV");
			return -1;
		}

		// Important to update
		// strcpy(curr_dataset.uri_,new_dataset_uri);
	}

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

	char key_[REQUEST_SIZE];

	// Request the concerned block to the involved servers.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		ucp_ep_h ep = curr_imss.conns.eps[repl_servers[i]];

		// Key related to the requested data element.
		sprintf(key_, "GET 5 0 %s,%s", old_dataset_uri, new_dataset_uri);
		fprintf(stderr, "Request - %s\n", key_);
		// printf("BLOCK %d ASKED TO %d SERVER with key: %s (%d)", data_id, repl_servers[i], key, key_length);
		if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) < 0)
		{
			perror("ERRIMSS_RLSIMSS_SENDADDR");
			return -1;
		}

		char result[RESPONSE_SIZE];
		if (recv_data(ucp_worker_data, ep, result, local_data_uid, 0) < 0)
		{
			perror("ERRIMSS_RECVDYNAMSTRUCT_RECV");
			return -1;
		}

		// Important to update
		strcpy(curr_dataset.uri_, new_dataset_uri);
	}

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

		if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) < 0)
		{
			perror("ERRIMSS_RLSIMSS_SENDADDR");
			return -1;
		}

		if (send_data(ucp_worker_data, ep, buf, size, local_data_uid) < 0)
		{
			perror("ERRIMSS_SETDATA_SEND");
			return -1;
		}
	}

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

		if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) < 0)
		{
			perror("ERRIMSS_RLSIMSS_SENDADDR");
			return -1;
		}

		// Receive data related to the previous read request directly into the buffer.
		if (recv_data(ucp_worker_data, ep, buffer, local_data_uid, 0) < 0)
		{
			if (errno != EAGAIN)
			{
				perror("ERRIMSS_GETDATA_RECV");
				return -1;
			}
			else
				break;
		}

		// Check if the requested key was correctly retrieved.
		if (strncmp((const char *)buffer, "$ERRIMSS_NO_KEY_AVAIL$", 22))
		{
			return 0;
		}
	}

	// slog_fatal( "ERRIMSS_GETDATA_UNAVAIL");
	return -1;
}

void *split_writev(void *th_argv)
{
	// Cast from generic pointer type to p_argv struct type pointer.
	thread_argv *arguments = (thread_argv *)th_argv;

	int32_t n_server;

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

		if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) < 0)
		{
			perror("ERRIMSS_RLSIMSS_SENDADDR");
			pthread_exit(NULL);
		}

		if (send_data(ucp_worker_data, ep, arguments->buffer, arguments->size * arguments->BLKSIZE * KB, local_data_uid) < 0)
		{
			perror("ERRIMSS_SETDATA_SEND");
			pthread_exit(NULL);
		}
	}

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

		if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) < 0)
		{
			perror("ERRIMSS_RLSIMSS_SENDADDR");
			pthread_exit(NULL);
		}

		// printf("[SPLIT READV] 4-Send_msg");
		if (send_data(ucp_worker_data, ep, arguments->msg, msg_length, local_data_uid) < 0)
		{
			perror("ERRIMSS_GETDATA_REQ");
			pthread_exit(NULL);
		}

		slog_debug("[IMSS] Request split_readv: client_id '%" PRIu32 "', mode '%s', key '%s', request '%s'", curr_imss.conns.id[repl_servers[i]], "GET", key_, arguments->msg);

		struct timeval start, end;
		/*long delta_us;
		  gettimeofday(&start, NULL);*/
		// Receive data related to the previous read request directly into the buffer.
		if (recv_data(ucp_worker_data, ep, arguments->buffer, local_data_uid, 0) < 0)
		{
			if (errno != EAGAIN)
			{
				perror("ERRIMSS_GETDATA_RECV");
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
		}
	}
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
int32_t get_data(int32_t dataset_id, int32_t data_id, char *buffer)
{
	slog_debug("[IMSS][get_data]");
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
	slog_debug("curr_dataset.repl_factor=%d", curr_dataset.repl_factor);
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
		slog_debug("[IMSS][get_data] Request - '%s' in repl_servers %ld", key_, repl_servers[i]);
		ep = curr_imss.conns.eps[repl_servers[i]];

		if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) < 0)
		{
			perror("ERRIMSS_RLSIMSS_SENDADDR");
			return -1;
		}

		/*	gettimeofday(&end, NULL);
			delta_us = (long) (end.tv_usec - start.tv_usec);
			printf("[CLIENT] [GET DATA] send petition delta_us=%6.3f",(delta_us/1000.0F));*/

		// t = clock() - t;
		// time_taken = ((double)t) / CLOCKS_PER_SEC; // in seconds
		// slog_debug("[IMSS][get_data] send_data %f s", time_taken);

		int size = 0;
		if (data_id)
			size = curr_dataset.data_entity_size;
		else
			size = sizeof(struct stat);

		//	gettimeofday(&start, NULL);
		// printf("GET_DATA after send petition to read");
		// Receive data related to the previous read request directly into the buffer.
		// t = clock();
		size_t length = 0;
		length = recv_data(ucp_worker_data, ep, buffer, local_data_uid, 0);
		if (length < 0)
		{
			if (errno != EAGAIN)
			{
				perror("ERRIMSS_GETDATA_RECV");
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
			return (int32_t)length;
		}
		else
		{
			slog_error("[IMSS][get_data]ERRIMSS_NO_KEY_AVAIL");
		}
	}

	return 1;
}

// Method retrieving a data element associated to a certain dataset.
int32_t get_ndata(int32_t dataset_id, int32_t data_id, char *buffer, size_t to_read, off_t offset)
{
	// slog_debug("[IMSS][get_data]");
	// slog_fatal("Caller name: %pS", __builtin_return_address(0));
	int32_t n_server;

	// Server containing the corresponding data to be retrieved.
	if ((n_server = TIMING(get_data_location(dataset_id, data_id, GET), "[imss_read]get_data_location", int32_t)) == -1)
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
		// slog_info("[IMSS][get_data] Request - '%s'", key_);
		ep = curr_imss.conns.eps[repl_servers[i]];
		slog_debug("[get_ndata] Sending request %s", key_);
		if (TIMING(send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_), "[imss_read]send_req", size_t) < 0)
		{
			perror("ERRIMSS_RLSIMSS_SENDADDR");
			return -1;
		}

		// Receive data related to the previous read request directly into the buffer.
		size_t length = 0;
		length = TIMING(recv_data(ucp_worker_data, ep, buffer, local_data_uid, 0), "[imss_read]recv_data", size_t);
		slog_debug("[get_ndata] Receiving request, length=%ld", length);
		if (length < 0)
		{
			if (errno != EAGAIN)
			{
				perror("ERRIMSS_GETDATA_RECV");
				return -1;
			}
			else
				break;
		}

		// Check if the requested key was correctly retrieved.
		if (strncmp((const char *)buffer, "$ERRIMSS_NO_KEY_AVAIL$", 22))
		{
			return (int32_t)length;
		}
		else
		{
			slog_debug("[IMSS][get_data]ERRIMSS_NO_KEY_AVAIL");
		}
	}

	return 1;
}

// Method retrieving a data element associated to a certain dataset.
int32_t get_data_mall(int32_t dataset_id, int32_t data_id, char *buffer, size_t to_read, off_t offset, int32_t num_storages)
{
	// slog_debug("[IMSS][get_data]");
	// slog_fatal("Caller name: %pS", __builtin_return_address(0));
	int32_t n_server;

	curr_imss.info.num_storages = num_storages;

	// Server containing the corresponding data to be retrieved.
	if ((n_server = TIMING(get_data_location(dataset_id, data_id, GET), "[imss_read]get_data_location", int32_t)) == -1)
	{
		return -1;
	}

	// Servers that the data block is going to be requested to.
	int32_t repl_servers[curr_dataset.repl_factor];
	int32_t curr_imss_storages = 0;
	curr_imss_storages = curr_imss.info.num_storages;
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
		// slog_info("[IMSS][get_data] Request - '%s'", key_);
		ep = curr_imss.conns.eps[repl_servers[i]];

		if (TIMING(send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_), "[imss_read]send_req", size_t) < 0)
		{
			perror("ERRIMSS_RLSIMSS_SENDADDR");
			return -1;
		}

		// Receive data related to the previous read request directly into the buffer.
		size_t length = 0;
		length = TIMING(recv_data(ucp_worker_data, ep, buffer, local_data_uid, 0), "[imss_read]recv_data", size_t);
		if (length < 0)
		{
			if (errno != EAGAIN)
			{
				perror("ERRIMSS_GETDATA_RECV");
				return -1;
			}
			else
				break;
		}

		// Check if the requested key was correctly retrieved.
		if (strncmp((const char *)buffer, "$ERRIMSS_NO_KEY_AVAIL$", 22))
		{
			return (int32_t)length;
		}
		else
		{
			slog_debug("[IMSS][get_data]ERRIMSS_NO_KEY_AVAIL");
		}
	}

	return 1;
}

// Method storing a specific data element.
int32_t set_data(int32_t dataset_id, int32_t data_id, char *buffer, size_t size, off_t offset)
{
	int32_t n_server;
	clock_t t;
	// size_t (*const send_choose_stream)(ucp_worker_h ucp_worker, ucp_ep_h ep, const char *msg, size_t msg_length) = (IMSS_WRITE_ASYNC == 1) ? send_istream : send_data;

	// slog_debug("[IMSS][set_data]");
	t = clock();

	// Server containing the corresponding data to be written.
	if ((n_server = get_data_location(dataset_id, data_id, SET)) == -1)
	{
		perror("ERRIMSS_GET_DATA_LOCATION");
		slog_error("ERRIMSS_GET_DATA_LOCATION");
		return -1;
	}
	char key_[REQUEST_SIZE];
	int32_t curr_imss_storages = curr_imss.info.num_storages;

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

		if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) < 0)
		{
			perror("ERRIMSS_RLSIMSS_SENDADDR");
			return -1;
		}

		// slog_debug("[IMSS][set_data] send_data(curr_imss.conns.id[%ld]:%ld, key_:%s, REQUEST_SIZE:%d)", n_server_, curr_imss.conns.id[n_server_], key_, REQUEST_SIZE);

		if (send_data(ucp_worker_data, ep, buffer, size, local_data_uid) < 0)
		{
			perror("ERRIMSS_SETDATA_SEND");
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
	return 1;
}

// Method storing a specific data element.
int32_t set_data_mall(int32_t dataset_id, int32_t data_id, char *buffer, size_t size, off_t offset, int32_t num_storages)
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

		if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) < 0)
		{
			perror("ERRIMSS_RLSIMSS_SENDADDR");
			return -1;
		}

		// slog_debug("[IMSS][set_data] send_data(curr_imss.conns.id[%ld]:%ld, key_:%s, REQUEST_SIZE:%d)", n_server_, curr_imss.conns.id[n_server_], key_, REQUEST_SIZE);

		if (send_data(ucp_worker_data, ep, buffer, size, local_data_uid) < 0)
		{
			perror("ERRIMSS_SETDATA_SEND");
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

		if (send_req(ucp_worker_data, ep, local_addr_data, local_addr_len_data, key_) < 0)
		{
			perror("ERRIMSS_RLSIMSS_SENDADDR");
			return -1;
		}

		// Send read request message specifying the block data.
		if (send_data(ucp_worker_data, ep, buffer, size, local_data_uid) < 0)
		{
			perror("ERRIMSS_SETDATA_SEND");
			return -1;
		}

		slog_debug("[IMSS] Request set_ndata: client_id '%" PRIu32 "', mode 'SET', key '%s'", curr_imss.conns.id[n_server_], key_);
	}

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
	switch (stat_dataset(dataset, &where_dataset))
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

// Method specifying the type (DATASET or IMSS INSTANCE) of a provided URI.
int32_t get_type(char *uri)
{
	ucp_ep_h ep;
	// Formated uri to be sent to the metadata server.
	char formated_uri[REQUEST_SIZE];

	slog_debug("[IMSS][get_type]");
	// Discover the metadata server that handles the entity.
	uint32_t m_srv = discover_stat_srv(uri);

	ep = stat_eps[m_srv];

	sprintf(formated_uri, "%" PRIu32 " GET 0 %s", stat_ids[m_srv], uri);
	// printf("get_type=%s",uri);
	// Send the request.
	if (send_req(ucp_worker_meta, ep, local_addr_meta, local_addr_len_meta, formated_uri) < 0)
	{
		perror("ERRIMSS_RLSIMSS_SENDADDR");
		return -1;
	}

	slog_info("[IMSS][get_type] Request - '%s'", formated_uri);
	// fprintf(stderr, "[IMSS] Request - '%s'\n", formated_uri);

	imss_info *data;

	// Receive the answer.
	char result[RESPONSE_SIZE];

	if (recv_dynamic_stream(ucp_worker_meta, ep, result, BUFFER, local_meta_uid) < 0)
	{
		slog_fatal("ERRIMSS_GETTYPE_REQ");
		return -1;
	}

	data = (imss_info *)result;

	// Determine what was retrieved from the metadata server.
	if (data->type == 'I')
		return 1;
	else if (data->type == 'D' || data->type == 'L')
		return 2;
	return 0;
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
