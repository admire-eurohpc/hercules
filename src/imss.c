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

uint32_t 	process_rank;		//Process identifier within the deployment.

uint32_t    n_stat_servers;     //Number of metadata servers available.
ucp_ep_h * 	stat_client;		//Metadata server sockets.
uint32_t *  stat_ids;
void * 		stat_mon;		    //Metadata monitoring socket.

int32_t		current_dataset;	//Dataset whose policy has been set last.
dataset_info	curr_dataset;		//Currently managed dataset.
imss		curr_imss;

GArray *	imssd;			//Set of IMSS metadata and connection structures currently used.
GArray *	free_imssd;		//Set of free entries within the 'imssd' vector.
int32_t		imssd_pos;		//Next position within the vextor were a new IMSS will be inserted.
int32_t		imssd_max_size;		//Maximum number of elements that could be introduced into the imss array.

GArray *	datasetd;		//Set of dataset metadata structures.
GArray *	free_datasetd;		//Set of free entries within the 'datasetd' vector.
int32_t		datasetd_pos;		//Next position within the vextor were a new dataset will be inserted.
int32_t		datasetd_max_size;	//Maximum number of elements that could be introduced into the dataset array.

int32_t 	tried_conn = 0;		//Flag specifying if a connection has been attempted.

char 		client_node[512];	//Node name where the client is running.
int32_t 	len_client_node;	//Length of the previous node name.
char		client_ip[16];		//IP number of the node where the client is taking execution.

dataset_info	empty_dataset;
imss		empty_imss;

int32_t		found_in;		//Variable storing the position where a certain structure was stored in a certain vector.

extern uint16_t	connection_port; //FIXME

char        att_deployment[URI_];


int32_t  IMSS_DEBUG = 0;
//int32_t  IMSS_WRITE_ASYNC = 1;

/* UCP objects */
ucp_context_h ucp_context_client;
ucp_worker_h  ucp_worker_client;

void * map_ep; // map_ep used for async write
int32_t is_client = 1; // also used for async write

//Method inserting an element into a certain control GArray vector.
	int32_t
GInsert (int32_t * pos,
		int32_t * max, 
		char *    item,
		GArray *  garray_insert,
		GArray *  garray_free)
{
	//Position where the element will be inserted.
	int32_t inserted_pos = -1;

	*pos = -1;

	//Updating the position where the following item will be inserted within a GArray.
	if (garray_free->len)
	{
		//Retrieve a free position from the existing wholes within the vector.

		*pos = g_array_index(garray_free, int32_t, 0);
		g_array_remove_index(garray_free, 0);
	}

	if (*pos == -1)
	{
		//Append an element into the corresponding array if there was no space left.

		g_array_append_val(garray_insert, *item);
		inserted_pos = ++(*max) - 1;
	}
	else
	{
		//Insert an element in a certain position within the provided garray.

		if (*pos < garray_insert->len)

			g_array_remove_index(garray_insert, *pos);

		g_array_insert_val(garray_insert, *pos, *item);
		inserted_pos = *pos;
	}

	return inserted_pos;
}

//Method inserting an element into a certain control GArray vector.
	int32_t
Get_fd (int32_t * pos,
		int32_t * max, 
		GArray *  garray_insert,
		GArray *  garray_free)
{
	//Position where the element will be inserted.
	int32_t inserted_pos = -1;

	*pos = -1;

	//Updating the position where the following item will be inserted within a GArray.
	if (garray_free->len)
	{
		//Retrieve a free position from the existing wholes within the vector.
		*pos = g_array_index(garray_free, int32_t, 0);
		//g_array_remove_index(garray_free, 0);
	}

	if (*pos == -1)
	{
		//Append an element into the corresponding array if there was no space left.
		//g_array_append_val(garray_insert, *item);
		inserted_pos = ++(*max) - 1;
	}
	else
	{
		//Insert an element in a certain position within the provided garray.

		if (*pos < garray_insert->len)
			//g_array_remove_index(garray_insert, *pos);

			//g_array_insert_val(garray_insert, *pos, *item);
			inserted_pos = *pos;
	}

	return inserted_pos;
}

//Check the existance within the session of the IMSS that the dataset is to be created in.
int32_t imss_check(char * dataset_uri)
{
	imss imss_;
	//Traverse the whole set of IMSS structures in order to find the one.
	for (int32_t i = 0; i < imssd->len; i++)
	{
		imss_ = g_array_index(imssd, imss, i);

		int32_t imss_uri_len = strlen(imss_.info.uri_);
		if ((imss_uri_len > 0) && !strncmp(dataset_uri, imss_.info.uri_, imss_uri_len))
			return i;
	}
	return -1;
}

//Method searching for a certain IMSS in the vector.
int32_t find_imss(char * imss_uri, imss * imss_)
{
	//Search for a certain IMSS within the vector.
	for (int32_t i = 0; i < imssd->len; i++)
	{
		*imss_ = g_array_index(imssd, imss, i);
		if (!strncmp(imss_uri, imss_->info.uri_, URI_))
			return i;
	}
	return -1;
}

//Method deleting a certains IMSS in the vector
	int32_t
delete_imss(char * imss_uri,
		imss * imss_)
{
	int32_t pos = find_imss(imss_uri, imss_);
	if(pos != -1){
		g_array_remove_index(imssd,pos);
		return 0;
	}else{
		return -1;
	}
}




/**********************************************************************************/
/********************* METADATA SERVICE MANAGEMENT FUNCTIONS  *********************/
/**********************************************************************************/


//Method creating a communication channel with the IMSS metadata server. Besides, the stat_imss method initializes a set of elements that will be used through the session.
int32_t stat_init(char *   stat_hostfile,
		uint16_t port,
		int32_t  num_stat_servers,
		uint32_t  rank)
{
	//Number of metadata servers to connect to.
	n_stat_servers = num_stat_servers;
	//Initialize memory required to deal with metadata sockets.
	stat_client = (ucp_ep_h *) malloc(n_stat_servers * sizeof(ucp_ep_h));
	//Dataset whose policy was set last.
	current_dataset = -1;
	//Rank of the current process.
	process_rank = rank;
	//Next position within the imss vector to be occupied.
	imssd_pos = 0;
	//Next position within the dataset vector to be occupied.
	datasetd_pos = 0;
	//Current size of the imss array.
	imssd_max_size = ELEMENTS;
	//Current size of the dataset array.
	datasetd_max_size = ELEMENTS;
	int ret = 0;

	if (getenv("IMSS_DEBUG") != NULL) {
		IMSS_DEBUG = 1;
	}

    DPRINT("[IMSS] Calling stat_init.\n");

	/* Initialize the UCX required objects */
	ret = init_context(&ucp_context_client, NULL, &ucp_worker_client, CLIENT_SERVER_SEND_RECV_STREAM);
	if (ret != 0) {
		perror("ERRIMSS_INIT_CONTEXT");
		return -1;
	}

	memset(&empty_dataset, 0, sizeof(dataset_info));
	memset(&empty_imss, 0, sizeof(imss));
	memset(&att_deployment, 0, URI_);
	//Initialize the set of GArrays dealing with the underlying set of structures.

	if ((imssd = g_array_sized_new (FALSE, FALSE, sizeof(imss), ELEMENTS)) == NULL)
	{
		perror("ERRIMSS_STATINIT_GARRAYIMSS");
		return -1;
	}
	if ((free_imssd = g_array_sized_new (FALSE, FALSE, sizeof(int32_t), ELEMENTS)) == NULL)
	{
		perror("ERRIMSS_STATINIT_GARRAYIMSSREG");
		return -1;
	}

	if ((datasetd = g_array_sized_new (FALSE, FALSE, sizeof(dataset_info), ELEMENTS)) == NULL)
	{
		perror("ERRIMSS_STATINIT_GARRAYDATASET");
		return -1;
	}

	if ((free_datasetd = g_array_sized_new (FALSE, FALSE, sizeof(int32_t), ELEMENTS)) == NULL)
	{
		perror("ERRIMSS_STATINIT_GARRAYDATASETREG");
		return -1;
	}

	//Fill the free positions arrays.
	for (int32_t i = 0; i < ELEMENTS; i++)
	{
		g_array_insert_val(free_imssd, i, i);
		g_array_insert_val(free_datasetd, i, i);
	}

	//Retrieve the hostname where the current process is running.
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

	strcpy(client_ip, inet_ntoa(*((struct in_addr*) host_entry->h_addr_list[0])));

	//FILE entity managing the IMSS metadata hostfile.
	FILE * stat_nodes;
	//Number of characters successfully read from the line.
	int n_chars;

	//Open the file containing the IMSS metadata server nodes.
	if ((stat_nodes = fopen(stat_hostfile, "r+")) == NULL)
	{
		perror("ERRIMSS_OPEN_STATFILE");
		return -1;
	}

	stat_ids = (uint32_t *) malloc (n_stat_servers * sizeof(uint32_t));

	char * stat_node = (char *) malloc(LINE_LENGTH);
	//Connect to all servers.
	char request[REQUEST_SIZE];

	for (int i = 0; i < n_stat_servers; i++)
	{
		ucs_status_t status;
		ucp_ep_h     client_ep;
		size_t l_size = LINE_LENGTH;

		//Save IMSS metadata deployment.
		n_chars = getline(&stat_node, &l_size, stat_nodes);
		//Erase the new line character ('\n') from the string.
		stat_node[n_chars - 1] = '\0';
		DPRINT("[IMSS] stat_client=%s\n",stat_node);
		DPRINT("[IMSS] i=%d, stat_node=%s, port=%d, rank=%" PRIu32 "\n",i,stat_node, port, rank);
		DPRINT("[IMSS] stat_int: Contacting stat dispatcher at %s:%d\n", stat_node, port);
		status = start_client(ucp_worker_client, stat_node, port, &client_ep); // port, rank,
		if (status != UCS_OK) {
			fprintf(stderr, "failed to start client (%s)\n", ucs_status_string(status));
			return -1;
		}

        sprintf (request, "%" PRIu32 " GET HELLO!", rank);
		if (send_stream(ucp_worker_client, client_ep, request, REQUEST_SIZE) < 0)
		{
			perror("ERRIMSS_STAT_HELLO");
			return -1;
		}

		char connection_info[RESPONSE_SIZE];
		if (recv_stream(ucp_worker_client, client_ep, connection_info, RESPONSE_SIZE) < 0)
		{
			perror("ERRIMSS_STAT_HELLO");
			return -1;
		}

		//Port that the new client must connect to.
		int32_t stat_port;
		//Separator.
		char host[255];

		//Read the previous information from the message received.
		sscanf(connection_info, "%[^':']:%d:%" PRIu32 "", host, &stat_port,  &stat_ids[i]);

        /* Close the endpoint to the server */
        ep_close(ucp_worker_client, client_ep, UCP_EP_CLOSE_MODE_FLUSH);

		// Create the connection to the metadata server dispatcher thread.
		if (!strcmp(host, "none"))
			status = start_client(ucp_worker_client, stat_node, stat_port, stat_client + i);
		else
			status = start_client(ucp_worker_client, host, stat_port, stat_client + i);

		if (status != UCS_OK) {
			fprintf(stderr, "failed to start client (%s)\n", ucs_status_string(status));
			return -1;
		}
		DPRINT("[IMSS] stat_init: Created endpoint with metadata server at %s:%d\n", stat_node, stat_port);
	}
	//Close the file.
	if (fclose(stat_nodes) != 0)
	{
		perror("ERR_CLOSE_STATFILE");
		return -1;
	}

	return 0;
}

//Method disabling the communication channel with the metadata server. Besides, the current method releases session-related elements previously initialized.
int32_t stat_release()
{
	DPRINT("[IMSS] Calling stat_release.\n");
	//Release the underlying set of vectors.

	/*	G_ARRAY_FREE:

		"Returns the element data if free_segment is FALSE, otherwise NULL.
		The element data should be freed using g_free()."

		Yet, comparing the result to != NULL or == NULL will 'fail'.

		The "g_array_free(GArray *, FALSE)" function will be returning the content
		iself if something was stored or NULL otherwise.
		*/

	g_array_free (imssd, FALSE);
	g_array_free (free_imssd, FALSE);
	g_array_free (datasetd, FALSE);
	g_array_free (free_datasetd, FALSE);

	//Disconnect from all metadata servers.
	for (int i = 0; i < n_stat_servers; i++)
	{
		char release_msg[REQUEST_SIZE];

        sprintf (release_msg, "%" PRIu32 " GET 2 RELEASE", process_rank);
		if (send_stream(ucp_worker_client, stat_client[i], release_msg, REQUEST_SIZE) < 0)
		{
			perror("ERRIMSS_RLSIMSS_SENDREQ");
			return -1;
		}

		ep_close(ucp_worker_client, stat_client[i], UCP_EP_CLOSE_MODE_FLUSH);
	}

	ucp_worker_destroy(ucp_worker_client);
	ucp_cleanup(ucp_context_client);
	return 0;
}

//Method discovering which metadata server is in charge of a certain URI.
uint32_t discover_stat_srv(char * _uri)
{
	//Calculate a crc from the provided URI.
	uint64_t crc_ = crc64(0, (unsigned char *) _uri, strlen(_uri));

	//Return the metadata server within the set that shall deal with the former entity.
	return crc_ % n_stat_servers;
}

//FIXME: fix implementation for multiple servers.
//Method retrieving the whole set of elements contained by a specific URI.
uint32_t get_dir(char * 	 requested_uri,
		char **  buffer,
		char *** items)
{

	int ret = 0;
	//Discover the metadata server that shall deal with the former URI.
	uint32_t m_srv = discover_stat_srv(requested_uri);

	//GETDIR request.
	char getdir_req[REQUEST_SIZE];
	sprintf(getdir_req, "%" PRIu32 " GET %d %s", stat_ids[m_srv],  GETDIR, requested_uri);

	//Send the request.
	if (send_stream(ucp_worker_client, stat_client[m_srv], getdir_req, REQUEST_SIZE) < 0)
	{
		perror("ERRIMSS_GETDIR_REQ");
		return -1;
	}

	size_t uris_size = 0;
	char msg[REQUEST_SIZE];
	if (recv_stream(ucp_worker_client, stat_client[m_srv], msg, REQUEST_SIZE) < 0)
	{
		perror("ERRIMSS_GETDIR_RECV");
		return -1;
	}

	uris_size = atoi(msg);
	char elements[uris_size];
	//Retrieve the set of elements within the requested uri.
	ret = recv_dynamic_stream(ucp_worker_client, stat_client[m_srv], elements, BUFFER);
	if (ret < 0)
	{
		perror("ERRIMSS_GETDIR_RECV");
		return -1;
	}


	if (!strncmp("$ERRIMSS_NO_KEY_AVAIL$", elements, 22))
	{
		fprintf(stderr, "ERRIMSS_GETDIR_NODIR\n");
		return -1;
	}

	uint32_t elements_size = ret; 

	//*buffer = (char *) malloc(sizeof(char)*elements_size);
	//memcpy(*buffer, elements, elements_size);
	//elements = *buffer;

	uint32_t num_elements = elements_size/URI_;
	*items = (char **) malloc(sizeof(char *) * num_elements);

	//Identify each element within the buffer provided.
	char * curr = elements;
	for (int32_t i = 0; i < num_elements; i++)
	{
		(*items)[i] = (char *) malloc (URI_);
		memcpy((*items)[i], curr, URI_);
		//(*items)[i] = elements;

		curr += URI_;
	}
	return num_elements;
}


/**********************************************************************************/
/***************** IN-MEMORY STORAGE SYSTEM MANAGEMENT FUNCTIONS  *****************/
/**********************************************************************************/


//Method initializing an IMSS deployment.
int32_t init_imss(char *   imss_uri,
		char *   hostfile,
		char *   meta_hostfile,
		int32_t  n_servers,
		uint16_t conn_port,
		uint64_t buff_size,
		uint32_t deployment,
		char *   binary_path,
		uint16_t metadata_port)
{
	int ret = 0;
	imss_info aux_imss;
    ucp_config_t *config;
	ucs_status_t status;

	if (getenv("IMSS_DEBUG") != NULL) {
		IMSS_DEBUG = 1;
	}

    if (IMSS_DEBUG) {
        status = ucp_config_read(NULL, NULL, &config);
		ucp_config_print(config, stderr, NULL, UCS_CONFIG_PRINT_CONFIG);
        ucp_config_release(config);
    }


	//Check if the new IMSS uri has been already assigned.
	int32_t existing_imss = stat_imss(imss_uri, &aux_imss);
	if (existing_imss) 
	{
		fprintf(stderr, "ERRIMSS_INITIMSS_ALREADYEXISTS\n");
		return -1;
	}
	//Once it has been notified that no other IMSS instance had the same URI, the deployment will be performed in case of a DETACHED instance.
	if (deployment == DETACHED)
	{
		if (!binary_path)
		{
			fprintf(stderr, "ERRIMSS_INITIMSS_NOBINARY\n");
			return -1;
		}

		char *command = (char *) calloc(2048, sizeof(char));

		FILE *in_file = fopen(meta_hostfile, "r");
		if (!in_file)
		{
			perror("fopen");
			return 0;
		}
		struct stat sb;
		if (stat(meta_hostfile, &sb) == -1) {
			perror("stat");
			return 0;
		}

		char *textRead = (char*)malloc(sb.st_size);
		int i = 1;

		while (fscanf(in_file, "%[^\n] ", textRead) != EOF)
		{
			//printf("Line %d is  %s\n",i, textRead);
			//sprintf(command, "mpirun -np %d -hostfile %s %s %s %d %lu %s %d %d %s %d &", n_servers, hostfile, binary_path, imss_uri, conn_port, buff_size, textRead,metadata_port, n_servers ,hostfile, THREAD_POOL);
			i++;
		}
		fclose(in_file);
		//Original
		//sprintf(command, "mpirun -np %d -f %s %s %s %d %lu foo %d %d %s &", n_servers, hostfile, binary_path, imss_uri, conn_port, buff_size, 0, n_servers, "");
		//sprintf(command, "mpirun -np %d -hostfile %s %s %s %d %lu foo %d %d %s &", n_servers, hostfile, binary_path, imss_uri, conn_port, buff_size, 0, n_servers, "");

		//Imss server(data)
		//mpirun -np $num_servers -f $imss_hostfile $server_binary $imss_uri $imss_port_number $imss_buffer_size $metadata_server_address $metadata_server_port $num_servers $imss_hostfile $io_threads &
		//["imss://", "5555", "1048576000", "compute-6-2", "5569", "1", "./hostfile", "1"]
		//sprintf(command, "mpirun -np %d -hostfile %s %s %s %d %lu %s %d %d %s %d &", n_servers, hostfile, binary_path, imss_uri, conn_port, buff_size, meta_hostfile, metadata_port, n_servers ,hostfile, THREAD_POOL);
		sprintf(command, "mpirun.mpich -np %d -f %s %s %s %d %lu %s %d %d %s %d &", 2, hostfile, binary_path, imss_uri, conn_port, buff_size, textRead,metadata_port, 2 ,hostfile, THREAD_POOL);

		sprintf(command, "mpirun -np %d -hostfile %s %s %s %d %lu %s %d %d", n_servers, hostfile, binary_path, imss_uri, conn_port, buff_size ,"compute-6-2", 5569, n_servers);

		printf("command=%s\n",command);
		//Perform the deployment (FROM LINUX MAN PAGES: "system() returns after the command has been completed").
		if (system(command) == -1)
		{
			perror("ERRIMSS_INITIMSS_DEPLOY");
			return -1;
		}
		free(command);
	}
	//IMSS creation.

	imss new_imss;
	strcpy(new_imss.info.uri_, imss_uri);
	new_imss.info.num_storages  = n_servers;
	new_imss.info.conn_port     = conn_port;
	new_imss.info.type     = 'I';

	if (deployment == ATTACHED)
		new_imss.info.conn_port     = connection_port;

	new_imss.info.ips           = (char **) malloc(n_servers * sizeof(char *));

	//Resources required to connect to the corresponding IMSS.
	new_imss.conns.eps_ = (ucp_ep_h *) malloc(n_servers * sizeof(ucp_ep_h));

	//FILE entity managing the IMSS hostfile.
	FILE * svr_nodes;
	//Open the file containing the IMSS server nodes.
	if ((svr_nodes = fopen(hostfile, "r+")) == NULL)
	{
		perror("ERRIMSS_OPEN_FILE");
		return -1;
	}
	//Number of characters successfully read from the line.
	int n_chars;

	new_imss.conns.matching_server = -1;

	new_imss.conns.id = (uint32_t *) malloc (n_servers * sizeof(uint32_t));

	//Connect to all servers.
	for (int i = 0; i < n_servers; i++)
	{
		ucs_status_t status;
		//Allocate resources in the metadata structure so as to store the current IMSS's IP.
		(new_imss.info.ips)[i] = (char *) malloc(LINE_LENGTH);
		size_t l_size = LINE_LENGTH;
		//Save IMSS metadata deployment.
		n_chars = getline(&((new_imss.info.ips)[i]), &l_size, svr_nodes);

		//Erase the new line character ('\n') from the string.
		((new_imss.info.ips)[i])[n_chars - 1] = '\0';

		//Save the current socket value when the IMSS ip matches the clients' one.
		if (!strncmp((new_imss.info.ips)[i], client_node, len_client_node) || !strncmp((new_imss.info.ips)[i], client_ip, strlen(new_imss.info.ips[i])))
		{
			new_imss.conns.matching_server = i;
			strcpy(att_deployment, imss_uri);
		}
		//Create the connection to the IMSS server dispatcher thread.
		DPRINT("[IMSS] imss_init: Contacting dispatcher at %s:%d\n",  (new_imss.info.ips)[i], new_imss.info.conn_port);
		status = start_client(ucp_worker_client, (new_imss.info.ips)[i], new_imss.info.conn_port, &(new_imss.conns.eps_[i])); // port, rank,
		if (status != UCS_OK) {
			fprintf(stderr, "failed to start client (%s)\n", ucs_status_string(status));
			return -1;
		}

		process_rank = CLOSE_EP;

		char request[REQUEST_SIZE];
		sprintf(request, "%" PRIu32 " GET HELLO! %ld %s", process_rank, buff_size, imss_uri);
		//Send the IMSS server connection request.
		if (send_stream(ucp_worker_client, new_imss.conns.eps_[i], request, REQUEST_SIZE) < 0)
		{
			perror("ERRIMSS_INITIMSS_HELLO");
			return -1;
		}

		char connection_info[RESPONSE_SIZE];
		if (recv_stream(ucp_worker_client, new_imss.conns.eps_[i], connection_info, RESPONSE_SIZE) < 0)
		{
			perror("ERRIMSS_INITIMSS_CONNINFOINIT");
			return -1;
		}
		//Close the previous connection.
		ep_close(ucp_worker_client, new_imss.conns.eps_[i], UCP_EP_CLOSE_MODE_FLUSH);

		//Port that the new client must connect to.
		int32_t imss_port;
		//ID that the new client must take.
		int32_t imss_id;
		//Separator.
		char host[256];
		//Read the previous information from the message received.
		sscanf(connection_info, "%[^':']:%d:%" PRIu32 "", host, &imss_port, &new_imss.conns.id[i]);

		//Create the connection to the metadata server dispatcher thread.
		if(!strcmp(host, "none"))
			status = start_client(ucp_worker_client, (new_imss.info.ips)[i], imss_port, &(new_imss.conns.eps_[i])); // port, rank,
		else
			status = start_client(ucp_worker_client, host, imss_port, &(new_imss.conns.eps_[i])); // port, rank,

		if (status != UCS_OK) {
			fprintf(stderr, "failed to start client (%s)\n", ucs_status_string(status));
			return -1;
		}

		DPRINT("[IMSS]: Created endpoint with %s:%d\n",  (new_imss.info.ips)[i], new_imss.info.conn_port);
	}
	//Close the file.
	if (fclose(svr_nodes) != 0)
	{
		perror("ERR_CLOSE_FILE");
		return -1;
	}

	//Discover the metadata server that shall deal with the new IMSS instance.
	uint32_t m_srv = discover_stat_srv(new_imss.info.uri_);

	//Send the created structure to the metadata server.
	char key_plus_size[REQUEST_SIZE];
	sprintf(key_plus_size, "%" PRIu32 " SET %lu %s", stat_ids[m_srv], (sizeof(imss_info)+new_imss.info.num_storages*LINE_LENGTH), new_imss.info.uri_);

	if (send_stream(ucp_worker_client, stat_client[m_srv], key_plus_size, REQUEST_SIZE) < 0) // SNDMORE
	{
		perror("ERRIMSS_INITIMSS_SENDKEY");
		return -1;
	}

	//Send the new IMSS metadata structure to the metadata server entity.
	if (send_dynamic_stream(ucp_worker_client, stat_client[m_srv], (void *) &new_imss.info, IMSS_INFO) < 0)
		return -1;

	ep_flush(stat_client[m_srv], ucp_worker_client);
	//Add the created struture into the underlying IMSS vector.
	GInsert (&imssd_pos, &imssd_max_size, (char *) &new_imss, imssd, free_imssd);

	return 0;
}

//Method initializing the required resources to make use of an existing IMSS.
int32_t open_imss(char * imss_uri)
{
	//New IMSS structure storing the entity to be created.
	imss new_imss;

    ucp_config_t *config;
    ucs_status_t status;

    if (getenv("IMSS_DEBUG") != NULL) {
        IMSS_DEBUG = 1;
    }

    if (IMSS_DEBUG) {
        status = ucp_config_read(NULL, NULL, &config);
        ucp_config_print(config, stderr, NULL, UCS_CONFIG_PRINT_CONFIG);
        ucp_config_release(config);
    }

	int32_t not_initialized = 0;

	DPRINT("[IMSS] open_imss: starting function\n");
	//Retrieve the actual information from the metadata server.
	int32_t imss_existance = stat_imss(imss_uri, &new_imss.info);
	//Check if the requested IMSS did not exist or was already stored in the local vector.
	switch (imss_existance)
	{
		case 0:
			{
				fprintf(stderr, "ERRIMSS_OPENIMSS_NOTEXISTS\n");
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

	new_imss.conns.eps_ = (ucp_ep_h *) malloc(new_imss.info.num_storages*sizeof(ucp_ep_h));
	new_imss.conns.id = (uint32_t *) malloc(new_imss.info.num_storages*sizeof(uint32_t));

	new_imss.conns.matching_server = -1;

	//Connect to the requested IMSS.
	for  (int32_t i = 0; i < new_imss.info.num_storages; i++)
	{
		ucs_status_t status;
		DPRINT("[IMSS] open_imss: contacting dispatcher at %s:%d\n",(new_imss.info.ips)[i], new_imss.info.conn_port);
		status = start_client(ucp_worker_client, (new_imss.info.ips)[i], new_imss.info.conn_port,  &(new_imss.conns.eps_[i])); // port, rank,
		if (status != UCS_OK) {
			fprintf(stderr, "failed to start client (%s)\n", ucs_status_string(status));
			return -1;
		}

		char request[REQUEST_SIZE];
		sprintf(request, "%" PRIu32 " GET %s", process_rank, "HELLO!JOIN");
		//Send the IMSS server connection request.
		if (send_stream(ucp_worker_client, new_imss.conns.eps_[i], request, REQUEST_SIZE) < 0)
		{
			perror("ERRIMSS_OPENIMSS_HELLO");
			return -1;
		}

		//ZMQ message retrieving the connection information.
		char connection_info[RESPONSE_SIZE];
		if (recv_stream(ucp_worker_client, new_imss.conns.eps_[i], connection_info, RESPONSE_SIZE) < 0)
		{
			perror("ERRIMSS_OPENIMSS_CONNINFOOPEN");
			return -1;
		}

		//Close the previous connection.
		ep_close(ucp_worker_client, new_imss.conns.eps_[i], UCP_EP_CLOSE_MODE_FLUSH);

		//Port that the new client must connect to.
		int32_t imss_port;
		//ID that the new client must take.
		int32_t imss_id;
		//Separator.
		char sep_;

		//Read the previous information from the message received.
		sscanf(connection_info, "%d%c%d", &imss_port, &sep_, &imss_id);

		//Create the connection to the metadata server dispatcher thread.
		status = start_client(ucp_worker_client, (new_imss.info.ips)[i], imss_port,  &(new_imss.conns.eps_[i])); // port, rank,
		if (status != UCS_OK) {
			fprintf(stderr, "failed to start client (%s)\n", ucs_status_string(status));
			return -1;
		}

		new_imss.conns.id[i] = imss_id;

		//Save the current socket value when the IMSS ip matches the clients' one.
		if (!strncmp((new_imss.info.ips)[i], client_node, len_client_node) || !strncmp((new_imss.info.ips)[i], client_ip, strlen(new_imss.info.ips[i])))
		{
			new_imss.conns.matching_server = i;
			strcpy(att_deployment, imss_uri);
		}
		DPRINT("[IMSS] open_imss: Created endpoint with %s:%d\n",(new_imss.info.ips)[i], imss_port);

	}

	//If the struct was found within the vector but uninitialized, once updated, store it in the same position.
	if (not_initialized)
	{
		g_array_remove_index(imssd, found_in);
		g_array_insert_val(imssd, found_in, new_imss);

		return 0;
	}

	//Add the created struture into the underlying IMSSs.
	GInsert (&imssd_pos, &imssd_max_size, (char *) &new_imss, imssd, free_imssd);

	return 0;
}

// Method releasing client-side and/or server-side resources related to a certain IMSS instance. 
int32_t release_imss(char * imss_uri, uint32_t release_op)
{
	//Search for the requested IMSS.

	imss imss_;
	int32_t imss_position;
	if ((imss_position = find_imss(imss_uri, &imss_)) == -1)
	{
		fprintf(stderr, "ERRIMSS_RLSIMSS_NOTFOUND\n");
		return -1;
	}

	//Release the set of connections to the corresponding IMSS.

	for (int32_t i = 0; i < imss_.info.num_storages; i++)
	{
		//Request IMSS instance closure per server if the instance is a DETACHED one and the corresponding argumet was provided.
		if (release_op == CLOSE_DETACHED)
		{
			char release_msg[REQUEST_SIZE];
            sprintf(release_msg, "%" PRIu32 " GET 2 RELEASE", process_rank);

			if (send_stream(ucp_worker_client, imss_.conns.eps_[i], release_msg, REQUEST_SIZE) < 0)
			{
				perror("ERRIMSS_RLSIMSS_SENDREQ");
				return -1;
			}
		}

		ep_close(ucp_worker_client, imss_.conns.eps_[i], UCP_EP_CLOSE_MODE_FLUSH);
		free(imss_.info.ips[i]);
	}

	free(imss_.info.ips);

	g_array_remove_index(imssd, imss_position);
	g_array_insert_val(imssd, imss_position, empty_imss);
	//Add the released position to the set of free positions.
	g_array_append_val(free_imssd, imss_position);

	if (!memcmp(att_deployment, imss_uri, URI_))
		memset(att_deployment, '\0', URI_);

	return 0;
}

//Method retrieving information related to a certain IMSS instance.
int32_t stat_imss(char *      imss_uri, imss_info * imss_info_)
{
	//Check for the IMSS info structure in the local vector.
	int32_t imss_found_in;
	imss searched_imss;
	int ret = 0;

	DPRINT("[IMSS] Calling stat_imss.\n");
	if ((imss_found_in = find_imss(imss_uri, &searched_imss)) != -1)
	{
		memcpy(imss_info_, &searched_imss.info, sizeof(imss_info));
		imss_info_->ips = (char **) malloc((imss_info_->num_storages)*sizeof(char *));
		for (int32_t i = 0; i < imss_info_->num_storages; i++)
		{
			imss_info_->ips[i] = (char *) malloc(LINE_LENGTH*sizeof(char));
			strcpy(imss_info_->ips[i], searched_imss.info.ips[i]);
		}

		return 2;
	}

	//Formated imss uri to be sent to the metadata server.
	char formated_uri[REQUEST_SIZE];

	//Discover the metadata server that handles the IMSS instance.
	uint32_t m_srv = discover_stat_srv(imss_uri);
	sprintf(formated_uri, "%" PRIu32 " GET 0 %s", stat_ids[m_srv], imss_uri);

	//Send the request.
	if (send_stream(ucp_worker_client, stat_client[m_srv], formated_uri, REQUEST_SIZE) < 0)
	{
		fprintf(stderr, "ERRIMSS_IMSS_REQ\n");
		return -1;
	}

	ret = recv_dynamic_stream(ucp_worker_client, stat_client[m_srv], (char *)imss_info_, IMSS_INFO);
	if (ret < sizeof(imss_info))
		return 0;
	return 1;
}

//Method providing the URI of the attached IMSS instance.
char * get_deployed() {
	if (att_deployment[0] != '\0')
	{
		char * att_dep_uri = (char *) malloc(URI_ * sizeof(char));
		strcpy(att_dep_uri, att_deployment);
		return att_dep_uri;
	}

	return NULL;
}

//Method providing the URI of the instance deployed in some endpoint.
char * get_deployed(char * endpoint) {
	return endpoint;
}



/**********************************************************************************/
/************************** DATASET MANAGEMENT FUNCTIONS **************************/
/**********************************************************************************/

//Method creating a dataset and the environment enabling READ or WRITE operations over it.
int32_t create_dataset(char *  dataset_uri,
		char *  policy,
		int32_t num_data_elem,
		int32_t data_elem_size,
		int32_t repl_factor)
{	
	int err = 0;

	DPRINT("[IMSS] dataset_create: starting.\n");

	curr_imss = g_array_index(imssd, imss, curr_dataset.imss_d);

	if ((dataset_uri == NULL) || (policy == NULL) || !num_data_elem || !data_elem_size)
	{
		fprintf(stderr, "ERRIMSS_CRTDATASET_WRONGARG\n");
		return -EINVAL;
	}
	if ((repl_factor < NONE) || (repl_factor > TRM))
	{
		fprintf(stderr, "ERRIMSS_CRTDATASET_BADREPLFACTOR\n");
		return -EINVAL;
	}

	int32_t associated_imss_indx;
	//Check if the IMSS storing the dataset exists within the clients session.
	if ((associated_imss_indx = imss_check(dataset_uri)) == -1)
	{
		DPRINT("[IMSS] create_dataset: ERRIMSS_OPENDATA_IMSSNOTFOUND\n");
		fprintf(stderr, "ERRIMSS_OPENDATA_IMSSNOTFOUND\n");
		return -ENOENT;
	}

	imss associated_imss;
	associated_imss = g_array_index(imssd, imss, associated_imss_indx);

	dataset_info new_dataset;

	//Dataset metadata request.
	/*if (stat_dataset(dataset_uri, &new_dataset))
	{
		DPRINT("[IMSS] create_dataset: ERRIMSS_CREATEDATASET_ALREADYEXISTS\n");
		return -EEXIST;
	}*/

	stat_dataset(dataset_uri, &new_dataset);


	//Save the associated metadata of the current dataset.
	strcpy(new_dataset.uri_, 	dataset_uri);
	strcpy(new_dataset.policy, 	policy);
	new_dataset.num_data_elem 	 = num_data_elem;
	new_dataset.data_entity_size = data_elem_size*1024;//dataset in kilobytes
	new_dataset.imss_d 		     = associated_imss_indx;
	new_dataset.local_conn 		 = associated_imss.conns.matching_server;
	new_dataset.repl_factor		 = repl_factor;
	new_dataset.size 			 = 0;
	new_dataset.n_servers		 = curr_imss.info.num_storages;

	//*****NEXT LINE NEED FOR DIFERENT POLICIES TO WORK IN DISTRIBUTED*****//
	strcpy(new_dataset.original_name, dataset_uri);
	//*****BEFORE LINE NEED FOR DIFERENT POLICIES TO WORK IN DISTRIBUTED*****//

	//Size of the message to be sent.
	uint64_t msg_size = sizeof(dataset_info);

	//Reserve memory so as to store the position of each data element if the dataset is a LOCAL one.
	if (!strcmp(new_dataset.policy, "LOCAL"))
	{
		uint32_t info_size = new_dataset.num_data_elem * sizeof(uint16_t);

		/*new_dataset.data_locations = (uint16_t *) malloc(info_size);
		  memset(new_dataset.data_locations, 0, info_size);*/
		new_dataset.data_locations = (uint16_t *) calloc(info_size,sizeof(uint16_t));

		//Specify that the created dataset is a LOCAL one.
		new_dataset.type = 'L';

		//Add additional bytes that will be sent.
		msg_size += info_size;
	}
	else
		new_dataset.type = 'D';

	//Discover the metadata server that handle the new dataset.
	uint32_t m_srv = discover_stat_srv(new_dataset.uri_);

	char formated_uri[REQUEST_SIZE];
	sprintf(formated_uri, "%" PRIu32 " SET %lu %s", stat_ids[m_srv], msg_size, new_dataset.uri_);
	DPRINT("[IMSS] dataset_create: sending request %s.\n", formated_uri );
	//Send the dataset URI associated to the dataset metadata structure to be sent.
	if (send_stream(ucp_worker_client, stat_client[m_srv], formated_uri, REQUEST_SIZE) < 0) // SNDMORE
	{
		perror("ERRIMSS_DATASET_SNDURI");
		return -1;
	}

	DPRINT("[IMSS] dataset_create: sending dataset_info\n");
	//Send the new dataset metadata structure to the metadata server entity.
	if (send_dynamic_stream(ucp_worker_client, stat_client[m_srv], (void *) &new_dataset, DATASET_INFO) < 0)
	{
		perror("ERRIMSS_DATASET_SNDDS");
		return -1;
    }
	DPRINT("[IMSS] dataset_create: sent dataset_info\n");
	//Initialize dataset fields monitoring the dataset itself if it is a LOCAL one.
	if (!strcmp(new_dataset.policy, "LOCAL"))
	{
		//Current number of blocks written by the client.
		new_dataset.num_blocks_written = (uint64_t *) malloc(1*sizeof(uint64_t));
		*(new_dataset.num_blocks_written) = 0;
		//Specific blocks written by the client.

		/*new_dataset.blocks_written = (uint32_t *) malloc(new_dataset.num_data_elem*sizeof(uint32_t));
		  memset(new_dataset.blocks_written, 0, new_dataset.num_data_elem*sizeof(uint32_t));*/

		new_dataset.blocks_written = (uint32_t *) calloc(new_dataset.num_data_elem, sizeof(uint32_t));
	}

	//	//Set the specified policy.
	//	if (set_policy(&new_dataset) == -1)
	//	{
	//		perror("ERRIMSS_DATASET_SETPLCY");
	//		return -1;
	//	}

	//Add the created struture into the underlying IMSSs.
	err = GInsert (&datasetd_pos, &datasetd_max_size, (char *) &new_dataset, datasetd, free_datasetd);
	DPRINT("[IMSS] dataset_create: GIsinser %d\n", err);
	return err;
}

//Method creating the required resources in order to READ and WRITE an existing dataset.
	int32_t
open_dataset(char * dataset_uri)
{
	int32_t associated_imss_indx;
	//printf("OPEN DATASET INSIDE dataset_uri=%s\n",dataset_uri);
	//Check if the IMSS storing the dataset exists within the clients session.
	if ((associated_imss_indx = imss_check(dataset_uri)) == -1)
	{
		fprintf(stderr, "ERRIMSS_OPENDATA_IMSSNOTFOUND\n");
		return -1;
	}

	imss associated_imss;
	associated_imss = g_array_index(imssd, imss, associated_imss_indx);

	dataset_info new_dataset;
	//Dataset metadata request.
	int32_t stat_dataset_res = stat_dataset(dataset_uri, &new_dataset);
	int32_t not_initialized = 0;

	//Check if the requested dataset did not exist or was already stored in the local vector.
	switch (stat_dataset_res)
	{
		case 0:
			{
				fprintf(stderr, "ERRIMSS_OPENDATASET_NOTEXISTS\n");
				return -1;
			}
		case 2:
			{
				if (new_dataset.local_conn != -2)
				{
					fprintf(stderr, "ERRIMSS_OPENDATASET_ALREADYSTORED\n");
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

	//Assign the associated IMSS descriptor to the new dataset structure.
	new_dataset.imss_d 	= associated_imss_indx;
	new_dataset.local_conn 	= associated_imss.conns.matching_server;

	//Initialize dataset fields monitoring the dataset itself if it is a LOCAL one.
	if (!strcmp(new_dataset.policy, "LOCAL"))
	{
		//Current number of blocks written by the client.
		new_dataset.num_blocks_written = (uint64_t *) malloc(1*sizeof(uint64_t));
		*(new_dataset.num_blocks_written) = 0;
		//Specific blocks written by the client.
		/*new_dataset.blocks_written = (uint32_t *) malloc(new_dataset.num_data_elem*sizeof(uint32_t));
		  memset(new_dataset.blocks_written, '\0', new_dataset.num_data_elem*sizeof(uint32_t));*/

		new_dataset.blocks_written = (uint32_t *) calloc(new_dataset.num_data_elem, sizeof(uint32_t));
	}

	//	//Set the specified policy.
	//	if (set_policy(&new_dataset) == -1)
	//	{
	//		perror("ERRIMSS_DATASET_SETPLCY");
	//		return -1;
	//	}

	//If the struct was found within the vector but uninitialized, once updated, store it in the same position.
	if (not_initialized)
	{
		g_array_remove_index(datasetd, found_in);
		g_array_insert_val(datasetd, found_in, new_dataset);

		return found_in;
	}

	//Add the created struture into the underlying IMSSs.
	return (GInsert (&datasetd_pos, &datasetd_max_size, (char *) &new_dataset, datasetd, free_datasetd));
}

//Method releasing the set of resources required to deal with a dataset.
int32_t release_dataset(int32_t dataset_id)
{
	//Check if the provided descriptor corresponds to a position within the vector.
	if ((dataset_id < 0) || (dataset_id >= datasetd_max_size))
	{
		fprintf(stderr, "ERRIMSS_RELDATASET_BADDESCRIPTOR\n");
		return -1;
	}

	//Dataset to be released.
	dataset_info release_dataset = g_array_index(datasetd, dataset_info, dataset_id);

	//If the dataset is a LOCAL one, the position of the data elements must be updated.
	if (!strcmp(release_dataset.policy, "LOCAL"))
	{
		//Discover the metadata server that handles the dataset.
		uint32_t m_srv = discover_stat_srv(release_dataset.uri_);

		//Formated dataset uri to be sent to the metadata server.
		char formated_uri[REQUEST_SIZE];
		sprintf(formated_uri, "%" PRIu32 " SET 0 %s", stat_ids[m_srv], release_dataset.uri_);
		//Send the LOCAL dataset positions update.
		if (send_stream(ucp_worker_client, stat_client[m_srv], formated_uri, REQUEST_SIZE) < 0) // SNDMORE
		{
			perror("ERRIMSS_RELDATASET_SENDURI");
			return -1;
		}

		//Format update message to be sent to the metadata server.

		uint64_t blocks_written_size = *(release_dataset.num_blocks_written) * sizeof(uint32_t);
		uint64_t update_msg_size = 8 + blocks_written_size;

		/*char update_msg[update_msg_size];
		  memset(update_msg, '\0', update_msg_size);*/
		char * update_msg = (char *) calloc(update_msg_size,sizeof(char));

		uint16_t update_value = (release_dataset.local_conn + 1);
		memcpy(update_msg, release_dataset.blocks_written, blocks_written_size);
		memcpy((update_msg+blocks_written_size), &update_value, sizeof(uint16_t));

		//Send the list of servers storing the data elements.
		char mode2[] = "SET";
		if (send_stream(ucp_worker_client, stat_client[m_srv], mode2, MODE_SIZE) < 0)
		{
			perror("ERRIMSS_STAT_HELLO");
			return -1;
		}

		if (send_stream(ucp_worker_client, stat_client[m_srv], update_msg, REQUEST_SIZE) < 0)
		{
			perror("ERRIMSS_RELDATASET_SENDPOSITIONS");
			return -1;
		}

		char update_result[RESPONSE_SIZE];

		if (recv_stream(ucp_worker_client, stat_client[m_srv], update_result, RESPONSE_SIZE) < 0)
		{
			perror("ERRIMSS_RELDATASET_RECVUPDATERES");
			return -1;
		}

		if (strcmp(update_result, "UPDATED!"))
		{
			perror("ERRIMSS_RELDATASET_UPDATE");
			return -1;
		}

		//Free the data locations vector.
		free(release_dataset.data_locations);
		//Freem the monitoring vector.
		free(release_dataset.blocks_written);
		free(release_dataset.num_blocks_written);
		free(update_msg);
	}

	g_array_remove_index(datasetd, dataset_id);
	g_array_insert_val(datasetd, dataset_id, empty_dataset);
	//Add the index to the set of free positions within the dataset vector.
	g_array_append_val(free_datasetd, dataset_id);

	return 0;
}

//Method deleting a dataset.
int32_t delete_dataset(const char * 	    dataset_uri)
{
	//Formated dataset uri to be sent to the metadata server.
	char formated_uri[REQUEST_SIZE];

	//Discover the metadata server that handles the dataset.
	uint32_t m_srv = discover_stat_srv((char *) dataset_uri);

	sprintf(formated_uri, "%" PRIu32 " GET 4 %s", stat_ids[m_srv], dataset_uri); // delete
	//Send the request.
	if (send_stream(ucp_worker_client, stat_client[m_srv], formated_uri, REQUEST_SIZE) < 0)
	{
		perror("ERRIMSS_DATASET_REQ");
		return -1;
	}

	char result[RESPONSE_SIZE];
	if (recv_stream(ucp_worker_client, stat_client[m_srv], result, RESPONSE_SIZE) < 0)
	{
		perror("ERRIMSS_RECVDYNAMSTRUCT_RECV");
		return -1;
	}

	return 1;
}

int32_t  rename_dataset_metadata_dir_dir(char * old_dir, char * rdir_dest){

	/*********RENAME GARRAY DATASET*******/
	dataset_info dataset_info_;

	for (int32_t i = 0; i < datasetd->len; i++)
	{
		dataset_info_ = g_array_index(datasetd, dataset_info, i);

		if(strstr(dataset_info_.uri_, old_dir) != NULL) {
			char * path = dataset_info_.uri_;

			size_t len = strlen(old_dir);
			if (len > 0) {
				char *p = path;
				while ((p = strstr(p, old_dir)) != NULL) {
					memmove(p, p + len, strlen(p + len) + 1);
				}
			}
			//char * new_path = (char *) malloc(strlen(rdir_dest) + 1); 
			char * new_path = (char *) malloc(256); 
			strcpy(new_path, rdir_dest);
			strcat(new_path,"/");
			strcat(new_path,path);

			strcpy(dataset_info_.uri_,new_path);
			g_array_remove_index(datasetd,i);
			g_array_insert_val(datasetd,i,dataset_info_);
		}

	}

	/*********RENAME METADATA*******/
	//Formated dataset uri to be sent to the metadata server.
	char formated_uri[REQUEST_SIZE];

	//Discover the metadata server that handles the dataset.
	uint32_t m_srv = discover_stat_srv((char *) old_dir);

	//Send the request.
	sprintf(formated_uri, "%" PRIu32 " GET 6 %s %s", stat_ids[m_srv], old_dir,rdir_dest);
	if (send_stream(ucp_worker_client, stat_client[m_srv], formated_uri, REQUEST_SIZE) < 0)
	{
		perror("ERRIMSS_DATASET_REQ");
		return -1;
	}

	char result[RESPONSE_SIZE];
	if (recv_stream(ucp_worker_client, stat_client[m_srv], result, RESPONSE_SIZE) < 0)
	{
		perror("ERRIMSS_RECVDYNAMSTRUCT_RECV");
		return -1;
	}

	return 0;
}

int32_t 
rename_dataset_metadata(char * old_dataset_uri, char * new_dataset_uri){

	/*********RENAME GARRAY DATASET*******/
	dataset_info dataset_info_;

	for (int32_t i = 0; i < datasetd->len; i++)
	{
		dataset_info_ = g_array_index(datasetd, dataset_info, i);
		if (!strcmp(old_dataset_uri, dataset_info_.uri_)){
			strcpy(dataset_info_.uri_,new_dataset_uri);
			g_array_remove_index(datasetd,i);
			g_array_insert_val(datasetd,i,dataset_info_);
		}
	}

	/*********RENAME METADATA*******/
	//Formated dataset uri to be sent to the metadata server.
	char formated_uri[REQUEST_SIZE];

	//Discover the metadata server that handles the dataset.
	uint32_t m_srv = discover_stat_srv((char *) old_dataset_uri);

	//Send the request.
	sprintf(formated_uri, "%" PRIu32 " GET 5 %s %s", stat_ids[m_srv], old_dataset_uri,new_dataset_uri);
	if (send_stream(ucp_worker_client, stat_client[m_srv], formated_uri, REQUEST_SIZE) < 0)
	{
		perror("ERRIMSS_DATASET_REQ");
		return -1;
	}

	char result[RESPONSE_SIZE];
	if (recv_stream(ucp_worker_client, stat_client[m_srv], result, RESPONSE_SIZE) < 0)
	{
		perror("ERRIMSS_RECVDYNAMSTRUCT_RECV");
		return -1;
	}

	return 0;
}

//Method retrieving information related to a certain dataset.
int32_t stat_dataset(const char * dataset_uri, dataset_info * dataset_info_)
{
	int ret = 0;
	//Search for the requested dataset in the local vector.
	for (int32_t i = 0; i < datasetd->len; i++)
	{
		*dataset_info_ = g_array_index(datasetd, dataset_info, i);
		if (!strcmp(dataset_uri, dataset_info_->uri_))
			return 2;
	}

	//Formated dataset uri to be sent to the metadata server.
	char formated_uri[REQUEST_SIZE];

	//Discover the metadata server that handles the dataset.
	uint32_t m_srv = discover_stat_srv((char *) dataset_uri);

	sprintf(formated_uri, "%" PRIu32 " GET 0 %s", stat_ids[m_srv], dataset_uri);
	//Send the request.
	if (send_stream(ucp_worker_client, stat_client[m_srv], formated_uri, REQUEST_SIZE) < 0)
	{
		perror("ERRIMSS_DATASET_REQ");
		return -1;
	}

	//Receive the associated structure.
	ret = recv_dynamic_stream(ucp_worker_client, stat_client[m_srv], dataset_info_, DATASET_INFO);
	if (ret < sizeof(dataset_info)) {
		DPRINT("[IMSS] stat_dataset: dataset does not exist.\n");
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


//Method retrieving the location of a specific data object.
int32_t get_data_location(int32_t dataset_id,
		int32_t data_id,
		int32_t op_type)
{
	//If the current dataset policy was not established yet.

	if (current_dataset != dataset_id)
	{
		//Retrieve the corresponding dataset_info structure and the associated IMSS.

		curr_dataset = g_array_index(datasetd, dataset_info, dataset_id);
		curr_imss = g_array_index(imssd, imss, curr_dataset.imss_d);

		//Set the corresponding.
		if (set_policy(&curr_dataset) == -1)
		{
			return -1;
		}

		current_dataset = dataset_id;
	}

	int32_t server;
	//Search for the server that is supposed to have the specified data element.
	if ((server = find_server(curr_imss.info.num_storages, data_id, curr_dataset.uri_, op_type)) < 0)
	{
		fprintf(stderr, "ERRIMSS_FIND_SERVER\n");
		return -1;
	}

	return server;
}

//Method renaming a dir_dir
int32_t rename_dataset_srv_worker_dir_dir(char * old_dir, char * rdir_dest,
		int32_t 	 dataset_id,	 int32_t 	 data_id)
{
	int32_t n_server;
	//Server containing the corresponding data to be retrieved.
	if ((n_server = get_data_location(dataset_id, data_id, GET)) == -1)

		return -1;

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

	char key_[REQUEST_SIZE];
	//Key related to the requested data element.

	//Request the concerned block to the involved servers.
	//for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	for (int32_t i = 0; i < curr_imss.info.num_storages; i++)
	{
	    sprintf(key_, "%" PRIu32 " GET 6 %s %s", curr_imss.conns.id[i], old_dir, rdir_dest);
		//if (comm_send(curr_imss.conns.eps_[repl_servers[i]], key, key_length, 0) != key_length)
		if (send_stream(ucp_worker_client, curr_imss.conns.eps_[i], key_, REQUEST_SIZE) < 0)
		{
			perror("ERRIMSS_GETDATA_REQ");
			return -1;
		}	

		char result[RESPONSE_SIZE];
		if (recv_stream(ucp_worker_client, curr_imss.conns.eps_[i], result, RESPONSE_SIZE) < 0)
		{
			perror("ERRIMSS_RECVDYNAMSTRUCT_RECV");
			return -1;
		}
		//Important to update
		//strcpy(curr_dataset.uri_,new_dataset_uri);
	}

	return 0;
}

//Method renaming a dataset.
int32_t rename_dataset_srv_worker(char * old_dataset_uri, char * new_dataset_uri,
		int32_t 	 dataset_id,	 int32_t 	 data_id)
{
	int32_t n_server;
	//Server containing the corresponding data to be retrieved.
	if ((n_server = get_data_location(dataset_id, data_id, GET)) == -1)
		return -1;

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

	char key_[REQUEST_SIZE];

	//Request the concerned block to the involved servers.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		//printf("BLOCK %d ASKED TO %d SERVER with key: %s (%d)\n", data_id, repl_servers[i], key, key_length);

	    //Key related to the requested data element.
	    sprintf(key_, "%" PRIu32 " GET 5 %s %s", curr_imss.conns.id[i], old_dataset_uri, new_dataset_uri);
		if (send_stream(ucp_worker_client, curr_imss.conns.eps_[repl_servers[i]], key_, REQUEST_SIZE) < 0)
		{
			perror("ERRIMSS_GETDATA_REQ");
			return -1;
		}	

		char result[RESPONSE_SIZE];
		if (recv_stream(ucp_worker_client, curr_imss.conns.eps_[repl_servers[i]], result, RESPONSE_SIZE) < 0)
		{
			perror("ERRIMSS_RECVDYNAMSTRUCT_RECV");
			return -1;
		}
		//Important to update
		strcpy(curr_dataset.uri_,new_dataset_uri);
	}

	return 0;
}

//Method storing a specific data element.
int32_t writev_multiple(const char * buf, int32_t dataset_id,int64_t data_id,
		int64_t end_blk, int64_t start_offset, int64_t end_offset, int64_t IMSS_DATA_BSIZE, int64_t size)
{

	int32_t n_server;
	//Server containing the corresponding data to be written.
	if ((n_server = get_data_location(dataset_id, data_id, SET)) == -1) {
		perror("ERRIMSS_GET_DATA_LOCATION");
		return -1;
	}

	char key_[REQUEST_SIZE];
	//Key related to the requested data element.

	int32_t curr_imss_storages = curr_imss.info.num_storages;

	//Send the data block to every server implementing redundancy.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		//Server receiving the current data block.
		uint32_t n_server_ = (n_server + i*(curr_imss_storages/curr_dataset.repl_factor)) % curr_imss_storages;

		//printf("BLOCK %ld SENT TO %d SERVER with key: %s (%d)\n", data_id, n_server_, key, key_length);
	    sprintf(key_, "%" PRIu32 " SET %d %s$%ld %ld %ld %ld %ld %ld %ld", curr_imss.conns.id[i], curr_dataset.data_entity_size, curr_dataset.uri_, data_id, data_id, end_blk, start_offset, end_offset, IMSS_DATA_BSIZE, size);
		if (send_stream(ucp_worker_client, curr_imss.conns.eps_[n_server_], key_, REQUEST_SIZE) < 0) //SNDMORE
		{
			perror("ERRIMSS_SETDATA_REQ");
			return -1;
		}

		if (send_stream(ucp_worker_client, curr_imss.conns.eps_[n_server_], buf, size) < 0)
		{
			perror("ERRIMSS_SETDATA_SEND");
			return -1;
		}
	}

	return 0;
}

//Method retrieving multiple data
int32_t readv_multiple(int32_t 	 dataset_id,
		int32_t 	 curr_block,
		int32_t 	 end_block,
		char * buffer,
		uint64_t 	 BLOCKSIZE,
		int64_t    start_offset,
		int64_t	size)
{
	//printf("readv size=%d\n",size);
	int32_t n_server;
	//Server containing the corresponding data to be retrieved.
	if ((n_server = get_data_location(dataset_id, curr_block, GET)) == -1)

		return -1;

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

	char key_[REQUEST_SIZE];
	//Key related to the requested data element.


	//Request the concerned block to the involved servers.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		//printf("BLOCK %d ASKED TO %d SERVER with key: %s (%d)\n", curr_block, repl_servers[i], key, key_length);

		//Send read request message specifying the block URI.
	    sprintf(key_, "%" PRIu32 " GET 8 %s$%d %d %ld %ld %ld", curr_imss.conns.id[repl_servers[i]], curr_dataset.uri_, curr_block, end_block, BLOCKSIZE, start_offset, size);
		if (send_stream(ucp_worker_client, curr_imss.conns.eps_[repl_servers[i]], key_, REQUEST_SIZE) < 0)
		{
			perror("ERRIMSS_GETDATA_REQ");
			return -1;
		}
		//Receive data related to the previous read request directly into the buffer.
		if (recv_stream(ucp_worker_client, curr_imss.conns.eps_[repl_servers[i]], buffer, size) < 0)
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

	//fprintf(stderr, "ERRIMSS_GETDATA_UNAVAIL\n");
	return -1;
}

	void *
split_writev(void * th_argv)
{
	//Cast from generic pointer type to p_argv struct type pointer.
	thread_argv * arguments = (thread_argv *) th_argv;

	int32_t n_server;


	char key_[REQUEST_SIZE];
    int32_t curr_imss_storages = curr_imss.info.num_storages;

	//Send the data block to every server implementing redundancy.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		//Server receiving the current data block.
		uint32_t n_server_ = (arguments->n_server + i*(curr_imss_storages/curr_dataset.repl_factor)) % curr_imss_storages;

        //Key related to the requested data element.
	    sprintf(key_, "%" PRIu32 " SET %d [OP]=2 %s %ld %ld %d %s", curr_imss.conns.id[n_server_],  curr_dataset.data_entity_size, 
			arguments->path, arguments->BLKSIZE, arguments->start_offset, 
			arguments->stats_size, arguments->msg);


		//if (comm_send(curr_imss.conns.eps_[n_server_], key, KEY, ZMQ_SNDMORE) < 0)
		if (send_stream(ucp_worker_client, curr_imss.conns.eps_[n_server_], key_, REQUEST_SIZE) < 0 ) //SNDMORE
		{
			perror("ERRIMSS_SETDATA_REQ");
		}

		if (send_stream(ucp_worker_client, curr_imss.conns.eps_[n_server_], arguments->buffer, arguments->size*arguments->BLKSIZE*KB) < 0)
		{
			perror("ERRIMSS_SETDATA_SEND");
		}
	}

	pthread_exit(NULL);
}

void * split_readv(void * th_argv)
{
	//Cast from generic pointer type to p_argv struct type pointer.
	thread_argv * arguments = (thread_argv *) th_argv;

	//Servers that the data block is going to be requested to.
	int32_t repl_servers[curr_dataset.repl_factor];

	int32_t curr_imss_storages = curr_imss.info.num_storages;

	//Retrieve the corresponding connections to the previous servers.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		//Server storing the current data block.
		uint32_t n_server_ = (arguments->n_server + i*(curr_imss_storages/curr_dataset.repl_factor)) % curr_imss_storages;

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
	//	printf("[CLIENT] [Split_readv]\n");

	char key_[REQUEST_SIZE];
	//Key related to the requested data element.
	int msg_length = strlen(arguments->msg)+1; 
		//printf("key=%s\n",key);
	//Request the concerned block to the involved servers.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		//Send read request message specifying the block URI.
		//if (comm_send(curr_imss.conns.eps_[repl_servers[i]], key, KEY, 0) < 0)
		//printf("[SPLIT READV] 1-Send_stream\n");
		sprintf(key_, "%" PRIu32 " GET 9 %s %ld %ld %d %d", curr_imss.conns.id[repl_servers[i]],
			arguments->path, arguments->BLKSIZE, arguments->start_offset, 
			arguments->stats_size, msg_length);

//printf("[SPLIT READV] 3-Send_stream\n");
		if (send_stream(ucp_worker_client, curr_imss.conns.eps_[repl_servers[i]], key_, REQUEST_SIZE) < 0)
		{
			perror("ERRIMSS_GETDATA_REQ");
		}

		//printf("[SPLIT READV] 4-Send_msg\n");
				if (send_stream(ucp_worker_client, curr_imss.conns.eps_[repl_servers[i]], arguments->msg, msg_length) < 0)
				{
					perror("ERRIMSS_GETDATA_REQ");
				}

		DPRINT("[IMSS] Request split_readv: client_id '%" PRIu32 "', mode '%s', key '%s', request '%s'\n", curr_imss.conns.id[repl_servers[i]], "GET", key_, arguments->msg);

		struct timeval start, end;
		/*long delta_us;
		  gettimeofday(&start, NULL);*/
		//Receive data related to the previous read request directly into the buffer.
		if (recv_stream(ucp_worker_client, curr_imss.conns.eps_[repl_servers[i]], arguments->buffer, arguments->size*arguments->BLKSIZE*KB) < 0)
		{
			if (errno != EAGAIN)
			{
				perror("ERRIMSS_GETDATA_RECV");
			}
			else
				break;
		}
		gettimeofday(&end, NULL);
		/*delta_us = (long) (end.tv_usec - start.tv_usec);
		  printf("\n[CLIENT] [S_SPLIT_READ] recv data delta_us=%6.3f\n",(delta_us/1000.0F));*/
		//Check if the requested key was correctly retrieved.
		if (strncmp((const char *) arguments->buffer, "$ERRIMSS_NO_KEY_AVAIL$", 22)){
		}

	}
	pthread_exit(NULL);
}
//Method retrieving multiple data from a specific server
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
  printf("n_server=%d msg=%s size=%d\n", n_server, msg, size);

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
//printf("BLOCK %d ASKED TO %d SERVER with key: %s (%d)\n", curr_block, repl_servers[i], key, key_length);

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

//fprintf(stderr, "ERRIMSS_GETDATA_UNAVAIL\n");
return -1;
}
*/

//Method retrieving a data element associated to a certain dataset.
int32_t get_data(int32_t 	 dataset_id,
		int32_t 	 data_id,
		char * buffer)
{
	int32_t n_server;


	//Server containing the corresponding data to be retrieved.
	if ((n_server = get_data_location(dataset_id, data_id, GET)) == -1){
		return -1;
	}

	//Servers that the data block is going to be requested to.
	int32_t repl_servers[curr_dataset.repl_factor];
	int32_t curr_imss_storages = curr_imss.info.num_storages;

	//Retrieve the corresponding connections to the previous servers.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		//Server storing the current data block.
		uint32_t n_server_ = (n_server + i*(curr_imss_storages/curr_dataset.repl_factor)) % curr_imss_storages;
		//printf("Server storing is=%d\n",n_server_);
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

	char key_[REQUEST_SIZE];

    //Request the concerned block to the involved servers.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
	    //Key related to the requested data element.
	    sprintf(key_, "%" PRIu32 " GET 0 %s$%d", curr_imss.conns.id[repl_servers[i]],  curr_dataset.uri_, data_id);

		//if (comm_send(curr_imss.conns.eps_[repl_servers[i]], key, KEY, 0) < 0)
		if (send_stream(ucp_worker_client, curr_imss.conns.eps_[repl_servers[i]], key_, REQUEST_SIZE) < 0)
		{
			perror("ERRIMSS_GETDATA_REQ");
			return -1;
		}

		/*	gettimeofday(&end, NULL);
			delta_us = (long) (end.tv_usec - start.tv_usec);
			printf("\n[CLIENT] [GET DATA] send petition delta_us=%6.3f\n",(delta_us/1000.0F));*/

		DPRINT("[IMSS] Request get_data: client_id '%" PRIu32 "', mode 'GET', key '%s'\n", curr_imss.conns.id[repl_servers[i]], key_);
		clock_t t;
		t = clock();

		//	gettimeofday(&start, NULL);
		//printf("GET_DATA after send petition to read\n");
		//Receive data related to the previous read request directly into the buffer.
		if (recv_stream(ucp_worker_client, curr_imss.conns.eps_[repl_servers[i]], buffer, curr_dataset.data_entity_size) < 0)
		{
			if (errno != EAGAIN)
			{
				perror("ERRIMSS_GETDATA_RECV");
				return -1;
			}
			else
				break;
		}
		t = clock() - t;
		double time_taken = ((double)t)/CLOCKS_PER_SEC; // in seconds

		DPRINT("[IMSS] get_data %f s\n",time_taken);

		//Check if the requested key was correctly retrieved.
		if (strncmp((const char *) buffer, "$ERRIMSS_NO_KEY_AVAIL$", 22)){
			return 0;
		}

	}
	return 1;
}


//Method retrieving a data element associated to a certain dataset.
int32_t get_ndata(int32_t 	 dataset_id,
		int32_t 	 data_id,
		char * buffer,
		int64_t  * len)
{
	int32_t n_server;

	*len = 0;
	//Server containing the corresponding data to be retrieved.
	if ((n_server = get_data_location(dataset_id, data_id, GET)) == -1)

		return -1;

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

	char key_[REQUEST_SIZE];
	
	//Request the concerned block to the involved servers.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		//printf("BLOCK %d ASKED TO %d SERVER with key: %s (%d)\n", data_id, repl_servers[i], key, key_length);
        
		//Key related to the requested data element.
	    sprintf(key_, "%" PRIu32 " GET 0 %s$%d",curr_imss.conns.id[repl_servers[i]], curr_dataset.uri_, data_id);

		//Send read request message specifying the block URI.
		//if (comm_send(curr_imss.conns.eps_[repl_servers[i]], key, KEY, 0) < 0)
		if (send_stream(ucp_worker_client, curr_imss.conns.eps_[repl_servers[i]], key_, REQUEST_SIZE) < 0)
		{
			perror("ERRIMSS_GETDATA_REQ");
			return -1;
		}

		DPRINT("[IMSS] Request get_ndata: client_id '%" PRIu32 "', mode 'GET', key '%s'\n", curr_imss.conns.id[repl_servers[i]],  key_);

		//Receive data related to the previous read request directly into the buffer.
		if (recv_stream(ucp_worker_client, curr_imss.conns.eps_[repl_servers[i]], buffer, curr_dataset.data_entity_size) < 0)
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
		if (!strncmp((const char *) buffer, "$ERRIMSS_NO_KEY_AVAIL$", 22))
			continue;

		*len = curr_dataset.data_entity_size;

		return 0;
	}

	fprintf(stderr, "ERRIMSS_GETDATA_UNAVAIL\n");
	return -1;
}


//Method storing a specific data element.
int32_t set_data(int32_t 	 dataset_id,
		int32_t 	 data_id,
		char * buffer)
{
	int32_t n_server;
	//size_t (* const send_choose_stream)(ucp_worker_h ucp_worker, ucp_ep_h ep, const char * msg, size_t msg_length) = (IMSS_WRITE_ASYNC == 1) ? send_istream : send_stream;

	/*	struct timeval start, end;
		long delta_us;
		gettimeofday(&start, NULL);*/

	//Server containing the corresponding data to be written.
	if ((n_server = get_data_location(dataset_id, data_id, SET)) == -1) {
		perror("ERRIMSS_GET_DATA_LOCATION");
		return -1;
	}

	/*	gettimeofday(&end, NULL);
		delta_us = (long) (end.tv_usec - start.tv_usec);
		printf("[CLIENT] [SWRITE GET_DATA_LOCATION] delta_us=%6.3f\n",(delta_us/1000.0F));*/


	char key_[REQUEST_SIZE];
	int32_t curr_imss_storages = curr_imss.info.num_storages;

	//Send the data block to every server implementing redundancy.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		//Server receiving the current data block.
		uint32_t n_server_ = (n_server + i*(curr_imss_storages/curr_dataset.repl_factor)) % curr_imss_storages;

		//printf("BLOCK %d SENT TO %d SERVER with key: %s (%d)\n", data_id, n_server_, key, key_length);

		//	gettimeofday(&start, NULL);
		//Send read request message specifying the block URI.
	  	//Key related to the requested data element.
	    sprintf(key_, "%" PRIu32 " SET %d %s$%d", curr_imss.conns.id[n_server_], curr_dataset.data_entity_size, curr_dataset.uri_, data_id);

		if (send_stream(ucp_worker_client, curr_imss.conns.eps_[n_server_], key_, REQUEST_SIZE) < 0) // SNDMORE
		{
			perror("ERRIMSS_SETDATA_REQ");
			return -1;
		}
		/*	gettimeofday(&end, NULL);
			delta_us = (long) (end.tv_usec - start.tv_usec);
			printf("[CLIENT] [SWRITE SEND_OP] delta_us=%6.3f\n",(delta_us/1000.0F));*/

		clock_t t;
		t = clock();

		//	gettimeofday(&start, NULL);
		//Send read request message specifying the block data.
		//printf("[SET DATA] msg=%s, size=%d\n",buffer,curr_dataset.data_entity_size);
		if (send_stream(ucp_worker_client, curr_imss.conns.eps_[n_server_], buffer, curr_dataset.data_entity_size) < 0)
		{
			perror("ERRIMSS_SETDATA_SEND");
			return -1;
		}
		/*	gettimeofday(&end, NULL);
			delta_us = (long) (end.tv_usec - start.tv_usec);
			printf("[CLIENT] [SWRITE SEND_DATA] delta_us=%6.3f\n",(delta_us/1000.0F));*/

		DPRINT("[IMSS] Request set_data: client_id '%" PRIu32 "', mode 'SET', key '%s'\n", curr_imss.conns.id[n_server_], key_);

		t = clock() - t;
		double time_taken = ((double)t)/CLOCKS_PER_SEC; // in seconds

		//printf("[CLIENT] [SET DATA] sent data %f s\n",time_taken);
	}
	return 0;
}

//Method storing a specific data element.
	int32_t
set_ndata(int32_t 	 dataset_id,
		int32_t 	 data_id,
		const char * buffer,
		uint32_t size)
{
	int32_t n_server;
	//Server containing the corresponding data to be written.
	if ((n_server = get_data_location(dataset_id, data_id, SET)) == -1)

		return -1;

	char key_[REQUEST_SIZE];
	int32_t curr_imss_storages = curr_imss.info.num_storages;

	//Send the data block to every server implementing redundancy.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		//Server receiving the current data block.
		uint32_t n_server_ = (n_server + i*(curr_imss_storages/curr_dataset.repl_factor)) % curr_imss_storages;

		//printf("BLOCK %d SENT TO %d SERVER with key: %s (%d)\n", data_id, n_server_, key, key_length);
        //Key related to the requested data element.
	    sprintf(key_, "%" PRIu32 " SET %d %s$%d", curr_imss.conns.id[n_server_], size, curr_dataset.uri_, data_id);

		//Send read request message specifying the block URI.
		//if (comm_send(curr_imss.conns.eps_[n_server_], key, KEY, ZMQ_SNDMORE) < 0)
		if (send_stream(ucp_worker_client, curr_imss.conns.eps_[n_server_], key_, REQUEST_SIZE) < 0) //SNDMORE
		{
			perror("ERRIMSS_SETDATA_REQ");
			return -1;
		}

		//Send read request message specifying the block data.
		if (send_stream(ucp_worker_client, curr_imss.conns.eps_[n_server_], buffer, size) < 0)
		{
			perror("ERRIMSS_SETDATA_SEND");
			return -1;
		}

		DPRINT("[IMSS] Request set_ndata: client_id '%" PRIu32 "', mode 'SET', key '%s'\n", curr_imss.conns.id[n_server_], key_);
	}

	return 0;
}

//WARNING! This function allocates memory that must be released by the user.

//Method retrieving the location of a specific data object.
char ** get_dataloc(const char *    dataset,
		int32_t   data_id,
		int32_t * num_storages)
{
	//Dataset structure of the one requested.
	dataset_info where_dataset;

	//Check which resource was used to retrieve the concerned dataset.
	switch (stat_dataset(dataset, &where_dataset))
	{
		//No dataset was found with the requested name.
		case 0:
			{
				fprintf(stderr, "ERRIMSS_GETDATALOC_DATASETNOTEXISTS\n");
				return NULL;
			}
			//The dataset was retrieved from the metadata server.
		case 1:
			{
				//The dataset structure will not be stored if it is a LOCAL one as those are dynamically updated.
				if (strcmp(where_dataset.policy, "LOCAL"))
				{
					//Hint specifying that the dataset was retrieved but not initialized.
					where_dataset.local_conn = -2;
					GInsert (&datasetd_pos, &datasetd_max_size, (char *) &where_dataset, datasetd, free_datasetd);
				}

				break;
			}
	}

	int32_t dataset_name_length = strlen(dataset);

	//Position where the first '/' character within the dataset name has been found.
	int32_t end_imss_name;

	//TODO: retrieving the imss uri from the dataset one must be updated.

	for (end_imss_name = dataset_name_length; end_imss_name > 0; end_imss_name--)
	{
		if (dataset[end_imss_name] == '/')

			break;
	}

	//Name of the IMSS entity managing the concerned dataset.
	char imss_name[end_imss_name];

	memcpy(imss_name, dataset, end_imss_name);

	//IMSS structure storing the information related to the concerned IMSS entity.
	imss where_imss;

	int32_t found_imss_in = stat_imss(imss_name, &where_imss.info);

	//Check which resource was used to retrieve the concerned IMSS structure.
	if (found_imss_in) {
		//Hint specifying that the IMSS structure was retrieved but not initialized.
		where_imss.conns.matching_server = -2;
		GInsert (&imssd_pos, &imssd_max_size, (char *) &where_imss, imssd, free_imssd);
	} else {
		fprintf(stderr, "ERRIMSS_GETDATALOC_IMSSNOTEXISTS\n");
		return NULL;
	}

	//Set the policy corresponding to the retrieved dataset.
	/* if (set_policy(&where_dataset) == -1)
	   {
	   fprintf(stderr, "ERRIMSS_GETDATALOC_SETPOLICY\n");
	   return NULL;
	   }
	   */

	current_dataset = -1;

	int32_t server;
	//Find the server storing the corresponding block.
	if ((server = find_server(where_imss.info.num_storages, data_id, where_dataset.uri_, GET)) < 0)
	{
		fprintf(stderr, "ERRIMSS_GETDATALOC_FINDSERVER\n");
		return NULL;
	}

	*num_storages = where_dataset.repl_factor;

	char ** machines = (char **) malloc(*num_storages * sizeof(char *));

	for (int32_t i = 0; i < *num_storages; i++)
	{
		//Server storing the current data block.
		uint32_t n_server_ = (server + i*(where_imss.info.num_storages/where_dataset.repl_factor)) % where_imss.info.num_storages;
		machines[i] = (char *) malloc(strlen(where_imss.info.ips[n_server_])*sizeof(char));
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

//Method specifying the type (DATASET or IMSS INSTANCE) of a provided URI.
int32_t get_type(char * uri)
{
	//Formated uri to be sent to the metadata server.
	char formated_uri[REQUEST_SIZE];

	//Discover the metadata server that handles the entity.
	uint32_t m_srv = discover_stat_srv(uri);
	//printf("get_type=%s\n",uri);
	//Send the request.

	sprintf(formated_uri, "%" PRIu32 " GET 0 %s", stat_ids[m_srv], uri);
	if (send_stream(ucp_worker_client, stat_client[m_srv], formated_uri, REQUEST_SIZE) < 0)
	{
		fprintf(stderr, "ERRIMSS_GETTYPE_REQ\n");
		return -1;
	}

	imss_info * data;

	//Receive the answer.
	char result[RESPONSE_SIZE];

	if (recv_dynamic_stream(ucp_worker_client, stat_client[m_srv], result, BUFFER) < 0)
	{
		fprintf(stderr, "ERRIMSS_GETTYPE_REQ\n");
		return -1;
	}

	data = (imss_info *)result;

	//Determine what was retrieved from the metadata server.
	if (data->type == 'I')
		return 1;
	else if (data->type == 'D' || data->type == 'L')
		return 2;
	return 0;
}

//Method retriving list of servers to read.
int32_t split_location_servers(int** list_servers,int32_t dataset_id,  int32_t curr_blk, int32_t end_blk)
{
	int size = end_blk - curr_blk + 1;
	//printf("size=%d\n",size);

	for(int i = 0; i < size; i++){
		int32_t n_server;

		//Server containing the corresponding data to be retrieved.
		if ((n_server = get_data_location(dataset_id, curr_blk, GET)) == -1){
			return -1;
		}
		//printf("list_servers[%d][%d]=%d\n",n_server,i,curr_blk);
		list_servers[n_server][i]=curr_blk;
		curr_blk++;
	}

	return 0;
}
/**********************************************************************************/
/***************************** DATA RELEASE RESOURCES *****************************/
/**********************************************************************************/


//Method releasing an imss_info structure previously provided to the client.
int32_t free_imss(imss_info * imss_info_)
{
	for (int32_t i = 0; i < imss_info_->num_storages; i++)
		free(imss_info_->ips[i]);

	free(imss_info_->ips);

	return 0;
}

//Method releasing a dataset structure previously provided to the client.
int32_t free_dataset(dataset_info * dataset_info_)
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
