#include <zmq.h>
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
#include "imss.h"
#include "comms.h"
#include "workers.h"
#include "policies.h"



/**********************************************************************************/
/******************************** GLOBAL VARIABLES ********************************/
/**********************************************************************************/

int32_t 	process_rank;		//Process identifier within the deployment.

void * 		ctx;			//Applications' comms context.

void * 		stat_client;		//Metadata server socket.
void * 		stat_mon;		//Metadata monitoring socket.

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


/**********************************************************************************/
/*********************** IMSS INTERNAL MANAGEMENT FUNCTIONS ***********************/
/**********************************************************************************/


//Method creating a ZMQ socket connection of type DEALER over a certain ip + port couple.
int32_t
conn_crt_(void **  socket,
	  char *   ip_,
	  uint16_t port,
	  int32_t  ident_,
	  int32_t  monitor,
	  void **  socket_mon)
{
	//Create the actual ZMQ socket.
	if ((*socket = zmq_socket(ctx, ZMQ_DEALER)) == NULL)
	{
		perror("ERRIMSS_CONN_SOCKET_CRT");
		return -1;
	}

	//Receive timeout in milliseconds.
	//int32_t rcvtimeo = TIMEOUT_MS;
	int32_t rcvtimeo = -1;

	//Monitor the current socket if it was requested.
	if (monitor)
	{
		if (zmq_socket_monitor(*socket, "inproc://monitor-socket", ZMQ_EVENT_CONNECTED) == -1)
		{
			perror("ERRIMSS_CONN_MONITOR");
			return -1;
		}
		
		if ((*socket_mon = zmq_socket(ctx, ZMQ_PAIR)) == NULL)
		{
			perror("ERRIMSS_CONN_MONSOCKET");
			return -1;
		}

		if (zmq_setsockopt(*socket_mon, ZMQ_RCVTIMEO, &rcvtimeo, sizeof(int32_t)) == -1)
		{
			perror("ERRIMSS_CONN_MONSOCKET_RCVTIMEO");
			return -1;
		}

		if (zmq_connect(*socket_mon, "inproc://monitor-socket") == -1)
		{
			perror("ERRIMSS_CONN_MONCONNECT");
			return -1;
		}
	}

	//Set communication id.
	if (zmq_setsockopt(*socket, ZMQ_IDENTITY, &ident_, sizeof(int32_t)) == -1)
	{
		perror("ERRIMSS_CONN_SETSOCKET_IDEN");
		return -1;
	}

	//Set a timeout to receive operations.
	if (zmq_setsockopt(*socket, ZMQ_RCVTIMEO, &rcvtimeo, sizeof(int32_t)) == -1)
	{
		perror("ERRIMSS_CONN_SETSOCKET_RCVTIMEO");
		return -1;
	}

	//Connection address.
	char addr_[LINE_LENGTH]; 
	sprintf(addr_, "%s%s%c%d", "tcp://", ip_, ':', port);
	//Connect to the specified endpoint.
	if (zmq_connect(*socket, (const char *) addr_) == -1)
	{
		perror("ERRIMSS_CONN_CONNECT");
		return -1;
	}

	int32_t send_hwm = 0;
	//Unset limit in the number of messages to be enqueued in the socket's receiving buffer.
	if (zmq_setsockopt(*socket, ZMQ_SNDHWM, &send_hwm, sizeof(int32_t)) == -1)
	{
		perror("ERRIMSS_CONN_SETSOCKET_HWM");
		return -1;
	}

	return 0;
}

//Method destroying an existing ZMQ socke connection.
int32_t
conn_dstr_(void * socket)
{
	//Close the provided socket.
	if (zmq_close(socket) == -1)
	{
		perror("ERRIMSS_CONN_SCKT_CLOSE");
		return -1;
	}

	return 0;
}



////Method stating if a socket has been connected to an endpoint.
//int32_t
//socket_connected(void ** monitor)
//{
//	zmq_msg_t event_msg;
//	zmq_msg_init (&event_msg);
//	//Message storing the event.
//	if (zmq_msg_recv(&event_msg, *monitor, 0) == -1)
//
//		return -1;
//
//	//Elements formalizing the retrieved data.
//	uint8_t * data = (uint8_t *) zmq_msg_data(&event_msg);
//	uint16_t event = *(uint16_t *) data;
//	zmq_msg_close(&event_msg);
//
//	return event == ZMQ_EVENT_CONNECTED;
//}

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

//Check the existance within the session of the IMSS that the dataset is to be created in.
int32_t
imss_check(char * dataset_uri)
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
int32_t
find_imss(char * imss_uri,
	  imss * imss_)
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



/**********************************************************************************/
/********************* METADATA SERVICE MANAGEMENT FUNCTIONS  *********************/
/**********************************************************************************/


//Method creating a communication channel with the IMSS metadata server. Besides, the stat_imss method initializes a set of elements that will be used through the session.
int32_t
stat_init(char *   address,
	  uint16_t port,
	  int32_t  rank)
{
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

	//Create ZMQ application context.
	if ((ctx = zmq_ctx_new()) == NULL)
	{
		perror("ERRIMSS_CREATE_CONTEXT");
		return -1;
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

	//Create the connection to the metadata server dispatcher thread.
	if (conn_crt_(&stat_client, address, port, rank, 0, NULL) == -1)

		return -1;

	//ZMQ message requesting a connection to the metadata server.
	char request[] = "HELLO!\0";
	//Send the metadata server connection request.
	if (zmq_send(stat_client, request, 7, 0)  != 7)
	{
		perror("ERRIMSS_STAT_HELLO");
		return -1;
	}

	//ZMQ message retrieving the connection information.
	zmq_msg_t connection_info;
	zmq_msg_init (&connection_info);
	if (zmq_msg_recv(&connection_info, stat_client, 0) == -1)
	{
		perror("ERRIMSS_STAT_ACK");
		return -1;
	}

	//Close the previous connection.
	if (conn_dstr_(stat_client) == -1)

		return -1;

	//Port that the new client must connect to.
	int32_t stat_port;
	//ID that the new client must take.
	int32_t stat_id;
	//Separator.
	char sep_;

	//Read the previous information from the message received.
	sscanf((const char *) zmq_msg_data(&connection_info), "%d%c%d", &stat_port, &sep_, &stat_id);
	zmq_msg_close(&connection_info);

	//Create the connection to the metadata server dispatcher thread.
	if (conn_crt_(&stat_client, address, stat_port, stat_id, 0, NULL) == -1)

		return -1;

	return 0;
}

//Method disabling the communication channel with the metadata server. Besides, the current method releases session-related elements previously initialized.
int32_t stat_release()
{
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

	//WARNING! zmq_ctx_destroy will block unless all associated sockets have been released.

	if (conn_dstr_(stat_client) == -1)
	{
		perror("ERRIMSS_CLOSE_SOCKET");
		return -1;
	}

	if (zmq_ctx_destroy(ctx) == -1)
	{
		perror("ERRIMSS_CLOSE_CONTEXT");
		return -1;
	}

	return 0;
}

//Method retrieving the whole set of elements contained by a specific URI.
uint32_t
get_dir(char * 	 requested_uri,
	char **  buffer,
	char *** items)
{
	//GETDIR request.
	char getdir_req[strlen(requested_uri)+3];
	sprintf(getdir_req, "%d %s%c", GETDIR, requested_uri, '\0');

	//Send the request.
	if (zmq_send(stat_client, getdir_req, strlen(getdir_req), 0)  == -1)
	{
		perror("ERRIMSS_GETDIR_REQ");
		return -1;
	}

	//Retrieve the set of elements within the requested uri.
	zmq_msg_t uri_elements;
	if (zmq_msg_init(&uri_elements) != 0)
	{
		perror("ERRIMSS_GETDIR_MSGINIT");
		return -1;
	}
	if (zmq_msg_recv(&uri_elements, stat_client, 0) == -1)
	{
		perror("ERRIMSS_GETDIR_RECV");
		return -1;
	}

	char * elements = (char *) zmq_msg_data(&uri_elements);

	if (!strncmp("$ERRIMSS_NO_KEY_AVAIL$", elements, 22))
	{
		zmq_msg_close(&uri_elements);
		fprintf(stderr, "ERRIMSS_GETDIR_NODIR\n");
		return -1;
	}

	uint32_t elements_size = zmq_msg_size(&uri_elements);

	*buffer = (char *) malloc(sizeof(char)*elements_size);
	memcpy(*buffer, elements, elements_size);
	elements = *buffer;

	zmq_msg_close(&uri_elements);

	uint32_t num_elements = elements_size/URI_;

	*items = (char **) malloc(sizeof(char *) * num_elements);

	//Identify each element within the buffer provided.
	for (int32_t i = 0; i < num_elements; i++)
	{
		(*items)[i] = elements;

		elements += URI_;
	}

	return num_elements;
}




/**********************************************************************************/
/***************** IN-MEMORY STORAGE SYSTEM MANAGEMENT FUNCTIONS  *****************/
/**********************************************************************************/


//Method initializing an IMSS deployment.
int32_t
init_imss(char *   imss_uri,
	  char *   hostfile,
	  int32_t  n_servers,
	  uint16_t conn_port,
	  uint64_t buff_size,
	  uint32_t deployment,
	  char *   binary_path)
{
	imss_info aux_imss;
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

		char command[2048]; 
		memset(command, 0, 2048);

		sprintf(command, "mpirun -np %d -f %s %s %s %d %lu foo %d %d %s &", n_servers, hostfile, binary_path, imss_uri, conn_port, buff_size, 0, n_servers, "");

		//Perform the deployment (FROM LINUX MAN PAGES: "system() returns after the command has been completed").
		if (system(command) == -1)
		{
			perror("ERRIMSS_INITIMSS_DEPLOY");
			return -1;
		}
	}

	//IMSS creation.
	imss new_imss;
	strcpy(new_imss.info.uri_, imss_uri);
	new_imss.info.num_storages  = n_servers;
	new_imss.info.conn_port     = conn_port;

	if (deployment == ATTACHED)

		new_imss.info.conn_port     = connection_port;

	new_imss.info.ips           = (char **) malloc(n_servers * sizeof(char *));

	//Resources required to connect to the corresponding IMSS.
	new_imss.conns.sockets_ = (void **) malloc(n_servers * sizeof(void *));

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

	//Connect to all servers.
	for (int i = 0; i < n_servers; i++)
	{
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
		if (conn_crt_(&(new_imss.conns.sockets_[i]), (new_imss.info.ips)[i], new_imss.info.conn_port, process_rank, 0, NULL) == -1)

			return -1;

        uint32_t request_size = 16 + URI_;
		char     request[request_size];

		//ZMQ message requesting a connection to the metadata server.
		sprintf(request, "%s %ld %s%c", "HELLO!", buff_size, imss_uri, '\0');
		//Send the IMSS server connection request.
		if (zmq_send(new_imss.conns.sockets_[i], request, request_size, 0)  != request_size)
		{
			perror("ERRIMSS_INITIMSS_HELLO");
			return -1;
		}

		//ZMQ message retrieving the connection information.
		zmq_msg_t connection_info;
		zmq_msg_init (&connection_info);
		if (zmq_msg_recv(&connection_info, new_imss.conns.sockets_[i], 0) == -1)
		{
			perror("ERRIMSS_INITIMSS_CONNINFOINIT");
			return -1;
		}

		//Close the previous connection.
		if (conn_dstr_((new_imss.conns.sockets_[i])) == -1)
			return -1;

		//Port that the new client must connect to.
		int32_t imss_port;
		//ID that the new client must take.
		int32_t imss_id;
		//Separator.
		char sep_;

		//Read the previous information from the message received.
		sscanf((const char *) zmq_msg_data(&connection_info), "%d%c%d", &imss_port, &sep_, &imss_id);

		zmq_msg_close(&connection_info);

		//Create the connection to the metadata server dispatcher thread.
		if (conn_crt_(&(new_imss.conns.sockets_[i]), (new_imss.info.ips)[i], imss_port, imss_id, 0, NULL) == -1)

			return -1;
	}

	//Close the file.
	if (fclose(svr_nodes) != 0)
	{
		perror("ERR_CLOSE_FILE");
		return -1;
	}

	//Send the created structure to the metadata server.
	char key_plus_size[KEY+16];
	sprintf(key_plus_size, "%lu %s", (sizeof(imss_info)+new_imss.info.num_storages*LINE_LENGTH), new_imss.info.uri_);

	if (zmq_send(stat_client, key_plus_size, KEY+16, ZMQ_SNDMORE) != (KEY+16))
	{
		perror("ERRIMSS_INITIMSS_SENDKEY");
		return -1;
	}

	//Send the new IMSS metadata structure to the metadata server entity.
	if (send_dynamic_struct(stat_client, (void *) &new_imss.info, IMSS_INFO) == -1)

		return -1;

	//Add the created struture into the underlying IMSS vector.
	GInsert (&imssd_pos, &imssd_max_size, (char *) &new_imss, imssd, free_imssd);

	return 0;
}

//Method initializing the required resources to make use of an existing IMSS.
int32_t
open_imss(char * imss_uri)
{
	//New IMSS structure storing the entity to be created.
	imss new_imss;

	int32_t not_initialized = 0;

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

	new_imss.conns.sockets_ = (void **) malloc(new_imss.info.num_storages*sizeof(void*));

	new_imss.conns.matching_server = -1;

	//Connect to the requested IMSS.
	for  (int32_t i = 0; i < new_imss.info.num_storages; i++)
	{
		//Create the connection to the IMSS server dispatcher thread.
		if (conn_crt_(&(new_imss.conns.sockets_[i]), (new_imss.info.ips)[i], new_imss.info.conn_port, process_rank, 0, NULL) == -1)

			return -1;

		//ZMQ message requesting a connection to the dispatcher thread.
		char request[16];
		sprintf(request, "%s", "HELLO!JOIN");
		//Send the IMSS server connection request.
		if (zmq_send(new_imss.conns.sockets_[i], request, 16, 0)  != 16)
		{
			perror("ERRIMSS_OPENIMSS_HELLO");
			return -1;
		}

		//ZMQ message retrieving the connection information.
		zmq_msg_t connection_info;
		zmq_msg_init (&connection_info);
		if (zmq_msg_recv(&connection_info, new_imss.conns.sockets_[i], 0) == -1)
		{
			perror("ERRIMSS_OPENIMSS_CONNINFOOPEN");
			return -1;
		}

		//Close the previous connection.
		if (conn_dstr_((new_imss.conns.sockets_[i])) == -1)

			return -1;

		//Port that the new client must connect to.
		int32_t imss_port;
		//ID that the new client must take.
		int32_t imss_id;
		//Separator.
		char sep_;

		//Read the previous information from the message received.
		sscanf((const char *) zmq_msg_data(&connection_info), "%d%c%d", &imss_port, &sep_, &imss_id);

		zmq_msg_close(&connection_info);

		//Create the connection to the metadata server dispatcher thread.
		if (conn_crt_(&(new_imss.conns.sockets_[i]), (new_imss.info.ips)[i], imss_port, imss_id, 0, NULL) == -1)

			return -1;

		//Save the current socket value when the IMSS ip matches the clients' one.
		if (!strncmp((new_imss.info.ips)[i], client_node, len_client_node) || !strncmp((new_imss.info.ips)[i], client_ip, strlen(new_imss.info.ips[i])))
		{
			new_imss.conns.matching_server = i;

			strcpy(att_deployment, imss_uri);
		}

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
int32_t
release_imss(char *   imss_uri,
	     uint32_t release_op)
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
			char release_msg[] = "2 RELEASE\0";

			if (zmq_send(imss_.conns.sockets_[i], release_msg, strlen(release_msg), 0) < 0)
			{
				perror("ERRIMSS_RLSIMSS_SENDREQ");
				return -1;
			}
		}

		if (conn_dstr_((imss_.conns.sockets_[i])) == -1)
		{
			perror("ERRIMSS_RLSIMSS_CONNDSTRY");
			return -1;
		}

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
int32_t
stat_imss(char *      imss_uri,
	  imss_info * imss_info_)
{
	//Check for the IMSS info structure in the local vector.

	int32_t imss_found_in;
	imss searched_imss;

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
	char formated_uri[strlen(imss_uri)+2];
	sprintf(formated_uri, "0 %s%c", imss_uri, '\0');
	size_t formated_uri_length = strlen(formated_uri);

	//Send the request.
	if (zmq_send(stat_client, formated_uri, formated_uri_length, 0)  != formated_uri_length)
	{
		fprintf(stderr, "ERRIMSS_IMSS_REQ\n");
		return -1;
	}

	//Receive the associated structure.
	return recv_dynamic_struct(stat_client, imss_info_, IMSS_INFO);
}

//Method providing the URI of the attached IMSS instance.
char *
get_deployed()
{
    if (att_deployment[0] != '\0')
    {
        char * att_dep_uri = (char *) malloc(URI_ * sizeof(char));

        strcpy(att_dep_uri, att_deployment);

        return att_dep_uri;
    }

    return NULL;
}

//Method providing the URI of the instance deployed in some endpoint.
char *
get_deployed(char * endpoint)
{
    //Socket used for the request.
    void * probe_socket;

    uint32_t timeout = 500;
	//Set a timeout to receive the requested URI.
	if (zmq_setsockopt(probe_socket, ZMQ_RCVTIMEO, &timeout, sizeof(uint32_t)) == -1)
	{
		perror("ERRIMSS_GETDEPLOYED_SETRCVTIMEO");
		return NULL;
	}

	//Connection address.
	char addr_[LINE_LENGTH]; 
	sprintf(addr_, "tcp://%s", endpoint);
	//Connect to the specified endpoint.
	if (zmq_connect(probe_socket, (const char *) addr_) == -1)
	{
		perror("ERRIMSS_GETDEPLOYED_CONNECT");
		return NULL;
	}

	char who_request[16];
	sprintf(who_request, "%d blabla", WHO);
	size_t who_request_length = strlen(who_request);

	//Send the request.
	if (zmq_send(probe_socket, who_request, who_request_length, 0) != who_request_length)
	{
		fprintf(stderr, "ERRIMSS_GETDEPLOYED_REQ\n");
		return NULL;
	}

    char * deployed_uri = (char *) malloc(URI_ * sizeof(char));
    if (zmq_recv(probe_socket, deployed_uri, URI_, 0) == -1)
	{
        if (errno != EAGAIN)
            fprintf(stderr, "ERRIMSS_GETDEPLOYED_RESP\n");

		return NULL;
	}

    zmq_close(probe_socket);

    return deployed_uri;
}



/**********************************************************************************/
/************************** DATASET MANAGEMENT FUNCTIONS **************************/
/**********************************************************************************/


//Method creating a dataset and the environment enabling READ or WRITE operations over it.
int32_t
create_dataset(char *  dataset_uri,
	       char *  policy,
	       int32_t num_data_elem,
	       int32_t data_elem_size,
	       int32_t repl_factor)
{
	if ((dataset_uri == NULL) || (policy == NULL) || !num_data_elem || !data_elem_size)
	{
		fprintf(stderr, "ERRIMSS_CRTDATASET_WRONGARG\n");
		return -1;
	}

	if ((repl_factor < NONE) || (repl_factor > TRM))
	{
		fprintf(stderr, "ERRIMSS_CRTDATASET_BADREPLFACTOR\n");
		return -1;
	}

	int32_t associated_imss_indx;
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
	if (stat_dataset(dataset_uri, &new_dataset))
	{
		fprintf(stderr, "ERRIMSS_CRTDATASET_ALREADYEXISTS\n");
		return -1;
	}

	//Save the associated metadata of the current dataset.
	strcpy(new_dataset.uri_, 	dataset_uri);
	strcpy(new_dataset.policy, 	policy);
	new_dataset.num_data_elem 	= num_data_elem;
	new_dataset.data_entity_size 	= data_elem_size*1024;
	new_dataset.imss_d 		= associated_imss_indx;
	new_dataset.local_conn 		= associated_imss.conns.matching_server;
	new_dataset.repl_factor		= repl_factor;

	//Size of the message to be sent.
	uint64_t msg_size = sizeof(dataset_info);

	//Reserve memory so as to store the position of each data element if the dataset is a LOCAL one.
	if (!strcmp(new_dataset.policy, "LOCAL"))
	{
		uint32_t info_size = new_dataset.num_data_elem * sizeof(uint16_t);

		new_dataset.data_locations = (uint16_t *) malloc(info_size);
		memset(new_dataset.data_locations, 0, info_size);

		//Specify that the created dataset is a LOCAL one.
		new_dataset.type = 'L';

		//Add additional bytes that will be sent.
		msg_size += info_size;
	}
	else
		new_dataset.type = 'D';

	char formated_uri[REQ_MSG];
	sprintf(formated_uri, "%lu %s", msg_size, new_dataset.uri_);

	//Send the dataset URI associated to the dataset metadata structure to be sent.
	if (zmq_send(stat_client, formated_uri, REQ_MSG, ZMQ_SNDMORE) < 0)
	{
		perror("ERRIMSS_DATASET_SNDURI");
		return -1;
	}

	//Send the new dataset metadata structure to the metadata server entity.
	if (send_dynamic_struct(stat_client, (void *) &new_dataset, DATASET_INFO) == -1)

		return -1;

	//Initialize dataset fields monitoring the dataset itself if it is a LOCAL one.
	if (!strcmp(new_dataset.policy, "LOCAL"))
	{
		//Current number of blocks written by the client.
		new_dataset.num_blocks_written = (uint64_t *) malloc(1*sizeof(uint64_t));
		*(new_dataset.num_blocks_written) = 0;
		//Specific blocks written by the client.
		new_dataset.blocks_written = (uint32_t *) malloc(new_dataset.num_data_elem*sizeof(uint32_t));

		memset(new_dataset.blocks_written, 0, new_dataset.num_data_elem*sizeof(uint32_t));
	}

//	//Set the specified policy.
//	if (set_policy(&new_dataset) == -1)
//	{
//		perror("ERRIMSS_DATASET_SETPLCY");
//		return -1;
//	}

	//Add the created struture into the underlying IMSSs.
	return (GInsert (&datasetd_pos, &datasetd_max_size, (char *) &new_dataset, datasetd, free_datasetd));
}

//Method creating the required resources in order to READ and WRITE an existing dataset.
int32_t
open_dataset(char * dataset_uri)
{
	int32_t associated_imss_indx;

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
		new_dataset.blocks_written = (uint32_t *) malloc(new_dataset.num_data_elem*sizeof(uint32_t));

		memset(new_dataset.blocks_written, '\0', new_dataset.num_data_elem*sizeof(uint32_t));
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
int32_t
release_dataset(int32_t dataset_id)
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
		//Formated dataset uri to be sent to the metadata server.
		char formated_uri[REQ_MSG];
		sprintf(formated_uri, "0 %s", release_dataset.uri_);
		//Send the LOCAL dataset positions update.
		if (zmq_send(stat_client, formated_uri, REQ_MSG, ZMQ_SNDMORE) < 0)
		{
			perror("ERRIMSS_RELDATASET_SENDURI");
			return -1;
		}

		//Format update message to be sent to the metadata server.

		uint64_t blocks_written_size = *(release_dataset.num_blocks_written) * sizeof(uint32_t);
		uint64_t update_msg_size = 8 + blocks_written_size;

		char update_msg[update_msg_size];
		memset(update_msg, '\0', update_msg_size);

		uint16_t update_value = (release_dataset.local_conn + 1);
		memcpy(update_msg, release_dataset.blocks_written, blocks_written_size);
		memcpy((update_msg+blocks_written_size), &update_value, sizeof(uint16_t));

		//Send the list of servers storing the data elements.
		if (zmq_send(stat_client, update_msg, update_msg_size, 0) < 0)
		{
			perror("ERRIMSS_RELDATASET_SENDPOSITIONS");
			return -1;
		}

		zmq_msg_t update_result;

		if (zmq_msg_init(&update_result) != 0)
		{
			perror("ERRIMSS_RELDATASET_INITUPDATERES");
			return -1;
		}

		if (zmq_msg_recv(&update_result, stat_client, 0) == -1)
		{
			perror("ERRIMSS_RELDATASET_RECVUPDATERES");
			return -1;
		}

		if (strcmp((char *) zmq_msg_data(&update_result), "UPDATED!"))
		{
			perror("ERRIMSS_RELDATASET_UPDATE");
			return -1;
		}

		zmq_msg_close(&update_result);

		//Free the data locations vector.
		free(release_dataset.data_locations);
		//Freem the monitoring vector.
		free(release_dataset.blocks_written);

		free(release_dataset.num_blocks_written);

	}

	g_array_remove_index(datasetd, dataset_id);
	g_array_insert_val(datasetd, dataset_id, empty_dataset);
	//Add the index to the set of free positions within the dataset vector.
	g_array_append_val(free_datasetd, dataset_id);

	return 0;
}

//Method retrieving information related to a certain dataset.
int32_t
stat_dataset(const char * 	    dataset_uri,
	     dataset_info * dataset_info_)
{
	//Search for the requested dataset in the local vector.
	for (int32_t i = 0; i < datasetd->len; i++)
	{
		*dataset_info_ = g_array_index(datasetd, dataset_info, i);

		if (!strcmp(dataset_uri, dataset_info_->uri_))
		
			return 2;
	}

	//Formated dataset uri to be sent to the metadata server.
	char formated_uri[REQ_MSG];
	sprintf(formated_uri, "0 %s", dataset_uri);

	//Send the request.
	if (zmq_send(stat_client, formated_uri, REQ_MSG, 0) < 0)
	{
		perror("ERRIMSS_DATASET_REQ");
		return -1;
	}

	//Receive the associated structure.
	return recv_dynamic_struct(stat_client, dataset_info_, DATASET_INFO);
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
int32_t
get_data_location(int32_t dataset_id,
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
			fprintf(stderr, "ERRIMSS_SET_POLICY\n");
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

//Method retrieving a data element associated to a certain dataset.
int32_t
get_data(int32_t 	 dataset_id,
	 int32_t 	 data_id,
	 unsigned char * buffer)
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

	char key_[KEY];
	//Key related to the requested data element.
	sprintf(key_, "0 %s$%d",  curr_dataset.uri_, data_id);

	int key_length = strlen(key_)+1;
	char key[key_length];
	memcpy((void *) key, (void *) key_, key_length);
	key[key_length-1] = '\0';

	//Request the concerned block to the involved servers.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		//printf("BLOCK %d ASKED TO %d SERVER with key: %s (%d)\n", data_id, repl_servers[i], key, key_length);

		//Send read request message specifying the block URI.
		//if (zmq_send(curr_imss.conns.sockets_[repl_servers[i]], key, KEY, 0) < 0)
		if (zmq_send(curr_imss.conns.sockets_[repl_servers[i]], key, key_length, 0) != key_length)
		{
			perror("ERRIMSS_GETDATA_REQ");
			return -1;
		}

		//Receive data related to the previous read request directly into the buffer.
		if (zmq_recv(curr_imss.conns.sockets_[repl_servers[i]], buffer, curr_dataset.data_entity_size, 0) == -1)
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
		if (strncmp((const char *) buffer, "$ERRIMSS_NO_KEY_AVAIL$", 22))

			return 0;
	}

	fprintf(stderr, "ERRIMSS_GETDATA_UNAVAIL\n");
	return -1;
}


//Method retrieving a data element associated to a certain dataset.
int32_t
get_ndata(int32_t 	 dataset_id,
	 int32_t 	 data_id,
	 unsigned char * buffer,
	 int64_t  * len)
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

	char key_[KEY];
	//Key related to the requested data element.
	sprintf(key_, "0 %s$%d",  curr_dataset.uri_, data_id);

	int key_length = strlen(key_)+1;
	char key[key_length];
	memcpy((void *) key, (void *) key_, key_length);
	key[key_length-1] = '\0';

	//Request the concerned block to the involved servers.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		//printf("BLOCK %d ASKED TO %d SERVER with key: %s (%d)\n", data_id, repl_servers[i], key, key_length);

		//Send read request message specifying the block URI.
		//if (zmq_send(curr_imss.conns.sockets_[repl_servers[i]], key, KEY, 0) < 0)
		if (zmq_send(curr_imss.conns.sockets_[repl_servers[i]], key, key_length, 0) != key_length)
		{
			perror("ERRIMSS_GETDATA_REQ");
			return -1;
		}

		//Receive data related to the previous read request directly into the buffer.
		if (zmq_recv(curr_imss.conns.sockets_[repl_servers[i]], buffer, curr_dataset.data_entity_size, 0) == -1)
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
		if (strncmp((const char *) buffer, "$ERRIMSS_NO_KEY_AVAIL$", 22))
			return 0;

	    *len = curr_dataset.data_entity_size;
	}

	fprintf(stderr, "ERRIMSS_GETDATA_UNAVAIL\n");
	return -1;
}


//Method storing a specific data element.
int32_t
set_data(int32_t 	 dataset_id,
	 int32_t 	 data_id,
	 unsigned char * buffer)
{
	int32_t n_server;
	//Server containing the corresponding data to be written.
	if ((n_server = get_data_location(dataset_id, data_id, SET)) == -1)

		return -1;

	char key_[KEY];
	//Key related to the requested data element.
	sprintf(key_, "%d %s$%d", curr_dataset.data_entity_size, curr_dataset.uri_, data_id);

	int key_length = strlen(key_)+1;
	char key[key_length];
	memcpy((void *) key, (void *) key_, key_length);
	key[key_length-1] = '\0';

	int32_t curr_imss_storages = curr_imss.info.num_storages;

	//Send the data block to every server implementing redundancy.
	for (int32_t i = 0; i < curr_dataset.repl_factor; i++)
	{
		//Server receiving the current data block.
		uint32_t n_server_ = (n_server + i*(curr_imss_storages/curr_dataset.repl_factor)) % curr_imss_storages;

		//printf("BLOCK %d SENT TO %d SERVER with key: %s (%d)\n", data_id, n_server_, key, key_length);

		//Send read request message specifying the block URI.
		//if (zmq_send(curr_imss.conns.sockets_[n_server_], key, KEY, ZMQ_SNDMORE) < 0)
		if (zmq_send(curr_imss.conns.sockets_[n_server_], key, key_length, ZMQ_SNDMORE) != key_length)
		{
			perror("ERRIMSS_SETDATA_REQ");
			return -1;
		}

		//Send read request message specifying the block data.
		if (zmq_send (curr_imss.conns.sockets_[n_server_], buffer, curr_dataset.data_entity_size, 0) != curr_dataset.data_entity_size)
		{
			perror("ERRIMSS_SETDATA_SEND");
			return -1;
		}
	}

	return 0;
}

//WARNING! This function allocates memory that must be released by the user.

//Method retrieving the location of a specific data object.
char **
get_dataloc(const char *    dataset,
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
	switch (found_imss_in)
	{
		//No IMSS was found with the requested name.
		case 0:
		{
			fprintf(stderr, "ERRIMSS_GETDATALOC_IMSSNOTEXISTS\n");
			return NULL;
		}
		//The IMSS was retrieved from the metadata server.
		case 1:
		{
			//Hint specifying that the IMSS structure was retrieved but not initialized.
			where_imss.conns.matching_server = -2;

			GInsert (&imssd_pos, &imssd_max_size, (char *) &where_imss, imssd, free_imssd);

			break;
		}
	}

	//Set the policy corresponding to the retrieved dataset.
	if (set_policy(&where_dataset) == -1)
	{
		fprintf(stderr, "ERRIMSS_GETDATALOC_SETPOLICY\n");
		return NULL;
	}

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
int32_t
get_type(char * uri)
{
	//Formated uri to be sent to the metadata server.
	char formated_uri[strlen(uri)+1];
	sprintf(formated_uri, "0 %s", uri);
	size_t formated_uri_length = strlen(formated_uri);

	//Send the request.
	if (zmq_send(stat_client, formated_uri, formated_uri_length, 0) != formated_uri_length)
	{
		fprintf(stderr, "ERRIMSS_GETTYPE_REQ\n");
		return -1;
	}

	zmq_msg_t entity_info;
	zmq_msg_init (&entity_info);
	//Receive the answer.
	if (zmq_msg_recv(&entity_info, stat_client, 0) == -1)
	{
		fprintf(stderr, "ERRIMSS_GETTYPE_REQ\n");
		return -1;
	}

	//Access the information received.
	imss_info * data = (imss_info *) zmq_msg_data(&entity_info);

	//Determine what was retrieved from the metadata server.
	if (data->type == 'I')

		return 1;

	else if (data->type == 'D' || data->type == 'L')

		return 2;

	zmq_msg_close(&entity_info);

	return 0;
}


/**********************************************************************************/
/***************************** DATA RELEASE RESOURCES *****************************/
/**********************************************************************************/


//Method releasing an imss_info structure previously provided to the client.
int32_t
free_imss(imss_info * imss_info_)
{
	for (int32_t i = 0; i < imss_info_->num_storages; i++)

		free(imss_info_->ips[i]);

	free(imss_info_->ips);

	return 0;
}

//Method releasing a dataset structure previously provided to the client.
int32_t
free_dataset(dataset_info * dataset_info_)
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
