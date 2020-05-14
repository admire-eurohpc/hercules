#include <zmq.h>
#include <glib.h>
#include <errno.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <sys/types.h>
#include "imss.h"
#include "comms.h"
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

dataset_info	empty_dataset;
imss		empty_imss;




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
	int32_t rcvtimeo = TIMEOUT_MS;

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

//Method stating if a socket has been connected to an endpoint.
int32_t
socket_connected(void ** monitor)
{
	zmq_msg_t event_msg;
	zmq_msg_init (&event_msg);
	//Message storing the event.
	if (zmq_msg_recv(&event_msg, *monitor, 0) == -1)

		return -1;

	//Elements formalizing the retrieved data.
	uint8_t * data = (uint8_t *) zmq_msg_data(&event_msg);
	uint16_t event = *(uint16_t *) data;
	zmq_msg_close(&event_msg);

	return event == ZMQ_EVENT_CONNECTED;
}

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


//Method creating a connection to the metadata server.
int32_t
stat_init(char *   ip,
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

	//TODO: check performance changing the number of threads.
	//zmq_ctx_set(ctx, ZMQ_IO_THREADS, 4);

	//Retrieve the hostname where the current process is running.
	if (gethostname(client_node, 512) == -1)
	{
		perror("ERRIMSS_GETHOSTNAME");
		return -1;
	}
	len_client_node = strlen(client_node);

	//Create the connection to the metadata server dispatcher thread.
	if (conn_crt_(&stat_client, ip, port, rank, 0, NULL) == -1)

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
	if (conn_crt_(&stat_client, ip, stat_port, stat_id, 0, NULL) == -1)

		return -1;

	return 0;
}

//Method disabling connection resources to the metadata server.
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




/**********************************************************************************/
/***************** IN-MEMORY STORAGE SYSTEM MANAGEMENT FUNCTIONS  *****************/
/**********************************************************************************/


//Method deploying the storage system.
int32_t
init_imss(char *   imss_uri,
	  int32_t  n_servers,
	  int32_t  buff_size,
	  char *   hostfile,
	  uint16_t conn_port)
{
	imss_info aux_imss;
	//Check if the new IMSS uri has been already assigned.
	int32_t existing_imss = stat_imss(imss_uri, &aux_imss);

	if (existing_imss)
	{
		perror("ERRIMSS_INITIMSS_ALREADYEXISTS");
		return -1;
	}

	//Final command to be executed.
	char command[1024]; 
        memset(command, 0, 1024);

	//Path to the IMSS server binary.
	char binary[] 	= "./buffer";
	char mpirun_1[]	= "mpirun -np ";
	char mpirun_2[]	= " -f ";

	//Obtain the mpi deployment to be executed.
	sprintf(command, "%s%d%s%s%c%s%c%d%c%d%c%c", mpirun_1, n_servers, mpirun_2, hostfile, ' ', binary, ' ', conn_port, ' ', buff_size, ' ', '&');

	//Perform the deployment (FROM LINUX MAN PAGES: "system() returns after the command has been completed").
	if (system(command) == -1)
	{
		perror("ERRIMSS_INITIMSS_DEPLOY");
		return -1;
	}

	//IMSS creation.
	imss new_imss;
	strcpy(new_imss.info.uri_, imss_uri);
	new_imss.info.num_storages  = n_servers;
	new_imss.info.conn_port     = conn_port;
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
		if (!strncmp((new_imss.info.ips)[i], client_node, len_client_node))

			new_imss.conns.matching_server = i;

		//Create the connection to the IMSS server dispatcher thread.
		if (conn_crt_(&(new_imss.conns.sockets_[i]), (new_imss.info.ips)[i], new_imss.info.conn_port, process_rank, 0, NULL) == -1)

			return -1;

		char request[16];
		//ZMQ message requesting a connection to the metadata server.
		sprintf(request, "%s %d%c", "HELLO!", buff_size, '\0');
		//Send the IMSS server connection request.
		if (zmq_send(new_imss.conns.sockets_[i], request, 16, 0)  != 16)
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


//Method creating the set of required connections with an existing IMSS.
int32_t
open_imss(char * imss_uri)
{
	//New IMSS structure storing the entity to be created.
	imss new_imss;

	//Retrieve the actual information from the metadata server.
	int32_t imss_existance = stat_imss(imss_uri, &new_imss.info);
	//Check if the requested IMSS did not exist or was already stored in the local vector.
	switch (imss_existance)
	{
		case 0:
		{
			perror("ERRIMSS_OPENIMSS_NOTEXISTS");
			return -1;
		}
		case 2:
		{
			perror("ERRIMSS_OPENIMSS_ALREADYSTORED");
			return -1;
		}
	}

	new_imss.conns.sockets_ = (void **) malloc(new_imss.info.num_storages*sizeof(void*));

	//Connect to the requested IMSS.
	for  (int32_t i = 0; i < new_imss.info.num_storages; i++)
	{
		//Create the connection to the IMSS server dispatcher thread.
		if (conn_crt_(&(new_imss.conns.sockets_[i]), (new_imss.info.ips)[i], new_imss.info.conn_port, process_rank, 0, NULL) == -1)

			return -1;

		//ZMQ message requesting a connection to the dispatcher thread.
		char request[16];
		sprintf(request, "%s", "HELLO!");
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
		if (!strncmp((new_imss.info.ips)[i], client_node, len_client_node))

			new_imss.conns.matching_server = i;

	}

	//Add the created struture into the underlying IMSSs.
	GInsert (&imssd_pos, &imssd_max_size, (char *) &new_imss, imssd, free_imssd);

	return 0;
}

//Method disabling all connection resources.
int32_t
release_imss(char * imss_uri)
{
	//Search for the requested IMSS.

	imss imss_;
	int32_t imss_position;
	if ((imss_position = find_imss(imss_uri, &imss_)) == -1)
	{
		perror("ERRIMSS_RELIMSS_NOTFOUND");
		return -1;
	}

	//Release the set of connections to the corresponding IMSS.

	for (int32_t i = 0; i < imss_.info.num_storages; i++)
	{
		if (conn_dstr_((imss_.conns.sockets_[i])) == -1)
		{
			perror("ERRIMSS_RELEASE_CONNDSTRY2");
			return -1;
		}

		free(imss_.info.ips[i]);
	}

	free(imss_.info.ips);

	g_array_remove_index(imssd, imss_position);
	g_array_insert_val(imssd, imss_position, empty_imss);
	//Add the released position to the set of free positions.
	g_array_append_val(free_imssd, imss_position);

	return 0;
}

//Method retrieving information related to a certain backend.
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
		perror("ERRIMSS_DATASET_REQ");
		return -1;
	}

	//Receive the associated structure.
	return recv_dynamic_struct(stat_client, imss_info_, IMSS_INFO);
}




/**********************************************************************************/
/************************** DATASET MANAGEMENT FUNCTIONS **************************/
/**********************************************************************************/


//Method creating a dataset and the environment enabling READ or WRITE operations over it.
int32_t
create_dataset(char *  dataset_uri,
	       char *  policy,
	       int32_t num_data_elem,
	       int32_t data_elem_size)
{
	if ((dataset_uri == NULL) || (policy == NULL) || !num_data_elem || !data_elem_size)
	{
		perror("ERRIMSS_CRTDATASET_WRONGARG");
		return -1;
	}

	int32_t associated_imss_indx;
	//Check if the IMSS storing the dataset exists within the clients session.
	if ((associated_imss_indx = imss_check(dataset_uri)) == -1)

		return -1;

	imss associated_imss;
	associated_imss = g_array_index(imssd, imss, associated_imss_indx);

	dataset_info new_dataset;
	//Dataset metadata request.
	if (stat_dataset(dataset_uri, &new_dataset))
	{
		perror("ERRIMSS_CRTDATASET_ALREADYEXISTS");
		return -1;
	}

	//Save the associated metadata of the current dataset.
	strcpy(new_dataset.uri_, 	dataset_uri);
	strcpy(new_dataset.policy, 	policy);
	new_dataset.num_data_elem 	= num_data_elem;
	new_dataset.data_entity_size 	= data_elem_size*1024;
	new_dataset.imss_d 		= associated_imss_indx;
	new_dataset.local_conn 		= associated_imss.conns.matching_server;

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

	//Set the specified policy.
	if (set_policy(&new_dataset) == -1)
	{
		perror("ERRIMSS_DATASET_SETPLCY");
		return -1;
	}

	//Add the created struture into the underlying IMSSs.
	return (GInsert (&datasetd_pos, &datasetd_max_size, (char *) &new_dataset, datasetd, free_datasetd));
}

//Method retrieving an existing dataset.
int32_t
open_dataset(char * dataset_uri)
{
	int32_t associated_imss_indx;

	//Check if the IMSS storing the dataset exists within the clients session.
	if ((associated_imss_indx = imss_check(dataset_uri)) == -1)
	{
		perror("ERRIMSS_OPENDATA_IMSSNOTFOUND");
		return -1;
	}

	imss associated_imss;
	associated_imss = g_array_index(imssd, imss, associated_imss_indx);

	dataset_info new_dataset;
	//Dataset metadata request.
	int32_t stat_dataset_res = stat_dataset(dataset_uri, &new_dataset);

	//Check if the requested dataset did not exist or was already stored in the local vector.
	switch (stat_dataset_res)
	{
		case 0:
		{
			perror("ERRIMSS_OPENDATASET_NOTEXISTS");
			return -1;
		}
		case 2:
		{
			perror("ERRIMSS_OPENDATASET_ALREADYSTORED");
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

	//Set the specified policy.
	if (set_policy(&new_dataset) == -1)
	{
		perror("ERRIMSS_DATASET_SETPLCY");
		return -1;
	}

	//Add the created struture into the underlying IMSSs.
	return (GInsert (&datasetd_pos, &datasetd_max_size, (char *) &new_dataset, datasetd, free_datasetd));
}

//Method releasing the storage resources.
int32_t
release_dataset(int32_t dataset_id)
{
	//Check if the provided descriptor corresponds to a position within the vector.
	if ((dataset_id < 0) || (dataset_id >= datasetd_max_size))
	{
		perror("ERRIMSS_RELDATASET_BADDESCRIPTOR");
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
stat_dataset(char * 	    dataset_uri,
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


//Method performing a retrieval operation of a specific data object.
int32_t
get_data(int32_t 	 dataset_id,
	 int32_t 	 data_id,
	 unsigned char * buffer)
{
	//Server containing the corresponding data to be retrieved.
	int32_t n_server;
	if ((n_server = get_data_location(dataset_id, data_id, GET)) == -1)

		return -1;

	//Key related to the requested data element.
	char key[KEY];
	sprintf(key, "0 %s$%d",  curr_dataset.uri_, data_id);
	//Send read request message specifying the block URI.
	if (zmq_send(curr_imss.conns.sockets_[n_server], key, KEY, 0) < 0)
	{
		perror("ERRIMSS_GETDATA_REQ");
		return -1;
	}

	//Receive data related to the previous read request directly into the buffer.
	if (zmq_recv(curr_imss.conns.sockets_[n_server], buffer, curr_dataset.data_entity_size, 0) == -1)
	{
		perror("ERRIMSS_GETDATA_RECV");
		return -1;
	}

	//Check if the requested key was correctly retrieved.
	int32_t strncmp_ = strncmp((const char *) buffer, "$ERRIMSS_NO_KEY_AVAIL$", 22);

	return (!strncmp_ ? -1 : 0);
}

//Method storing a specific data object.
int32_t
set_data(int32_t 	 dataset_id,
	 int32_t 	 data_id,
	 unsigned char * buffer)
{
	//Server containing the corresponding data to be written.
	int32_t n_server;
	if ((n_server = get_data_location(dataset_id, data_id, SET)) == -1)

		return -1;

	//Key related to the requested data element.
	char key[KEY];
	sprintf(key, "%d %s$%d", curr_dataset.data_entity_size, curr_dataset.uri_, data_id);
	//Send read request message specifying the block URI.
	if (zmq_send(curr_imss.conns.sockets_[n_server], key, KEY, ZMQ_SNDMORE) < 0)
	{
		perror("ERRIMSS_SETDATA_REQ");
		return -1;
	}

	//Message containing the data associated to the previous key.
	zmq_msg_t content;
	zmq_msg_init_data (&content, buffer, curr_dataset.data_entity_size, free_msg, NULL);
	//Send read request message specifying the block data.
	if (zmq_msg_send (&content, curr_imss.conns.sockets_[n_server], 0) != curr_dataset.data_entity_size)
	{
		perror("ERRIMSS_SETDATA_SEND");
		return -1;
	}

	zmq_msg_close(&content);

	return 0;
}


/*
//Method storing a certain data object and checking that it has been correctly stored.
int32_t setv_data(int32_t datasetd, uint64_t data_id, unsigned char * buffer)
{}
*/
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
			perror("ERRIMSS_SET_POLICY");
			return -1;
		}

		current_dataset = dataset_id;
	}

	int32_t server;
	//Search for the server that is supposed to have the specified data element.
	if ((server = find_server(curr_imss.info.num_storages, data_id, curr_dataset.uri_, op_type)) < 0)
	{
		perror("ERRIMSS_FIND_SERVER");
		return -1;
	}

	return server;
}




/**********************************************************************************/
/***************************** DATA RELEASE RESOURCES *****************************/
/**********************************************************************************/


//Method releasing an imss_info structure previously provided to the client.
int32_t
free_imss(imss_info * struct_)
{
	for (int32_t i = 0; i < struct_->num_storages; i++)

		free(struct_->ips[i]);

	free(struct_->ips);

	return 0;
}

//Method releasing a dataset structure previously provided to the client.
int32_t
free_dataset(dataset_info * struct_)
{
	if (!strcmp(struct_->policy, "LOCAL"))
	{
		if (struct_->data_locations)				

			free(struct_->data_locations);

		if (struct_->num_blocks_written)				

			free(struct_->num_blocks_written);

		if (struct_->blocks_written)				

			free(struct_->blocks_written);
	}

	return 0;
}

