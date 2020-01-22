#include <zmq.h>
#include <stdint.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <mpi.h>
#include "imss.h"
#include "policies.h"


/******************************** GLOBAL VARIABLES ********************************/

void * stat_context;				//Metadata server context.
void * stat_client;				//Metadata server socket.
imss_info imss_;				//Structure storing current IMSS metadata information.
imss_descriptor imss_d;				//Structure storing resources required to connect to a certain IMSS.
dataset_info dataset_;
int32_t current_dataset;			//Last dataset whose policy was established as the current one.
char client_node[MPI_MAX_PROCESSOR_NAME];	//Node name where the client is running.
int32_t len_client_node;			//Length of the previous node name.



/*********************** IMSS INTERNAL MANAGEMENT FUNCTIONS ***********************/


//FREE function provided to zmq_msg_init_data in order to free the buffer once sent.
void free_msg (void * data, void * hint) {free(data);}

//Method creating a ZMQ socket connection of type DEALER over a certain ip + port couple.
int32_t conn_crt_(void ** context, void ** socket, char * ip_, int16_t port, int32_t ident_)
{
	//Create ZMQ socket context.
	if ((*context = zmq_ctx_new()) == NULL)
	{
		perror("ERRIMSS_CONN_CTXNEW");
		return -1;
	}

	//Create the actual ZMQ socket.
	if ((*socket = zmq_socket(*context, ZMQ_DEALER)) == NULL)
	{
		perror("ERRIMSS_CONN_SOCKET_CRT");
		return -1;
	}

	//Set communication id.
	if (zmq_setsockopt(*socket, ZMQ_IDENTITY, &ident_, sizeof(int32_t)) == -1)
	{
		perror("ERRIMSS_CONN_SETSOCKET_IDEN");
		return -1;
	}

	int32_t rcvtimeo = 60000;
	//Set a timeout to receive operations.
	if (zmq_setsockopt(*socket, ZMQ_RCVTIMEO, &rcvtimeo, sizeof(int32_t)) == -1)
	{
		perror("ERRIMSS_CONN_SETSOCKET_RCVTIMEO");
		return -1;
	}

	//Connection address.
	char addr_[512]; 
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

//Method destroying an existing ZMQ socke connectiono.
int32_t conn_dstr_(void ** context, void ** socket)
{
	//Close the provided socket.
	if (zmq_close(*socket) == -1)
	{
		perror("ERRIMSS_CONN_SCKT_CLOSE");
		return -1;
	}

	//Close the associated context.
	if (zmq_ctx_destroy(*context) == -1)
	{
		perror("ERRIMSS_CONN_CTX_CLOSE");
		return -1;
	}

	return 0;
}

//Check the existance within the session of the IMSS that the dataset is to be created in.
int32_t imss_check(char * dataset_uri)
{
	//Traverse the whole set of IMSS structures in order to find the one.
	/*
	for (int i = 0; i < imss_.length(); i++)

		if (!strncmp(dataset_uri, imss_[i].uri_, strlen(imss_[i].uri_)))

			return i;

	return -1;
	*/

	return 0;
}



/********************* METADATA SERVICE MANAGEMENT FUNCTIONS  *********************/


// Method creating a connection to the metadata server.
int32_t stat_init(char * ip, uint16_t port)
{
	current_dataset = -1;

	//Obtain the node name where the client is running.
	MPI_Get_processor_name(client_node, &len_client_node);

	//Create the connection to the metadata server dispatcher thread.
	if (conn_crt_(&stat_context, &stat_client, ip, port, -1) == -1)

		return -1;

	//ZMQ message requesting a connection to the metadata server.
	char request[] = "HELLO!\0";
	zmq_msg_t connection_req;
	zmq_msg_init_size(&connection_req, 7);
	memcpy(zmq_msg_data(&connection_req), request, 7);

	//Send the metadata server connection request.
	if (zmq_msg_send(&connection_req, stat_client, 0) != 7)
	{
		perror("ERRIMSS_STAT_HELLO");
		return -1;
	}

	zmq_msg_close(&connection_req);

	//ZMQ message retrieving the connection information.
	zmq_msg_t connection_info;
	zmq_msg_init (&connection_info);

	if (zmq_msg_recv(&connection_info, stat_client, 0) == -1)
	{
		perror("ERRIMSS_STAT_CONN");
		return -1;
	}

	//Close the previous connection.
	if (conn_dstr_(&stat_context, &stat_client) == -1)

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
	if (conn_crt_(&stat_context, &stat_client, ip, stat_port, stat_id) == -1)

		return -1;

	return 0;
}

//Method disabling connection resources to the metadata server.
int32_t release_stat()
{
	return (conn_dstr_(&stat_context, &stat_client));
}



/***************** IN-MEMORY STORAGE SYSTEM MANAGEMENT FUNCTIONS  *****************/

// Method deploying the storage system.
int32_t init_imss(char * imss_uri, int32_t n_servers, int32_t buff_size, char * hostfile, uint16_t conn_port)
{
	//TODO: check if an IMSS already exists with the same name.

	//Final command to be executed.
	char command[1024]; 
        memset(command, 0, 1024);

	//Path to the IMSS server binary.
	char binary[] 	= "./buffer";
	char mpirun_1[]	= "mpirun -np ";
	char mpirun_2[]	= " -f ";

	//Obtain the mpi deployment to be executed.
	sprintf(command, "%s%d%s%s%c%s%c%d%c%d%c%c", mpirun_1, n_servers, mpirun_2, hostfile, ' ', binary, ' ', conn_port, ' ', buff_size, ' ', '&');

	printf("%s\n", command);

	//Perform the deployment (FROM LINUX MAN PAGES: "system() returns after the command has been completed").
	if (system(command) == -1)
	{
		perror("ERRIMSS_INITIMSS_DEPLOY");
		return -1;
	}

	//Save IMSS metadata deployment.
	strcpy(imss_.uri_, imss_uri);
	imss_.num_storages = n_servers;
	imss_.conn_port    = conn_port;
	imss_.ips          = (char **) malloc(n_servers * sizeof(char *));

	//TODO: check if one context could be used to create all sockets.
	//Resources required to connect to the corresponding IMSS.
	imss_d.contexts_ = (void **) malloc(n_servers * sizeof(void *));
	imss_d.sockets_ = (void **) malloc(n_servers * sizeof(void *));

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

	imss_d.matching_server = -1;

	//Connect to all servers.
	for (int i = 0; i < n_servers; i++)
	{
		//Allocate resources in the metadata structure so as to store the current IMSS's IP.
		(imss_.ips)[i] = (char *) malloc(LINE_LENGTH);
		size_t l_size = LINE_LENGTH;

		//Save IMSS metadata deployment.
		n_chars = getline(&((imss_.ips)[i]), &l_size, svr_nodes);

		//Erase the new line character ('\n') from the string.
		((imss_.ips)[i])[n_chars - 1] = '\0';

		//Save the current socket value when the IMSS ip matches the clients' one.
		if (!strncmp((imss_.ips)[i], client_node, len_client_node))
		
			imss_d.matching_server = i;

		//Create the connection to the IMSS server dispatcher thread.
		if (conn_crt_(&(imss_d.contexts_[i]), &(imss_d.sockets_[i]), (imss_.ips)[i], imss_.conn_port, -1) == -1)

			return -1;

		//ZMQ message requesting a connection to the metadata server.
		char request[] = "HELLO!\0";
		zmq_msg_t connection_req;
		zmq_msg_init_size(&connection_req, 7);
		memcpy(zmq_msg_data(&connection_req), request, 7);

		//Send the IMSS server connection request.
		if (zmq_msg_send(&connection_req, imss_d.sockets_[i], 0) != 7)
		{
			perror("ERRIMSS_INITIMSS_HELLO");
			return -1;
		}

		zmq_msg_close(&connection_req);

		//ZMQ message retrieving the connection information.
		zmq_msg_t connection_info;
		zmq_msg_init (&connection_info);

		if (zmq_msg_recv(&connection_info, imss_d.sockets_[i], 0) == -1)
		{
			perror("ERRIMSS_INITIMSS_CONN");
			return -1;
		}

		//Close the previous connection.
		if (conn_dstr_(&(imss_d.contexts_[i]), &(imss_d.sockets_[i])) == -1)
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
		if (conn_crt_(&(imss_d.contexts_[i]), &(imss_d.sockets_[i]), (imss_.ips)[i], imss_port, imss_id) == -1)

			return -1;
	}

	//Close the file.
	if (fclose(svr_nodes) != 0)
	{
		perror("ERR_CLOSE_FILE");
		return -1;
	}

	//TODO: add the created IMSS into the IMSS_STRUCT data control vector.

	return 0;
}

// Method disabling all connection resources and tearing down all servers.
int32_t release_imss(char * imss_uri)
{
	//TODO add vectors managing the whole set of data structures underneath.

	//TODO add vectorial behavior.

	//Send a release message to each buffer within the corresponding IMSS.
	for (int32_t i = 0; i < imss_.num_storages; i++)
	{
		//Resources required to reconnect to the dispatcher thread of each buffer.
		void * ctx;
		void * skt;
		
		//Create a connection to the corresponding dispatcher thread.
		if (conn_crt_(&ctx, &skt, (imss_.ips)[i], imss_.conn_port, -1))
		{
			perror("ERRIMSS_RELEASE_CONNCTR");
			return -1;
		}

		//ZMQ message specifying a tear down request.
		char request[] = "TEAR_DOWN!\0";
		zmq_msg_t teardown_req;
		zmq_msg_init_size(&teardown_req, 11);
		memcpy(zmq_msg_data(&teardown_req), request, 11);

		//Send the IMSS server tear down request.
		if (zmq_msg_send(&teardown_req, skt, 0) != 11)
		{
			perror("ERRIMSS_RELEASE_HELLO");
			return -1;
		}

		zmq_msg_close(&teardown_req);

		//Close the previous connection to the corresponding buffer's dispatcher thread.
		if (conn_dstr_(&ctx, &skt) == -1)
		{
			perror("ERRIMSS_RELEASE_CONNDSTRY1");
			return -1;
		} 

		//Close the connetion to the server thread to the same buffer.
		if (conn_dstr_(&(imss_d.contexts_[i]), &(imss_d.sockets_[i])) == -1)
		{
			perror("ERRIMSS_RELEASE_CONNDSTRY2");
			return -1;
		}
	}

	return 0;
}
/*
//Method creating the set of required connections with an existing IMSS.
int32_t open_imss(char * imss_uri);

//Method destroying the client's current session with a certain IMSS.
int32_t close_imss(char * imss_uri);

// Method retrieving information related to a certain backend.
int32_t stat_imss(char * imss_uri, imss_info * imss_info_);
{}
*/


/************************** DATASET MANAGEMENT FUNCTIONS **************************/


// Method creating a dataset and the environment enabling READ or WRITE operations over it.
int32_t create_dataset(char * dataset_uri, char * policy, int32_t num_data_elem, int32_t data_elem_size)
{
	int32_t imssd;

	//Check if the IMSS storing the dataset exists within the clients session.
	if ((imssd = imss_check(dataset_uri)) == -1)

		return -1;

	//Dataset metadata request.
	if (!stat_dataset(dataset_uri, &dataset_))
	{
		//TODO: free resources through to-be-implemented free function.
		return -1;
	}

	//Save the associated metadata of the current dataset.
	strcpy(dataset_.uri_, dataset_uri);
	strcpy(dataset_.policy, policy);
	//TODO: will we be knowing the number of elements that a dataset will be compossed by beforehand?
	dataset_.num_data_elem = num_data_elem;
	dataset_.data_entity_size = data_elem_size*1024;
	dataset_.imss_d = imssd;

	char msg[REQ_MSG];
	sprintf(msg, "%lu%c%s", sizeof(dataset_info), '$', dataset_uri);
	zmq_msg_t dataset_req;
	zmq_msg_init_size(&dataset_req, REQ_MSG);
	memcpy(zmq_msg_data(&dataset_req), msg, REQ_MSG);

	//Send the dataset URI associated to the dataset metadata structure to be sent.
	if (zmq_msg_send(&dataset_req, stat_client, ZMQ_SNDMORE) != REQ_MSG)
	{
		perror("ERRIMSS_DATASET_SNDURI");
		return -1;
	}

	//Release message resources for the dataset request message class.
	zmq_msg_close(&dataset_req);

	//Send the created dataset metadata structure to the metadata server.
	if (zmq_send(stat_client, &dataset_, sizeof(dataset_info), 0) == -1)
	{
		perror("ERRIMSS_DATASET_SNDDATA");
		return -1;
	}

	//Set the specified policy.
	if (set_policy(dataset_.policy, dataset_.num_data_elem, imss_d.matching_server) == -1)
	{
		perror("ERRIMSS_DATASET_SETPLCY");
		return -1;
	}

	//TODO: return the descriptor within the vector of datasets.
	return 0;
}

//Method retrieving an existing dataset.
int32_t open_dataset(char * dataset_uri)
{
	int32_t imssd;

	//Check if the IMSS containing the dataset exists within the clients session.
	if ((imssd = imss_check(dataset_uri)) == -1)
		return -1;

	//Dataset metadata request.
	if (stat_dataset(dataset_uri, &dataset_) == -1)
		return -1;

	//Assign the associated IMSS descriptor to the new dataset structure.
	dataset_.imss_d = imssd;

	printf("%s - %s - %d - %d\n", dataset_.uri_, dataset_.policy, dataset_.num_data_elem, dataset_.data_entity_size);

	//Set the specified policy.
	if (set_policy(dataset_.policy, dataset_.num_data_elem, imss_d.matching_server) == -1)
	{
		perror("ERRIMSS_DATASET_SETPLCY");
		return -1;
	}

	//TODO: return the descriptor within the vector of datasets.
	return 0;
}

//Method releasing the storage resources.
int32_t release_dataset(int32_t datasetd)
{
	return 0;
}

//Method retrieving information related to a certain dataset.
int32_t stat_dataset(char * dataset_uri, dataset_info * dataset_info_)
{
	//Formated dataset uri to be sent to the metadata server.
	char formated_uri[strlen(dataset_uri)+1];
	sprintf(formated_uri, "$%s", dataset_uri);

	//Message containing the dataset request.
	zmq_msg_t dataset_req;
	zmq_msg_init_size(&dataset_req, URI_);
	memcpy(zmq_msg_data(&dataset_req), formated_uri, URI_);

	//Send the request.
	if (zmq_msg_send(&dataset_req, stat_client, 0) != URI_)
	{
		free(dataset_info_);
		perror("ERRIMSS_DATASET_REQ");
		return -1;
	}

	//Receive the associated structure.
	if (zmq_recv(stat_client, dataset_info_, sizeof(dataset_info), 0) == -1)
	{
		free(dataset_info_);
		perror("ERRIMSS_DATASET_RCV");
		return -1;
	}

	//Release message resources for the dataset request message class.
	zmq_msg_close(&dataset_req);

	return (!strcmp("$ERRIMSS_NO_KEY_AVAIL$", dataset_info_->uri_) ? -1 : 0);

	//TODO: receive remaining information if the name did not correspond to the ERR msg.
}

/*
//Method retrieving a whole dataset parallelizing the procedure.
unsigned char * get_dataset(char * dataset_uri, uint64_t * buff_length);
{}

//Method storing a whole dataset parallelizing the procedure.
int32_t set_dataset(char * dataset_uri, unsigned char * buffer, uint64_t offset)
{}

//Method retrieving the location of the whole set of data elements composing the dataset.
struct dataset_location get_dataset_location(char * dataset_uri)
{}

//Method releasing information allocated by 'get_dataset_location'.
int32_t free_dataset_location(struct dataset_location * dataset_location_);
{}
*/


/************************ DATA OBJECT MANAGEMENT FUNCTIONS ************************/


//Method performing a retrieval operation of a specific data object.
int32_t get_data(int32_t datasetd, int32_t data_id, unsigned char * buffer, int64_t * buff_length)
{
	//Server containing the corresponding data to be retrieved.
	int32_t n_server;

	if ((n_server = get_data_location(datasetd, data_id)) == -1)
	{
		perror("ERRIMSS_GETDATA_FINDSERVER");
		return -1;
	}

	//Key related to the requested data element.
	char key[KEY]; sprintf(key, "0$%s$%d",  dataset_.uri_, data_id);

	//Message containing the key of the block to be retrieved.
	zmq_msg_t msg;
	zmq_msg_init_size(&msg, KEY);
	memcpy(zmq_msg_data(&msg), key, KEY);

	//Send read request message specifying the block URI.
	if (zmq_msg_send (&msg, imss_d.sockets_[n_server], 0) != KEY)
	{
		perror("ERRIMSS_GETDATA_REQ");
		return -1;
	}

	zmq_msg_close(&msg);

	//Receive data related to the previous read request directly into the buffer.
	if ((*buff_length = zmq_recv(imss_d.sockets_[n_server], buffer, dataset_.data_entity_size, 0)) == -1)
	{
		perror("ERRIMSS_GETDATA_RECV");
		return -1;
	}

	//Check if the requested key was correctly retrieved.
	int32_t strncmp_ = strncmp((const char *) buffer, "$ERR_NO_KEY_AVAIL$", 18);

	return (!strncmp_ ? -1 : 0);
}

//Method storing a specific data object.
int32_t set_data(int32_t datasetd, int32_t data_id, unsigned char * buffer)
{
	//Server containing the corresponding data to be written.
	int32_t n_server;

	if ((n_server = get_data_location(datasetd, data_id)) == -1)
	{
		perror("ERRIMSS_SETDATA_FINDSERVER");
		return -1;
	}

	//Key related to the requested data element.
	char key[KEY]; sprintf(key, "%d%c%s%c%d", dataset_.data_entity_size, '$', dataset_.uri_, '$', data_id);

	//Message containing the key of the block to be retrieved.
	zmq_msg_t msg;
	zmq_msg_init_size(&msg, KEY);
	memcpy(zmq_msg_data(&msg), key, KEY);

	//Send read request message specifying the block URI.
	if (zmq_msg_send (&msg, imss_d.sockets_[n_server], ZMQ_SNDMORE) != KEY)
	{
		perror("ERRIMSS_SETDATA_REQ");
		return -1;
	}

	zmq_msg_close(&msg);

	//Message containing the data associated to the previous key.
	zmq_msg_t content;
	zmq_msg_init_data (&content, buffer, dataset_.data_entity_size, free_msg, NULL);

	//Send read request message specifying the block data.
	if (zmq_msg_send (&content, imss_d.sockets_[n_server], 0) != dataset_.data_entity_size)
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
int32_t get_data_location(int32_t datasetd, int32_t data_id)
{
	//If the current dataset policy was not established yet.
	if (current_dataset != datasetd)
	{
		//Set the corresponding.
		if (set_policy(dataset_.policy, dataset_.num_data_elem, imss_d.matching_server) == -1)
		{
			perror("ERRIMSS_SET_POLICY");
			return -1;
		}

		current_dataset = datasetd;
	}

	int32_t server;
	//Search for the server that is supposed to have the specified data element.
	if ((server = find_server(imss_.num_storages, data_id, dataset_.uri_)) < 0)
	{
		perror("ERRIMSS_FIND_SERVER");
		return -1;
	}

	return server;
}



/***************************** DATA RELEASE RESOURCES *****************************/


//FIXME
//Method releasing typedef structures.
int32_t free_(int32_t typedef_, void * typedef_ref_)
{
	//Specify the release procedure to follow.
	switch(typedef_)
	{
		case IMSS_INFO:

			break;

		case DATASET_INFO:

			break;

		default:

			perror("ERRIMSS_FREE_NODATATYPE");
			break;
	}

	return 0;
}
