#ifndef IMSS_WRAP_
#define IMSS_WRAP_

#include <stdint.h>


#define SET_DATASET	0
#define GET_DATASET	1
#define URI_		256
#define REQ_MSG		272
#define LINE_LENGTH	512
#define KEY		512
#define TIMEOUT_MS	10000
#define MONITOR		1
#define ELEMENTS	16
#define IMSS		0
#define DATASET		1





//Structure storing all information related to a certain backend.
typedef struct {

	//IMSS URI.
	char uri_[URI_];
	//Byte specifying the type of structure.
	char type;
	//Set of ips comforming the IMSS.
	char ** ips;
	//Number of IMSS servers.
	int32_t num_storages;
	//Server's dispatcher thread connection port.
	uint16_t conn_port;

} imss_info;

//Structure storing the required connection resources to the IMSS in the client side.
typedef struct {

	//Set of actual sockets.
	void ** sockets_;
	//Socket connecting the corresponding client to the server running in the same node.
	int32_t matching_server;

} imss_conn;

//Structure merging the previous couple.
typedef struct {

	imss_info info;

	imss_conn conns;

} imss;

//Structure storing all information related to a certain dataset.
typedef struct {

	//URI identifying a certain dataset.
	char uri_[URI_];
	//Byte specifying the type of structure.
	char type;
	//Policy that was followed in order to write the dataset.
	char policy[8];
	//Number of data elements conforming the dataset entity.
	int32_t num_data_elem;
	//Size of each data element (in KB).
	int32_t data_entity_size;
	//IMSS descriptor managing the dataset in the current client session.
	int32_t imss_d;
	//Connection to the IMSS server running in the same machine.
	int32_t local_conn;


	/*************** USED EXCLUSIVELY BY LOCAL DATASETS ***************/


	//Vector of characters specifying the position of each data element.
	uint16_t * data_locations;
	//Number of blocks written by the client in the current session.
	uint64_t * num_blocks_written;
	//Actual blocks written by the client.
	uint32_t * blocks_written;

} dataset_info;





/********************* METADATA SERVICE MANAGEMENT FUNCTIONS  *********************/


// Method creating a connection to the metadata server.
int32_t stat_init(char * ip, uint16_t port, int32_t rank);

//Method disabling connection resources to the metadata server.
int32_t stat_release();



/***************** IN-MEMORY STORAGE SYSTEM MANAGEMENT FUNCTIONS  *****************/


//Method deploying the storage system.
int32_t init_imss(char *   imss_uri, int32_t  n_servers, int32_t  buff_size, char *   hostfile, uint16_t conn_port);

//Method creating the set of required connections with an existing IMSS.
int32_t open_imss(char * imss_uri);

//Method disabling all connection resources.
int32_t release_imss(char * imss_uri);

//Method retrieving information related to a certain backend.
int32_t stat_imss(char * imss_uri, imss_info * imss_info_);



/************************** DATASET MANAGEMENT FUNCTIONS **************************/


//Method creating a dataset and the environment enabling READ or WRITE operations over it.
int32_t create_dataset(char * dataset_uri, char * policy, int32_t num_data_elem, int32_t data_elem_size);

//Method retrieving an existing dataset.
int32_t open_dataset(char * dataset_uri);

//Method releasing the storage resources.
int32_t release_dataset(int32_t datasetd);

//Method retrieving information related to a certain dataset.
int32_t stat_dataset(char * dataset_uri, dataset_info * dataset_info_);

//Method retrieving a whole dataset parallelizing the procedure.
unsigned char * get_dataset(char * dataset_uri, uint64_t * buff_length);

//Method storing a whole dataset parallelizing the procedure.
int32_t set_dataset(char * dataset_uri, unsigned char * buffer, uint64_t offset);



/************************ DATA OBJECT MANAGEMENT FUNCTIONS ************************/


//Method performing a retrieval operation of a specific object.
int32_t get_data(int32_t datasetd, int32_t data_id, unsigned char * buffer);

//Method performing a retrieval operation of a specific object.
int32_t get_ndata(int32_t datasetd, int32_t data_id, unsigned char * buffer, int64_t * size);

//Method storing a specific data object.
int32_t set_data(int32_t datasetd, int32_t data_id, unsigned char * buffer);

//Method retrieving the location of a specific data object.
int32_t get_data_location(int32_t dataset_id, int32_t data_id, int32_t op_type);
char * get_data_location_host(char *  dataset, int32_t data_id);


/***************************** DATA RELEASE RESOURCES *****************************/


//Method releasing an imss_info structure previously provided to the client.
int32_t free_imss(imss_info * struct_);

//Method releasing a dataset structure previously provided to the client.
int32_t free_dataset(dataset_info * struct_);


#endif

