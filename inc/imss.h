#ifndef IMSS_WRAP_
#define IMSS_WRAP_

#include <stdint.h>





//Maximum number of bytes assigned to a dataset or IMSS URI.
#define URI_		256

//Maximum number of bytes retrieved from the imss deployment file per line.
#define LINE_LENGTH	512

//Maximum number of milliseconds that a zmq_recv operation will be waiting until it raises a timeout.
#define TIMEOUT_MS	10000

//Replication factor assigned to each dataset in creation time.
#define NONE  1
#define DRM  2
#define TRM  3

//Type of IMSS instance to be deployed.
#define DETACHED	 0
#define ATTACHED 	 1

//IMSS release operation possibilities.
#define DISCONNECT  0
#define CLOSE_DETACHED  1
#define CLOSE_ATTACHED  2





//Inside HERCULES directives.
#define REQ_MSG		272
#define KEY		512
#define MONITOR		1
#define ELEMENTS	5120
#define IMSS		0
#define DATASET		1


//Structure storing all information related to a certain IMSS.
typedef struct {

	//IMSS URI.
	char uri_[URI_];
	//Byte specifying the type of structure.
	char type;// = 'I';
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
	char type;// = 'D';
	//Policy that was followed in order to write the dataset.
	char policy[8];
	//Number of data elements conforming the dataset entity.
	int32_t num_data_elem;
	//Size of each data element (in KB).
	int32_t data_entity_size;
	//Number of replications performed along the corresponding IMSS.
	int32_t repl_factor;
	//IMSS descriptor managing the dataset in the current client session.
	int32_t imss_d;
	//Connection to the IMSS server running in the same machine.
	int32_t local_conn;
	//Actual size
	int64_t size;


	/*************** USED EXCLUSIVELY BY LOCAL DATASETS ***************/


	//Vector of characters specifying the position of each data element.
	uint16_t * data_locations;
	//Number of blocks written by the client in the current session.
	uint64_t * num_blocks_written;
	//Actual blocks written by the client.
	uint32_t * blocks_written;

} dataset_info;

#ifndef FUSE
extern "C" {
#endif
/****************************************************************************************************************************/
/****************************************** METADATA SERVICE MANAGEMENT FUNCTIONS  ******************************************/


/* Method creating a communication channel with the IMSS metadata server. Besides, the stat_imss method initializes a set of elements that will be used through the session.

	RECEIVES:	stat_hostfile    - File containing an IP sequence (or DNS) per line where an IMSS metadata server has been deployed.
                port             - Port number which the metadata server is listening to within the previous machine.
                num_stat_servers - Number of metadata servers to connect to.
                rank	         - Application process identifier used as communications ID in the concerned metadata server-client channel.

	RETURNS:	 0 - Communication channel and initializations performed successfully.
                -1 - In case of error.
*/
int32_t stat_init(char * stat_hostfile, uint16_t port, int32_t num_stat_servers, int32_t rank);

/* Method disabling the communication channel with the metadata server. Besides, the current method releases session-related elements previously initialized.

	RETURNS:	 0 - Release operations were successfully performed.
                -1 - In case of error.
*/
int32_t stat_release();

/* Method retrieving the whole set of elements contained by a specific URI.

	RECEIVES:	requested_uri - URI whose elements are to be retrieved.
			buffer        - Reference to a char * variable that will be pointing to a buffer storing all URIs contained within the requested.
			items         - Reference to a char ** variable that will be used to point to all URIs within the buffer.

	RETURNS:	> 0 - Number of items contained by the specified URI.
			-1  - In case of error or if the URI was not found.

	WARNING:	The get_dir function allocates memory (performs malloc operations). Therefore, the provided pointers (*buffer & *items) MUST BE FREED once done.
*/
//FIXME: fix implementation for multiple servers.
uint32_t get_dir(char * requested_uri, char ** buffer, char *** items);





/****************************************************************************************************************************/
/************************************** IN-MEMORY STORAGE SYSTEM MANAGEMENT FUNCTIONS ***************************************/


/* Method initializing an IMSS deployment.

	RECEIVES:	imss_uri    - URI assigned to the concerned IMSS system (256 characters long MAX).
			hostfile    - File containing an IP sequence (or DNS) per line where an IMSS server will be initialized.
			n_servers   - Number of servers conforming the IMSS instance (first n_servers lines taken from the previous file).
			buff_size   - Storage size in KILOBYTES assigned to each server conforming the IMSS deployment.
			deployment  - Specifies the type of instance that will be initialized. This parameter will take the following values: ATTACHED | DETACHED.
			binary_path - Path to the 'server.c' binary. It must be provided if the 'deployment' parameter took the DETACHED value. Otherwise, NULL is just fine.

	RETURNS:	 0 - Initialization procedure was successfully performed.
			-1 - In case of error.
*/
int32_t init_imss(char * imss_uri, char * hostfile, int32_t n_servers, uint16_t conn_port, uint64_t buff_size, uint32_t deployment, char * binary_path);

/* Method initializing the required resources to make use of an existing IMSS.

	RECEIVES:	imss_uri - URI assigned to the IMSS instance that the client desires to connect.

	RETURNS:	 0 - Resources successfully initialized. Communication channels created.
			-1 - In case of error.
			-2 - The imss instance has been already opened or created.
*/
int32_t open_imss(char * imss_uri);

/* Method releasing client-side and/or server-side resources related to a certain IMSS instance. 

	RECEIVES:	imss_uri   - IMSS URI that the client desires to release.
			release_op - Specifies if the client will tear down or just disconnect from from an IMSS instance. The following parameters are considered:

					1. DISCONNECT	  - Release intance-related communication resources.
					2. CLOSE_DETACHED - Release communication resources and tear down a DETACHED IMSS instance.
					3. CLOSE_ATTACHED - Release communication resources and tear down an ATTACHED IMSS instance.

	RETURNS:	 0 - Release operation successfully performed.
			-1 - In case of error.
*/
int32_t release_imss(char * imss_uri, uint32_t release_op);

/* Method retrieving information related to a certain IMSS instance.

	RECEIVES:	imss_uri   - IMSS URI that the client is interested in.
			imss_info_ - Reference to an imss_info variable where the requested information will be stored.

	RETURNS:	 0 - No IMSS was found with the provided URI.
			 1 - The information was successfully retrieved from the metadata server.
			 2 - The information was successfully retrieved from a local storage (the IMSS must have been already created or opened).
			-1 - In case of error.

	WARNING:	The stat_imss function allocates memory (performs malloc operations).

			The following function must be called over the provided imss_info structure once done:

				free_imss(imss_info_);
*/
int32_t stat_imss(char * imss_uri, imss_info * imss_info_);

/* Method providing the URI of the attached IMSS instance.

	RETURNS:	 char * - Instance URI.
			 NULL   - No instance was deployed.

	WARNING:	The get_deployed function allocates memory (performs malloc operations).

                The following function must be called over the provided char * element once done:

                char * attached_deployment = get_deployed();

                free(attached_deployment);
*/
char * get_deployed();

/* Method providing the URI of the IMSS instance executing at some endpoint.

	RECEIVES:	endpoint - string following a ip/DNS:port style.

	RETURNS:	char * - Instance URI.
			NULL   - No instance was deployed.

	WARNING:	The get_deployed function allocates memory (performs malloc operations).

                The following function must be called over the provided char * element once done:

                char * deployment = get_deployed(uri);

                free(deployment);
*/
char * get_deployed_(char * endpoint);


/****************************************************************************************************************************/
/*********************************************** DATASET MANAGEMENT FUNCTIONS ***********************************************/

/* Method creating a dataset and the environment enabling READ and WRITE operations over it.

	RECEIVES:	dataset_uri    - URI assigned to the concerned dataset (256 characters long MAX).
			policy         - Data distribution policy assigned to the concerned dataset: RR, BUCKETS, HASH, CRC16b, CRC64b or LOCAL.
			num_data_elem  - Number of data blocks conforming the concerned dataset.
			data_elem_size - Size in KILOBYTES of each data block conforming the dataset.
			repl_factor    - Replication factor assigned to the concerned dataset: NONE, DRM or TRM.

	RETURNS:	> 0 - Number identifying the created dataset among the client's session.
			 -1 - In case of error.
*/
int32_t create_dataset(char * dataset_uri, char * policy, int32_t num_data_elem, int32_t data_elem_size, int32_t repl_factor);

/* Method creating the required resources in order to READ and WRITE an existing dataset.

	RECEIVES:	dataset_uri - URI identifying the dataset to be opened.

	RETURNS:	> 0 - Number identifying the retrieved dataset among the client's session.
			 -1 - In case of error.
*/
int32_t open_dataset(char * dataset_uri);


/*Method deleting a dataset.

	RETURNS:	 0 - Release operation took place successfully.
				-1 - In case of error.*/
int32_t delete_dataset(const char * dataset_uri);

/*Method renaming a dataset in metadata.

	RETURNS:	 0 - Release operation took place successfully.
				-1 - In case of error.*/
int32_t rename_dataset_metadata(char * old_dataset_uri, char * new_dataset_uri);

/*Method renaming a dataset in srv_worker.

	RETURNS:	 0 - Release operation took place successfully.
				-1 - In case of error.*/
int32_t rename_dataset_srv_worker(char * old_dataset_uri, char * new_dataset_uri,int32_t 	 dataset_id,
	 int32_t 	 data_id);


/* Method releasing the set of resources required to deal with a dataset.

	RECEIVES:	dataset_id - Number identifying the concerned dataset among the client's session. This number should have been
				     provided by the create_dataset or open_dataset method.

	RETURNS:	 0 - Release operation took place successfully.
			-1 - In case of error.
*/
int32_t release_dataset(int32_t dataset_id);

/* Method retrieving information related to a certain dataset.

	RECEIVES:	dataset_uri   - Dataset URI that the client is interested in.
			dataset_info_ - Reference to a dataset_info variable where the requested information will be stored.

	RETURNS:	 0 - No dataset was found with the provided URI.
			 1 - The information was successfully retrieved from the metadata server.
			 2 - The information was successfully retrieved from a local storage (the dataset must have been already created or opened).
			-1 - In case of error.

	The current function does not allocate memory.
*/
int32_t stat_dataset(const char * dataset_uri, dataset_info * dataset_info_);

////Method retrieving a whole dataset parallelizing the procedure.
//unsigned char * get_dataset(char * dataset_uri, uint64_t * buff_length);
//
////Method storing a whole dataset parallelizing the procedure.
//int32_t set_dataset(char * dataset_uri, unsigned char * buffer, uint64_t offset);





/****************************************************************************************************************************/
/********************************************* DATA OBJECT MANAGEMENT FUNCTIONS *********************************************/


/* Method retrieving a data element associated to a certain dataset.

	RECEIVES:	dataset_id - Number identifying the concerned dataset among the client's session.
			data_id    - Data block number identifying the data block to be retrieved.
			buffer     - Memory address where the requested block will be received. WARNING: memory must have been allocated.

	RETURNS:	 0 - The requested block was successfully retrieved.
			-1 - In case of error.
*/
int32_t get_data(int32_t dataset_id, int32_t data_id, unsigned char * buffer);

int32_t get_ndata(int32_t dataset_id, int32_t data_id, unsigned char * buffer, int64_t * len);
/* Method storing a specific data element.

	RECEIVES:	dataset_id - Number identifying the concerned dataset among the client's session.
			data_id    - Data block number identifying the data block to be stored.
			buffer     - Buffer containing the data block information.

	RETURNS:	 0 - The requested block was successfully stored.
			-1 - In case of error.
*/
int32_t set_data(int32_t dataset_id, int32_t data_id, unsigned char * buffer);

/* Method retrieving the location of a specific data object.

	RECEIVES:	dataset      - Dataset URI whose blocks location are to be retrieved.
			data_id      - ID identifying the data block whose location shall be retrieved.
			num_storages - Reference to an int32_t variable where the number of storages containing the concerned data block is stored.

	RETURNS:	char ** - List of IPs or DNSs where the concerned block is stored.
			NULL    - The requested data block did not existed.

	WARNING:	The get_dataloc function allocates memory (performs malloc operations). 

			Therefore, the next steps must be followed in order to free the reserved memory:

				char ** locations = get_dataloc(datasetd, data_id, &num_storages);

				...

				//FREE RESOURCES.
				for (int32_t i = 0; i < num_storages; i++)

					free(locations[i]);

				free(locations);
*/

int32_t
set_ndata(int32_t 	 dataset_id,
	 int32_t 	 data_id,
	 unsigned char * buffer,
	 uint32_t size);

char ** get_dataloc(const char * dataset, int32_t data_id, int32_t * num_storages);

/* Method specifying the type (DATASET or IMSS INSTANCE) of a provided URI.

	RECEIVES:	uri      - URI of the corresponding element.

	RETURNS:	0 - No entity associated to the URI provided exists.
                1 - The URI provided corresponds to an IMSS.
                2 - The URI provided corresponds to a dataset.
               -1 - In case of error.
*/
int32_t get_type(char * uri);





/****************************************************************************************************************************/
/************************************************** DATA RELEASE RESOURCES **************************************************/


/* Method releasing an imss_info structure provided by the stat_imss function.

	RECEIVES:	imss_info_ - Reference to the imss_info structure that must be freed.

	RETURNS:	0 - Resources were released successfully.
*/
int32_t free_imss(imss_info * imss_info_);

/* Method releasing a dataset structure previously provided to the client.

	RECEIVES:	dataset_info_ - Reference to the dataset_info structure to be freed.

	RETURNS:	0 - Resources were released successfully.
*/
int32_t free_dataset(dataset_info * dataset_info_);

#ifndef FUSE
}
#endif

#endif
