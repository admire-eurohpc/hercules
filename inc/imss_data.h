#ifndef IMSS_DATA_
#define IMSS_DATA_

#define SET_DATASET	0
#define GET_DATASET	1
#define URI_		256
#define REQ_MSG		272

#define IMSS_INFO	2
#define DATASET_INFO	3

#define LINE_LENGTH	16

#define KEY		512

//Structure storing all information related to a certain backend.
typedef struct {

	//IMSS URI.
	char uri_[URI_];
	//Set of ips comforming the IMSS.
	char ** ips;
	//Number of IMSS servers.
	int32_t num_storages;
	//Server's dispatcher thread connection port.
	int16_t conn_port;

} imss_info;

//Structure storing the required connection resources to the IMSS in the client side.
typedef struct {

	//Set of context elements.
	void ** contexts_;
	//Set of actual sockets.
	void ** sockets_;
	//ID that the client takes in the current connections.
	int32_t c_id;
	//Socket connecting the corresponding client to the server running in the same node.
	int32_t matching_server;

} imss_descriptor;

//Structure storing all information related to a certain dataset.
typedef struct {

	//URI identifying a certain dataset.
	char uri_[URI_];
	//Policy that was followed in order to write the dataset.
	char policy[8];
	//Number of data elements conforming the dataset entity.
	int32_t num_data_elem;
	//Size of each data element (in KB).
	int32_t data_entity_size;
	//IMSS descriptor managing the dataset in the current client session.
	int32_t imss_d;
	//Node were the dataset was stored if the LOCAL distribution policy was used.
	int32_t root_node;

} dataset_info;

//Structure specifying data distribution of a certain dataset among all servers.
/*typedef struct {

	...

} dataset_location;*/

#endif
