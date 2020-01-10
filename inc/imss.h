#ifndef IMSS_WRAP_
#define IMSS_WRAP_

#include <stdint.h>
#include "imss_data.h"


/********************* METADATA SERVICE MANAGEMENT FUNCTIONS  *********************/


// Method creating a connection to the metadata server.
int32_t stat_init(char * ip, uint16_t port);

//Method disabling connection resources to the metadata server.
int32_t release_stat();



/***************** IN-MEMORY STORAGE SYSTEM MANAGEMENT FUNCTIONS  *****************/


//Method deploying the storage system.
int32_t init_imss(char * imss_uri, int32_t n_servers, int32_t buff_size, char * hostfile, uint16_t conn_port);

//Method disabling all connection resources and tearing down all servers.
int32_t release_imss(char * imss_uri);

//Method creating the set of required connections with an existing IMSS.
int32_t open_imss(char * imss_uri);

//Method destroying the client's current session with a certain IMSS.
int32_t close_imss(char * imss_uri);

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

//Method retrieving the location of the whole set of data elements composing the dataset.
struct dataset_location get_dataset_location(char * dataset_uri);

//Method releasing information allocated by 'get_dataset_location'.
int32_t free_dataset_location(struct dataset_location * dataset_location_);



/************************ DATA OBJECT MANAGEMENT FUNCTIONS ************************/


//Method performing a retrieval operation of a specific object.
int32_t get_data(int32_t datasetd, int32_t data_id, unsigned char * buffer, int64_t * buff_length);

//Method storing a specific data object.
int32_t set_data(int32_t datasetd, int32_t data_id, unsigned char * buffer);

//Method storing a certain data object and checking that it has been correctly stored.
int32_t setv_data(char * dataset_uri, uint64_t data_id, unsigned char * buffer);

//Method retrieving the location of a specific data object.
int32_t get_data_location(int32_t datasetd, int32_t data_id);



/***************************** DATA RELEASE RESOURCES *****************************/


//Method releasing typedef structures.
int32_t free_(int32_t typedef_, void * typedef_ref_);

#endif
