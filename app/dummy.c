#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "hercules.h"


int32_t main (int32_t argc, char **argv) 
{
	
	int rank = 0;

	//Hercules init -- Attached deploy
	if (hercules_init(rank, 2048, 5555, 5569, 1024, "./metadata") == -1) exit(-1);

	//Metadata server
	if (stat_init("localhost", 5569, rank) == -1) exit(-1);

	//Imss deploy
	if (init_imss("imss://", "./hostfile", 1, 5555, 1024, ATTACHED, NULL) == -1) exit(-1);

	int datasetd_;
	//Create dataset, 1 Block of 1 Kbyte 
	if ((datasetd_ = create_dataset("imss://A", "RR", 3, 1, NONE)) < 0) exit(-1);

	//Dump data -- Remark: DATA MUST BE IN DYNAMIC MEMORY
	for(int i = 0; i < 3; ++i){
		char * buffer = (char *) malloc(1024*sizeof(char));
		//Fill the buffer with \n
		memset((void*) buffer, 't', 1024);
		//Copy the used data
		char const * testdata = "\na\nab\nabc\nabcd\nabcde\nabcdef\nabcdefg\na\nbb\nccc\ndddd\neeeee\nffffff\nggggggg\n1\n22\n\n333\n";
		memcpy(buffer, testdata, strlen(testdata));

		//Set the data in 2 Blocks
		int32_t data_sent = set_data(datasetd_, i, (unsigned char*)buffer);
	}

	release_dataset(datasetd_);	

	datasetd_ = open_dataset("imss://A");

	for(int i = 0; i < 3; ++i){
		char * buffer = (char *) malloc(1024*sizeof(char));

		get_data(datasetd_, i, (unsigned char*)buffer);

		printf("DATA %d: %s\n", i, buffer);
		free(buffer);
	}

	release_dataset(datasetd_);	
	release_imss("imss://", CLOSE_ATTACHED);
	hercules_release(0);

	return 0;
}

