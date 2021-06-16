#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "hercules.h"


int32_t main (int32_t argc, char **argv) 
{
//	int rank = 0;
//
//	//Hercules init -- Attached deploy
//	if (hercules_init(rank, 2048, 5555, 5569, 1024, "./metadata") == -1) exit(-1);
//
//	//Metadata server
//	if (stat_init("localhost", 5569, rank) == -1) exit(-1);
//
//	//Imss deploy
//	if (init_imss("imss://test", "./hostfile", 1, 5555, 1024, ATTACHED, NULL) == -1) exit(-1);
//
//	//WRITE
//	for(int i = 0; i < 3; ++i){
//
//		int datasetd_;
//		char dataset_uri[32];
//		sprintf(dataset_uri, "imss://test/%i", i);
//		//Create dataset, 1 Block of 1 Kbyte 
//		if ((datasetd_ = create_dataset(dataset_uri, "RR", 1, 1, NONE)) < 0) exit(-1);
//
//		char * buffer = (char *) malloc(1024*sizeof(char));
//		//Fill the buffer with \n
//		memset((void*) buffer, '0', 1024);
//		//Copy the used data
//		char const * testdata = "\na\nab\nabc\nabcd\nabcde\nabcdef\nabcdefg\na\nbb\nccc\ndddd\neeeee\nffffff\nggggggg\n1\n22\n\n333\n";
//		memcpy(buffer, testdata, strlen(testdata));
//
//		//Set the data in 2 Blocks
//		int32_t data_sent = set_data(datasetd_, 0, (unsigned char*)buffer);
//
//		release_dataset(datasetd_);	
//	}
//
//	//GET THE TYPE OF A CERTAIN URI.
//
//	char imss_[] = "imss://test";
//	char dat1_[] = "imss://test/0";
//	char dat2_[] = "imss://test/123";
//	int32_t is_dat1 = get_type(dat1_);
//	int32_t is_imss = get_type(imss_);
//	int32_t is_dat2 = get_type(dat2_);
//
//	printf("imss://test -> %d\nimss://test/0 -> %d\nimss://test/123 -> %d\n", is_imss, is_dat1, is_dat2);
//
//	//GET THE URI OF THE INSTANCE DEPLOYED IN THE SAME MACHINE.
//
//	printf("\nIn the same machine deployment: %s\n", get_deployed());
//
//	//READ
//	for(int i = 0; i < 3; ++i)
//	{
//		int datasetd_;
//		char dataset_uri[32];
//		sprintf(dataset_uri, "imss://test/%i", i);
//
//		datasetd_ = open_dataset(dataset_uri);
//
//		char * buffer = (char *) malloc(1024*sizeof(char));
//
//		get_data(datasetd_, 0, (unsigned char*)buffer);
//
//		//printf("DATA %s: %s\n", dataset_uri, buffer);
//		free(buffer);
//		release_dataset(datasetd_);	
//	}
//
//	release_imss("imss://test", CLOSE_ATTACHED);
//
//	stat_release();
//	hercules_release(0);

	char endpoint[] = "tucan:5556";

	char * discovered_uri = get_deployed(endpoint);

	printf("Discovered IMSS: %s\n", discovered_uri);

	free(discovered_uri);

	return 0;
}

