#include <time.h>
#include <chrono>
#include <stdio.h>
#include <iostream>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "hercules.h"

int32_t main (int32_t argc, char **argv) 
{
	
	int rank = 0;

	using clk = std::chrono::high_resolution_clock;

	char metadata[]      = "./module_metadata";
	char localhost[]     = "./module_stat_hostfile";
	char imss_test[]     = "imss://berries";
	char hostfile[]	     = "./module_hostfile";
	char policy[]        = "LOCAL";
	char dataset_uri[]   = "imss://berries/dataset";
	uint32_t iterations  = atoi(argv[1]);
	uint32_t block_size  = atoi(argv[2]);

	//Humber these two no necesary, add it in the case of detached in imssfs.c
	char meta_hostfile[]	     = "";
	uint16_t metadata_port=0;

	auto t1 = clk::now();
	//Hercules init -- Attached deploy
	if (hercules_init(0, 4194304, 5555, 1, 5569, 2048, metadata) == -1) exit(-1);
	auto t2 = clk::now();
	auto hercules_init_time = std::chrono::duration_cast<std::chrono::nanoseconds>(t2-t1);

	t1 = clk::now();
	//Metadata server
	if (stat_init(localhost, 5569, 1, rank) == -1) exit(-1);
	t2 = clk::now();
	auto stat_init_time = std::chrono::duration_cast<std::chrono::nanoseconds>(t2-t1);

	t1 = clk::now();
	//Imss deploy
	if (init_imss(imss_test, hostfile, meta_hostfile, 1, 5555, 4192256, ATTACHED, NULL,metadata_port) == -1) exit(-1);
	t2 = clk::now();
	auto init_imss_time = std::chrono::duration_cast<std::chrono::nanoseconds>(t2-t1);

	release_imss(imss_test, DISCONNECT);

	t1 = clk::now();
	get_type(imss_test);
	t2 = clk::now();
	auto get_type_time = std::chrono::duration_cast<std::chrono::nanoseconds>(t2-t1);

	t1 = clk::now();
	if (open_imss(imss_test) == -1) exit(-1);
	t2 = clk::now();
	auto open_imss_time = std::chrono::duration_cast<std::chrono::nanoseconds>(t2-t1);

	int32_t datasetd_;
	t1 = clk::now();
	if ((datasetd_ = create_dataset(dataset_uri, policy, 1, block_size, NONE)) < 0) exit(-1);
	t2 = clk::now();
	auto create_dataset_time = std::chrono::duration_cast<std::chrono::nanoseconds>(t2-t1);

	release_dataset(datasetd_);	

	t1 = clk::now();
	datasetd_ = open_dataset(dataset_uri);
	t2 = clk::now();
	auto open_dataset_time = std::chrono::duration_cast<std::chrono::nanoseconds>(t2-t1);

	auto set_data_time = std::chrono::duration_cast<std::chrono::nanoseconds>(t2-t1);
	auto get_data_time = std::chrono::duration_cast<std::chrono::nanoseconds>(t2-t1);

	for(int i = 0; i < iterations; ++i){

		char * buffer = (char *) malloc(1024 * block_size);
		memset((void *) buffer, 'A', 1024 * block_size);

		t1 = clk::now();
		int32_t data_sent = set_data(datasetd_, 0, (unsigned char*)buffer);
		t2 = clk::now();

		if (!i)
			set_data_time = std::chrono::duration_cast<std::chrono::nanoseconds>(t2-t1);
		else
			set_data_time = set_data_time + std::chrono::duration_cast<std::chrono::nanoseconds>(t2-t1);

		char * rcv_buffer = (char *) malloc(1024 * block_size);

		t1 = clk::now();
		get_data(datasetd_, 0, (unsigned char*) rcv_buffer);
		t2 = clk::now();

		if (!i)
			get_data_time = std::chrono::duration_cast<std::chrono::nanoseconds>(t2-t1);
		else
			get_data_time = get_data_time + std::chrono::duration_cast<std::chrono::nanoseconds>(t2-t1);

		free(buffer);
	}

	set_data_time /= iterations;	
	get_data_time /= iterations;

	t1 = clk::now();
	release_dataset(datasetd_);	
	t2 = clk::now();
	auto release_dataset_time = std::chrono::duration_cast<std::chrono::nanoseconds>(t2-t1);

	t1 = clk::now();
	release_imss(imss_test, CLOSE_ATTACHED);
	t2 = clk::now();
	auto release_imss_time = std::chrono::duration_cast<std::chrono::nanoseconds>(t2-t1);

	t1 = clk::now();
	stat_release();
	t2 = clk::now();
	auto stat_release_time = std::chrono::duration_cast<std::chrono::nanoseconds>(t2-t1);

	t1 = clk::now();
	hercules_release();
	t2 = clk::now();
	auto hercules_release_time = std::chrono::duration_cast<std::chrono::nanoseconds>(t2-t1);

	std::cout << hercules_init_time.count()/1000000000.0 <<","<< hercules_release_time.count()/1000000000.0 <<","<< stat_init_time.count()/1000000000.0 <<","<< stat_release_time.count()/1000000000.0 <<","<< init_imss_time.count()/1000000000.0 <<","<< open_imss_time.count()/1000000000.0 <<","<< release_imss_time.count()/1000000000.0 <<","<< create_dataset_time.count()/1000000000.0 <<","<< open_dataset_time.count()/1000000000.0 <<","<< release_dataset_time.count()/1000000000.0 <<","<< get_data_time.count()/1000000000.0 <<","<< set_data_time.count()/1000000000.0 <<","<< get_type_time.count()/1000000000.0 << std::endl;

	return 0;
}

