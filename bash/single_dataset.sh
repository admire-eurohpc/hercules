#!/bin/bash

#Check for the total number of arguments.
if [ "$#" -ne 5 ]

   then

	echo -e "\nNOT ENOUGH ARGUMENTS!\n\nUSAGE:\n\n\t./multiple_datasets.sh <METADATA_FILE> <STAT_PORT> <SERVER_PORT> <METADATA_SERVER_ADDRESS> <THROUGHPUT_MBs>\n\n"

	exit 1
fi


test_file=../tst/collective_io_single_dataset.csv


metadata_file=$1
mpi_hostfile=./collective_write/deployfile
imss_hostfile=./collective_write/imsshostfile
stat_port=$2
stat_buffer=1048576
server_port=$3
server_buffer=8388608

if [ ! -e $test_file ]

   then
	touch $test_file

	for clients in 64 #4 8 16 32 128 
	   do
		echo "ENTITIES (client+server): $clients" >> $test_file

		for dataset_size in 8388608 #2097152 8388608 33554432 # 2GB 8GB & 32GB
		   do
			echo "DATASET SIZE: $dataset_size KB" >> $test_file

			for policy in RR BUCKETS HASH LOCAL # 
			   do
				echo "POLICY: $policy" >> $test_file

				for block_size in 4 16 64 256 1024 4096 16384 # 4KB 16KB 64KB 256KB 1MB 4MB & 16MB  
				   do
					echo -n "$block_size KB," >> $test_file

					for it in {0..2}
					   do
						> $metadata_file

						mpirun -np $clients -f $mpi_hostfile ../build/collective_write $server_port  \
						$server_buffer $stat_port $stat_buffer $metadata_file $imss_hostfile $policy \
						$dataset_size $block_size $4 $5 >> $test_file

						server_port=$((server_port + 5))
						if [ $server_port -gt 63000 ]
						   then
							server_port=1024
						   fi
						stat_port=$((stat_port + 5))
						if [ $stat_port -gt 63000 ]
						   then
							stat_port=1034
						   fi

						echo "TEST $it: CLIENTS: $clients // DATASET: $dataset_size KB // POLICY: $policy // BLOCK_SIZE: $block_size PERFORMED!"
					   done

					echo -e "\nEVALUATION (CLIENTS $clients + DATASET $dataset_size KB + POLICY $policy + BLOCK_SIZE $block_size) \e[36mCONCLUDED\e[0m!\n"

					echo -en "\n" >> $test_file 
				   done

				echo -e "\n" >> $test_file 
			   done
			
			echo -e "\n" >> $test_file 
		   done

		echo -e "\n" >> $test_file 
	   done
   else

	echo "ERR_TEST_FILE_EXISTS"
fi
