#!/bin/bash

#Check for the total number of arguments.
if [ "$#" -ne 5 ]

   then

	echo -e "\nNOT ENOUGH ARGUMENTS!\n\nUSAGE:\n\n\t./metadata.sh <METADATA_FILE> <STAT_PORT> <SERVER_PORT> <METADATA_SERVER_ADDRESS> <THROUGHPUT_MBs>\n\n"

	exit 1
fi


test_file=../tst/metadata.csv


metadata_file=$1
mpi_hostfile=./metadata/deployfile
imss_hostfile=./metadata/imsshostfile
stat_port=$2
stat_buffer=1048576
server_port=$3
server_buffer=8388608

if [ ! -e $test_file ]

   then
	touch $test_file

	for clients in 32 # 4 8 16 32 64 128 
	   do
		echo "ENTITIES (clients & servers): $clients" >> $test_file

		for file_size in 8388608 # 2097152 8388608 33554432 # 2GB 8GB & 32GB
		   do
			echo -e "TOTAL KILOBYTES: $file_size KB\n" >> $test_file

			for policy in RR BUCKETS HASH LOCAL #
			   do
				echo "POLICY: $policy" >> $test_file

				for datasets_client in 2 4 8 16 32 64 128 # 256 1024 4096 16384 # 4KB 16KB 64KB 256KB 1MB 4MB & 16MB 
				   do
					echo -n "$datasets_client datasets/client," >> $test_file

					num_datasets=$(( datasets_client * clients ))	# Total number of datasets handled in the current test.
					dataset_size=$(( file_size / num_datasets ))	# Size of each dataset handled within the test.
					block_size=$dataset_size

					if [ $block_size -ge 4096 ]
					   then
						block_size=4096
					   fi

					for it in {0..2}
					   do
						> $metadata_file

						mpirun -np $clients -f $mpi_hostfile ../build/individual_write $server_port  \
						$server_buffer $stat_port $stat_buffer $metadata_file $imss_hostfile $policy \
						$dataset_size $block_size $datasets_client $4 $5 >> $test_file

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

						echo "TEST $it: CLIENTS: $clients // DATASETS/CLIENT: $datasets_client // POLICY: $policy PERFORMED!"
					   done

					echo -e "\nEVALUATION (CLIENTS: $clients + DATASETS/CLIENT: $datasets_client + POLICY: $policy) \e[36mCONCLUDED\e[0m!\n"

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
