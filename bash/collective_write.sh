#!/bin/bash

#Check for the total number of arguments.
if [ "$#" -ne 6 ]

   then

	echo -e "\nNOT ENOUGH ARGUMENTS!\n\nUSAGE:\n\n\t./collective_write.sh <METADATA_FILE> <NUM_PROCESSES> <STAT_PORT> <STAT_BUFFER> <SERVER_PORT> <SERVER_BUFFER>\n\n"

	exit 1
fi


test_file=../tst/collective_write_replication.csv


metadata_file=$1
mpi_hostfile=./collective_write/deployfile
imss_hostfile=./collective_write/imsshostfile
num_processes=$2
stat_port=$3
stat_buffer=$4
server_port=$5
server_buffer=$6

if [ ! -e $test_file ]

   then

	#Create csv file 
	touch $test_file

	echo "MPI deployment size: $num_processes" >> $test_file
	num_servers_in_imss=$(cat $imss_hostfile | wc -l);
	echo "IMSS deployment size: $num_servers_in_imss" >> $test_file

	echo -e "\n" >> $test_file 

	#Dataset size (in KB) associated to the current set of tests.
	for dataset_size in 262144 1048576 4194304 #64 128 256 512 1024 2048 4096 8192 16384 32768 65536 131072 262144 
	#for dataset_size in 
	   do
		echo "DATASET SIZE in KB: $dataset_size" >> $test_file

		#Policy corresponding to the current set of tests.
		for policy in RR BUCKETS HASH CRC16b CRC64b LOCAL
		   do
			echo "POLICY: $policy" >> $test_file

			#Block size (in KB) associated to the current set of tests.
			for block_size in 4 16 64 256 1024 4096 16384 65536
			   do
				echo -n "$block_size KB," >> $test_file

				for it in {0..9}
				   do
					> $metadata_file

					mpirun -np $num_processes -f $mpi_hostfile ../build/collective_write $server_port $server_buffer $stat_port $stat_buffer $metadata_file $imss_hostfile $policy $dataset_size $block_size >> $test_file

					echo "TEST: DATASET_SIZE $dataset_size KB // POLICY: $policy // BLOCK_SIZE: $block_size (IT: $it) PERFORMED!"
				   done

				echo -en "\n" >> $test_file 
			   done

			echo -e "\n" >> $test_file 
		   done
		
		echo -e "\n" >> $test_file 
	   done
   else

	echo "ERR_TEST_FILE_EXISTS"
fi
