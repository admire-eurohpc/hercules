#!/bin/bash

#Check for the total number of arguments.
if [ "$#" -ne 6 ]

   then

	echo -e "\nNOT ENOUGH ARGUMENTS!\n\nUSAGE:\n\n\t./block_size.sh <METADATA_FILE> <NUM_PROCESSES> <STAT_PORT> <STAT_BUFFER> <SERVER_PORT> <SERVER_BUFFER>\n\n"

	exit 1
fi


test_file=../tst/small_blocks.csv


metadata_file=$1
mpi_hostfile=./small_blocks/deployfile
imss_hostfile=./small_blocks/imsshostfile
num_processes=$2
stat_port=$3
stat_buffer=$4
server_port=$5
server_buffer=$6

if [ ! -e $test_file ]

   then

	#Create csv file 
	touch $test_file

	#Policy corresponding to the current set of tests.
	for policy in RR BUCKETS HASH CRC16b CRC64b #LOCAL
	   do
		#Dataset size (in KB) associated to the current set of tests.
		for dataset_size in 64 128 256 512 1024 2048 4096 8192 16384 32768 65536 131072 262144 524288
		#for dataset_size in 
		   do
			#Block size (in KB) associated to the current set of tests.
			for block_size in 2 4 8 16 32 64
			   do
				for it in {0..9}
				   do
					> $metadata_file

					mpirun -np $num_processes -f $mpi_hostfile ../build/blocks $server_port $server_buffer $stat_port $stat_buffer $metadata_file $imss_hostfile $policy $dataset_size $block_size

					if [ $it -lt 9 ]

					   then
						echo -n "$it, "
					   else
						echo "$it"
					fi
				   done

				echo "Tests for BLOCK SIZE of $block_size KB performed!"
			   done
			
			echo "Tests for DATASET SIZE $dataset_size KB performed!" && echo -e "\n"
		   done

		echo "Tests for $policy POLICY performed!" && echo -e "\n\n\n"
	   done
   else

	echo "ERR_TEST_FILE_EXISTS"
fi
