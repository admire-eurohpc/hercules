#!/bin/bash

#Check for the total number of arguments.
if [ "$#" -ne 6 ]

   then

	echo -e "\nNOT ENOUGH ARGUMENTS!\n\nUSAGE:\n\n\t./metadata_measure.sh <METADATA_FILE> <NUM_PROCESSES> <STAT_PORT> <STAT_BUFFER> <SERVER_PORT> <SERVER_BUFFER>\n\n"

	exit 1
fi

test_file=../tst/metadata_measure.csv

metadata_file=$1
mpi_hostfile=./metadata_measure/deployfile
imss_hostfile=./metadata_measure/imsshostfile
num_processes=$2
stat_port=$3
stat_buffer=$4
server_port=$5
server_buffer=$6

if [ ! -e $test_file ]

   then
	#Create csv file 
	touch $test_file

	echo "HERCULES INIT,,,STAT_INIT,,INIT IMSS,STAT_IMSS,OPEN IMSS,CREATE DATASET,STAT DATASET,OPEN DATASET,RELEASE DATASET,,RELEASE IMSS,,RELEASE_STAT,,HERCULES RELEASE" >> $test_file

	for it in {0..99}
	   do
		> $metadata_file

		mpirun -np $num_processes -f $mpi_hostfile ../build/metadata_measure $server_port $server_buffer $stat_port $stat_buffer $metadata_file $imss_hostfile "RR" "8192" "64" >> $test_file

		echo -ne "\n" >> $test_file
	   done
   else
	echo "ERR_TEST_FILE_EXISTS"
fi
