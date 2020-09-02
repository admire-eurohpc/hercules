#!/bin/bash

#Check for the total number of arguments.
if [ "$#" -ne 2 ]

   then

	echo -e "\nNOT ENOUGH ARGUMENTS!\n\nUSAGE:\n\n\t./posix_individual_write.sh <TETS_FILE_PATH> <CSV_FILE_PATH>\n\n"

	exit 1
fi

file_path=$1
test_file=$2

if [ ! -e $test_file ]

   then

	#Create csv file 
	touch $test_file

	#Dataset size (in KB) associated to the current set of tests.
	for dataset_size in 64 256 1024 
	   do
		echo "DATASET SIZE in KB: $dataset_size" >> $test_file

		#Block size (in KB) associated to the current set of tests.
		for block_size in 2 4 8 16 32 64
		   do
			echo -n "$block_size KB," >> $test_file

			for it in {0..9}
			   do
				../build/posix_individual_write $dataset_size $block_size $file_path >> $test_file

				echo "TEST: DATASET_SIZE $dataset_size KB // BLOCK_SIZE: $block_size (IT: $it) PERFORMED!"
			   done

			echo -en "\n" >> $test_file 
		   done

		echo -e "\n" >> $test_file 
	   done
   else

	echo "ERR_TEST_FILE_EXISTS"
fi
