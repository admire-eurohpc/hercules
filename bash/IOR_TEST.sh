#!/bin/bash


script_name=runIORSlurm.sh
number_of_nodes=0
FILE_SIZE=$((1024*1024*1))
TEST_TYPE="strong"

set -x

jid=1


for NODES_FOR_CLIENTS in {16..16}
do
	for CLIENTS_PER_NODE in {1,2,4,8,16}
        do
        	for BLOCK_SIZE in {1024..1024}
                do


			if [ "$TEST_TYPE" = "weak" ]
                        then
                                FILE_SIZE_PER_CLIENT=$((100*1024))
                        elif [ "$TEST_TYPE" = "strong" ]
                        then
                                TOTAL_NUMBER_OF_CLIENTS=$(($NODES_FOR_CLIENTS*$CLIENTS_PER_NODE))
                                FILE_SIZE_PER_CLIENT=$(($FILE_SIZE/$TOTAL_NUMBER_OF_CLIENTS))
                        else # strong by defualt
                                TOTAL_NUMBER_OF_CLIENTS=$(($NODES_FOR_CLIENTS*$CLIENTS_PER_NODE))
                        	FILE_SIZE_PER_CLIENT=$(($FILE_SIZE/$TOTAL_NUMBER_OF_CLIENTS))
                        fi


			if [ $jid -eq 1 ]
			then
				jid=$(sbatch -N $NODES_FOR_CLIENTS $script_name $NODES_FOR_CLIENTS $CLIENTS_PER_NODE $FILE_SIZE_PER_CLIENT | cut -d ' ' -f4)
		        else
            			jid=$(sbatch --dependency=afterok:${jid} -N $NODES_FOR_CLIENTS $script_name $NODES_FOR_CLIENTS $CLIENTS_PER_NODE $FILE_SIZE_PER_CLIENT | cut -d ' ' -f4)
		        fi
			#exit 0
		done
	done
done
