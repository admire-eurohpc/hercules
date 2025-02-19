#!/bin/bash

port1=3000
port2=3050
script_name=IORTestSlurm.sh
number_of_nodes=0
FILE_SIZE=$((1024*1024*10))
SHARED_NODES=0
TEST_TYPE="weak"
MALLEABILITY=0
LOWER_BOUND=0

set -x

jid=1
v=2

if [ $SHARED_NODES -eq 1 ]
then
	v=1
fi

##for p in {1,2,4,8,16,32,64,98}
for NUM_SERVERS in {1,8,16,32,64,100}
do
	LOWER_BOUND=$NUM_SERVERS
	for NODES_FOR_CLIENTS in {1..1}
	do
		NODES_FOR_CLIENTS=$NUM_SERVERS
		for CLIENTS_PER_NODE in {1..1}
	        do
			
        	        for BLOCK_SIZE in {512..512}
	                do
				if [ $SHARED_NODES -eq 1 ]
				then
					NUMBER_OF_NODES=$(($NUM_SERVERS+1)) # num. server (clients will be deployed inside every server's node)+meta servers
				else
					NUMBER_OF_NODES=$(($NUM_SERVERS+$NODES_FOR_CLIENTS+1))
				fi

        	                if [ "$TEST_TYPE" = "weak" ]
	                        then
                	                FILE_SIZE_PER_CLIENT=$((1024*1024))
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
        	                    echo "sbatch -N $NUMBER_OF_NODES $script_name 1 $NUM_SERVERS $CLIENTS_PER_NODE $BLOCK_SIZE $port1 $port2 $FILE_SIZE_PER_CLIENT $SHARED_NODES $NODES_FOR_CLIENTS $MALLEABILITY $LOWER_BOUND"
        	                    jid=$(sbatch -N $NUMBER_OF_NODES $script_name 1 $NUM_SERVERS $CLIENTS_PER_NODE $BLOCK_SIZE $port1 $port2 $FILE_SIZE_PER_CLIENT $SHARED_NODES $NODES_FOR_CLIENTS $MALLEABILITY $LOWER_BOUND | cut -d ' ' -f4)
	                        else
                        	    echo "sbatch --dependency=afterany:${jid} -N $NUMBER_OF_NODES $script_name 1 $NUM_SERVERS $CLIENTS_PER_NODE $BLOCK_SIZE $port1 $port2 $FILE_SIZE_PER_CLIENT $SHARED_NODES $NODES_FOR_CLIENTS $MALLEABILITY $LOWER_BOUND"
                        	    jid=$(sbatch --dependency=afterany:${jid} -N $NUMBER_OF_NODES $script_name 1 $NUM_SERVERS $CLIENTS_PER_NODE $BLOCK_SIZE $port1 $port2 $FILE_SIZE_PER_CLIENT $SHARED_NODES $NODES_FOR_CLIENTS $MALLEABILITY $LOWER_BOUND | cut -d ' ' -f4)
                	        fi
        	        done
	        done	 
	done
done

