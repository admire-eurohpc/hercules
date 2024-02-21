#!/bin/bash

SCRIPT_NAME=ior_hercules_slurm.sh
# FILE_SIZE=$((1024*1024*10))
FILE_SIZE=$((1024*1024*1))
ATTACHED=0
#TEST_TYPE="weak"
TEST_TYPE="strong"
TEMPLATE_CONFIG_PATH="../conf/hercules-template.conf"


NUM_SERVERS_RANGE=( 16 )
#NUM_SERVERS_RANGE=( 4 8 16 )
# NODES_FOR_CLIENTS_RANGE=( 16 )
NODES_FOR_CLIENTS_RANGE=( 1 2 4 8 16 )
#CLIENTS_PER_NODE_RANGE=( 1 2 4 8 16 32 )
CLIENTS_PER_NODE_RANGE=( 16 )
BLOCK_SIZE_RANGE=( 512 )

# set -o xtrace
#set -x

jid=1

for NUM_SERVERS in "${NUM_SERVERS_RANGE[@]}"
do
	LOWER_BOUND=$NUM_SERVERS
	for NODES_FOR_CLIENTS in "${NODES_FOR_CLIENTS_RANGE[@]}"
	do
		#NODES_FOR_CLIENTS=$NUM_SERVERS
		for CLIENTS_PER_NODE in "${CLIENTS_PER_NODE_RANGE[@]}"
		do
			for BLOCK_SIZE in "${BLOCK_SIZE_RANGE[@]}"
			do
				## Calculates the number of nodes to be allocated.
				## +1 because the metadata node.
				if [ $ATTACHED -eq 1 ]; then
					NUMBER_OF_NODES=$((NUM_SERVERS+1))
				else
					NUMBER_OF_NODES=$((NUM_SERVERS+NODES_FOR_CLIENTS+1))
				fi

				## Calculates the amount of data per client according 
				## to the type of test. 
				if [ "$TEST_TYPE" = "weak" ]; then
					FILE_SIZE_PER_CLIENT=$((1024*1024))
				## Strong by default.
				else
					TOTAL_NUMBER_OF_CLIENTS=$((NODES_FOR_CLIENTS*CLIENTS_PER_NODE))
					FILE_SIZE_PER_CLIENT=$((FILE_SIZE/TOTAL_NUMBER_OF_CLIENTS))
				fi

				CONFIG_PATH="../conf/${NUM_SERVERS}s-${NODES_FOR_CLIENTS}nfc-${CLIENTS_PER_NODE}cpd.conf"
				## If the configuration file does not exist then we create one by using a template and modifying the necessary variables.
				if ! [ -f "$CONFIG_PATH" ]; then
					
					cp $TEMPLATE_CONFIG_PATH $TEMPLATE_CONFIG_PATH."_TEMP"
					sed -i "s/^NUM_DATA_SERVERS = [0-9]/NUM_DATA_SERVERS = $NUM_SERVERS/g"  "$TEMPLATE_CONFIG_PATH"
					sed -i "s/^NUM_NODES_FOR_CLIENTS = [0-9]/NUM_NODES_FOR_CLIENTS = $NODES_FOR_CLIENTS/g"  "$TEMPLATE_CONFIG_PATH"
					sed -i "s/^NUM_CLIENTS_PER_NODE = [0-9]/NUM_CLIENTS_PER_NODE = $CLIENTS_PER_NODE/g"  "$TEMPLATE_CONFIG_PATH"
				
					cat $TEMPLATE_CONFIG_PATH > "$CONFIG_PATH"
					cp $TEMPLATE_CONFIG_PATH."_TEMP" $TEMPLATE_CONFIG_PATH
				fi
			
				### continue	FIXED
				set -x

				## The first job does not have dependencie (do not wait for another job to end). 
				if [ "$jid" -eq 1 ]; then
					jid=$(sbatch -N $NUMBER_OF_NODES $SCRIPT_NAME "$CONFIG_PATH" "$FILE_SIZE_PER_CLIENT" "$TOTAL_NUMBER_OF_CLIENTS" "$CLIENTS_PER_NODE" | cut -d ' ' -f4)
					echo $jid
				## The following jobs wait for the previous job to finish.
				else
					jid=$(sbatch --dependency=afterany:"${jid}" -N $NUMBER_OF_NODES $SCRIPT_NAME "$CONFIG_PATH" "$FILE_SIZE_PER_CLIENT"  "$TOTAL_NUMBER_OF_CLIENTS" "$CLIENTS_PER_NODE" | cut -d ' ' -f4)
				fi
				### exit 0
				set +x

            done
	    done	 
	done
done


set +o xtrace
