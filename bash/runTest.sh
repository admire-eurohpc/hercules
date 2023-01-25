#!/bin/bash
### Command exmaple: ./runTest.sh -c y 1 1 2 512 3000 3050 file 1 1
####################################################################
# This part has been taken from: https://linuxconfig.org/bash-script-display-usage-and-check-user
display_usage() { 
	#echo "This script must be run with super-user privileges." 
	echo -e "\nUsage: $0 [number_of_clients] \n" 
	}

# if less than six arguments supplied, display usage 
if [  $# -le 5 ] 
then 
	display_usage
	exit 1
fi 

# check whether user had supplied -h or --help . If yes display usage 
if [[ ( $@ == "--help") ||  $@ == "-h" ]] 
then 
	display_usage
	exit 0
fi
#######################################################################


kill_servers() {
	is_alive=`pgrep server`
#	echo "is_alive="$is_alive
	if [  -n "$is_alive" ] 
	then 
		#echo "Is Alive"
		pkill server
		### call this function recursively until there are no alive servers.
		kill_servers 0
	fi
	if [  $1 -eq 1 ] 
	then 
		echo "Exit"
		exit 0
	fi
}

wait_for_server() {
	IMSS_INIT_SERVER=0
	NUM_SERVERS=$2
	until [[ $IMSS_INIT_SERVER -eq $NUM_SERVERS ]]
	do
		IMSS_INIT_SERVER=`cat server.log | grep "IMSS_INIT_SERVER" | cut -c 18-`
		#echo -n "."
		#echo "The value of IMSS_INIT_SERVER is="$IMSS_INIT_SERVER
		#if [[ $IMSS_INIT_SERVER == '' ]]
		#then
		#	IMSS_INIT_SERVER=0
		#fi
	#	kill_servers
		sleep 1
	done
	rm server.log
	echo "[$1 server is running]"
}

rm ./m-server-2023-01-25.log d-server-2023-01-25.log client-2023-01-25.log

#set -x
### setup
IMSS_INIT=$2
NUM_METADATA=$3
NUM_DATA=$4
NUM_CLIENT=$5
BLOCK_SIZE=$6
STORAGE_SIZE=2
META_PORT=$7
DATA_PORT=$8
export IMSS_DEBUG=$9
FILE_SIZE=${10}
ITERATIONS_TEST=${11}
IMSS_PATH=$(dirname `pwd`)/build
IOR_PATH=/usr/local/bin

while getopts ":c" opt; 
do
	echo ${opt}
	case ${opt} in
		c)
			### Compiling project
			cd ../build/ && make -j && cd ../bash
			mpicc.mpich WRITE-AND-READ-TEST-BIFURCADO.c -o exe_WRITE-AND-READ-TEST-BIFURCADO
		;;
	esac
done

#exit 0
for i in $(eval echo {1..$ITERATIONS_TEST});
do
	echo "Running test " $i

	### To ensure that all servers are down.
	kill_servers 0

	### Init metadata server.
	$IMSS_PATH/server m --server-id=0 --stat-logfile=./metadata --port=$META_PORT --bufsize=0 1> server.log &
	### Wait for metadata server.
	wait_for_server Metadata $NUM_METADATA

	### Init data server.
	#set -x
	META_NODE=$(head -n 1 meta_hostfile)
	$IMSS_PATH/server d --server-id=0 --imss-uri=imss:// --port=$DATA_PORT --bufsize=0 --stat-host=$META_NODE --stat-port=$META_PORT --num-servers=$NUM_DATA --deploy-hostfile=./data_hostfile --block-size=$BLOCK_SIZE --storage-size=$STORAGE_SIZE 1> server.log &
	### Wait for data server
	wait_for_server Data $NUM_DATA
#	sleep 10
	#kill_servers

	FILE_NAME="data.out"
#	COMMAND="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}kb -b ${FILE_SIZE}kb -s 1 -k -E -w -i 1"
	#COMMAND2="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}kb -b ${FILE_SIZE}kb -s 1 -k -E -W -i 1"
	#COMMAND2="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}kb -b ${FILE_SIZE}kb -s 1 -i 1 -WR -F"
	#COMMAND="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}m -b ${FILE_SIZE}m -s 1 -i 1 -k -WR -F"
	COMMAND="./exe_WRITE-AND-READ-TEST-BIFURCADO /mnt/imss/$FILE_NAME $FILE_SIZE"

#	echo $BLOCK_SIZE

	### Init client(s)
	if [ $IMSS_INIT == 'y' ]
	then
	echo "[Running under IMSS]"
	mpiexec.mpich -l -n $NUM_CLIENT -f ./client_hostfile \
		-env LD_PRELOAD $IMSS_PATH/tools/libimss_posix.so \
		-env IMSS_MOUNT_POINT /mnt/imss \
		-env IMSS_HOSTFILE $PWD/data_hostfile \
		-env IMSS_N_SERVERS $NUM_DATA \
		-env IMSS_SRV_PORT $DATA_PORT \
		-env IMSS_BUFFSIZE 1 \
		-env IMSS_BLKSIZE $BLOCK_SIZE \
		-env IMSS_META_HOSTFILE $PWD/meta_hostfile \
		-env IMSS_META_PORT $META_PORT \
		-env IMSS_META_SERVERS $NUM_METADATA \
		-env IMSS_STORAGE_SIZE $STORAGE_SIZE \
		-env IMSS_METADATA_FILE $PWD/metadata \
		-env IMSS_DEPLOYMENT 2 \
		$COMMAND

	continue

	kill_servers 1
	echo "[Running under IMSS2]"
	mpiexec.mpich -l -n $NUM_CLIENT -f ./client_hostfile \
		-env LD_PRELOAD $IMSS_PATH/tools/libimss_posix.so \
		-env IMSS_MOUNT_POINT /mnt/imss \
		-env IMSS_HOSTFILE $PWD/data_hostfile \
		-env IMSS_N_SERVERS $NUM_DATA \
		-env IMSS_SRV_PORT $DATA_PORT \
		-env IMSS_BUFFSIZE 1 \
		-env IMSS_BLKSIZE $BLOCK_SIZE \
		-env IMSS_META_HOSTFILE $PWD/meta_hostfile \
		-env IMSS_META_PORT $META_PORT \
		-env IMSS_META_SERVERS $NUM_METADATA \
		-env IMSS_STORAGE_SIZE $STORAGE_SIZE \
		-env IMSS_METADATA_FILE $PWD/metadata \
		-env IMSS_DEPLOYMENT 2 \
		$COMMAND2

	else
	echo "[Running under UNIX]"
	mpiexec.mpich -l -n $NUM_CLIENT \
		./exe_WRITE-TEST-BIFURCADO ./$FILE_NAME 1 1 1
	kill_servers 1
	fi
done 
### end for

#mpiexec.mpich -l -n $1 -f ./client_hostfile -env LD_PRELOAD /home/genarog/Documents/UC3M/Codes/UPDATED_IMSS/imss/build/tools/libimss_posix.so -env IMSS_MOUNT_POINT /mnt/imss -env IMSS_HOSTFILE /home/genarog/Documents/UC3M/Codes/UPDATED_IMSS/imss/bash/data_hostfile -env IMSS_N_SERVERS 1 -env IMSS_SRV_PORT 3050 -env IMSS_BUFFSIZE 1 -env IMSS_BLKSIZE $BLOCK_SIZE -env IMSS_META_HOSTFILE /home/genarog/Documents/UC3M/Codes/UPDATED_IMSS/imss/bash/meta_hostfile -env IMSS_META_PORT 3000 -env IMSS_META_SERVERS 1 -env IMSS_STORAGE_SIZE 2 -env IMSS_METADATA_FILE /home/genarog/Documents/UC3M/Codes/UPDATED_IMSS/imss/bash/metadata -env IMSS_DEPLOYMENT 2 -env UCX_NET_DEVICES all ./exe_WRITE-AND-READ-TEST-BIFURCADO /mnt/imss/data.out 1 1 1
### kill servers' process
kill_servers 1
