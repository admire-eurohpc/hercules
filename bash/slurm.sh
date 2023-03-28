#!/bin/bash
#SBATCH --job-name=imss    # Job name
#SBATCH --time=00:60:00               # Time limit hrs:min:sec
#SBATCH --output=logs/hercules/%j_imss.log   # Standard output and error log
#SBATCH --mem=0
##SBATCH --mem-per-cpu=3G
##SBATCH --cpus-per-task=1
##SBATCH --ntasks-per-node=10
##SBATCH --nodes=1
##SBATCH --ntasks=6
#SBATCH --overcommit
##SBATCH --sockets-per-node=1
##SBATCH --cores-per-socket=4
##SBATCH --threads-per-core=1
#SBATCH --oversubscribe
##SBATCH --nodelist=compute-11-6,compute-11-7
##SBATCH --nodelist=compute-7-[1-2]
##SBATCH --nodelist=compute-11-[5-6]
##SBATCH --nodelist=compute-2-[2-4]
##SBATCH --nodelist=broadwell-000
##SBATCH --nodelist=broadwell-[002-003]

#SETUP
NUM_METADATA=$1
NUM_DATA=$2
NUM_CLIENT=$3
BLOCK_SIZE=$4
STORAGE_SIZE=0
META_PORT=$5
DATA_PORT=$6
FILE_SIZE_PER_CLIENT=$7
SHARED_NODES=$8
NODES_FOR_CLIENTS=$9
MALLEABILITY=${10}
IMSS_LOWER_BOUND_MALLEABILITY=${11}
IMSS_UPPER_BOUND_MALLEABILITY=$2

echo "# METADATA SERVERS: "$NUM_METADATA
echo "# DATA SERVERS: "$NUM_DATA
echo "# CLIENTS PER NODE: "$NUM_CLIENT
echo "BLOCK SIZE: "$BLOCK_SIZE
echo "STORAGE SIZE: "$STORAGE_SIZE
echo "META PORT: "$META_PORT
echo "DATA PORT: "$DATA_PORT
echo "FILE SIZE PER CLIENT: "$FILE_SIZE_PER_CLIENT
echo "SHARED NODES: "$SHARED_NODES
echo "NODES FOR CLIENTS: "$NODES_FOR_CLIENTS
echo "MALLEABILITY: "$MALLEABILITY
echo "LOWER_BOUND: "$IMSS_LOWER_BOUND_MALLEABILITY
echo "UPPER_BOUND: "$IMSS_UPPER_BOUND_MALLEABILITY

IMSS_PATH=$(dirname `pwd`)/build
echo "> IMSS_PATH: "$IMSS_PATH

wait_for_server() {
	NUM_SERVERS=$2
	for ((i=0;i<$NUM_SERVERS;i++));
	do
		IMSS_INIT_SERVER=0
		until [[ $IMSS_INIT_SERVER -eq 1 ]]
		do
			IMSS_INIT_SERVER=`cat server$i.log | grep "IMSS_INIT_SERVER" | cut -c 18-`
			#echo -n "."
			echo "The value of IMSS_INIT_SERVER in server$i.log is="$IMSS_INIT_SERVER
			#if [[ $IMSS_INIT_SERVER == '' ]]
			#then
			#	IMSS_INIT_SERVER=0
			#fi
		#	kill_servers
			sleep 1
		done
	done
	rm server.log
	echo "[$1 server is running]"
}


## Uncomment when working in Tucan.
#IOR_PATH=/home/software/io500/bin
#module unload mpi
#module load mpi/mpich3/3.2.1
#module load mpi/openmpi

## Uncomment when working in MN4.
module unload impi
module load gcc/9.2.0
module load java/8u131
module load openmpi/4.1.0
module load ucx/1.13.1
module load cmake/3.15.4
module unload openmpi
module load impi
module load ior
IOR_PATH=/apps/IOR/3.3.0/INTEL/IMPI/bin
MPI_PATH=/gpfs/apps/MN4/INTEL/2017.4/compilers_and_libraries_2017.4.196/linux/mpi/intel64/bin
USE_OPEN_MPI=0

## Uncomment when working in Italia cluster.
#export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HOME/rdma-core/build/lib
#IOR_PATH=/beegfs/home/javier.garciablas/opt/spack/linux-ubuntu20.04-zen/gcc-9.4.0/ior-3.3.0-ssyaxpxjajmhy3v5icfqoo63kaeii6wv/bin
#MPI_PATH=/beegfs/home/javier.garciablas/opt/spack/linux-ubuntu20.04-zen/gcc-9.4.0/openmpi-4.1.3-4bpvwm3lcbftmjki6en35c4i5od6wjbr/bin
#USE_OPEN_MPI=1

## Avaible interfaces in Italia cluster: 'eno2'(tcp), 'ibs1'(tcp), 'lo'(tcp), 'opap6s0:1'(ib)
network_devices_list=all
transports_to_use=all

export UCX_NET_DEVICES=$network_devices_list
export UCX_TLS=$transports_to_use
#export UCX_IB_RCACHE_MAX_REGIONS="262144"
#export UCX_NUM_EPS=24
export UCX_RCACHE_ENABLE=n
export UCX_IB_REG_METHODS=direct
#export UCX_RC_VERBS_TIMEOUT=5000000.00us

#IMSS_DEBUG="SLOG_TIME"
IMSS_DEBUG=none
#IMSS_DEBUG="SLOG_LIVE"
#IMSS_DEBUG=all
export IMSS_DEBUG=$IMSS_DEBUG

echo $UCX_NET_DEVICES
echo $UCX_TLS

echo "##############################"
lscpu
free -h
echo "##############################"

#MAX_CLIENTS_PER_NODE=`lscpu | grep "CPU(s)" | head -n 1 | cut -c 22-`

set -x
rm *-2023-03-22.log

NODES_SUM=$((NUM_METADATA+NUM_DATA+NUM_CLIENT))

# SCRIPT
PWD=`pwd`
#srun -n $NODES_SUM hostname |sort > hostfile
srun -pernode hostname |sort > hostfile

#cp hostfile meta_hostfile
#cp hostfile data_hostfile
#cp hostfile client_hostfile

head -n $NUM_METADATA hostfile > meta_hostfile
tail -n +$((NUM_METADATA+1)) hostfile | head -n $NUM_DATA > data_hostfile
tail -n +$((NUM_METADATA+NUM_DATA+1)) hostfile | head -n $NODES_FOR_CLIENTS > client_hostfile

if [ $SHARED_NODES -eq 1 ]
then
	cp data_hostfile client_hostfile
fi

echo "# IMMS: Running metadata servers"
rm metadata &> /dev/null
touch metadata

readarray -t hosts < meta_hostfile
for ((i=0;i<$NUM_METADATA;i++));
do
	srun --exclusive --export=ALL -N 1 -n 1 -w ${hosts[$i]} $IMSS_PATH/server m --server-id=$i --stat-logfile=./metadata --port=$META_PORT --bufsize=0 & > server.log
done
sleep 5
#wait_for_server Metadata $NUM_METADATA

echo "# IMMS: Running data servers"
META_NODE=$(head -n 1 meta_hostfile)

readarray -t hosts < data_hostfile

for ((i=0;i<$NUM_DATA;i++));
do
        # PORT=`expr $DATA_PORT + $i`
        srun --exclusive --export=ALL -N 1 -n 1 -w ${hosts[$i]} $IMSS_PATH/server d --server-id=$i --imss-uri=imss:// --port=$DATA_PORT --bufsize=0 --stat-host=$META_NODE --stat-port=$META_PORT --num-servers=$NUM_DATA --deploy-hostfile=./data_hostfile --block-size=$BLOCK_SIZE --storage-size=$STORAGE_SIZE & > server.log
done
sleep 10
#wait_for_server Data $NUM_DATA

FILE_NAME="data.out"
## 64 M
#FILE_SIZE_PER_CLIENT=$((128*BLOCK_SIZE/NUM_CLIENT))
#FILE_SIZE_PER_CLIENT=$((100*1024))
#MAX_CLIENTS_PER_NODE=$((NUM_CLIENT/NUM_DATA))
#$((1600/$NUM_CLIENT))
#file_size_per_client=$BLOCK_SIZE
echo "# IMMS: Running IOR"
## Clients are executed in different nodes.
#tail -n +$((NUM_METADATA+NUM_DATA+1)) hostfile | head -n $NUM_CLIENT > client_hostfile

## Same hostfile to data servers and clients.
#cp data_hostfile client_hostfile
#cp hostfile client_hostfile

#TRANSFER_SIZE=$((FILE_SIZE_PER_CLIENT))
TRANSFER_SIZE=$((1024*16))

#COMMAND="$IOR_PATH/ior -t ${TRANSFER_SIZE}kb -b ${FILE_SIZE_PER_CLIENT}kb -s 1 -i 1 -k -w -W -o /mnt/imss/data.out"
#COMMAND="./exe_WRITE-AND-READ-TEST-BIFURCADO /mnt/imss/$FILE_NAME $FILE_SIZE_PER_CLIENT"
COMMAND="$IOR_PATH/ior -t ${TRANSFER_SIZE}kb -b ${FILE_SIZE_PER_CLIENT}kb -s 1 -i 10 -o /mnt/imss/data.out"

# COMMAND="$IOR_PATH/ior -o /mnt/imss/data.out -t ${file_size_per_client}m -b ${file_size_per_client}m -s 1 -i 10 -F"
# COMMAND="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}kb -b ${FILE_SIZE}kb -s 1 -k -E -w -i 1"
# COMMAND2="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}kb -b ${FILE_SIZE}kb -s 1 -k -E -W -i 1"
# COMMAND="$IOR_PATH/ior -w -W -k -o /mnt/imss/data.out -t ${FILE_SIZE}kb -b ${FILE_SIZE}kb -s 1 -i 1"
# COMMAND2="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}kb -b ${FILE_SIZE}kb -s 1 -i 1 -WR -F"
# COMMAND="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}kb -b ${FILE_SIZE}kb -s 1 -w -N=$NUM_CLIENT"
# COMMAND="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}m -b ${FILE_SIZE}m -s 1 -i 1 -k -WR -F"
# COMMAND="./exe_WRITE-AND-READ-TEST-BIFURCADO /mnt/imss/$FILE_NAME $FILE_SIZE_PER_CLIENT"
#COMMAND="$IOR_PATH/ior -o /mnt/imss/data.out -t ${FILE_SIZE_PER_CLIENT}kb -b ${FILE_SIZE_PER_CLIENT}kb -s 1 -i 10"
#COMMAND="$IOR_PATH/ior -o /mnt/imss/data.out -t 1m -b 16m -s 16"

if [ $USE_OPEN_MPI -eq 1 ] 
then
	$MPI_PATH/mpiexec --hostfile ./client_hostfile -npernode $NUM_CLIENT \
        	-x LD_PRELOAD=$IMSS_PATH/tools/libimss_posix.so \
	        -x IMSS_MOUNT_POINT=/mnt/imss \
        	-x IMSS_HOSTFILE=$PWD/data_hostfile \
	        -x IMSS_N_SERVERS=$NUM_DATA \
        	-x IMSS_SRV_PORT=$DATA_PORT \
	        -x IMSS_BUFFSIZE=1 \
        	-x IMSS_BLKSIZE=$BLOCK_SIZE \
	        -x IMSS_META_HOSTFILE=$PWD/meta_hostfile \
        	-x IMSS_META_PORT=$META_PORT \
	        -x IMSS_META_SERVERS=$NUM_METADATA \
	        -x IMSS_STORAGE_SIZE=$STORAGE_SIZE \
	        -x IMSS_METADATA_FILE=$PWD/metadata \
	        -x IMSS_DEPLOYMENT=2 \
	        -x IMSS_DEBUG=$IMSS_DEBUG  \
		-x UCX_NET_DEVICES=$network_devices_list \
		-x UCX_TLS=$transports_to_use \
		-x IMSS_MALLEABILITY=$MALLEABILITY \
		-x IMSS_LOWER_BOUND_MALLEABILITY=$IMSS_LOWER_BOUND_MALLEABILITY \
		-x IMSS_UPPER_BOUND_MALLEABILITY=$IMSS_UPPER_BOUND_MALLEABILITY \
	        $COMMAND
else
	$MPI_PATH/mpiexec -f ./client_hostfile --ppn $NUM_CLIENT \
	        -env LD_PRELOAD $IMSS_PATH/tools/libimss_posix.so \
	        -env IMSS_MOUNT_POINT /mnt/imss \
	        -env IMSS_HOSTFILE $PWD/data_hostfile \
	        -env IMSS_N_SERVERS $NUM_DATA \
	        -env IMSS_SRV_PORT $DATA_PORT \
	        -env IMSS_BUFFSIZE 1 \
	        -env IMSS_DEBUG $IMSS_DEBUG \
        	-env IMSS_BLKSIZE $BLOCK_SIZE \
	        -env IMSS_META_HOSTFILE $PWD/meta_hostfile \
        	-env IMSS_META_PORT $META_PORT \
	        -env IMSS_META_SERVERS $NUM_METADATA \
        	-env IMSS_STORAGE_SIZE $STORAGE_SIZE \
	        -env IMSS_METADATA_FILE $PWD/metadata \
        	-env IMSS_DEPLOYMENT 2 \
                -env IMSS_DEBUG $IMSS_DEBUG  \
                -env UCX_NET_DEVICES $network_devices_list \
                -env UCX_TLS $transports_to_use \
                -env IMSS_MALLEABILITY $MALLEABILITY \
                -env IMSS_LOWER_BOUND_MALLEABILITY $IMSS_LOWER_BOUND_MALLEABILITY \
                -env IMSS_UPPER_BOUND_MALLEABILITY $IMSS_UPPER_BOUND_MALLEABILITY \
		$COMMAND
fi
