#!/bin/bash

while getopts :m:d:o:c: flag
do
    case "${flag}" in
	m) META_SERVER_FILE=${OPTARG};;
        d) DATA_SERVER_FILE=${OPTARG};;
	o) OUTPUT_FILE=${OPTARG};;
	c) CLIENT_FILE=${OPTARG};;
    esac
done
echo "DATA_SERVER_FILE=$DATA_SERVER_FILE";
echo "META_SERVER_FILE=$META_SERVER_FILE";
echo "OUTPUT_FILE=$OUTPUT_FILE";


#SETUP
META_PORT=70000
DATA_PORT=80000
MALLEABILITY=0
IMSS_LOWER_BOUND_MALLEABILITY=0
IMSS_UPPER_BOUND_MALLEABILITY=0

H_PATH=$(dirname `pwd`)
H_BUILD_PATH=$H_PATH/build
H_BASH_PATH=$H_PATH/bash
echo "> IMSS_PATH: "$IMSS_PATH

## Uncomment when working in Tucan.
IOR_PATH=/home/software/io500/bin
module unload mpi
module load mpi/mpich3/3.2.1
#module load mpi/openmpi
USE_OPEN_MPI=0

## Uncomment when working in MN4.
#module unload impi
#module load gcc/9.2.0
#module load java/8u131
#module load openmpi/4.1.0
#module load ucx/1.13.1
#module load cmake/3.15.4
#module unload openmpi
#module load impi
#module load ior
#IOR_PATH=/apps/IOR/3.3.0/INTEL/IMPI/bin
#MPI_PATH=/gpfs/apps/MN4/INTEL/2017.4/compilers_and_libraries_2017.4.196/linux/mpi/intel64/bin
#USE_OPEN_MPI=0

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
#export UCX_RCACHE_ENABLE=n
#export UCX_IB_REG_METHODS=direct
#export UCX_RC_VERBS_TIMEOUT=5000000.00us

#IMSS_DEBUG="SLOG_TIME"
IMSS_DEBUG=none
#IMSS_DEBUG="SLOG_LIVE"
#IMSS_DEBUG=all
export IMSS_DEBUG=$IMSS_DEBUG

echo $UCX_NET_DEVICES
echo $UCX_TLS

#echo "##############################"
#lscpu
#free -h
#echo "##############################"

#MAX_CLIENTS_PER_NODE=`lscpu | grep "CPU(s)" | head -n 1 | cut -c 22-`

##set -x
#rm *-2023-03-22.log

#NODES_SUM=$((NUM_METADATA+NUM_DATA+NUM_CLIENT))

# SCRIPT
PWD=`pwd`
#srun -n $NODES_SUM hostname |sort > hostfile
#srun -pernode hostname |sort > hostfile 

#cp hostfile meta_hostfile
#cp hostfile data_hostfile
#cp hostfile client_hostfile

##head -n $NUM_METADATA hostfile > meta_hostfile
##tail -n +$((NUM_METADATA+1)) hostfile | head -n $NUM_DATA > data_hostfile
##tail -n +$((NUM_METADATA+NUM_DATA+1)) hostfile | head -n $NODES_FOR_CLIENTS > client_hostfile

##if [ $SHARED_NODES -eq 1 ]
##then
##	cp data_hostfile client_hostfile
##fi

echo "# Hercules: Running metadata servers"
readarray -t meta_hosts < $META_SERVER_FILE
i=0
for node in "${meta_hosts[@]}"
do
	(ssh $node "cd $H_BASH_PATH; $H_BUILD_PATH/hercules_server m --server-id=$i --stat-logfile=./metadata --port=$META_PORT --bufsize=0") &
	i=$(($i+1))
done

sleep 10

echo "# Hercules: Running data servers"
readarray -t data_hosts < $DATA_SERVER_FILE
META_NODE=$(head -n 1 $META_SERVER_FILE)
NUM_METADATA=${#meta_hosts[@]}
NUM_DATA=${#data_hosts[@]}
BLOCK_SIZE=512
STORAGE_SIZE=0
i=0
for node in "${data_hosts[@]}"
do
        (ssh $node "cd $H_BASH_PATH; $H_BUILD_PATH/hercules_server d --server-id=$i --imss-uri=imss:// --port=$DATA_PORT --bufsize=0 --stat-host=$META_NODE --stat-port=$META_PORT --num-servers=$NUM_DATA --deploy-hostfile=./data_hostfile --block-size=$BLOCK_SIZE --storage-size=$STORAGE_SIZE") &
	i=$(($i+1))
done

sleep 30

FILE_NAME="data.out"
## 64 M
#FILE_SIZE_PER_CLIENT=$((128*BLOCK_SIZE/NUM_CLIENT))
#FILE_SIZE_PER_CLIENT=$((100*1024))
#MAX_CLIENTS_PER_NODE=$((NUM_CLIENT/NUM_DATA))
#$((1600/$NUM_CLIENT))
#file_size_per_client=$BLOCK_SIZE
echo "# IMMS: Running IOR"

rm env_hercules.txt
echo "export LD_PRELOAD=$H_BUILD_PATH/tools/libhercules_posix.so" >> env_hercules.txt
echo "export IMSS_MOUNT_POINT=/mnt/imss" >> env_hercules.txt
echo "export IMSS_HOSTFILE=$H_BASH_PATH/data_hostfile" >> env_hercules.txt
echo "export IMSS_N_SERVERS=$NUM_DATA" >> env_hercules.txt
echo "export IMSS_SRV_PORT=$DATA_PORT" >> env_hercules.txt
echo "export IMSS_BUFFSIZE=1" >> env_hercules.txt
echo "export IMSS_BLKSIZE=$BLOCK_SIZE" >> env_hercules.txt
echo "export IMSS_META_HOSTFILE=$H_BASH_PATH/meta_hostfile" >> env_hercules.txt
echo "export IMSS_META_PORT=$META_PORT" >> env_hercules.txt
echo "export IMSS_META_SERVERS=$NUM_METADATA" >> env_hercules.txt
echo "export IMSS_STORAGE_SIZE=$STORAGE_SIZE" >> env_hercules.txt
echo "export IMSS_METADATA_FILE=$H_BUILD_PATH/metadata" >> env_hercules.txt
echo "export IMSS_DEPLOYMENT=2" >> env_hercules.txt
echo "export IMSS_DEBUG=$IMSS_DEBUG"  >> env_hercules.txt
echo "export UCX_NET_DEVICES=$network_devices_list" >> env_hercules.txt
echo "export UCX_TLS=$transports_to_use" >> env_hercules.txt
echo "export IMSS_MALLEABILITY=$MALLEABILITY" >> env_hercules.txt
echo "export IMSS_LOWER_BOUND_MALLEABILITY=$IMSS_LOWER_BOUND_MALLEABILITY" >> env_hercules.txt
echo "export IMSS_UPPER_BOUND_MALLEABILITY=$IMSS_UPPER_BOUND_MALLEABILITY" >> env_hercules.txt

## Same hostfile to data servers and clients.
#cp data_hostfile client_hostfile
#cp hostfile client_hostfile

#TRANSFER_SIZE=$((FILE_SIZE_PER_CLIENT))
TRANSFER_SIZE=$((1024*16))

#COMMAND="$IOR_PATH/ior -t ${TRANSFER_SIZE}kb -b ${FILE_SIZE_PER_CLIENT}kb -s 1 -i 1 -k -w -W -o /mnt/imss/data.out"
#COMMAND="./exe_WRITE-AND-READ-TEST-BIFURCADO /mnt/imss/$FILE_NAME $FILE_SIZE_PER_CLIENT"
COMMAND="$IOR_PATH/ior -t ${TRANSFER_SIZE}kb -b ${FILE_SIZE_PER_CLIENT}kb -s 1 -i 10 -o $OUTPUT_FILE"

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

readarray -t client_hosts < $CLIENT_FILE
for node in "${data_hosts[@]}"
do
	(ssh $node "cd $H_BASH_PATH; source env_hercules.txt; $COMMAND" ) &
done

sleep 30

echo "# Hercules: Stopping metadata servers"
for node in "${data_hosts[@]}"
do
        (ssh $node "pkill hercules_server" ) &
done

echo "# Hercules: Stopping data servers"
for node in "${meta_hosts[@]}"
do
        (ssh $node "pkill hercules_server" ) &
done
