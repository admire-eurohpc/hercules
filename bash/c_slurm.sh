#!/bin/bash
#SBATCH --job-name=imss    # Job name
#SBATCH --time=00:60:00               # Time limit hrs:min:sec
#SBATCH --output=logs/hercules/%j_imss.log   # Standard output and error log
#SBATCH --mem=0
#SBATCH --overcommit
#SBATCH --oversubscribe

## Uncomment when working in Tucan.
#IOR_PATH=/home/software/io500/bin
#module unload mpi
#module load mpi/openmpi
#USE_OPEN_MPI=1

## Uncomment when working in Italia cluster.
USE_OPEN_MPI=1


./hercules start
#H_NUM_DATA=$(cat ".hercules.env" | grep "H_H_NUM_DATA " | awk '{print $2}')
readarray -t data_hosts < data_hostfile
readarray -t meta_hosts < meta_hostfile
readarray -t client_hosts < client_hostfile
H_NUM_DATA=${#data_hosts[@]}
H_NUM_METADATA=${#meta_hosts[@]}
H_NUM_CLIENT=${#client_hosts[@]}

set -x
H_PATH=$(cat .hercules.conf | grep "H_PATH" | awk '{print $2}')
H_BUILD_PATH=$(cat .hercules.conf | grep "H_BUILD_PATH" | awk '{print $2}')
H_BASH_PATH=$(cat .hercules.conf | grep "H_BASH_PATH" | awk '{print $2}')
H_META_PORT=$(cat .hercules.conf | grep "H_META_PORT" | awk '{print $2}')
H_DATA_PORT=$(cat .hercules.conf | grep "H_DATA_PORT" | awk '{print $2}')
H_BLOCK_SIZE=$(cat .hercules.conf | grep "H_BLOCK_SIZE" | awk '{print $2}')
H_STORAGE_SIZE=$(cat .hercules.conf | grep "H_STORAGE_SIZE" | awk '{print $2}')


#COMMAND="echo 'hello' > /tmp/hello"
IOR_PATH=/beegfs/home/javier.garciablas/io500/bin
COMMAND="$IOR_PATH/ior -t 100M -b 100M -s 1 -i 10 -o /mnt/imss/data.out"

mpiexec --hostfile ./client_hostfile -npernode $H_NUM_CLIENT \
       	-x LD_PRELOAD=$H_BUILD_PATH/tools/libhercules_posix.so \
        -x IMSS_MOUNT_POINT=/mnt/imss \
       	-x IMSS_HOSTFILE=$PWD/data_hostfile \
        -x IMSS_N_SERVERS=$H_NUM_DATA \
       	-x IMSS_SRV_PORT=$H_DATA_PORT \
        -x IMSS_BUFFSIZE=1 \
       	-x IMSS_BLKSIZE=$H_BLOCK_SIZE \
        -x IMSS_META_HOSTFILE=$PWD/meta_hostfile \
       	-x IMSS_META_PORT=$H_META_PORT \
        -x IMSS_META_SERVERS=$H_NUM_METADATA \
        -x IMSS_STORAGE_SIZE=$H_STORAGE_SIZE \
        -x IMSS_METADATA_FILE=$PWD/metadata \
        -x IMSS_DEPLOYMENT=2 \
	-x IMSS_DEBUG=all \
	-x IMSS_MALLEABILITY=0 \
	-x IMSS_LOWER_BOUND_H_MALLEABILITY=0 \
	-x IMSS_UPPER_BOUND_H_MALLEABILITY=0 \
        $COMMAND

sleep 10

./hercules stop
