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

H_PATH=$(cat .hercules.conf | grep "H_PATH" | awk '{print $2}')
H_BUILD_PATH=$(cat .hercules.conf | grep "H_BUILD_PATH" | awk '{print $2}')
H_BASH_PATH=$(cat .hercules.conf | grep "H_BASH_PATH" | awk '{print $2}')
H_META_PORT=$(cat .hercules.conf | grep "H_META_PORT" | awk '{print $2}')
H_DATA_PORT=$(cat .hercules.conf | grep "H_DATA_PORT" | awk '{print $2}')
H_BLOCK_SIZE=$(cat .hercules.conf | grep "H_BLOCK_SIZE" | awk '{print $2}')
H_STORAGE_SIZE=$(cat .hercules.conf | grep "H_STORAGE_SIZE" | awk '{print $2}')

H_PATH=$(dirname `pwd`)


#COMMAND="echo 'hello' > /tmp/hello"
echo "Running client"
IOR_PATH=/beegfs/home/javier.garciablas/io500/bin
COMMAND="$IOR_PATH/ior -t 1M -b 1M -s 1 -i 1 -o /mnt/imss/data.out"
#COMMAND="free -h"

spack load ucx@1.14.0%gcc@9.4.0 arch=linux-ubuntu20.04-zen glib@2.74.1%gcc@9.4.0 arch=linux-ubuntu20.04-zen pcre@8.45%gcc@9.4.0 arch=linux-ubuntu20.04-zen
set -x
whereis mpiexec

mpiexec --hostfile ./client_hostfile -npernode $H_NUM_CLIENT \
	-x LD_PRELOAD=$H_PATH/build/tools/libhercules_posix.so \
	-x IMSS_DEBUG=all \
        $COMMAND

sleep 10

./hercules stop
