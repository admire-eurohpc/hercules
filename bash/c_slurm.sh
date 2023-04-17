#!/bin/bash
#SBATCH --job-name=imss    # Job name
#SBATCH --time=00:60:00               # Time limit hrs:min:sec
#SBATCH --output=logs/hercules/%j_imss.log   # Standard output and error log
#SBATCH --mem=0
#SBATCH --overcommit
#SBATCH --oversubscribe

## Uncomment when working in Tucan.
IOR_PATH=/home/software/io500/bin
module unload mpi
module load mpi/openmpi
USE_OPEN_MPI=1

## Uncomment when working in Italia cluster.
#USE_OPEN_MPI=1

./hercules start

#exit 0

readarray -t client_hosts < client_hostfile
H_NUM_CLIENT=${#client_hosts[@]}
H_PATH=$(dirname `pwd`)


echo "Running client on $client_hosts"
# COMMAND="$IOR_PATH/ior -t 1M -b 100M -s 1 -i 5 -o /mnt/imss/data.out"
COMMAND="hostname"
#COMMAND="echo 'hello' > /tmp/hello"
#COMMAND="free -h"

set -x

mpiexec --hostfile ./client_hostfile \
	$COMMAND

# mpiexec --hostfile ./client_hostfile -npernode $H_NUM_CLIENT \
# 	-x LD_PRELOAD=$H_PATH/build/tools/libhercules_posix.so \
# 	-x IMSS_DEBUG=none \
#         $COMMAND

sleep 10

./hercules stop
