#!/bin/bash
#SBATCH --job-name=imss    # Job name
#SBATCH --time=00:60:00               # Time limit hrs:min:sec
#SBATCH --output=logs/hercules/%j_imss.log   # Standard output and error log
#SBATCH --mem=0
#SBATCH --overcommit
#SBATCH --oversubscribe
##SBATCH --nodelist=compute-2-[1,3-4]
#SBATCH --nodelist=compute-6-2,compute-7-[1-2]
H_PATH=$(dirname `pwd`)

## Uncomment when working in Tucan.
IOR_PATH=/home/software/io500/bin
module unload mpi
module load mpi/openmpi
USE_OPEN_MPI=1

## Uncomment when working in Italia cluster.
#USE_OPEN_MPI=1
./hercules start

# retn_code=$?

# echo "return code =$retn_code"
# echo "return code =$val"

# exit 0

# readarray -t client_hosts < client_hostfile
# H_NUM_CLIENT=${#client_hosts[@]}


# Count the occurrences of every node in the hostfile.

# NUM_CLIENTS_PER_NODE=$(cat client_hostfile | grep "slots=" | awk '{print $1}' | head -n 1)


echo "Running clients"
COMMAND="$IOR_PATH/ior -t 1M -b 10M -s 1 -i 100 -o /mnt/imss/data.out"
# COMMAND="./exe_WRITE-AND-READ-TEST-BIFURCADO /mnt/imss/data.out 10240"
# COMMAND="hostname"
#COMMAND="echo 'hello' > /tmp/hello"
#COMMAND="free -h"

set -x
#NUM_CLIENTS_PER_NODE=$(cat client_hostfile | uniq -c | awk '{print $1}' | head -n 1)
#NUM_CLIENTS_PER_NODE=$(cat client_hostfile | grep "slots" | awk '{print $2}' | awk '{print substr($0, 7, 10)}' | sort -n | tail -1)

# export IMSS_DEBUG=all
# export LD_PRELOAD=$H_PATH/build/tools/libhercules_posix.so

mpiexec --hostfile ./client_hostfile -n 1 \
 	-x LD_PRELOAD=$H_PATH/build/tools/libhercules_posix.so \
	$COMMAND > kk.out

# mpiexec --hostfile ./client_hostfile -npernode $H_NUM_CLIENT \
# 	-x LD_PRELOAD=$H_PATH/build/tools/libhercules_posix.so \
# 	-x IMSS_DEBUG=none \
#         $COMMAND

# sleep 10

./hercules stop
