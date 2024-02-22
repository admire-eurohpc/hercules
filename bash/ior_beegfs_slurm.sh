#!/bin/bash
#SBATCH --job-name=ior_g    # Job name
#SBATCH --time=01:00:00               # Time limit hrs:min:sec
#SBATCH --output=logs/beegfs/%j_hercules.log   # Standard output and error log
#SBATCH --mem=0
#SBATCH --overcommit
#SBATCH --oversubscribe

FILE_SIZE_PER_CLIENT=$1
NUMBER_OF_PROCESS=$2
PROCESS_PER_NODE=$3

## Uncomment when working in Unito.
IOR_PATH=/beegfs/home/javier.garciablas/gsanchez/ior/bin
spack unload mpich openmpi
spack load mpich@3.2.1%gcc@=9.4.0 arch=linux-ubuntu20.04-zen
whereis mpiexec

echo "Running processes, slurm nodes: $SLURM_NNODES"
# TRANSFER_SIZE=$((1024*1))
TRANSFER_SIZE=$FILE_SIZE_PER_CLIENT
COMMAND="$IOR_PATH/ior -t ${TRANSFER_SIZE}kb -b ${FILE_SIZE_PER_CLIENT}kb -s 1 -i 5 -F -k -o /beegfs/home/javier.garciablas/hercules/bash/ior_output/data.txt"


set -x

mpiexec -n $NUMBER_OF_PROCESS -ppn $PROCESS_PER_NODE  $COMMAND