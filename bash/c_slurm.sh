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
module load mpi/mpich3/3.2.1

## Uncomment when working in Unito.
# IOR_PATH=/home/software/io500/bin
# module unload mpi
# module load mpi/mpich3/3.2.1
# module load mpi/openmpi

## Uncomment when working in MN4.
# IOR_PATH=/apps/IOR/3.3.0/INTEL/IMPI/bin
# module unload impi
# module load gcc/9.2.0
# module load java/8u131
# module load openmpi/4.1.0
# module load ucx/1.13.1
# module load cmake/3.15.4
# module unload openmpi
# module load impi
# module load ior

source hercules start

echo "Running clients"
COMMAND="$IOR_PATH/ior -t 1M -b 10M -s 1 -i 10 -o /mnt/imss/data.out"
# COMMAND="./exe_WRITE-AND-READ-TEST-BIFURCADO /mnt/imss/data.out 10240"
# COMMAND="hostname"
#COMMAND="echo 'hello' > /tmp/hello"
#COMMAND="free -h

set -x
mpiexec $H_MPI_HOSTFILE_DEF ./client_hostfile -np $H_NCPN \
	$H_MPI_ENV_DEF $H_POSIX_PRELOAD \
	$COMMAND

# mpiexec --hostfile ./client_hostfile -npernode $H_NUM_CLIENT \
# 	-x LD_PRELOAD=$HERCULES_PATH/build/tools/libhercules_posix.so \
# 	-x IMSS_DEBUG=none \
#         $COMMAND

./hercules stop
