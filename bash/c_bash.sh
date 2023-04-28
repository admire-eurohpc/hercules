#!/bin/bash

## Uncomment when working in Tucan.
IOR_PATH=/home/software/io500/bin
module unload mpi
module load mpi/mpich3/3.2.1

## Uncomment when working in Unito.
# IOR_PATH=/home/software/io500/bin
# module unload mpi
# module load mpi/mpich3/3.2.1
# module load mpi/openmpi

source ./hercules start -m meta_hostfile -d data_hostfile -o /mnt/imss/data.out -s 0

COMMAND="$IOR_PATH/ior -t 1M -b 10M -s 1 -i 10 -o /mnt/imss/data.out"
mpiexec $H_MPI_HOSTFILE_DEF ./client_hostfile -np $H_NCPN \
	$H_MPI_ENV_DEF $H_POSIX_PRELOAD \
	$COMMAND

./hercules stop -s 0

