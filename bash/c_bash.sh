#!/bin/bash

## Uncomment when working in Tucan.
# IOR_PATH=/home/software/io500/bin
# module unload mpi
# module load mpi/mpich3/3.2.1

## Uncomment when working in Unito.
IOR_PATH=/beegfs/home/javier.garciablas/io500/bin
 spack load \
    cmake@3.24.3%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
    glib@2.74.1%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
    ucx@1.14.0%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
    pcre@8.45%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
    jemalloc
 spack load openmpi@4.1.5%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell


source ./hercules start -m meta_hostfile -d data_hostfile -o /mnt/imss/data.out -s 0

COMMAND="$IOR_PATH/ior -t 1M -b 10M -s 1 -i 10 -o /mnt/imss/data.out"
mpiexec $H_MPI_HOSTFILE_DEF ./client_hostfile -np $H_NCPN \
	$H_MPI_ENV_DEF $H_POSIX_PRELOAD \
	$COMMAND

./hercules stop -s 0

