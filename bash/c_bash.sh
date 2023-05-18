#!/bin/bash

## Uncomment when working in Tucan.
# IOR_PATH=/home/software/io500/bin
# module unload mpi
# module load mpi/mpich3/3.2.1

## Uncomment when working in Unito.
# IOR_PATH=/beegfs/home/javier.garciablas/io500/bin
#  spack load \
#     cmake@3.24.3%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#     glib@2.74.1%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#     ucx@1.14.0%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#     pcre@8.45%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#     load openmpi@4.1.5%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#     jemalloc

## Local
 IOR_PATH=/usr/local/bin

# source ./hercules start -m meta_hostfile -d data_hostfile -o /mnt/imss/data.out -s 0
source ./hercules start \
   -m /home/genarog/Documents/UC3M/Codes/Apps/Nek5000/run/eddy_uv/hercules/my_meta_hostfile \
   -d /home/genarog/Documents/UC3M/Codes/Apps/Nek5000/run/eddy_uv/hercules/my_data_hostfile \
   -c /home/genarog/Documents/UC3M/Codes/Apps/Nek5000/run/eddy_uv/hercules/my_client_hostfile

echo "[+] Running clients"


set -x

# COMMAND="$IOR_PATH/ior -t 1M -b 10M -s 1 -i 5 -F -o /mnt/imss/data.out"
# COMMAND="$IOR_PATH/ior -t 1M -b 10M -s 1 -i 5 -F -o /home/genarog/Documents/UC3M/Codes/UPDATED_IMSS/imss/bash/data.out"
COMMAND="/home/genarog/Documents/UC3M/Codes/Apps/Nek5000/run/eddy_uv/nek5000 eddy_uv"
# COMMAND="./exe_WRITE-AND-READ-TEST-BIFURCADO /mnt/imss/eddy hola.txt 1024"
# COMMAND="./exe_READ_EXISTING_FILE /home/genarog/Documents/UC3M/Codes/UPDATED_IMSS/imss/bash/test_file.txt 11"
# COMMAND="./exe_READ_EXISTING_FILE /mnt/imss/test_file.txt 11"

mpiexec -np $H_NNFC -npernode $H_NCPN $H_MPI_HOSTFILE_DEF $H_MPI_HOSTFILE_NAME  \
   $H_MPI_ENV_DEF H_CONF=$H_CONF \
   $H_MPI_ENV_DEF $H_POSIX_PRELOAD \
	$COMMAND > logfile
	# strace -s 2000 -o strace.log $COMMAND
	#ltrace -s 2000 -o ltrace.log $COMMAND



# sleep 20
# export $H_POSIX_PRELOAD 
# export LD_PRELOAD=/home/genarog/Documents/UC3M/Codes/UPDATED_IMSS/imss/build/tools/libhercules_posix.so
# sleep 20
# $COMMAND

# LD_PRELOAD=/home/genarog/Documents/UC3M/Codes/UPDATED_IMSS/imss/build/tools/libhercules_posix.so ./exe_READ_EXISTING_FILE /mnt/imss/test_file.txt 11


# unset LD_PRELOAD

set +x


# mpiexec $H_MPI_HOSTFILE_DEF $H_MPI_HOSTFILE_NAME -np $H_NNFC \
   # -wdir  /home/genarog/Documents/UC3M/Codes/Apps/Nek5000/run/eddy_uv \


./hercules stop