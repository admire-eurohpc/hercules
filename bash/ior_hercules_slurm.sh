#!/bin/bash
#SBATCH --job-name=hercules    # Job name
#SBATCH --time=01:00:00               # Time limit hrs:min:sec
#SBATCH --output=logs/hercules/%j_hercules.log   # Standard output and error log
#SBATCH --mem=0
##SBATCH --oversubscribe
##SBATCH --exclude=broadwell-[000-002]
##SBATCH --nodelist=broadwell-[038-043]
###SBATCH --exclusive=user
##SBATCH --overcommit

CONFIG_PATH=$1
FILE_SIZE_PER_CLIENT=$2
NUMBER_OF_PROCESS=$3
PROCESS_PER_NODE=$4

## Uncomment when working in Tucan.
# IOR_PATH=/home/software/io500/bin
# module unload mpi
# module load mpi/mpich3/3.2.1

## Uncomment when working in Unito.
#IOR_PATH=/beegfs/home/javier.garciablas/io500/bin
IOR_PATH=/beegfs/home/javier.garciablas/gsanchez/ior/bin
#spack load mpich@3.2.1%gcc@=9.4.0
#spack load openmpi@4.1.5
spack unload mpich openmpi
#spack load openmpi@4.1.5%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell
spack load mpich@3.2.1%gcc@=9.4.0 arch=linux-ubuntu20.04-zen
whereis mpiexec
# spack load \
#    cmake@3.24.3%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#    glib@2.74.1%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#    ucx@1.14.0%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#    pcre@8.45%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#    openmpi@4.1.5%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#    jemalloc

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

## Local
# IOR_PATH=/usr/local/bin
# export UCX_TLS=ib
# export UCX_NET_DEVICES=ibs1 # Slow!
# spack load /sxjvb77
set -x
# export UCX_NET_DEVICES="opap6s0:1"

# mpiexec -env UCX_NET_DEVICES "opap6s0:1" -n=1 ucx_info -T
mpiexec -n=1 ucx_info -T
set +x


echo "Starting Hercules"
start_=$(date +%s.%N)
if [ -z "$CONFIG_PATH" ]; then
   echo "No configuration file"
   source hercules start
else
   echo "Configuration file pass $CONFIG_PATH"
   source /beegfs/home/javier.garciablas/hercules/scripts/hercules start \
   -f "$CONFIG_PATH" 
fi
end_=$(date +%s.%N)
runtime=$(echo "$end_ - $start_" | bc -l)
echo "Hercules started in $runtime seconds, start=$start_, end=$end_"

echo "DATA SERVERS $HERCULES_NUM_DATA"

echo "Running clients"
#TRANSFER_SIZE=$((1024 * 16))
# TRANSFER_SIZE=$((1024*1))
TRANSFER_SIZE=$FILE_SIZE_PER_CLIENT
#COMMAND="$IOR_PATH/ior -o /mnt/hercules/data.out -t 100M -b 100M -s 1 -i 5 -w -r -W -R -k"
COMMAND="$IOR_PATH/ior -t ${TRANSFER_SIZE}kb -b ${FILE_SIZE_PER_CLIENT}kb -s 1 -i 5 -F -o /mnt/hercules/data.out"
#COMMAND="../../bin/nekbmpi eddy_uv 2"
#COMMAND="/beegfs/home/javier.garciablas/nek5000/run/eddy_uv/nek5000"
#COMMAND="./exe_WRITE_AND_READ-TEST /mnt/hercules/eddy hola.txt 1024"
# COMMAND="/beegfs/home/javier.garciablas/nek5000/run/eddy_uv_spack/nek5000"
#COMMAND="~/Nek5000/run/turbPipe/nek5000"
#COMMAND="strace -o strace.out ./exe_test_mpi_set_view /mnt/hercules/example.txt"
#COMMAND="ls -lh /mnt/hercules"
#COMMAND="echo \"hola\" > /mnt/hercules/hola.txt"

# MPIEXEC="mpiexec"
set -x
mpiexec -n=$NUMBER_OF_PROCESS $HERCULES_MPI_PPN=$HERCULES_NCPN  $HERCULES_MPI_HOSTFILE_DEF=$HERCULES_MPI_HOSTFILE_NAME \
   $HERCULES_MPI_ENV_DEF HERCULES_CONF=$HERCULES_CONF \
   $HERCULES_MPI_ENV_DEF LD_PRELOAD=$HERCULES_POSIX_PRELOAD \
   $COMMAND
