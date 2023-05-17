#!/bin/bash
#SBATCH --job-name=hercules    # Job name
#SBATCH --time=00:30:00               # Time limit hrs:min:sec
#SBATCH --output=logs/hercules/%j_hercules.log   # Standard output and error log
#SBATCH --mem=0
#SBATCH --overcommit
#SBATCH --oversubscribe
##SBATCH --nodelist=broadwell-[038-043]
##SBATCH --nodelist=broadwell-[000-004]
#SBATCH --exclude=broadwell-[036-067]
###SBATCH --exclusive=user


CONFIG_PATH=$1

## Uncomment when working in Tucan.
# IOR_PATH=/home/software/io500/bin
# module unload mpi
# module load mpi/mpich3/3.2.1

## Uncomment when working in Unito.
#  IOR_PATH=/beegfs/home/javier.garciablas/io500/bin
#  spack load \
#     cmake@3.24.3%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#     glib@2.74.1%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#     ucx@1.14.0%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#     pcre@8.45%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#     openmpi@4.1.5%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#     jemalloc 

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
IOR_PATH=/usr/local/bin


start_=`date +%s.%N`
if [ -z "$CONFIG_PATH" ]; then
   echo "here"
   source hercules start
else
   source hercules start -f "$CONFIG_PATH"
fi
end_=`date +%s.%N`
runtime=$( echo "$end_ - $start_" | bc -l )
echo "Hercules started in $runtime seconds, start=$start_, end=$end_"


echo "Running clients"
COMMAND="$IOR_PATH/ior -t 1M -b 10M -s 1 -i 5 -F -o /mnt/imss/data.out"
#COMMAND="../../bin/nekbmpi eddy_uv 2"

# set -x
echo "mpiexec $H_MPI_HOSTFILE_DEF $H_MPI_HOSTFILE_NAME -n $H_NNFC $H_MPI_PPN $H_NCPN \
	$H_MPI_ENV_DEF $H_POSIX_PRELOAD \
   $H_MPI_ENV_DEF IMSS_CONF=$CONFIG_PATH \
	$COMMAND"

mpiexec $H_MPI_HOSTFILE_DEF $H_MPI_HOSTFILE_NAME -n $H_NNFC $H_MPI_PPN $H_NCPN \
	$H_MPI_ENV_DEF $H_POSIX_PRELOAD \
   $H_MPI_ENV_DEF IMSS_CONF=$CONFIG_PATH \
	$COMMAND


./hercules stop
