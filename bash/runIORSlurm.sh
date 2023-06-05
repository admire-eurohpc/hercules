#!/bin/bash
#SBATCH --job-name=ior    # Job name
#SBATCH --time=00:60:00               # Time limit hrs:min:sec
#SBATCH --output=logs/ior/%j_ior.log   # Standard output and error log
#SBATCH --mem=0

## Uncomment when working in Tucan.
#IOR_PATH=/home/software/io500/bin
#module unload mpi
#module load mpi/mpich3/3.2.1

## Uncomment when working in MN4.
#IMSS_PATH=/home/uc3m15/uc3m15006/INFINIBAND-IMSS/imss/build
#IOR_PATH=/apps/IOR/3.3.0/INTEL/IMPI/bin

#set -x

## Uncomment when working in Beegfs.
IOR_PATH=/beegfs/home/javier.garciablas/opt/spack/linux-ubuntu20.04-zen/gcc-9.4.0/ior-3.3.0-ssyaxpxjajmhy3v5icfqoo63kaeii6wv/bin
#spack load cmake glib pcre ior openmpi
#spack load pmix ior openmpi


#whereis ior

#exit 0

set -x

#mpirun --version

#export OMPI_MCA_btl=self,tcp
#export PMIX_MCA_gds=hash

NODES_FOR_CLIENTS=$1
CLIENTS_PER_NODE=$2
FILE_SIZE_PER_CLIENT=$3

# SCRIPT

#PWD=`pwd`
srun -pernode hostname |sort > client_hostfile

FILE_PATH=/beegfs/home/javier.garciablas/imss/bash/data.out

#export OMPI_MCA_btl=self,tcp
#export PMIX_MCA_gds=hash
#export PMIX_MCA_gds=^ds21
/beegfs/home/javier.garciablas/opt/spack/linux-ubuntu20.04-zen/gcc-9.4.0/openmpi-4.1.3-4bpvwm3lcbftmjki6en35c4i5od6wjbr/bin/mpiexec --hostfile ./client_hostfile -npernode $CLIENTS_PER_NODE \
        $IOR_PATH/ior -o $FILE_PATH -t ${FILE_SIZE_PER_CLIENT}kb -b ${FILE_SIZE_PER_CLIENT}kb -s 1 -i 10
