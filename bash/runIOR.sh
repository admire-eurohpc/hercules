#!/bin/bash
#SBATCH --job-name=ior    # Job name
#SBATCH --time=00:60:00               # Time limit hrs:min:sec
#SBATCH --output=logs/%j_ior.log   # Standard output and error log
#SBATCH --mem=0
#SBATCH --overcommit
#SBATCH --oversubscribe

# Uncomment when working in Tucan.
#IOR_PATH=/home/software/io500/bin
#module unload mpi
#module load mpi/mpich3/3.2.1

# Uncomment when working in MN4.
##IMSS_PATH=/home/uc3m15/uc3m15006/INFINIBAND-IMSS/imss/build
##IOR_PATH=/apps/IOR/3.3.0/INTEL/IMPI/bin

# Uncomment when working in Beegfs.
IOR_PATH=/beegfs/home/javier.garciablas/opt/spack/linux-ubuntu20.04-zen/gcc-9.4.0/ior-3.3.0-ssyaxpxjajmhy3v5icfqoo63kaeii6wv/bin
spack load cmake glib pcre ucx ior openmpi

NUM_CLIENT=$1

set -x

# SCRIPT

PWD=`pwd`
srun --ntasks=1 hostname |sort > client_hostfile

FILE_PATH=/beegfs/home/javier.garciablas/imss/bash/data.out

mpiexec -np $NUM_CLIENT --hostfile ./client_hostfile \
        $IOR_PATH/ior -o $FILE_PATH -t 1m -b 10m -s 1 -i 100 -F  # writes 10MB in one file per process mode.
	        #$IOR_PATH/ior -o /tmp/data.out -t 1m -b 10m -s 1 -i 100 -F # writes 10MB in one file per process mode.
