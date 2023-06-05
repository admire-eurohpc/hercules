#!/bin/bash
#SBATCH --job-name=ior    # Job name
#SBATCH --time=00:60:00               # Time limit hrs:min:sec
#SBATCH --output=logs/%j_ior.log   # Standard output and error log
#SBATCH --exclusive
##SBATCH --cores-per-socket=1
##SBATCH --mem=0
##SBATCH --overcommit
##SBATCH --oversubscribe
##SBATCH --nodelist=broadwell-[004-005]


# Uncomment when working in Tucan.
#IOR_PATH=/home/software/io500/bin
#module unload mpi
#module load mpi/mpich3/3.2.1

# Uncomment when working in MN4.
##IMSS_PATH=/home/uc3m15/uc3m15006/INFINIBAND-IMSS/imss/build
##IOR_PATH=/apps/IOR/3.3.0/INTEL/IMPI/bin

#set -x

# Uncomment when working in Beegfs.
#IOR_PATH=/beegfs/home/javier.garciablas/opt/spack/linux-ubuntu20.04-zen/gcc-9.4.0/ior-3.3.0-ssyaxpxjajmhy3v5icfqoo63kaeii6wv/bin
#spack load cmake glib pcre ior openmpi
#spack load pmix ior openmpi


#whereis ior

#exit 0

set -x

mpirun --version

#export OMPI_MCA_btl=self,tcp
#export PMIX_MCA_gds=hash

NUM_CLIENT=$1
BLOCK_SIZE=$2

# SCRIPT

#PWD=`pwd`
srun hostname |sort > client_hostfile

IOR_PATH=/beegfs/home/javier.garciablas/opt/spack/linux-ubuntu20.04-zen/gcc-9.4.0/ior-3.3.0-ssyaxpxjajmhy3v5icfqoo63kaeii6wv/bin
FILE_PATH=/beegfs/home/javier.garciablas/imss/bash/data.out

export OMPI_MCA_btl=self,tcp
export PMIX_MCA_gds=hash
#export PMIX_MCA_gds=^ds21
mpirun -np $NUM_CLIENT --hostfile client_hostfile --pernode \
	--map-by node --mca coll ^hcoll \
	-x OMPI_MCA_btl=self,tcp \
	-x PMIX_MCA_gds=hash \
        $IOR_PATH/ior -o $FILE_PATH -t ${BLOCK_SIZE}k -b ${BLOCK_SIZE}k -s 1 -i 1  # writes 10MB in one file per process mode.
	        #$IOR_PATH/ior -o /tmp/data.out -t 1m -b 10m -s 1 -i 100 -F # writes 10MB in one file per process mode.
