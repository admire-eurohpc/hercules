#!/bin/bash
#SBATCH --job-name=imss    # Job name
#SBATCH --ntasks-per-node=1             # How many tasks on each node
#SBATCH --time=00:05:00               # Time limit hrs:min:sec
#SBATCH --output=imss_%j.log   # Standard output and error log

#SETUP

NUM_METADATA=$1
NUM_DATA=$2
NUM_CLIENT=$3
BLOCK_SIZE=$4
META_PORT=$5
DATA_PORT=$6

IMSS_PATH=$HOME/imss/build
IOR_PATH=/home/software/io500/bin

# SCRIPT
PWD=`pwd`

module unload mpi
module load mpi/mpich2/1.4.1 

mpiexec -ppn 1 hostname > hostfile

echo "# IMMS: Running metadata servers"
rm metadata &> /dev/null
touch metadata
head -n $NUM_METADATA hostfile > meta_hostfile
cat meta_hostfile
mpiexec -n $NUM_METADATA -ppn 1 -f ./meta_hostfile $IMSS_PATH/server m --stat-logfile=./metadata --port=$META_PORT --bufsize=0 &

sleep 5

echo "# IMMS: Running data servers"
tail -n +$((NUM_METADATA+1)) hostfile | head -n $NUM_DATA > data_hostfile
META_NODE=$(head -n 1 meta_hostfile)
cat data_hostfile
mpiexec -n $NUM_DATA -ppn 1 -f ./data_hostfile $IMSS_PATH/server d --imss-uri=imss:// --port=$DATA_PORT --bufsize=0 --stat-host=$META_NODE --stat-port=$META_PORT --num-servers=$NUM_DATA --deploy-hostfile=./data_hostfile &

sleep 25

echo "# IMMS: Running IOR"
tail -n +$((NUM_METADATA+NUM_DATA+1)) hostfile | head -n $NUM_CLIENT > client_hostfile
mpiexec -n $NUM_CLIENT -ppn 1 -f ./client_hostfile \
             -env LD_PRELOAD $IMSS_PATH/tools/libimss_posix.so \
             -env IMSS_MOUNT_POINT /mnt/imss \
			 -env IMSS_HOSTFILE $PWD/data_hostfile \
			 -env IMSS_N_SERVERS $NUM_DATA \
			 -env IMSS_SRV_PORT $DATA_PORT \
			 -env IMSS_BUFFSIZE 1 \
			 -env IMSS_BLKSIZE $BLOCK_SIZE \
             -env IMSS_META_HOSTFILE $PWD/meta_hostfile \
			 -env IMSS_META_PORT $META_PORT \
			 -env IMSS_META_SERVERS $NUM_METADATA \
			 -env IMSS_STORAGE_SIZE 8 \
			 -env IMSS_METADATA_FILE $PWD/metadata \
			 -env IMSS_DEPLOYMENT 2 \
			 $IOR_PATH/ior -o /mnt/imss/data.out -t 100m -b 100m -s 1

