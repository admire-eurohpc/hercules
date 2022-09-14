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

IMSS_PATH=$HOME/imss/build
IOR_PATH=/home/software/io500/bin

# SCRIPT

PWD=`pwd`
mpiexec hostname > hostfile

echo "# IMMS: Running metadata servers"
rm metadata &> /dev/null
touch metadata
head -n $NUM_METADATA hostfile > meta_hostfile
cat meta_hostfile
mpiexec -np $NUM_METADATA --pernode --hostfile ./meta_hostfile $IMSS_PATH/server ./metadata 5569 0 &

sleep 1

echo "# IMMS: Running data servers"
tail -n +$((NUM_METADATA+1)) hostfile | head -n $NUM_DATA > data_hostfile
META_NODE=$(head -n 1 meta_hostfile)
cat data_hostfile
mpirun -np $NUM_DATA --pernode --hostfile ./data_hostfile $IMSS_PATH/server imss:// 5555 0 $META_NODE 5569 $NUM_DATA ./data_hostfile 1 &

sleep 1

echo "# IMMS: Running IOR"
tail -n +$((NUM_METADATA+NUM_DATA+1)) hostfile | head -n $NUM_CLIENT > client_hostfile
mpirun -np $NUM_CLIENT --pernode --hostfile ./client_hostfile \
             -x LD_PRELOAD=$IMSS_PATH/tools/libimss_posix.so \
             -x IMSS_MOUNT_POINT=/mnt/imss \
			 -x IMSS_HOSTFILE=$PWD/data_hostfile \
			 -x IMSS_N_SERVERS=$NUM_DATA \
			 -x IMSS_SRV_PORT=5555 \
			 -x IMSS_BUFFSIZE=2000 \
			 -x IMSS_BLKSIZE=$BLOCK_SIZE \
             -x IMSS_META_HOSTFILE=$PWD/meta_hostfile \
			 -x IMSS_META_PORT=5569 \
			 -x IMSS_META_SERVERS=$NUM_METADATA \
			 -x IMSS_STORAGE_SIZE=1000 \
			 -x IMSS_METADATA_FILE=$PWD/metadata \
			 -x IMSS_DEPLOYMENT=2 \
			 $IOR_PATH/ior -o /mnt/imss/data.out -t 10m -b 100m -s 5

