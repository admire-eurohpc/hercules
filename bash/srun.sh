#!/bin/bash
#SBATCH --job-name=imss    # Job name
#SBATCH --time=00:05:00               # Time limit hrs:min:sec
#SBATCH --output=imss_%j.log   # Standard output and error log
#SBATCH --exclusive
#SBATCH --exclude=compute-8-1
#SETUP

NUM_METADATA=$1
NUM_DATA=$2
NUM_CLIENT=$3
BLOCK_SIZE=$4
STORAGE_SIZE=2
META_PORT=$6
DATA_PORT=$7

IMSS_PATH=$HOME/imss/build
IOR_PATH=/home/software/io500/bin

module unload mpi
module load mpi/mpich3/3.2.1 
#set -x

# SCRIPT

PWD=`pwd`
srun hostname | sort > hostfile

echo "# IMMS: Running metadata servers"
rm metadata &> /dev/null
touch metadata
head -n $NUM_METADATA hostfile > meta_hostfile
cat meta_hostfile

readarray -t hosts < meta_hostfile

export IMSS_DEBUG=true

for ((i=0;i<$NUM_METADATA;i++));
do
   srun --export=ALL -N 1 -w ${hosts[$i]} --exclusive $IMSS_PATH/server m --server-id=$i --stat-logfile=./metadata --port=$META_PORT --bufsize=0 2> meta.log &
done
sleep 2

echo "# IMMS: Running data servers"
tail -n +$((NUM_METADATA+1)) hostfile | head -n $NUM_DATA > data_hostfile
META_NODE=$(head -n 1 meta_hostfile)
cat data_hostfile

readarray -t hosts < data_hostfile

for ((i=0;i<$NUM_DATA;i++));
do
   srun --export=ALL -N 1 -w ${hosts[$i]} --exclusive $IMSS_PATH/server d --server-id=$i --imss-uri=imss:// --port=$DATA_PORT --bufsize=0 --stat-host=$META_NODE --stat-port=$META_PORT --num-servers=$NUM_DATA --deploy-hostfile=./data_hostfile --block-size=$BLOCK_SIZE --storage-size=$STORAGE_SIZE 2> data.log &
done
sleep 2

echo "# IMMS: Running IOR"
tail -n +$((NUM_METADATA+NUM_DATA+1)) hostfile | head -n $NUM_CLIENT > client_hostfile
mpiexec -l -n $NUM_CLIENT --ppn 2 -f ./client_hostfile \
             -env IMSS_DEBUG true \
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
			 -env IMSS_STORAGE_SIZE $STORAGE_SIZE \
			 -env IMSS_METADATA_FILE $PWD/metadata \
			 -env IMSS_DEPLOYMENT 2 \
			 $IOR_PATH/ior -o /mnt/imss/data.out -t 100m -b 100m -s 1
			 #/home/hcristobal/imss_parallel/imss/bash/test_simple /mnt/imss/data.out
