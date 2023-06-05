#!/bin/bash
#SBATCH --job-name=imss    # Job name
#SBATCH --time=00:60:00               # Time limit hrs:min:sec
#SBATCH --output=logs/%j_scale-in.log   # Standard output and error log
#SBATCH --mem=0
##SBATCH --mem-per-cpu=3G
##SBATCH --cpus-per-task=1
##SBATCH --ntasks-per-node=10
##SBATCH --nodes=1
##SBATCH --ntasks=6
#SBATCH --overcommit
##SBATCH --sockets-per-node=1
##SBATCH --cores-per-socket=4
##SBATCH --threads-per-core=1
#SBATCH --oversubscribe
##SBATCH --nodelist=compute-11-6,compute-11-7
##SBATCH --nodelist=compute-7-[1-2]
##SBATCH --nodelist=compute-11-[5-6]
##SBATCH --nodelist=compute-2-[2-4]

#SETUP
NUM_METADATA=$1
NUM_DATA=$2
NUM_CLIENT=$3
BLOCK_SIZE=$4
STORAGE_SIZE=2
META_PORT=$5
DATA_PORT=$6

IMSS_PATH=$(dirname `pwd`)/build
echo "> IMSS_PATH: "$IMSS_PATH

# Uncomment when working in Tucan.
#IOR_PATH=/home/software/io500/bin
#module unload mpi
#module load mpi/mpich3/3.2.1

# Uncomment when working in MN4.
#IOR_PATH=/apps/IOR/3.3.0/INTEL/IMPI/bin
#module unload impi
#module load gcc/9.2.0
#module load java/8u131
#module load openmpi/4.1.0
#module load ucx/1.13.1
#module load cmake/3.15.4
#module unload openmpi
#module load impi
#module load ior

# Uncomment when working in Beegfs.
IOR_PATH=/beegfs/home/javier.garciablas/opt/spack/linux-ubuntu20.04-zen/gcc-9.4.0/ior-3.3.0-ssyaxpxjajmhy3v5icfqoo63kaeii6wv/bin
spack load cmake glib pcre ucx ior mpich

network_devices_list=ibs1

UCX_NET_DEVICES=$network_devices_list
UCX_TLS=tcp

echo $UCX_NET_DEVICES
echo $UCX_TLS

echo "# METADATA SERVERS: "$NUM_METADATA
echo "# DATA SERVERS: "$NUM_DATA
echo "# CLIENTS: "$NUM_CLIENT

echo "##############################"
lscpu
free -h
echo "##############################"

#MAX_CLIENTS_PER_NODE=`lscpu | grep "CPU(s)" | head -n 1 | cut -c 22-`

set -x

NODES_SUM=$((NUM_METADATA+NUM_DATA+NUM_CLIENT))

# SCRIPT
PWD=`pwd`
srun -n $NODES_SUM hostname |sort > hostfile

#cp hostfile meta_hostfile
#cp hostfile data_hostfile
#cp hostfile client_hostfile

echo "# IMMS: Running metadata servers"
rm metadata &> /dev/null
touch metadata
head -n $NUM_METADATA hostfile > meta_hostfile
#cat meta_hostfile

readarray -t hosts < meta_hostfile

#IMSS_DEBUG="SLOG_TIME"
IMSS_DEBUG=none
export IMSS_DEBUG=$IMSS_DEBUG

for ((i=0;i<$NUM_METADATA;i++));
do
	UCX_TLS=tcp UCX_NET_DEVICES=$network_devices_list srun --exclusive --export=ALL -N 1 -n 1 -w ${hosts[$i]} $IMSS_PATH/server m --server-id=$i --stat-logfile=./metadata --port=$META_PORT --bufsize=0 & 2> metadata.log
done
sleep 10

echo "# IMMS: Running data servers"
tail -n +$((NUM_METADATA+1)) hostfile | head -n $NUM_DATA > data_hostfile
#tail -n $NUM_DATA hostfile > data_hostfile
META_NODE=$(head -n 1 meta_hostfile)
#cat data_hostfile

readarray -t hosts < data_hostfile

for ((i=0;i<$NUM_DATA;i++));
do
        # PORT=`expr $DATA_PORT + $i`
        srun --exclusive --export=ALL -N 1 -n 1 -w ${hosts[$i]} $IMSS_PATH/server d --server-id=$i --imss-uri=imss:// --port=$DATA_PORT --bufsize=0 --stat-host=$META_NODE --stat-port=$META_PORT --num-servers=$NUM_DATA --deploy-hostfile=./data_hostfile --block-size=$BLOCK_SIZE --storage-size=$STORAGE_SIZE & 2> dataserver.log
done
sleep 10

FILE_NAME="data.out"
# 64 M
FILE_SIZE_PER_CLIENT=$((128*BLOCK_SIZE/NUM_CLIENT))
MAX_CLIENTS_PER_NODE=$((NUM_CLIENT/NUM_DATA))
#$((1600/$NUM_CLIENT))
#file_size_per_client=100
#file_size_per_client=$BLOCK_SIZE
echo "# IMMS: Running IOR"
# Clients are executed in different nodes.
tail -n +$((NUM_METADATA+NUM_DATA+1)) hostfile | head -n $NUM_CLIENT > client_hostfile

# Same hostfile to data servers and clients.
#cp data_hostfile client_hostfile


# COMMAND="$IOR_PATH/ior -o /mnt/imss/data.out -t ${file_size_per_client}m -b ${file_size_per_client}m -s 1 -i 10 -F"
# COMMAND="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}kb -b ${FILE_SIZE}kb -s 1 -k -E -w -i 1"
# COMMAND2="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}kb -b ${FILE_SIZE}kb -s 1 -k -E -W -i 1"
# COMMAND="$IOR_PATH/ior -w -W -k -o /mnt/imss/data.out -t ${FILE_SIZE}kb -b ${FILE_SIZE}kb -s 1 -i 1"
# COMMAND2="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}kb -b ${FILE_SIZE}kb -s 1 -i 1 -WR -F"
# COMMAND="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}kb -b ${FILE_SIZE}kb -s 1 -w -N=$NUM_CLIENT"
# COMMAND="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}m -b ${FILE_SIZE}m -s 1 -i 1 -k -WR -F"
# COMMAND="./exe_WRITE-AND-READ-TEST-BIFURCADO /mnt/imss/$FILE_NAME $FILE_SIZE_PER_CLIENT"
 COMMAND="$IOR_PATH/ior -o /mnt/imss/data.out -t ${FILE_SIZE_PER_CLIENT}kb -b ${FILE_SIZE_PER_CLIENT}kb -s 1 -i 1"

UCX_TLS=tcp UCX_NET_DEVICES=$network_devices_list mpiexec -l -n $NUM_CLIENT -f ./client_hostfile \
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
        -env IMSS_DEBUG $IMSS_DEBUG  \
        -env UCX_NET_DEVICES $network_devices_list \
	-env UCX_TLS tcp \
        $COMMAND
