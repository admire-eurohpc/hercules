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
##SBATCH --nodelist=broadwell-000
##SBATCH --nodelist=broadwell-[002-003]

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

# Uncomment when working in Italia cluster.
export SPACK_ROOT=/beegfs/home/javier.garciablas/opt/spack/
#spack load cmake glib pcre ucx ior openmpi
#spack load cmake glib pcre ucx
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HOME/rdma-core/build/lib
#spack load openmpi
IOR_PATH=/beegfs/home/javier.garciablas/opt/spack/linux-ubuntu20.04-zen/gcc-9.4.0/ior-3.3.0-ssyaxpxjajmhy3v5icfqoo63kaeii6wv/bin

# Avaible interfaces in Italia cluster: 'eno2'(tcp), 'ibs1'(tcp), 'lo'(tcp), 'opap6s0:1'(ib)
network_devices_list=opap6s0:1
transports_to_use=all

UCX_NET_DEVICES=$network_devices_list
UCX_TLS=$transports_to_use

#IMSS_DEBUG="SLOG_TIME"
IMSS_DEBUG=none
export IMSS_DEBUG=$IMSS_DEBUG

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
#srun -n $NODES_SUM hostname |sort > hostfile
srun -pernode hostname |sort > hostfile

#cp hostfile meta_hostfile
#cp hostfile data_hostfile
cp hostfile client_hostfile

echo "# IMMS: Running metadata servers"
rm metadata &> /dev/null
touch metadata

head -n $NUM_METADATA hostfile > meta_hostfile
tail -n +$((NUM_METADATA+1)) hostfile | head -n $NUM_DATA > data_hostfile
#tail -n +$((NUM_METADATA+NUM_DATA+1)) hostfile | head -n $NUM_CLIENT > client_hostfile

readarray -t hosts < meta_hostfile

for ((i=0;i<$NUM_METADATA;i++));
do
	srun --exclusive --export=ALL -N 1 -n 1 -w ${hosts[$i]} $IMSS_PATH/server m --server-id=$i --stat-logfile=./metadata --port=$META_PORT --bufsize=0 & 2> metadata.log
done
sleep 10

echo "# IMMS: Running data servers"
META_NODE=$(head -n 1 meta_hostfile)

readarray -t hosts < data_hostfile

for ((i=0;i<$NUM_DATA;i++));
do
        # PORT=`expr $DATA_PORT + $i`
        srun --exclusive --export=ALL -N 1 -n 1 -w ${hosts[$i]} $IMSS_PATH/server d --server-id=$i --imss-uri=imss:// --port=$DATA_PORT --bufsize=0 --stat-host=$META_NODE --stat-port=$META_PORT --num-servers=$NUM_DATA --deploy-hostfile=./data_hostfile --block-size=$BLOCK_SIZE --storage-size=$STORAGE_SIZE & 2> dataserver.log
done
sleep 10

FILE_NAME="data.out"
# 64 M
#FILE_SIZE_PER_CLIENT=$((128*BLOCK_SIZE/NUM_CLIENT))
FILE_SIZE_PER_CLIENT=$((100*1024))
MAX_CLIENTS_PER_NODE=$((NUM_CLIENT/NUM_DATA))
#$((1600/$NUM_CLIENT))
#file_size_per_client=$BLOCK_SIZE
echo "# IMMS: Running IOR"
# Clients are executed in different nodes.
#tail -n +$((NUM_METADATA+NUM_DATA+1)) hostfile | head -n $NUM_CLIENT > client_hostfile

# Same hostfile to data servers and clients.
#cp data_hostfile client_hostfile
#cp hostfile client_hostfile

# COMMAND="$IOR_PATH/ior -o /mnt/imss/data.out -t ${file_size_per_client}m -b ${file_size_per_client}m -s 1 -i 10 -F"
# COMMAND="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}kb -b ${FILE_SIZE}kb -s 1 -k -E -w -i 1"
# COMMAND2="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}kb -b ${FILE_SIZE}kb -s 1 -k -E -W -i 1"
# COMMAND="$IOR_PATH/ior -w -W -k -o /mnt/imss/data.out -t ${FILE_SIZE}kb -b ${FILE_SIZE}kb -s 1 -i 1"
# COMMAND2="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}kb -b ${FILE_SIZE}kb -s 1 -i 1 -WR -F"
# COMMAND="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}kb -b ${FILE_SIZE}kb -s 1 -w -N=$NUM_CLIENT"
# COMMAND="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}m -b ${FILE_SIZE}m -s 1 -i 1 -k -WR -F"
# COMMAND="./exe_WRITE-AND-READ-TEST-BIFURCADO /mnt/imss/$FILE_NAME $FILE_SIZE_PER_CLIENT"
 COMMAND="$IOR_PATH/ior -o /mnt/imss/data.out -t ${FILE_SIZE_PER_CLIENT}kb -b ${FILE_SIZE_PER_CLIENT}kb -s 1 -i 10"

/beegfs/home/javier.garciablas/opt/spack/linux-ubuntu20.04-zen/gcc-9.4.0/openmpi-4.1.3-4bpvwm3lcbftmjki6en35c4i5od6wjbr/bin/mpiexec -np $NUM_CLIENT --hostfile ./client_hostfile --pernode \
        -x LD_PRELOAD=$IMSS_PATH/tools/libimss_posix.so \
        -x IMSS_MOUNT_POINT=/mnt/imss \
        -x IMSS_HOSTFILE=$PWD/data_hostfile \
        -x IMSS_N_SERVERS=$NUM_DATA \
        -x IMSS_SRV_PORT=$DATA_PORT \
        -x IMSS_BUFFSIZE=1 \
        -x IMSS_BLKSIZE=$BLOCK_SIZE \
        -x IMSS_META_HOSTFILE=$PWD/meta_hostfile \
        -x IMSS_META_PORT=$META_PORT \
        -x IMSS_META_SERVERS=$NUM_METADATA \
        -x IMSS_STORAGE_SIZE=$STORAGE_SIZE \
        -x IMSS_METADATA_FILE=$PWD/metadata \
        -x IMSS_DEPLOYMENT=2 \
        -x IMSS_DEBUG=$IMSS_DEBUG  \
	-x UCX_NET_DEVICES=$network_devices_list \
	-x UCX_TLS=$transports_to_use \
	-x LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HOME/rdma-core/build/lib \
        $COMMAND
