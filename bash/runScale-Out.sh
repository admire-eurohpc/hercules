#!/bin/bash
#SBATCH --job-name=imss    # Job name
#SBATCH --time=00:60:00               # Time limit hrs:min:sec
#SBATCH --output=logs/%j_scale-out.log   # Standard output and error log
##SBATCH --exclude=compute-3-1,compute-3-2,compute-6-1,compute-6-2
#SBATCH --exclusive
#SBATCH --mem=0
##SBATCH --mem-per-cpu=10G

# Fix for multiple servers.
wait_for_server() {
	IMSS_INIT_SERVER=0
	NUM_SERVERS=$2
	until [[ $IMSS_INIT_SERVER -eq $NUM_SERVERS ]]
	do
		IMSS_INIT_SERVER=`cat server.log | grep "IMSS_INIT_SERVER" | cut -c 18-`
		#echo -n "."
		#echo "The value of IMSS_INIT_SERVER is="$IMSS_INIT_SERVER
		#if [[ $IMSS_INIT_SERVER == '' ]]
		#then
		#	IMSS_INIT_SERVER=0
		#fi
	#	kill_servers
		sleep 1
	done
	rm server.log
	echo "[$1 server is running]"
}

#SETUP
NUM_METADATA=$1
NUM_DATA=$2
NUM_CLIENT=$3
BLOCK_SIZE=$4
STORAGE_SIZE=2
META_PORT=$5
DATA_PORT=$6

IMSS_PATH=$(dirname `pwd`)/build
echo $IMSS_PATH

# Uncomment when working in Tucan.
IOR_PATH=/home/software/io500/bin
module unload mpi
module load mpi/mpich3/3.2.1

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

set -x

# SCRIPT
PWD=`pwd`
srun hostname |sort > hostfile

echo "# IMMS: Running metadata servers"
rm metadata &> /dev/null
touch metadata
head -n $NUM_METADATA hostfile > meta_hostfile
cat meta_hostfile
readarray -t hosts < meta_hostfile

IMSS_DEBUG=none
export IMSS_DEBUG=$IMSS_DEBUG

### Init metadata server.
for ((i=0;i<$NUM_METADATA;i++));
do
	srun --export=ALL -N 1 -w ${hosts[$i]} --exclusive $IMSS_PATH/server m --server-id=$i --stat-logfile=./metadata --port=$META_PORT --bufsize=0 1> server.log &
done
### Wait for metadata server.
wait_for_server Metadata $NUM_METADATA

echo "# IMMS: Running data servers"
tail -n +$((NUM_METADATA+1)) hostfile | head -n $NUM_DATA > data_hostfile
META_NODE=$(head -n 1 meta_hostfile)
cat data_hostfile
readarray -t hosts < data_hostfile
### Init data server.
for ((i=0;i<$NUM_DATA;i++));
do
	srun --export=ALL -N 1 -w ${hosts[$i]} --exclusive $IMSS_PATH/server d --server-id=$i --imss-uri=imss:// --port=$DATA_PORT --bufsize=0 --stat-host=$META_NODE --stat-port=$META_PORT --num-servers=$NUM_DATA --deploy-hostfile=./data_hostfile --block-size=$BLOCK_SIZE --storage-size=$STORAGE_SIZE 1> server.log &
done
### Wait for data server
wait_for_server Data $NUM_DATA

FILE_NAME="data.out"
FILE_SIZE=512
#COMMAND="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}kb -b ${FILE_SIZE}kb -s 1 -k -E -w -i 1"
#COMMAND="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}kb -b ${FILE_SIZE}kb -s 1 -w -N=$NUM_CLIENT"
#COMMAND="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}kb -b ${FILE_SIZE}kb -s 1 -k -E -W -i 1"
#COMMAND2="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}kb -b ${FILE_SIZE}kb -s 1 -i 1 -WR -F"
#COMMAND="$IOR_PATH/ior -o /mnt/imss/$FILE_NAME -t ${FILE_SIZE}m -b ${FILE_SIZE}m -s 1 -i 1 -k -WR -F"
#COMMAND="./exe_WRITE-AND-READ-TEST-BIFURCADO /mnt/imss/$FILE_NAME $FILE_SIZE"
COMMAND="$IOR_PATH/ior -o /mnt/imss/data.out -t ${FILE_SIZE}kb -b ${FILE_SIZE}kb -s 1 -i 1"

echo "# IMMS: Running IOR"
tail -n +$((NUM_METADATA+NUM_DATA+1)) hostfile | head -n $NUM_CLIENT > client_hostfile
mpiexec -n $NUM_CLIENT --ppn 1 -f ./client_hostfile \
	-env LD_PRELOAD $IMSS_PATH/tools/libimss_posix.so \
	-env IMSS_MOUNT_POINT /mnt/imss \
	-env IMSS_HOSTFILE $PWD/data_hostfile \
	-env IMSS_N_SERVERS $NUM_DATA \
	-env IMSS_SRV_PORT $DATA_PORT \
	-env IMSS_BUFFSIZE 1 \
	-env IMSS_DEBUG $IMSS_DEBUG \
	-env IMSS_BLKSIZE $BLOCK_SIZE \
	-env IMSS_META_HOSTFILE $PWD/meta_hostfile \
	-env IMSS_META_PORT $META_PORT \
	-env IMSS_META_SERVERS $NUM_METADATA \
	-env IMSS_STORAGE_SIZE $STORAGE_SIZE \
	-env IMSS_METADATA_FILE $PWD/metadata \
	-env IMSS_DEPLOYMENT 2 \
	$COMMAND
