#!/bin/bash
#SBATCH --job-name=imss    # Job name
#SBATCH --time=00:60:00               # Time limit hrs:min:sec
#SBATCH --output=logs/hercules/%j_imss.log   # Standard output and error log
#SBATCH --mem=0
##SBATCH --mem-per-cpu=3G
##SBATCH --cpus-per-task=1
##SBATCH --ntasks-per-node=10
##SBATCH --nodes=1
##SBATCH --ntasks=6
##SBATCH --overcommit
##SBATCH --sockets-per-node=1
##SBATCH --cores-per-socket=4
##SBATCH --threads-per-core=1
##SBATCH --oversubscribe
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
#FILE_SIZE_PER_CLIENT=$7
#SHARED_NODES=$8
#NODES_FOR_CLIENTS=$9
#MALLEABILITY=${10}
#IMSS_LOWER_BOUND_MALLEABILITY=${11}
#IMSS_UPPER_BOUND_MALLEABILITY=$2

echo "# METADATA SERVERS: "$NUM_METADATA
echo "# DATA SERVERS: "$NUM_DATA
echo "# CLIENTS PER NODE: "$NUM_CLIENT
echo "BLOCK SIZE: "$BLOCK_SIZE
echo "STORAGE SIZE: "$STORAGE_SIZE
echo "META PORT: "$META_PORT
echo "DATA PORT: "$DATA_PORT
echo "FILE SIZE PER CLIENT: "$FILE_SIZE_PER_CLIENT
echo "SHARED NODES: "$SHARED_NODES
echo "NODES FOR CLIENTS: "$NODES_FOR_CLIENTS
echo "MALLEABILITY: "$MALLEABILITY
echo "LOWER_BOUND: "$IMSS_LOWER_BOUND_MALLEABILITY
echo "UPPER_BOUND: "$IMSS_UPPER_BOUND_MALLEABILITY

IMSS_PATH=$(dirname `pwd`)/build
echo "> IMSS_PATH: "$IMSS_PATH

wait_for_server() {
	NUM_SERVERS=$2
	for ((i=0;i<$NUM_SERVERS;i++));
	do
		IMSS_INIT_SERVER=0
		until [[ $IMSS_INIT_SERVER -eq 1 ]]
		do
			IMSS_INIT_SERVER=`cat server$i.log | grep "IMSS_INIT_SERVER" | cut -c 18-`
			#echo -n "."
			echo "The value of IMSS_INIT_SERVER in server$i.log is="$IMSS_INIT_SERVER
			#if [[ $IMSS_INIT_SERVER == '' ]]
			#then
			#	IMSS_INIT_SERVER=0
			#fi
		#	kill_servers
			sleep 1
		done
	done
	rm server.log
	echo "[$1 server is running]"
}


# Uncomment when working in Tucan.
#IOR_PATH=/home/software/io500/bin
#module unload mpi
#module load mpi/mpich3/3.2.1
#module load mpi/openmpi

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
#export SPACK_ROOT=/beegfs/home/javier.garciablas/opt/spack/
#spack load cmake glib pcre ucx ior openmpi
#spack load cmake glib pcre ucx
#spack load ucx
#export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HOME/rdma-core/build/lib
#spack load openmpi
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HOME/rdma-core/build/lib
IOR_PATH=/beegfs/home/javier.garciablas/opt/spack/linux-ubuntu20.04-zen/gcc-9.4.0/ior-3.3.0-ssyaxpxjajmhy3v5icfqoo63kaeii6wv/bin

# Avaible interfaces in Italia cluster: 'eno2'(tcp), 'ibs1'(tcp), 'lo'(tcp), 'opap6s0:1'(ib)
network_devices_list=all
transports_to_use=all

export UCX_NET_DEVICES=$network_devices_list
export UCX_TLS=$transports_to_use
#export UCX_IB_RCACHE_MAX_REGIONS="262144"
#export UCX_NUM_EPS=24
export UCX_RCACHE_ENABLE=n
export UCX_IB_REG_METHODS=direct
#export UCX_RC_VERBS_TIMEOUT=5000000.00us

#IMSS_DEBUG="SLOG_TIME"
IMSS_DEBUG=none
#IMSS_DEBUG="SLOG_LIVE"
#IMSS_DEBUG=all
export IMSS_DEBUG=$IMSS_DEBUG

echo $UCX_NET_DEVICES
echo $UCX_TLS

echo "##############################"
lscpu
free -h
echo "##############################"

#MAX_CLIENTS_PER_NODE=`lscpu | grep "CPU(s)" | head -n 1 | cut -c 22-`

rm *-2023-03-14.log

set -x

NODES_SUM=$((NUM_METADATA+NUM_DATA+NUM_CLIENT))

# SCRIPT
PWD=`pwd`
#srun -n $NODES_SUM hostname |sort > hostfile
srun -pernode hostname |sort > hostfile

#cp hostfile meta_hostfile
#cp hostfile data_hostfile
#cp hostfile client_hostfile

head -n $NUM_METADATA hostfile > meta_hostfile
tail -n +$((NUM_METADATA+1)) hostfile | head -n $NUM_DATA > data_hostfile
#tail -n +$((NUM_METADATA+NUM_DATA+1)) hostfile | head -n $NODES_FOR_CLIENTS > client_hostfile

echo "# IMMS: Running metadata servers"
rm metadata &> /dev/null
touch metadata

readarray -t hosts < meta_hostfile
for ((i=0;i<$NUM_METADATA;i++));
do
	srun --exclusive --export=ALL -N 1 -n 1 -w ${hosts[$i]} $IMSS_PATH/server m --server-id=$i --stat-logfile=./metadata --port=$META_PORT --bufsize=0 > meta_server.log &
done
sleep 5
#wait_for_server Metadata $NUM_METADATA

echo "# IMMS: Running data servers"
META_NODE=$(head -n 1 meta_hostfile)

readarray -t hosts < data_hostfile

for ((i=0;i<$NUM_DATA;i++));
do
        # PORT=`expr $DATA_PORT + $i`
        srun --exclusive --export=ALL -N 1 -n 1 -w ${hosts[$i]} $IMSS_PATH/server d --server-id=$i --imss-uri=imss:// --port=$DATA_PORT --bufsize=0 --stat-host=$META_NODE --stat-port=$META_PORT --num-servers=$NUM_DATA --deploy-hostfile=./data_hostfile --block-size=$BLOCK_SIZE --storage-size=$STORAGE_SIZE >> data_server.log &
done
sleep 10
#wait_for_server Data $NUM_DATA
exit 0
