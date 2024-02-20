#!/bin/bash
#SBATCH --job-name=lifesciences_h
#SBATCH -o %j_%x.log
#SBATCH -e %j_%x.err
#SBATCH --oversubscribe
#SBATCH --overcommit

# Number of processes.
NUMBER_OF_PROCESS=$1

# Hercules configuration path.
CONFIG_PATH=$2

# To load hercules.
spack load hercules
# To get the path where Hercules has been installed.
HERCULES_INSTALLATION_PATH=$(spack find --paths --loaded hercules | grep hercules | awk '{print $2}')
export LD_LIBRARY_PATH=${HERCULES_PATH}/lib:$LD_LIBRARY_PATH

# Setting "ibs1" to UCX fix a bug because --network-interface "ibs1" is used in horovodrun.
export UCX_NET_DEVICES=ibs1

echo "Starting Hercules"
start_=$(date +%s.%N)
echo "Hercules configuration file: $CONFIG_PATH"

# To start Hercules back-end (data and metadata servers).
hercules start -f "$CONFIG_PATH"

end_=$(date +%s.%N)
runtime=$(echo "$end_ - $start_" | bc -l)
echo "Hercules started in $runtime seconds, start=$start_, end=$end_"

set -x
# Needed enviroments variables to run Hercules front-end.
# To intercept all system calls by using Hercules.
export LD_PRELOAD=libhercules_posix.so
# Hercules' configuration file.
export HERCULES_CONF=$CONFIG_PATH

# Define the path of the Life Sciences projec.
project_path="/beegfs/home/javier.garciablas/gsanchez/Life-Sciences"

# Creates the "checkpoints" directory inside Hercules.
srun --export=ALL --nodes=1 --ntasks=1 mkdir /mnt/hercules/checkpoints
# Copy the "data" directory to Hercules.
srun --export=ALL --nodes=1 --ntasks=1 cp -r  /beegfs/home/javier.garciablas/gsanchez/Life-Sciences/admire-app/data /mnt/hercules/data

# Activate virtual enviroment.
source $project_path/elastic_horovod_venv/bin/activate
pwd
# Move to the "scripts" directory.
cd $project_path/admire-app/scripts/
#horovodrun --check-build
echo "Libraries loaded, starting horovodrun"
# client_hosts=$(cat $project_path/client_hostfile)
#ifconfig -a
horovodrun --verbose --log-level DEBUG --gloo-timeout-seconds 240 -np $NUMBER_OF_PROCESS --min-np $NUMBER_OF_PROCESS --max-np $NUMBER_OF_PROCESS --network-interface "ibs1" --host-discovery-script $project_path/admire-app/scripts/discover_hosts.sh --slots 1 python3 $project_path/admire-app/scripts/LS5_pipeline_elastic_horovod_hercules.py --epochs 5

echo "After running"
ls -l /mnt/hercules/checkpoints

# To stop Hercules data and metadata servers.
hercules stop -f "$CONFIG_PATH"
# To stop intercepting system calls by using Hercules.
unset LD_PRELOAD
set +x




