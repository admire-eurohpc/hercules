#!/usr/bin/env bash
#SBATCH --job-name=remotesensing_h
#SBATCH --partition=cascadelake
#SBATCH --output=%j_%x.log
#SBATCH --error=%j_%x.err
#SBATCH --oversubscribe
#SBATCH --overcommit

# Hercules configuration path.
CONFIG_PATH=$1

# To load mpich.
spack load mpich@3.2.1%gcc@=9.4.0 arch=linux-ubuntu20.04-zen
# To load Hercules.
spack load hercules
# To get the path where Hercules has been installed.
HERCULES_PATH=$(spack find --paths --loaded hercules | grep hercules | awk '{print $2}')
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

# Copy the "Data" directory to Hercules.
srun --export=ALL --nodes=1 --ntasks=1 cp -r ./Data /mnt/hercules/
# Creates the "checkpoints_200ep" directory inside Hercules.
srun --export=ALL --nodes=1 --ntasks=1 mkdir /mnt/hercules/checkpoints_200ep

# Activate virtual enviroment.
source /beegfs/home/j.arnold/admire_toymodel/bin/activate
#run Python program
#NODELIST=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
#printf '%s\n' "${NODELIST[@]}"
#--verbose --log-level TRACE
echo "Current directory=$PWD"
srun --export=ALL -N 1 horovodrun --verbose --log-level FATAL --gloo-timeout-seconds 120 -np 1 --min-np 1 --max-np 1 --network-interface "ibs1f1" --host-discovery-script /beegfs/home/javier.garciablas/gsanchez/Horovod-with-gloo/discover_hosts-new.sh --slots 1 python ./train_elastic_hvd_keras_aug_effnet_hercules.py

echo "After running"
ls -lh /mnt/hercules/checkpoints_200ep

# To stop intercepting system calls by using Hercules.
unset LD_PRELOAD

# To stop Hercules data and metadata servers.
hercules stop -f "$CONFIG_PATH"
set +x
