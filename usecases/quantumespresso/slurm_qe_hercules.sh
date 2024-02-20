#!/bin/bash
#SBATCH --job-name=h_qe    # Job name
#SBATCH --output=%j_%x.log   # Standard output and error log
#SBATCH --overcommit
#SBATCH --oversubscribe
#SBATCH --cpus-per-task=1
#SBATCH --hint=nomultithread

# Hercules configuration path.
CONFIG_PATH=$1

# To load mpich.
spack load mpich@3.2.1%gcc@=9.4.0 arch=linux-ubuntu20.04-zen
# To load Hercules.
spack load hercules
# To get the path where Hercules has been installed.
HERCULES_PATH=$(spack find --paths --loaded hercules | grep hercules | awk '{print $2}')
export LD_LIBRARY_PATH=${HERCULES_PATH}/lib:$LD_LIBRARY_PATH

echo "Starting Hercules"
start_=$(date +%s.%N)
echo "Configuration file pass $CONFIG_PATH"

# To start Hercules back-end (data and metadata servers).
hercules start -f "$CONFIG_PATH"

end_=$(date +%s.%N)
runtime=$(echo "$end_ - $start_" | bc -l)
echo "Hercules started in $runtime seconds, start=$start_, end=$end_"

export OMP_NUM_THREADS=1

NP=512
#! NPW*NI*NK==NP
NPW=16
NK=32
NI=1

set -x
# Needed enviroments variables to run Hercules front-end.
# To intercept all system calls by using Hercules.
export LD_PRELOAD=libhercules_posix.so
# Hercules' configuration file.
export HERCULES_CONF=$HERCULES_CONF

# Copy the "Siout" directory to Hercules.
srun --nodes=1 --ntasks=1 cp -r ./Siout/* /mnt/hercules/

start_date=$(date '+%d-%m-%Y_%H-%M-%S')
echo "Start date="$start_date
srun  --export=ALL --nodes=$NPW -n $NP --cpu-bind=core  ph.x -i ph_hercules.in -ni $NI -nk $NK > ph.N$SLURM_NNODES.np$SLURM_NPROCS.omp$OMP_NUM_THREADS-nk${NK}_Hercules_${start_date}.out

# To stop intercepting system calls by using Hercules.
unset LD_PRELOAD

# To stop Hercules data and metadata servers.
hercules stop -f "$CONFIG_PATH"

set +x
