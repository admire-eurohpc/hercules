#!/bin/bash
#SBATCH --job-name=hercules    # Job name
#SBATCH --time=00:30:00               # Time limit hrs:min:sec
#SBATCH --output=logs/hercules/%j_hercules.log   # Standard output and error log
##SBATCH --mem=0
#SBATCH --overcommit
#SBATCH --oversubscribe
##SBATCH --nodelist=broadwell-[038-043]
##SBATCH --nodelist=broadwell-[000-004]
##SBATCH --exclude=broadwell-[000-030]
###SBATCH --exclusive=user


CONFIG_PATH=$1
FILE_SIZE_PER_CLIENT=$2


## Uncomment when working in Tucan.
# IOR_PATH=/home/software/io500/bin
# module unload mpi
# module load mpi/mpich3/3.2.1

## Uncomment when working in Unito.
IOR_PATH=/beegfs/home/javier.garciablas/io500/bin
# spack load \
#    cmake@3.24.3%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#    glib@2.74.1%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#    ucx@1.14.0%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#    pcre@8.45%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#    openmpi@4.1.5%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#    jemalloc 

## Uncomment when working in MN4.
# IOR_PATH=/apps/IOR/3.3.0/INTEL/IMPI/bin
# module unload impi
# module load gcc/9.2.0
# module load java/8u131
# module load openmpi/4.1.0
# module load ucx/1.13.1
# module load cmake/3.15.4
# module unload openmpi
# module load impi
# module load ior

## Local
# IOR_PATH=/usr/local/bin

echo "Starting Hercules"
start_=`date +%s.%N`
if [ -z "$CONFIG_PATH" ]; then
   echo "No configuration file"
   source hercules start
else
   echo "Configuration file pass $CONFIG_PATH"
   source hercules start -f "$CONFIG_PATH"
fi
end_=`date +%s.%N`
runtime=$( echo "$end_ - $start_" | bc -l )
echo "Hercules started in $runtime seconds, start=$start_, end=$end_"

echo "Running clients"
TRANSFER_SIZE=$((1024*16))
# COMMAND="$IOR_PATH/ior -t ${TRANSFER_SIZE}kb -b ${FILE_SIZE_PER_CLIENT}kb -s 1 -i 5 -F -o /mnt/imss/data.out"
#COMMAND="../../bin/nekbmpi eddy_uv 2"
#COMMAND="/beegfs/home/javier.garciablas/nek5000/run/eddy_uv/nek5000"
# COMMAND="./exe_WRITE_AND_READ-TEST /mnt/imss/eddy hola.txt 1024"

set -x
#echo "mpiexec $H_MPI_HOSTFILE_DEF ./client_hostfile $H_MPI_PPN $H_NCPN \
#	$H_MPI_ENV_DEF $H_POSIX_PRELOAD \
#   $H_MPI_ENV_DEF IMSS_CONF=$CONFIG_PATH \
#	$COMMAND"

# : ' # this is a multi-line comment
# mpiexec $H_MPI_HOSTFILE_DEF ./client_hostfile $H_MPI_PPN $H_NCPN \
# 	$H_MPI_ENV_DEF $H_POSIX_PRELOAD \
#    	$H_MPI_ENV_DEF IMSS_CONF=$CONFIG_PATH \
# 	$COMMAND > output
# '


#$H_POSIX_PRELOAD
#LD_PRELOAD=/beegfs/home/javier.garciablas/imss/build/tools/libhercules_posix.so
#export LD_PRELOAD


# LD_PRELOAD=/beegfs/home/javier.garciablas/imss/build/tools/libhercules_posix.so touch /mnt/imss/example.wps

echo "Running the client"
export LD_PRELOAD=/beegfs/home/javier.garciablas/imss/build/tools/libhercules_posix.so
######################## Write, get size and read a File with Python #############################################
python writeFile.py /mnt/imss/example.txt
python getSizeFile.py /mnt/imss/example.txt
python readFile.py /mnt/imss/example.txt
# ltrace -S -o readFile.ltrace 
##################################################################################################################



######################## Create a directory and copy files with Bash #############################################
# mkdir /mnt/imss/envapp
# # strace -o copy.strace cp /beegfs/home/javier.garciablas/imss/README.md /beegfs/home/javier.garciablas/imss/README2.md
# # strace -o copy.strace 
# cp /beegfs/home/javier.garciablas/imss/README.md /mnt/imss/envapp/HERCULES_README.md
# # ./exe_fstatExample /mnt/imss/envapp/HERCULES_README.md
# cp /mnt/imss/envapp/HERCULES_README.md /mnt/imss/envapp/2HERCULES_README.md
# cat /mnt/imss/envapp/2HERCULES_README.md > output.cat
##################################################################################################################


# touch /mnt/imss/example.wps

# ln -sf $WRF_ROOT/$WRF/WPS/geogrid.exe
# ln -sf ./hostfile /mnt/imss/example.exe


# LD_PRELOAD=/beegfs/home/javier.garciablas/imss/build/tools/libhercules_posix.so strace -o out.txt cat /mnt/imss/nameList.wps
# cat > /mnt/imss/namelist.wps << EOF
# &share
#  wrf_core = 'ARW',
#  start_date = '$INITIAL','$INITIAL','$INITIAL','$INITIAL','$INITIAL','$INITIAL',
#  end_date   = '$FINAL','$FINAL','$FINAL','$FINAL','$FINAL','$FINAL',
#  interval_seconds = 10800
#  max_dom = 3,
#  io_form_geogrid = 2,
# /

# &geogrid
#  parent_id         =    1,      1,      2,      3,      4,     5,
#  parent_grid_ratio =    1,      5,      5,      5,      3,     3,     
#  i_parent_start    = 1,120,173,
#  j_parent_start    = 1,33,112,
#  e_we              = 280,361,301,
#  e_sn              = 209,336,306,
#  geog_data_res     = '30s','30s','30s','30s','30s',
#  dx = 25000,
#  dy = 25000,
#  map_proj = 'lambert',
#  ref_lat   =  50.36,
#  ref_lon   =   8.959,
#  truelat1  =  50.36,
#  truelat2  =  50.36,
#  stand_lon =   8.959,
#  geog_data_path = './geog'
#  OPT_GEOGRID_TBL_PATH = './geogrid'
# /

# &ungrib
#  out_format = 'WPS',
#  prefix = 'FILE',
# /


# &metgrid
#  fg_name = 'FILE'
#  io_form_metgrid = 2,
# /

# &mod_levs
#  press_pa = 201300 , 200100 , 100000 ,
#              95000 ,  90000 ,
#              85000 ,  80000 ,
#              75000 ,  70000 ,
#              65000 ,  60000 ,
#              55000 ,  50000 ,
#              45000 ,  40000 ,
#              35000 ,  30000 ,
#              25000 ,  20000 ,
#              15000 ,  10000 ,
#               5000 ,   1000
# /
# EOF


# COMMAND="python writeFile.py"
# mpiexec $H_MPI_HOSTFILE_DEF ./client_hostfile $H_MPI_PPN $H_NCPN \
# 	$H_MPI_ENV_DEF $H_POSIX_PRELOAD \
# 	$COMMAND

# COMMAND="python readFile.py"
# mpiexec $H_MPI_HOSTFILE_DEF ./client_hostfile $H_MPI_PPN $H_NCPN \
# 	$H_MPI_ENV_DEF $H_POSIX_PRELOAD \
# 	$COMMAND


# sleep 1000
unset LD_PRELOAD

set +x
./hercules stop
