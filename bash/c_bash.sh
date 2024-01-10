#!/bin/bash

## Uncomment when working in Tucan.
# IOR_PATH=/home/software/io500/bin
# module unload mpi
# module load mpi/mpich3/3.2.1

## Uncomment when working in Unito.
# IOR_PATH=/beegfs/home/javier.garciablas/io500/bin
#  spack load \
#     cmake@3.24.3%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#     glib@2.74.1%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#     ucx@1.14.0%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#     pcre@8.45%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#     load openmpi@4.1.5%gcc@9.4.0 arch=linux-ubuntu20.04-broadwell \
#     jemalloc

## Local
IOR_PATH=/usr/local/bin

# source ./hercules start -m meta_hostfile -d data_hostfile -o /mnt/hercules/data.out -s 0
source ./hercules start \
   -m /home/genarog/Documents/UC3M/Codes/UPDATED_IMSS/hercules/bash/my_meta_hostfile \
   -d /home/genarog/Documents/UC3M/Codes/UPDATED_IMSS/hercules/bash/my_data_hostfile \
   -c /home/genarog/Documents/UC3M/Codes/UPDATED_IMSS/hercules/bash/my_client_hostfile \
   -f /home/genarog/Documents/UC3M/Codes/UPDATED_IMSS/hercules/conf/genaro-hercules.conf

echo "[+] Running clients"

set -x

export LD_PRELOAD=$HERCULES_POSIX_PRELOAD
# mkdir /mnt/hercules/directory/
# touch /mnt/hercules/file.txt
# ls -l /mnt/hercules/directory/../file.txt
# ls -l /mnt/hercules/file.txt
# ls -l /mnt/hercules/./file.txt
# ls /mnt/../mnt/hercules/file.txt
# ls /mnt/../mnt/hercules/./../hercules/directory../file.txt
# ls /mnt/../mnt/hercules
# ls ../mnt/hercules/

# mkdir /mnt/hercules/output/
# ./tests/exe_OPENAT_TEST /mnt/hercules/output/ ../xfile.txt
# cat /mnt/hercules/output/xfile.txt
cp /home/genarog/Documents/UC3M/Codes/UPDATED_IMSS/hercules/bash/tests/data/wfc1.dat /mnt/hercules/
ls -l /mnt/hercules/
./tests/exe_Fortran_OPEN-READ_Hercules
ls -l /mnt/hercules/


unset LD_PRELOAD

./hercules stop

exit 0

COMMAND="$IOR_PATH/ior -t 100M -b 100M -s 1 -i 10 -o /mnt/hercules/data.out"
# COMMAND="$IOR_PATH/ior -t 1M -b 10M -s 1 -i 5 -F -o /home/genarog/Documents/UC3M/Codes/UPDATED_IMSS/hercules/bash/data.out"
# COMMAND="/home/genarog/Documents/UC3M/Codes/Apps/Nek5000/run/eddy_uv/nek5000 eddy_uv"
# COMMAND="./exe_WRITE-AND-READ-TEST-BIFURCADO /mnt/hercules/eddy hola.txt 1024"
# COMMAND="./exe_READ_EXISTING_FILE /home/genarog/Documents/UC3M/Codes/UPDATED_IMSS/hercules/bash/test_file.txt 11"
# COMMAND="./exe_READ_EXISTING_FILE /mnt/hercules/test_file.txt 11"

mpiexec -npernode $HERCULES_NCPN $HERCULES_MPI_HOSTFILE_DEF $HERCULES_MPI_HOSTFILE_NAME  \
   $HERCULES_MPI_ENV_DEF HERCULES_CONF=$HERCULES_CONF \
   $HERCULES_MPI_ENV_DEF LD_PRELOAD=$HERCULES_POSIX_PRELOAD \
	$COMMAND > logfile

	# strace -s 2000 -o strace.log $COMMAND
	#ltrace -s 2000 -o ltrace.log $COMMAND



# sleep 20
# export $H_POSIX_PRELOAD 
# export LD_PRELOAD=/home/genarog/Documents/UC3M/Codes/UPDATED_IMSS/hercules/build/tools/libhercules_posix.so
# sleep 20
# $COMMAND

# LD_PRELOAD=/home/genarog/Documents/UC3M/Codes/UPDATED_IMSS/hercules/build/tools/libhercules_posix.so ./exe_READ_EXISTING_FILE /mnt/hercules/test_file.txt 11


# export LD_PRELOAD="$HERCULES_POSIX_PRELOAD"


# mpiexec $HERCULES_MPI_HOSTFILE_DEF $HERCULES_MPI_HOSTFILE_NAME -np $H_NNFC \
   # -wdir  /home/genarog/Documents/UC3M/Codes/Apps/Nek5000/run/eddy_uv \

# touch /mnt/hercules/namelist.wps
# cat namelist.wps > /mnt/hercules/namelist.wps

# ltrace -s 2000 -o ltrace_cat.out cat > /mnt/hercules/namelist.wps << EOF
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

# ls -lh /mnt/hercules/

unset LD_PRELOAD

set +x

./hercules stop
