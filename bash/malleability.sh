#! /bin/bash

# 2 gigas y 10 gigas.

DIRECTORY_NAME="pdfs-200u"

# start servers.
/usr/bin/time -f "[HS] Start servers %E sec." /home/genarog/Documents/UC3M/Codes/hercules/scripts/hercules start -f ../conf/hercules_genaro.conf -m ./meta_hostfile -d ./data_hostfile

# set -x
# run commands (app).
export HERCULES_CONF=/home/genarog/Documents/UC3M/Codes/hercules/conf/hercules_genaro.conf
export LD_PRELOAD=/home/genarog/Documents/UC3M/Codes/hercules/build/tools/libhercules_posix.so
/usr/bin/time -f "[HS] Hercules upload %E sec." cp -r /home/genarog/Volumen/Source/$DIRECTORY_NAME /mnt/hercules/
ls -l /mnt/hercules/$DIRECTORY_NAME
/usr/bin/time -f "[HS] Hercules save %E sec." cp -r /mnt/hercules/$DIRECTORY_NAME /home/genarog/Documents/UC3M/Codes/hercules/bash/sink/
unset LD_PRELOAD
unset HERCULES_CONF

# stop servers.
/usr/bin/time -f "[HS] Hercules stop %E sec." /home/genarog/Documents/UC3M/Codes/hercules/scripts/hercules stop -f ../conf/hercules_genaro.conf -m ./meta_hostfile -d ./data_hostfile

sleep 30

# start servers with the new configuration.
/usr/bin/time -f "[HS] Restart servers %E sec." /home/genarog/Documents/UC3M/Codes/hercules/scripts/hercules start -f ../conf/hercules_genaro.conf -m ./meta_hostfile -d ./data_hostfile
# copy prev. backup (sink) to the server.
export HERCULES_CONF=/home/genarog/Documents/UC3M/Codes/hercules/conf/hercules_genaro.conf
export LD_PRELOAD=/home/genarog/Documents/UC3M/Codes/hercules/build/tools/libhercules_posix.so
/usr/bin/time -f "[HS] Hercules restore %E sec." cp -r /home/genarog/Documents/UC3M/Codes/hercules/bash/sink/$DIRECTORY_NAME /mnt/hercules/$DIRECTORY_NAME
ls -l /mnt/hercules/$DIRECTORY_NAME
unset LD_PRELOAD
unset HERCULES_CONF
# set +x

# stops servers.
/usr/bin/time -f "[HS] Hercules stop %E sec." /home/genarog/Documents/UC3M/Codes/hercules/scripts/hercules stop -f ../conf/hercules_genaro.conf -m ./meta_hostfile -d ./data_hostfile

