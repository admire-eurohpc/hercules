#!/bin/bash

port1=3000
port2=3050
script_name=slurm.sh
number_of_nodes=0

set -x

jid=1
##for p in {1,2,4,8,16,32,64,98}
for p in {1,2,4,8,16,32}
do
    for block_size in {1024..1024}
    do
        number_of_nodes=$(($p*2+1)) # num. clients*num. data servers+meta servers
        if [ $jid -eq 1 ]
        then
            jid=$(sbatch -N $number_of_nodes $script_name 1 $p $p $block_size $port1 $port2 | cut -d ' ' -f4)
        else
            jid=$(sbatch --dependency=afterany:${jid} -N $number_of_nodes $script_name 1 $p $p $block_size $port1 $port2 | cut -d ' ' -f4)
        fi
        #port1=$(($port1+$number_of_clients+10))
        #port2=$(($port2+$number_of_clients+10))
    done
done

