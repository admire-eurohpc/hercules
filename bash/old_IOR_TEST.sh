#!/bin/bash

set -x

jid=1
for number_of_clients in {32..32}
do
    for block_size in {64,128,512}
    do
        if [ $jid -eq 1 ]
        then
            echo "then"
            jid=$(sbatch runIORSlurm.sh $number_of_clients $block_size | cut -d ' ' -f4)
        else
            echo "else"
            jid=$(sbatch --dependency=afterok:${jid} runIORSlurm.sh $number_of_clients $block_size | cut -d ' ' -f4)
        fi
    done
done
