# !/bin/bash

GLOBAL_FILE_SIZE=128
set -x
for NUMBER_OF_NODES in {1,2,4,8,16,32,64}
do
	if [[ $NUMBER_OF_NODES -eq 0 ]]
	then
		NUMBER_OF_NODES=1
	fi
	FILE_SIZE_PER_NODE=$((GLOBAL_FILE_SIZE/NUMBER_OF_NODES))
	mpiexec -np $NUMBER_OF_NODES --pernode --hostfile ./64hostfile /beegfs/home/javier.garciablas/opt/spack/linux-ubuntu20.04-zen/gcc-9.4.0/ior-3.3.0-ssyaxpxjajmhy3v5icfqoo63kaeii6wv/bin/ior -o /beegfs/home/javier.garciablas/imss/bash/data.out -t ${FILE_SIZE_PER_NODE}m -b ${FILE_SIZE_PER_NODE}m -s 1 -i 10 -F > beefs_scale_strong/$NUMBER_OF_NODES.pernode.txt
	#exit 0
done
