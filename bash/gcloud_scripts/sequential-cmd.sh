#!/bin/bash

hosts=$(cat Hercules/bash/collective_write/deployfile)

for host in $hosts
   do
	echo -n "ssh $host $@: "

	res=$(ssh $host $@)

	echo "$res"
   done

#tar -xzvf Hercules/build.tar.gz -C Hercules
