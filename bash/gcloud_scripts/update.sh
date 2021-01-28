#!/bin/bash

hosts=$(cat ~/Hercules/bash/collective_write/deployfile)

tar -czvf build.tar.gz Hercules/build

./sequential-cmd.sh "rm -rf ~/Hercules/build"

for host in $hosts
   do
	   scp ~/build.tar.gz mariandr@$host:~/
   done

./sequential-cmd.sh "tar -xzvf ~/build.tar.gz"

./sequential-cmd.sh "rm -rf ~/build.tar.gz"
