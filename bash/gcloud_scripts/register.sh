#!/bin/bash

hosts=$(cat $1)

for host in $hosts
   do
	ssh -o "StrictHostKeyChecking=no" $host &

	sleep 2

	process_id=$(ps -e | grep -w ssh | awk '{print $1}')

	kill $process_id
   done
