#!/bin/bash

SERVER_TYPE=$1
SERVER_NUMBER=$2
ATTEMPS=100
i=1

FILE="/tmp/$SERVER_TYPE-hercules-$SERVER_NUMBER"
until [ -f $FILE ]; do
    echo "Waiting for $FILE, attemp $i"
    i=$(($i+1))
    if [ $i -gt $ATTEMPS ]; then
        exit 1;
    fi
    sleep 1
done
