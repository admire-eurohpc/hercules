#!/bin/bash

SERVER_TYPE=$1
SERVER_NUMBER=$2
ACTION=$3 # expected string action, e.g., down when servers are stopped.
ATTEMPS=10
i=1

FILE="/tmp/$SERVER_TYPE-hercules-$SERVER_NUMBER-$ACTION"
## Checks if the file exists.
until [ -f $FILE ]; do
    # echo "Waiting for $FILE, attemp $i"
    i=$(($i + 1))
    ## Waits "attemps" times, then an error is return.
    if [ $i -gt $ATTEMPS ]; then
        exit 1
    fi
    t=$(($i % 5))
    if [ $t -eq 0 ]; then
        echo "[+][$HOSTNAME] Waiting for server $SERVER_NUMBER"
    fi
    sleep 1
done

## Checks if the server was deploy correctly.
STATUS=$(cat $FILE | grep "STATUS" | awk '{print $3}')
if [ "$STATUS" != "OK" ]; then
    # echo "[X] Error deploying server $SERVER_NUMBER."
    exit 1
fi
