#! /bin/bash

./malleability.sh &> malleability_test.txt

current_datetime=$(date +"%Y-%m-%d_%H-%M-%S")
cat malleability_test.txt | grep -Fw "[HS]" > malleability_test_times_${current_datetime}.txt

