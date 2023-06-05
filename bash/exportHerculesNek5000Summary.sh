# !/bin/bash

NUMBER_OF_FILES=$1
FILES_LIST=$(ls -tr logs/hercules/* | tail -n${NUMBER_OF_FILES}); 
HEADER=("total elapsed time"
	"total solver time w/o IO"
	"time/timestep"
	"avg throughput per timestep"
	"total max memory usage");


echo $FILES_LIST

for file in $FILES_LIST; 
do 
	#grep "write\|read" $file | tail -n2;
	echo "File=$file"
	for ((i = 0; i < ${#HEADER[@]}; i++))
	do
		echo "Word=${HEADER[$i]}"
		out=$(sed -e 's/ \{1,\}/ /g' $file | grep "${HEADER[$i]}" | tail -n1 | cut -d ':' -f2)
		echo "Output=$out"
	done

#	echo $out5 $out6 $out4 $out3 $out1
#	echo $out5 $out6 $out4 $out3 $out2

done
