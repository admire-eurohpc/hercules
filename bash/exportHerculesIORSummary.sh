# !/bin/bash

NUMBER_OF_FILES=$1
FILES_LIST=$(ls -tr logs/hercules/* | tail -n${NUMBER_OF_FILES}); 

echo $FILES_LIST

for file in $FILES_LIST; 
do 
	#grep "write\|read" $file | tail -n2;
	out1=$(sed -e 's/ \{1,\}/ /g' $file | grep "write" | tail -n1;)
	out2=$(sed -e 's/ \{1,\}/ /g' $file | grep "read" | tail -n1;)
	out3=$(sed -e 's/ \{1,\}/ /g' $file | grep "DATA_SERVERS" | tail -n1 | awk '{print $2}';)
#	out4=$(sed -e 's/ \{1,\}/ /g' $file | grep "MALLEABILITY" | head -n1 | awk '{print $2}';)
#	out5=$(sed -e 's/ \{1,\}/ /g' $file | grep "LOWER_BOUND" | head -n1 | awk '{print $2}';)
#	out6=$(sed -e 's/ \{1,\}/ /g' $file | grep "UPPER_BOUND" | head -n1 | awk '{print $2}';)
	echo $out3 $out1
	echo $out3 $out2

#	echo $out5 $out6 $out4 $out3 $out1
#	echo $out5 $out6 $out4 $out3 $out2

done
