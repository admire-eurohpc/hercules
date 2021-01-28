#!/bin/bash


#TODO: consider providing a set of default arguments.


####################################################################
######	SCRIPT PUBLISHING RELEASE MESSAGES TO IMSS INSTANCES  ######
####################################################################


if [ "$#" -eq 0 ]
   then
	echo "Please, execute \"./release_imss.sh -h\" for usage description."

	exit
   fi

function usage
{
	echo "

The release_imss.sh script will release one or more IMSS
instances. Also, the script is able to release the meta-
data server.

The following options are available:

	-d	MPI hostfile specifying the set of nodes
		where an IMSS instance was deployed.
	-A	Address (IP or DNS) of the machine where
		the metadata server is taking execution.
	-p	Port assigned to each server conforming
		the IMSS deployment or to the metadata
		one.
	-x	Release binary executable location.

	In order to release an IMSS instance, the infor-
	mation representing the previous must be provi-
	ded using a '-d [..] -p [..]' couple within the
	arguments, for instance:

	1. Multiple IMSS instances release:

 ./release_imss.sh -d [..] -p [..] -d [..] -p [..] -x [..]

	2. Release of an IMSS instance and the metadata
	server:

 ./release_imss.sh -d [..] -p [..] -A [..] -p [..] -x [..]

	Notice that the metadata server is treated like
	another IMSS instance within the arguments (the
	'-d' argument just turns into the '-A' one).

	
"

	exit
}


#Path to the hostfile containing each machine where an IMSS server was deployed.
release_file="-1"
#Port assigned to each dispatcher thread within the IMSS instance.
release_port="-1"
#Path to the executable performing the release operation.
binary="-1"

counter=0

metadata_srv_flag=0

#Set of addresses that must be released.
declare -a addresses

#GETOPTS loop parsing the set of arguments provided.
while getopts "d:A:p:x:h" opt
   do
	case ${opt} in

	   d )
		((counter=counter+1))

		if [ $counter != 1 ]
		   then
			echo "release_imss.sh ERROR: \"-d [..] -p [..]\" argument pattern not followed."

			exit
		   fi
		
		release_file=$OPTARG
		;;
	   A )
		((counter=counter+1))

		if [ $counter != 1 ]
		   then
			echo "release_imss.sh ERROR: \"-d [..] -p [..]\" argument pattern not followed."

			exit
		   fi

		release_file=$OPTARG
		metadata_srv_flag=1
		;;
	   p )
		((counter=counter-1))

		if [ $counter != 0 ]
		   then
			echo "release_imss.sh ERROR: \"-d [..] -p [..]\" argument pattern not followed."

			exit
		   fi

		release_port=$OPTARG
		((release_port=release_port+1))

		if [ $metadata_srv_flag == 1 ]
		   then
			address=$(printf "tcp://%s:%d " "$release_file" "$release_port")

			addresses+=$address

			metadata_srv_flag=0
		   else
			addresses+=$(awk -v release_port="$release_port" '{printf("tcp://%s:%d ", $0, release_port)}' $release_file)
		fi

		;;
	   x )
		binary=$OPTARG
		;;

	   #Print the usage options of the script.
	   h )
		usage
		;;

	   #Capture non-considered options.
	   \? )
		echo "release_imss.sh ERROR: invalid option -$OPTARG"

		exit
		;;
	esac
   done


if [ "$release_address" == "-1" ]
   then
	echo "release_imss.sh ERROR: -d or -A arguments not provided."

	exit
   fi

if [ $counter == 1 ]
   then
	echo "release_imss.sh ERROR: -p argument not provided."

	exit
   fi

if [ "$binary" == "-1" ]
   then
	echo "release_imss.sh ERROR: -x argument not provided."

	exit
   fi



####################################################################
############################ EXECUTIONS ############################
####################################################################

./$binary $addresses


