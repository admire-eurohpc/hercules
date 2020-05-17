#!/bin/bash


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
instances. Also, the script is able to release the mata-
data server.

The following options are available:

	-u	IMSS instance to be released. The next
		semantic must be followed in order to
		release multiple instances:

		./release_imss.sh -u <URI> -u <URI> ...

	-m	Flag specifying that the metadata server
		must be released (no arg required).

	At least, one of the previous parameters must be
	provided in addition to the following ones in or-
	der to perform a successful release operation.

	-a	Address of the machine where the release
		operation will take place.
	-p	Port that will be used to perform the 
		previous release operation.
	-X	Release binary executable location.
	
"

	exit
}

function check_argument
{
	first_letter=$(echo $1 | head -c 1)

	if [ $first_letter == "-" ]
	   then
		echo "release_imss.sh ERROR: option -$2 requires an argument"

		exit
	   fi
}


#Flag specifying if the metadata server must be released.
release_metadata_server="N"
#Number of URIs provided to the "-u" option.
num_uris=0
#Address of the machine where the release operation will take place.
release_address="-1"
#Port number within the previous machine where the publish operation will be performed.
release_port="-1"
#Path to the executable performing the release operation.
binary_location="-1"

#Set of URIs that must be released.
declare -a uris

#GETOPTS loop parsing the set of arguments provided.
while getopts "mu:ha:p:X:" opt
   do
	case ${opt} in

	   u )
		check_argument "$OPTARG" "$opt"
		#Add the current uri to those that must be released.
		uris+="$OPTARG "
		((num_uris=num_uris+1))
		;;

	   m )
		if [ "$release_metadata_server" == "N" ]
		   then
			uris+="stat "
			((num_uris=num_uris+1))
			release_metadata_server="Y"
		fi

		;;
	   a )
		release_address=$OPTARG
		check_argument "$release_address" "$opt"
		;;
	   p )
		release_port=$OPTARG
		check_argument "$release_port" "$opt"
		;;
	   X )
		binary_location=$OPTARG
		check_argument "$binary_location" "$opt"
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

arguments_remaining="N"

if [ "$release_address" == "-1" ]
   then
	if [ "$arguments_remaining" == "N" ]
	   then
		echo -n "release_imss.sh ERROR: expected arguments not provided ("
	   fi

	echo -n " -a"
	arguments_remaining="Y"
   fi
if [ "$release_port" == "-1" ]
   then
	if [ "$arguments_remaining" == "N" ]
	   then
		echo -n "release_imss.sh ERROR: expected arguments not provided ("
	   fi

	echo -n " -p"
	arguments_remaining="Y"
   fi
if [ $num_uris -eq 0 ]
   then
	if [ "$arguments_remaining" == "N" ]
	   then
		echo -n "release_imss.sh ERROR: expected arguments not provided ("
	   fi

	echo -n " -u -m"
	arguments_remaining="Y"
   fi
if [ "$binary_location" == "-1" ]
   then
	if [ "$arguments_remaining" == "N" ]
	   then
		echo -n "release_imss.sh ERROR: expected arguments not provided ("
	   fi

	echo -n " -X"
	arguments_remaining="Y"
   fi


if [ "$arguments_remaining" == "Y" ]
   then
	echo " )"

	exit
   fi


echo "SET OF URIs PROVIDED: $uris RELEASE ADDRESS: $release_address RELEASE PORT: $release_port BINARY: $binary_location"

#./$binary_location $release_port $uris
