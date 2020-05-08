#!/bin/bash


####################################################################
######	SCRIPT PERFORMING THE DEPLOYMENT OF AN IMSS INSTANCE  ######
####################################################################


if [ "$#" -eq 0 ]
   then
	echo "Please, execute \"./init_imss.sh -h\" for usage description."

	exit
   fi


function usage
{
	echo "

The init_imss.sh script initializes an instance
of an In-Memory Storage System.

The following options are available:

	-u	IMSS URI string identifying the deployment. 
	-d	MPI hostfile specifying the set of nodes
		where an IMSS server will be deployed.
	-b	Buffer size (in GB) assigned to each ser-
		ver conforming the IMSS deployment.
	-p	Port assigned to each server conforming
		the IMSS deployment.

	The following arguments must match with the ones
	provided to the init_imss_stat.sh script.

	-A	Address (IP or DNS) of the machine where
		the metadata server is taking execution.
	-P	Port within the previous machine that the
		metadata server is listening to.

	-B	Buffer binary location.

The whole set of parameters must be provided in order to 
perform a successful deployment.

"
}


function check_argument
{
	first_letter=$(echo $1 | head -c 1)

	if [ $first_letter == "-" ]
	   then
		echo "init_imss.sh ERROR: option -$2 requires an argument"

		exit
	   fi
}


#Port assigned to the PUBLISHER socket performing the IMSS_RELEASE operation.
release_port=44075

#Variables conforming the IMSS deployment.
imss_uri="-1"
imss_hostfile="-1"
imss_buffer_size="-1"
imss_port_number="-1"
metadata_server_port="-1"
metadata_server_address="-1"
binary_location="-1"


#GETOPTS loop parsing the set of arguments provided. Just those options requiring an argument must be followed by a semicolon.
while getopts ":u:d:b:p:P:A:B:h" opt
   do
	case ${opt} in

	   u )
		imss_uri=$OPTARG
		check_argument "$imss_uri" "$opt"
		;;
	   d )
		imss_hostfile=$OPTARG
		check_argument "$imss_hostfile" "$opt"
		;;
	   b )
		imss_buffer_size=$OPTARG
		check_argument "$imss_buffer_size" "$opt"
		;;
	   p )
		imss_port_number=$OPTARG
		check_argument "$imss_port_number" "$opt"
		;;
	   P )
		metadata_server_port=$OPTARG
		check_argument "$metadata_server_port" "$opt"
		;;
	   A )
		metadata_server_address=$OPTARG
		check_argument "$metadata_server_address" "$opt"
		;;
	   B )
		binary_location=$OPTARG
		check_argument "$binary_location" "$opt"
		;;

	   #Print the usage options of the script.
	   h )
		usage

		exit
		;;

	   #Capture non-considered options.
	   \? )
		echo "init_imss.sh ERROR: invalid option -$OPTARG"

		exit
		;;

	   #Notify that no required arguments were provided.
	   : )
		echo "init_imss.sh ERROR: option -$OPTARG requires an argument"

		exit
		;;
	esac
   done



if [ "$#" -ne 14 ]
   then
	echo -n "init_imss.sh ERROR: expected arguments not provided ("

	if [ "$imss_uri" == "-1" ]
	   then
		echo -n " -u"
	   fi
	if [ "$imss_hostfile" == "-1" ]
	   then
		echo -n " -d"
	   fi
	if [ "$imss_buffer_size" == "-1" ]
	   then
		echo -n " -b"
	   fi
	if [ "$imss_port_number" == "-1" ]
	   then
		echo -n " -p"
	   fi
	if [ "$metadata_server_port" == "-1" ]
	   then
		echo -n " -P"
	   fi
	if [ "$metadata_server_address" == "-1" ]
	   then
		echo -n " -A"
	   fi
	if [ "$binary_location" == "-1" ]
	   then
		echo -n " -B"
	   fi

	echo " )"

	exit
   fi



#Required if non-getopts additional arguments were provided.
#shift $((OPTIND - 1))

echo "Arguments provided: LOCATION OF BINARY FILE $binary_location URI $imss_uri, HOSTFILE: $imss_hostfile, IMSS_BUFFER_SIZE: $imss_buffer_size, IMSS_PORT_NUMBER: $imss_port_number, METADATA_PORT: $metadata_server_port, METADATA_ADDRESS: $metadata_server_address"
