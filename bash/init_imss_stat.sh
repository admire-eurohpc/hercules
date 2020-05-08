#!/bin/bash


############################################################################
######	SCRIPT PERFORMING THE DEPLOYMENT OF THE IMSS METADATA SERVER  ######
############################################################################


if [ "$#" -eq 0 ]
   then
	echo "Please, execute \"./init_imss_stat.sh -h\" for usage description."

	exit
   fi


function usage
{
	echo "

The init_imss_stat.sh script initializes the metadata
server used by every IMSS deployed instance.

The following options are available:

	-d	MPI hostfile specifying the node where
		the IMSS metadata server will be deployed.
	-b	Buffer size (in GB) assigned to the meta-
		data server.
	-p	Port assigned to the metadata server.

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
		echo "init_imss_stat.sh ERROR: option -$2 requires an argument"

		exit
	   fi
}


#Port assigned to the PUBLISHER socket performing the IMSS_RELEASE operation.
release_port=44075

#Variables conforming the metadata server deployment.
metadata_hostfile="-1"
metadata_buffer_size="-1"
metadata_port_number="-1"
binary_location="-1"


#GETOPTS loop parsing the set of arguments provided. Just those options requiring an argument must be followed by a semicolon.
while getopts ":d:b:p:B:h" opt
   do
	case ${opt} in

	   d )
		metadata_hostfile=$OPTARG
		check_argument "$metadata_hostfile" "$opt"
		;;
	   b )
		metadata_buffer_size=$OPTARG
		check_argument "$metadata_buffer_size" "$opt"
		;;
	   p )
		metadata_port_number=$OPTARG
		check_argument "$metadata_port_number" "$opt"
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
		echo "init_imss_stat.sh ERROR: invalid option -$OPTARG"

		exit
		;;

	   #Notify that no required arguments were provided.
	   : )
		echo "init_imss_stat.sh ERROR: option -$OPTARG requires an argument"

		exit
		;;
	esac
   done


if [ "$#" -ne 8 ]
   then
	echo -n "init_imss_stat.sh ERROR: expected arguments not provided ("

	if [ "$metadata_hostfile" == "-1" ]
	   then
		echo -n " -d"
	   fi
	if [ "$metadata_buffer_size" == "-1" ]
	   then
		echo -n " -b"
	   fi
	if [ "$metadata_port_number" == "-1" ]
	   then
		echo -n " -p"
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

echo "Arguments provided: LOCATION OF BINARY FILE $binary_location HOSTFILE $metadata_hostfile BUFFER SIZE $metadata_buffer_size PORT NUMBER $metadata_port_number"
