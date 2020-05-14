#!/bin/bash


####################################################################
######	SCRIPT PERFORMING THE DEPLOYMENT OF AN IMSS INSTANCE  ######
####################################################################




#TODO: add release connection info.




if [ "$#" -eq 0 ]
   then
	echo "Please, execute \"./init_imss.sh -h\" for usage description."

	exit
   fi


function usage
{
	echo "

The init_imss.sh script initializes an instance of an In-Memory 
Storage System. The script is able to deploy the metadata server 
as well.

The following options are available:

	-u	IMSS URI string identifying the deployment. 
	-d	MPI hostfile specifying the set of nodes
		where an IMSS server will be deployed.
	-b	Buffer size (in GB) assigned to each ser-
		ver conforming the IMSS deployment.
	-p	Port assigned to each server conforming
		the IMSS deployment.
	-A	Address (IP or DNS) of the machine where
		the metadata server is taking execution.
	-P	Port within the previous machine that the
		metadata server is listening to.
	-X	Server executable location.

The whole set of parameters must be provided in order to 
perform a successful deployment of an IMSS instance.

If it is desired to deploy the metadata server, the
following parameters must be included.

	-M	Flag specifying that the metadata server
		will be deployed (no arg required).
	-B	Buffer size (in GB) assigned to the me-
		tadata server.
	-F	File where the metadata server will read
		and write IMSS-related structures.

Again, all parameters must be provided in order to perform
a successful deployment of the metadata server.

"
}


function check_argument
{
	echo "WHAT: $1"

	first_letter=$(echo $1 | head -c 1)

	if [ $first_letter == "-" ]
	   then
		echo "init_imss.sh ERROR: option -$2 requires an argument"

		exit
	   fi
}


#Port assigned to the PUBLISHER socket performing the IMSS_RELEASE operation.
release_port=44075

metadata_deployment="N"

#Variables conforming the IMSS deployment.
imss_uri="-1"
imss_hostfile="-1"
imss_buffer_size="-1"
imss_port_number="-1"
metadata_server_port="-1"
metadata_server_address="-1"
metadata_buffer_size="-1"
metadata_file="-1"
binary_location="-1"


#GETOPTS loop parsing the set of arguments provided. Just those options requiring an argument must be followed by a semicolon.
while getopts ":u:d:b:p:P:A:X:F:B:Mh" opt
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
	   X )
		binary_location=$OPTARG
		check_argument "$binary_location" "$opt"
		;;
	   F )
		metadata_file=$OPTARG
		check_argument "$metadata_file" "$opt"
		;;
	   B )
		metadata_buffer_size=$OPTARG
		check_argument "$metadata_buffer_size" "$opt"
		;;
	   M )
		metadata_deployment="Y"
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

	   #Notify that required arguments were not provided.
	   : )

		echo "init_imss.sh ERROR: option -$OPTARG requires an argument"

		exit
		;;
	esac
   done


expected_num_args=""

if [ $metadata_deployment == "N" ]
   then
	expected_num_args=14
   else
	expected_num_args=19
fi

if [ "$#" -ne $expected_num_args ]
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
		echo -n " -X"
	   fi

	if [ $metadata_deployment == "Y" ]
	   then
		if [ "$metadata_buffer_size" == "-1" ]
		   then
			echo -n " -B"
		   fi
		if [ "$metadata_file" == "-1" ]
		   then
			echo -n " -F"
		   fi
	   else
		if [ "$metadata_file" != "-1" ] || [ "$metadata_buffer_size" != "-1" ]
		   then
			echo -n " -M"
		   fi
	fi

	echo " )"

	exit
   fi



#Required if non-getopts additional arguments were provided.
#shift $((OPTIND - 1))

echo "Arguments provided: LOCATION OF BINARY FILE $binary_location URI $imss_uri, HOSTFILE: $imss_hostfile, IMSS_BUFFER_SIZE: $imss_buffer_size, IMSS_PORT_NUMBER: $imss_port_number, METADATA_PORT: $metadata_server_port, METADATA_ADDRESS: $metadata_server_address"
