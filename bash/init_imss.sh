#!/bin/bash


#TODO: consider providing a set of default arguments.


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

The init_imss.sh script initializes an instance of an In-Memory 
Storage System. The script is able to deploy the metadata server 
as well.

The following options are available:

	-u	IMSS URI string identifying the deployment. 
	-d	MPI hostfile specifying the set of nodes
		where an IMSS server will be deployed.
	-b	Buffer size (in KB) assigned to each ser-
		ver conforming the IMSS deployment.
	-p	Port assigned to each server conforming
		the IMSS deployment.
	-A	Address (IP or DNS) of the machine where
		the metadata server is taking execution.
	-P	Port within the previous machine that the
		metadata server is listening to.
	-t	Number of background I/O Threads assigned to
		each server of the corresponding instance.
	-x	Server executable location.

The whole set of parameters must be provided in order to 
perform a successful deployment of an IMSS instance.

If it is desired to deploy the metadata server, the
following parameters must be included.

	-M	Flag specifying that the metadata server
		will be deployed (NO ARG REQUIRED).
	-B	Buffer size (in KB) assigned to the me-
		tadata server.
	-F	File where the metadata server will read
		and write IMSS-related structures.

Again, all parameters must be provided in order to perform
a successful deployment of the metadata server.

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
server_binary="-1"
io_threads="-1"


#GETOPTS loop parsing the set of arguments provided. Just those options requiring an argument must be followed by a semicolon.
while getopts ":u:d:b:p:P:A:x:F:B:t:Mh" opt
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
	   x )
		server_binary=$OPTARG
		check_argument "$server_binary" "$opt"
		;;
	   F )
		metadata_file=$OPTARG
		check_argument "$metadata_file" "$opt"
		;;
	   B )
		metadata_buffer_size=$OPTARG
		check_argument "$metadata_buffer_size" "$opt"
		;;
	   t )
		io_threads=$OPTARG
		check_argument "$io_threads" "$opt"
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
	expected_num_args=16
   else
	expected_num_args=21
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
	if [ "$server_binary" == "-1" ]
	   then
		echo -n " -x"
	   fi
	if [ "$io_threads" == "-1" ]
	   then
		echo -n " -t"
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



####################################################################
############################ DEPLOYMENTS ###########################
####################################################################

metadata_deployfile="/tmp/IMSS_metadata_deployfile"

#Create MPI deployment file for the metadata server.
echo "$metadata_server_address" > $metadata_deployfile

if [ "$metadata_deployment" == "Y" ]
   then
	mpirun -np 1 -f $metadata_deployfile $server_binary $metadata_file $metadata_server_port $metadata_buffer_size &
fi

num_servers=$(cat $imss_hostfile | wc -l)

mpirun -np $num_servers -f $imss_hostfile $server_binary $imss_uri $imss_port_number $imss_buffer_size $metadata_server_address $metadata_server_port $num_servers $imss_hostfile $io_threads &
