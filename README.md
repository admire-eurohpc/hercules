![Screenshot](hercules.png)

# Design summary

The architectural design of IMSS follows a client-server design model where the client itself will be responsible of the server entities deployment. We propose an application-attached deployment constrained to application's nodes and an application-detached considering offshore nodes. 

The development of the present work was strictly conditioned by a set of well-defined objectives. Firstly, IMSS should provide flexibility in terms of deployment. To achieve this, the IMSS API provides a set of deployment methods where the number of servers conforming the instance, as well as their locations, buffer sizes, and their coupled or decoupled nature, can be specified. Second, parallelism should be maximised. To achieve this, IMSS follows a multi-threaded design architecture. Each server conforming an instance counts with a dispatcher thread and a pool of worker threads. The dispatcher thread distributes the incoming workload between the worker threads with the aim of balancing the workload in a multi-threaded scenario. Main entities conforming the architectural design are IMSS clients (front-end), IMSS server (back-end), and IMSS metadata server. Addressing the interaction between these components, the IMSS client will exclusively communicate with the IMSS metadata server whenever a metadata-related operation is performed, such as: *create_dataset* and *open_imss*. Data-related operations (*get_data* & *set_data*) will be handled directly by the corresponding storage server. Finally, IMSS offers to the application a set of distribution policies at dataset level increasing the application's awareness about the location of the data. As a result, the storage system will increase awareness in terms of data distribution at the client side, providing benefits such as data locality exploitation and load balancing.

IMSS takes advantage of UCX in order to handle communications between the different entities conforming an IMSS instance. UCX has been qualified as one of the most efficient libraries for creating distributed applications. UCX provides multiple communication patterns across various transport layers, such as inter-threaded, inter-process, TCP, UDP, and multicast. 

Furthermore, to deal with the IMSS dynamic nature, a distributed metadata server, resembling CEPH model, was included in the design step. The metadata server is in charge of storing the structures representing each IMSS and dataset instances. Consequently, clients are able to join an already created IMSS as well as accessing an existing dataset among other operations. 

# Use cases


Two strategies were considered so as to adapt the storage system to the application's requirements. On the one hand, the *application-detached* strategy, consisting of deploying IMSS clients and servers as process entities on decoupled nodes. IMSS clients will be deployed in the same computing nodes as the application, using them to take advantage of all available computing resources within an HPC cluster, while IMSS servers will be in charge of storing the application datasets and enabling the storage's execution in application's offshore nodes. In this strategy, IMSS clients do not store data locally, as this deployment was thought to provide an application-detached possibility. In this way, persistent IMSS storage servers could be created by the system and would be executed longer than a specific application, so as to avoid additional storage initialisation overheads in execution time. Figure \ref{Deployments} (left) illustrates the topology of an IMSS application-detached deployment over a set of compute and/or storage nodes where the IMSS instance does not belong to the application context nor its nodes.


On the other hand, the *application-attached* deployment strategy seeks empowering locality exploitation constraining deployment possibilities to the set of nodes where the application is running, so that each application node will also include an IMSS client and an IMSS server, deployed as a thread within the application. Consequently, data could be forced to be sent and retrieved from the same node, thus maximising locality possibilities for data. In this approach each process conforming the application will invoke a method initialising certain in-memory store resources preparing for future deployments. However, as the attached deployment executes in the applications machine, the amount of memory used by the storage system turns into a matter of concern. Considering that unexpectedly bigger memory buffers may harm the applications performance, we took the decision of letting the application determine the memory space that a set of servers (storage and metadata) executing in the same machine shall use through a parameter in the previous method. This decision was made because the final user is the only one conscious about the execution environment as well as the applications memory requirements. Flexibility aside, as main memory will be used as storage device, an in-memory store will be implemented so as to achieve faster data-related request management. Figure \ref{Deployments} (right) displays the topology of an IMSS application-attached deployment where the IMSS instance is contained within the application.



# Download and installation

The following software packages are required for the compilation of Hercules IMSS:

- CMake
- ZeroMQ
- Glib
- tcmalloc
- FUSE
- MPI (MPICH or OpenMPI)
    

Hercules IMSS is a CMAKE-based project, so the compilation process is quite simple:  
`
    mkdir build
    cd build
    cmake ..
    make
`

As a result the project generates the following outputs:
- mount.imss: run as daemons the necessary instances for Hercules IMSS. Later, it enables the usage of the interception library with execution persistency.
- umount.imss: umount the file system by killing the deployed processes.
- libimss_posix.so: dynamic library of intercepting I/O calls.
- libimss_shared.so: dynamic library of IMSS's API.
- libimss_static.a: static library of IMSS's API.
- imfssfs: application for mounting HERCULES IMSS at user space by using FUSE engine.
    

# Usage

The current prototype of Hercules IMSS enables the access to the storage infrastructure in three different ways: API library, FUSE, and LD_PRELOAD by overriding symbols. In the following subsections, we describe the characteristics of each alternative.

## API

Hercules IMSS is defectively accessible by using C-based API. This API includes the following calls:

-  *hercules_init*: initializes the infrastructure required to deploy an IMSS attached server within the calling client. Besides, it deploys an attached metadata server if requested.
- *hercules_release*: releases infrastructure resources of an attached deployment.
- *stat_init*: creates a communication channel with every metadata server. Besides, the former method declares additional resources that will be required throughout the client session.
- *stat_release*: releases resources required to communicate with the metadata servers and additional session parameters.
- *init_imss*: deploys an IMSS detached instance or initializes an IMSS attached one.
- *open_imss*: joins to an existing IMSS instance enabling access to the stored datasets.
- *release_imss*: releases resources required to communicate with an IMSS instance as well as the every IMSS instance server if requested.
- *create_dataset*: creates a new dataset within a previously created or joined IMSS instance.
- *open_dataset*: subscribes to an existing dataset within a previously created or joined IMSS instance.
- *release_dataset*: frees resources required to read and write blocks of a certain dataset.
- *get_data*: retrieves a certain block of a previously created or opened dataset from one of the IMSS servers conforming the instance.
- *set_data*: stores a certain block of a previously created or opened dataset into a set of IMSS servers part of the same instance.
- *get_type*: given a certain URI, the former method returns if it corresponds to an IMSS instance or dataset.

The below code excerpt depicts an usage example of Hercules IMSS by using the proposed API. The example creates a determined number of datasets in-memory.


## FUSE

We have constructed a file system layer on top of the previously described library. The in-memory file system currently supports both data and metadata operations, such as file permissions, ownership, folders, and namespaces. Files and folders are presented as datasets inside Hercules IMSS, conforming a hierarchical representation supported by URIs, which univocally identifies each dataset. Data is partitioned into multiple blocks, reserving the first block for storing metadata. We distinguish between inner metadata, which represents the classical POSIX-like metadata mainly represented by the *struct stat* representation and outer metadata, which depicts the metadata related to the data blocks location and the applied distribution policy. Inner metadata is stored as a data block on each data set. Outer metadata is maintained by a separate metadata server. Hercules IMSS supports attached and detached metadata servers.

Using the call above, we have to mount the Hercules IMSS file system using FUSE.

`
./imssfs -p 5555 -m 5569 -M ./metadata -h ./hostfile -b 1000000  -r imss:// -a ./stat_hostfile -S 10000000000 -d 0 -B 1048576000 -l /mnt/imss/}
`

The mount call requires the following parameters:
- *-p*: determines the listening port number of the I/O servers.
- *-m*: indicates the port number of the external/internal metadata server.
- *-h*: requires a file containing the hostnames of all I/O server involved.
- *-b*: specifies the block size employed for all network data transfers used by ZeroMQ.
- *-r*: determines the default dataset root for the deployed file system.
- *-a*: requires a file containing the hostnames of all metadata servers involved.
- *-s*: the maximum capacity in bytes of the storage system.
- *-d*: determines the deployment mode. 0 indicates an attached deployment strategy (metadata and data services are instantiated as a FUSE process) and 2 indicates a completely detached deployment, is such a way, both data and metadata servers have to be executed as independent processes.
- *-l*: indicates the mount point path.


## LD_PRELOAD

The project repository provides support for running Hercules IMSS overriding I/O calls by using the LD_PRELOAD environment variable. Both data and metadata calls are currently intercepted by the implemented dynamic library.

Initially, it is necesary to set up the configuration environment variables:


> export IMSS_MOUNT_POINT=/mnt/imss 
> export IMSS_HOSTFILE=./hostfile    
> export IMSS_N_SERVERS=3   
> export IMSS_SRV_PORT=5555    
> export IMSS_BUFFSIZE=1    
> export IMSS_META_HOSTFILE=./stat_hostfile   
> export IMSS_META_PORT=5569    
> export IMSS_META_SERVERS=1   
> export IMSS_STORAGE_SIZE=10    
> export IMSS_METADATA_FILE=./metadata   
> export IMSS_DEPLOYMENT=2  
 
In this case, IMSS will employ 3 data servers and one metadata server. The file _hostfile_ containts the hostnames of the data servers. File _stat_hostfile_ contains the hostname of the dedicated metadata hosts. Finally, file _metadata_ stores the current information of the employed metadata servers for future usage.

Once, the application can be executed using the aforementioned deployment modes:

`
LD_PRELOAD=libimss_posix.so ls -l /mnt/imss/
`

# Example deployment setup

Using this example deployement setup:

- node1: initial metadata node.
- node2: metadata node.
- node3: initial data node.
- node4: data node.
- client1
- client2

# Running without MPI


<details><summary>Click to expand</summary>
## Configuration Files

_hostfile_
> node3  
> node4  

_stat_hostfile_
> node1  
> node2  

## Metadata servers 

On each metadata node (node1, node2):

> ./server ./metadata 5569 0 


## Data servers

On each data node (node, node4):

> ./server imss:// 5555 0 node1 5569 4 ./hostfile 1
       

## Clients

> LD_PRELOAD=/home/imss/build/tools/libimss_posix.so /home/benchs/ior/bin/ior -k  -w -o /mnt/testimss/data.ot 10m -b 100m -s 5

</details>

# Running with MPI


<details><summary>Click to expand</summary>

## Metadata servers


> mpirun.mpich -np 1 -f ./stat_hostfile ./server ./metadata 5569 0 
> 
> or
> 
> mpiexec -np 1 --hostfile ./stat_hostfile ./server ./metadata 5569 0 


## Data servers


> mpirun.mpich -np 4 -f ./hostfile ./server imss:// 5555 0 node1 5569 4 ./hostfile 1
> 
> or
> 
> mpirun -np 4 --pernode --hostfile ./hostfile ./server imss:// 5555 0 node1 5569 4 ./hostfile 1

           

## Clients


> mpirun.mpich -np 1 -f ./clientfile -genv LD_PRELOAD /home/imss/build/tools/libimss_posix.so ./test_simple /mnt/testimss/data.out
> 
> or
> 
> mpirun -np 2 --pernode --hostfile ./clientfile -x LD_PRELOAD=/home/imss/build/tools/libimss_posix.so /home/benchs/ior/bin/ior -k  -w -o /mnt/testimss/data.ot 10m -b 100m -s 5

</details>



lustre path in Broadwell
/lustre/scratch/javier.garciablas
