![Screenshot](hercules.png)

# Design summary

The architectural design of Hercules follows a client-server design model where the client itself will be responsible of the server entities deployment. We propose an application-attached deployment constrained to application's nodes and an application-detached considering offshore nodes. 

The development of the present work was strictly conditioned by a set of well-defined objectives. Firstly, Hercules should provide flexibility in terms of deployment. To achieve this, the Hercules API provides a set of deployment methods where the number of servers conforming the instance, as well as their locations, buffer sizes, and their coupled or decoupled nature, can be specified. Second, parallelism should be maximised. To achieve this, Hercules follows a multi-threaded design architecture. Each server conforming an instance counts with a dispatcher thread and a pool of worker threads. The dispatcher thread distributes the incoming workload between the worker threads with the aim of balancing the workload in a multi-threaded scenario. Main entities conforming the architectural design are Hercules clients (front-end), Hercules server (back-end), and Hercules metadata server. Addressing the interaction between these components, the Hercules client will exclusively communicate with the Hercules metadata server whenever a metadata-related operation is performed, such as: *create_dataset* and *open_imss*. Data-related operations (*get_data* & *set_data*) will be handled directly by the corresponding storage server. Finally, Hercules offers to the application a set of distribution policies at dataset level increasing the application's awareness about the location of the data. As a result, the storage system will increase awareness in terms of data distribution at the client side, providing benefits such as data locality exploitation and load balancing.

Hercules takes advantage of UCX in order to handle communications between the different entities conforming an Hercules instance. UCX has been qualified as one of the most efficient libraries for creating distributed applications. UCX provides multiple communication patterns across various transport layers, such as inter-threaded, inter-process, TCP, UDP, and multicast. 

Furthermore, to deal with the Hercules dynamic nature, a distributed metadata server, resembling CEPH model, was included in the design step. The metadata server is in charge of storing the structures representing each Hercules and dataset instances. Consequently, clients are able to join an already created Hercules as well as accessing an existing dataset among other operations. 

# Use cases


Two strategies were considered so as to adapt the storage system to the application's requirements. On the one hand, the *application-detached* strategy, consisting of deploying Hercules clients and servers as process entities on decoupled nodes. Hercules clients will be deployed in the same computing nodes as the application, using them to take advantage of all available computing resources within an HPC cluster, while Hercules servers will be in charge of storing the application datasets and enabling the storage's execution in application's offshore nodes. In this strategy, Hercules clients do not store data locally, as this deployment was thought to provide an application-detached possibility. In this way, persistent Hercules storage servers could be created by the system and would be executed longer than a specific application, so as to avoid additional storage initialisation overheads in execution time. Figure \ref{Deployments} (left) illustrates the topology of an Hercules application-detached deployment over a set of compute and/or storage nodes where the Hercules instance does not belong to the application context nor its nodes.


On the other hand, the *application-attached* deployment strategy seeks empowering locality exploitation constraining deployment possibilities to the set of nodes where the application is running, so that each application node will also include an Hercules client and an Hercules server, deployed as a thread within the application. Consequently, data could be forced to be sent and retrieved from the same node, thus maximising locality possibilities for data. In this approach each process conforming the application will invoke a method initialising certain in-memory store resources preparing for future deployments. However, as the attached deployment executes in the applications machine, the amount of memory used by the storage system turns into a matter of concern. Considering that unexpectedly bigger memory buffers may harm the applications performance, we took the decision of letting the application determine the memory space that a set of servers (storage and metadata) executing in the same machine shall use through a parameter in the previous method. This decision was made because the final user is the only one conscious about the execution environment as well as the applications memory requirements. Flexibility aside, as main memory will be used as storage device, an in-memory store will be implemented so as to achieve faster data-related request management. Figure \ref{Deployments} (right) displays the topology of an Hercules application-attached deployment where the Hercules instance is contained within the application.



# Download and installation

The following software packages are required for the compilation of Hercules:

- CMake
- UCX
- Glib
- tcmalloc
- FUSE
- MPI (MPICH or OpenMPI)
    
## Project build

Hercules is a CMAKE-based project, so the compilation process is quite simple:  

```
mkdir build
cd build
cmake ..
make
make install                         
```

As a result the project generates the following outputs:
- mount.imss: run as daemons the necessary instances for Hercules. Later, it enables the usage of the interception library with execution persistency.
- umount.imss: umount the file system by killing the deployed processes.
- libhercules_posix.so: dynamic library of intercepting I/O calls.
- libimss_shared.so: dynamic library of IMSS's API.
- libimss_static.a: static library of IMSS's API.
- imfssfs: application for mounting Hercules at user space by using FUSE engine.
    
## Spack module

Clone the project from the project GIT repository:

```
git clone https://gitlab.arcos.inf.uc3m.es/admire/spack.git
```

Now, you can add repositories under the admire namespace:

```
spack repo add spack/adhoc-recipes 
spack install hercules
spack load hercules
```

# Usage

The current prototype of Hercules enables the access to the storage infrastructure in two different ways: API library and LD_PRELOAD by overriding symbols. In the following subsection we describe the characteristics of the preferred option.


## With Slurm 
We provide a script that launches a Hercules deployment (_scripts/hercules_). This script reads all initialization parameters from a provided configuration file (_hercules.conf_).

Custom configuration files can be specified launching Hercules in this manner, where "CONF_PATH" is the path to the configuration file:

```
hercules start -f <CONF_PATH>
```

If not configuration file is provided, Hercules looks for default files _/etc/hercules.conf_, _./hercules.conf_, _hercules.conf_, or _<PROJECT_PATH>/conf/hercules.conf_ in that order.

Hercules can override I/O calls by using the LD_PRELOAD environment variable. Both data and metadata calls are currently intercepted by the implemented dynamic library. As example:

```
export LD_PRELOAD=libhercules_posix.so ls /mnt/hercules_mount_point
```

To stop a Hercules deployment:

```
hercules stop
```

## Without Slurm
To run Hercules in a non-Slurm environment, the script need additional parameters:

```
hercules start -m <meta_server_hostfile> -d <data_server_hostfile> -f <CONF_PATH>

meta_server_hostfile: file containing hostnames of metadata servers
data_server_hostfile: file containing hostnames of data servers
```

#### Hostfile Example

```
node1
node2
node3
```


## Configuration File (_hercules.conf_)
Here we briefly explain each field of the configuration file.

<details><summary>Click to expand</summary>

### Used URI for internal items definition
> URI = imss://

### Block size (in KB)
> BLOCK_SIZE = 512

### Used mount point in the client side
> MOUNT_POINT = /mnt/imss/

### Where the Hercules project is
> HERCULES_PATH = /home/hercules

### Port listening in the metadata node service
> METADATA_PORT = 7500

### Port listening in the data node service
> DATA_PORT = 8500

### Total number of data nodes
> NUM_DATA_SERVERS = 1 

### Total number of metadadata nodes
> NUM_META_SERVERS = 1

### Total number of client nodes
> NUM_NODES_FOR_CLIENTS = 1

### Total number of clients per node
> NUM_CLIENTS_PER_NODE = 1

### 1: enables malleability functions
> MALLEABILITY = 0      
> UPPER_BOUND_MALLEABILITY = 0    
> LOWER_BOUND_MALLEABILITY = 0   

### File containing a list of nodes serving as data nodes
> DATA_HOSTFILE = /home/hercules/bash/data_hostfile

### File path of the persistence metadata
> METADA_PERSISTENCE_FILE = /home/hercules/bash/metadata

### Number of threads attending data requests
> THREAD_POOL = 1

### Maximum size used by the data nodes
> STORAGE_SIZE = 0 # No limit

### File containing a list of nodes serving as metadata nodes
> METADATA_HOSTFILE = /home/hercules/bash/meta_hostfile

### Debug mode (none or all)
> DEBUG_LEVEL = all
</details>
