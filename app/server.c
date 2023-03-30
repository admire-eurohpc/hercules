#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/signal.h>
#include "imss.h"
#include "stat.h"
#include "comms.h"
#include "workers.h"
#include "memalloc.h"
#include "directory.h"
#include "records.hpp"
#include "arg_parser.h"
#include "map_ep.hpp"
#include "cfg_parse.h"
#include <inttypes.h>
#include <unistd.h>

// Pointer to the tree's root node.
extern GNode *tree_root;
extern pthread_mutex_t tree_mut;
// URI of the created IMSS.
char *imss_uri;

// Initial buffer address.
extern char *buffer_address;
// Set of locks dealing with the memory buffer access.
extern pthread_mutex_t *region_locks;
// Segment size (amount of memory assigned to each thread).
extern uint64_t buffer_segment;

extern ucp_worker_h *ucp_worker_threads;
extern ucp_address_t **local_addr;
extern size_t *local_addr_len;

extern StsHeader *mem_pool;

/* UCP objects */
ucp_context_h ucp_context;
ucp_worker_h ucp_worker;

ucp_ep_h pub_ep;

void *map_ep;		   // map_ep used for async write; server doesn't use it
int32_t is_client = 0; // also used for async write

int32_t IMSS_DEBUG_FILE = 0;
int IMSS_DEBUG_LEVEL = SLOG_FATAL;
int32_t IMSS_DEBUG_SCREEN = 0;
int IMSS_THREAD_POOL = 1;

#define RAM_STORAGE_USE_PCT 0.75f // percentage of free system RAM to be used for storage

int32_t main(int32_t argc, char **argv)
{
	// Print off a hello world message
	struct cfg_struct *cfg;
	clock_t t;
	double time_taken;

	uint16_t bind_port, aux_bind_port;
	char *stat_add;
	char *metadata_file;
	char *deployfile;
	int64_t buffer_size, stat_port, num_servers;
	void *socket;
	ucp_address_t *req_addr;
	size_t req_addr_len;
	ucp_ep_params_t ep_params;
	ucp_address_t *peer_addr;
	size_t addr_len;

	// memory pool stuff
	uint64_t block_size;   // In KB
	uint64_t storage_size; // In GB

	ucs_status_t status;
	ucp_ep_h client_ep;

	ucp_am_handler_param_t param;
	int ret;
	ucp_config_t *config;
	ucp_worker_address_attr_t attr;

	uint64_t max_system_ram_allowed;
	uint64_t max_storage_size; // memory pool size
	uint32_t num_blocks;
	struct arguments args;

	t = clock();

	// get arguments.
	parse_args(argc, argv, &args);

	cfg = cfg_init();

	/***************************************************************/
	/******************* PARSE FILE ARGUMENTS **********************/
	/***************************************************************/

	char path_save[PATH_MAX];
	char abs_exe_path[PATH_MAX];
	char abs_exe_path2[PATH_MAX];
	char *p;

	if (!(p = strrchr(argv[0], '/')))
		getcwd(abs_exe_path, sizeof(abs_exe_path));
	else
	{
		*p = '\0';
		getcwd(path_save, sizeof(path_save));
		chdir(argv[0]);
		getcwd(abs_exe_path, sizeof(abs_exe_path));
		chdir(path_save);
	}

	strcat(abs_exe_path, "/../conf/hercules.conf");
	strcpy(abs_exe_path2, abs_exe_path);
	strcat(abs_exe_path2, "hercules.conf");

	fprintf(stderr, "Trying to load /etc/hercules.conf\n");
	if (cfg_load(cfg, "/etc/hercules.conf") > 0)
	{
		fprintf(stderr, "Trying to load %s\n", abs_exe_path);
		if (cfg_load(cfg, abs_exe_path) > 0)
		{
			fprintf(stderr, "Trying to load %s\n", abs_exe_path2);
			if (cfg_load(cfg, abs_exe_path2) > 0)
			{
				cfg_load(cfg, "hercules.conf");
			}
		}
	}

	if (cfg_get(cfg, "URI"))
	{
		const char *aux = cfg_get(cfg, "URI");
		strcpy(args.imss_uri, aux);
	}

	if (cfg_get(cfg, "BLOCK_SIZE"))
		args.block_size = atoi(cfg_get(cfg, "BLOCK_SIZE"));

	if (args.type == TYPE_DATA_SERVER)
	{
		if (cfg_get(cfg, "NUM_DATA_SERVERS"))
			args.block_size = atoi(cfg_get(cfg, "NUM_DATA_SERVERS"));
	}
	else
	{
		if (cfg_get(cfg, "NUM_META_SERVERS"))
			args.block_size = atoi(cfg_get(cfg, "NUM_META_SERVERS"));
	}

	if (cfg_get(cfg, "THREAD_POOL"))
		args.thread_pool = atoi(cfg_get(cfg, "THREAD_POOL"));

	if (cfg_get(cfg, "STORAGE_SIZE"))
		args.storage_size = atoi(cfg_get(cfg, "STORAGE_SIZE"));

	// if (cfg_get(cfg, "METADATA_HOST"))
	// {
	// 	const char *aux = cfg_get(cfg, "METADATA_HOST");
	// 	strcpy(args.stat_host, aux);
	// }
	
	if (cfg_get(cfg, "METADATA_PORT"))
		args.stat_port = atol(cfg_get(cfg, "METADATA_PORT"));

	if (cfg_get(cfg, "DATA_PORT"))
		args.port = strtoul(cfg_get(cfg, "DATA_PORT"), NULL, 0);

	fprintf(stderr,"args.port=%d\n", args.port);

	/***************************************************************/
	/******************** PARSE INPUT ARGUMENTS ********************/
	/***************************************************************/

	if (getenv("IMSS_DEBUG") != NULL)
	{
		if (strstr(getenv("IMSS_DEBUG"), "file"))
		{
			IMSS_DEBUG_FILE = 1;
			IMSS_DEBUG_SCREEN = 0;
			IMSS_DEBUG_LEVEL = SLOG_LIVE;
		}
		else if (strstr(getenv("IMSS_DEBUG"), "stdout"))
			IMSS_DEBUG_SCREEN = 1;
		else if (strstr(getenv("IMSS_DEBUG"), "debug"))
			IMSS_DEBUG_LEVEL = SLOG_DEBUG;
		else if (strstr(getenv("IMSS_DEBUG"), "live"))
			IMSS_DEBUG_LEVEL = SLOG_LIVE;
		else if (strstr(getenv("IMSS_DEBUG"), "all"))
		{
			IMSS_DEBUG_FILE = 1;
			IMSS_DEBUG_LEVEL = SLOG_PANIC;
		}
		else if (strstr(getenv("IMSS_DEBUG"), "none"))
			unsetenv("IMSS_DEBUG");
		else
		{
			IMSS_DEBUG_FILE = 1;
			IMSS_DEBUG_LEVEL = getLevel(getenv("IMSS_DEBUG"));
		}
	}

	if (getenv("IMSS_THREAD_POOL") != NULL)
	{
		args.thread_pool = strtoul(getenv("IMSS_THREAD_POOL"), NULL, 0);
	}

	IMSS_THREAD_POOL = args.thread_pool;

	// time_t t = time(NULL);
	// struct tm tm = *localtime(&t);
	char log_path[1000];
	// sprintf(log_path, "./%c-server.%02d-%02d-%02d", args.type, tm.tm_hour, tm.tm_min, tm.tm_sec);
	sprintf(log_path, "./%c-server", args.type);
	slog_init(log_path, SLOG_DEBUG, 1, 1, 1, 1, 1, args.id);
	slog_info("IMSS DEBUG FILE AT %s", log_path);
	slog_info(",Time(msec), Comment, RetCode");

	slog_debug("[SERVER] Starting server.");
	slog_debug("[CLI PARAMS] type = %c port = %" PRIu16 " bufsize = %" PRId64 " ", args.type, args.port, args.bufsize);
	if (args.type == TYPE_DATA_SERVER)
	{
		slog_debug("imss_uri = %s stat-host = %s stat-port = %" PRId64 " num-servers = %" PRId64 " deploy-hostfile = %s block-size = %" PRIu64 " storage-size = %" PRIu64 "",
				   args.imss_uri, args.stat_host, args.stat_port, args.num_servers, args.deploy_hostfile, args.block_size, args.storage_size);
	}
	else
	{
		slog_debug("stat-logfile = %s", args.stat_logfile);
	}

	status = ucp_config_read(NULL, NULL, &config);

	// bind port number.
	bind_port = args.port;
	aux_bind_port = bind_port;
	// buffer size provided
	buffer_size = args.bufsize;
	// set up imss uri (default value is already set up in args)
	imss_uri = (char *)calloc(32, sizeof(char));

	/* Initialize the UCX required objects */
	ret = init_context(&ucp_context, config, &ucp_worker, CLIENT_SERVER_SEND_RECV_STREAM);
	if (ret != 0)
	{
		perror("ERRIMSS_INIT_CONTEXT");
		return -1;
	}

	/* Set memory pool size */
	max_storage_size = args.storage_size * GB;
	// get max RAM we could use for storage
	max_system_ram_allowed = (uint64_t)sysconf(_SC_AVPHYS_PAGES) * sysconf(_SC_PAGESIZE) * RAM_STORAGE_USE_PCT;

	// make sure we don't use more memory than available
	if (max_storage_size >= max_system_ram_allowed || max_storage_size <= 0)
	{
		max_storage_size = max_system_ram_allowed;
	}

	fprintf(stderr, "max_storage_size=%ld\n", max_storage_size);

	// init memory pool
	mem_pool = StsQueue.create();
	// figure out how many blocks we need and allocate them
	num_blocks = max_storage_size / (args.block_size * KB);
	for (int i = 0; i < num_blocks; ++i)
	{
		char *buffer = (char *)calloc(args.block_size * KB, sizeof(char));
		StsQueue.push(mem_pool, buffer);
	}

	/* CHECK THIS OUT!
	 ***************************************************
	 In relation to the type argument provided, an IMSS or a metadata server will be deployed. */

	// IMSS server.
	if (args.type == TYPE_DATA_SERVER)
	{
		// IMSS name.
		strcpy(imss_uri, args.imss_uri);
		// machine name where the metadata server is being executed.
		stat_add = args.stat_host;
		// port that the metadata server is listening on.
		stat_port = args.stat_port;
		// number of servers conforming the IMSS deployment.
		num_servers = args.num_servers;
		// IMSS' MPI deployment file.
		deployfile = args.deploy_hostfile;
		// data block size
		block_size = args.block_size;
		// total storage size
		// storage_size = max_storage_size;

		int32_t imss_exists = 0;

		// Check if the provided URI has been already reserved by any other instance.
		slog_info("args.id=%d", args.id);
		if (!args.id)
		{
			ucs_status_t status;
			int oob_sock;
			int ret = 0;
			ucs_status_t ep_status = UCS_OK;

			uint32_t id = args.id;

			oob_sock = connect_common(stat_add, stat_port, AF_INET);

			char request[REQUEST_SIZE];
			sprintf(request, "%" PRIu32 " GET %s", id, "MAIN!QUERRY");

			if (send(oob_sock, request, REQUEST_SIZE, 0) < 0)
			{
				perror("ERRIMSS_STAT_HELLO");
				return -1;
			}

			ret = recv(oob_sock, &addr_len, sizeof(addr_len), MSG_WAITALL);
			peer_addr = (ucp_address *)malloc(addr_len);
			ret = recv(oob_sock, peer_addr, addr_len, MSG_WAITALL);
			close(oob_sock);

			/* Send client UCX address to server */
			ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS |
								   UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE |
								   UCP_EP_PARAM_FIELD_ERR_HANDLER |
								   UCP_EP_PARAM_FIELD_USER_DATA;
			ep_params.address = peer_addr;
			ep_params.err_mode = UCP_ERR_HANDLING_MODE_NONE;
			ep_params.err_handler.cb = err_cb_client;
			ep_params.err_handler.arg = NULL;
			ep_params.user_data = &ep_status;

			status = ucp_ep_create(ucp_worker, &ep_params, &client_ep);

			// Formated imss uri to be sent to the metadata server.
			char formated_uri[REQUEST_SIZE];
			sprintf(formated_uri, "%" PRIu32 " GET 0 %s", id, imss_uri);

			// status = ucp_worker_get_address(ucp_worker, &req_addr, &req_addr_len);

			ucp_worker_attr_t worker_attr;
			worker_attr.field_mask = UCP_WORKER_ATTR_FIELD_ADDRESS;
			status = ucp_worker_query(ucp_worker, &worker_attr);
			// printf ("Len %ld \n", worker_attr.address_length);
			req_addr_len = worker_attr.address_length;
			req_addr = worker_attr.address;

			attr.field_mask = UCP_WORKER_ADDRESS_ATTR_FIELD_UID;
			ucp_worker_address_query(req_addr, &attr);
			slog_debug("[srv_worker_thread] Server UID %" PRIu64 ".", attr.worker_uid);

			// Send the request.
			if (send_req(ucp_worker, client_ep, req_addr, req_addr_len, formated_uri) < 0)
			{
				perror("ERRIMSS_RLSIMSS_SENDADDR");
				return -1;
			}

			imss_info imss_info_;

			// Receive the associated structure.
			ret = recv_dynamic_stream(ucp_worker, client_ep, &imss_info_, BUFFER, attr.worker_uid);

			if (ret >= sizeof(imss_info))
			{
				imss_exists = 1;
				for (int32_t i = 0; i < imss_info_.num_storages; i++)
					free(imss_info_.ips[i]);
				free(imss_info_.ips);
			}
		}

		if (imss_exists)
		{
			fprintf(stderr, "ERRIMSS_IMSSURITAKEN");
			return 0;
		}
	}
	// Metadata server.
	else
	{
		// metadata file.
		metadata_file = args.stat_logfile;

		// Create the tree_root node.
		char *root_data = (char *)calloc(8, sizeof(char));
		strcpy(root_data, "imss://");
		tree_root = g_node_new((void *)root_data);

		if (pthread_mutex_init(&tree_mut, NULL) != 0)
		{
			perror("ERRIMSS_TREEMUT_INIT");
			pthread_exit(NULL);
		}
	}

	/***************************************************************/
	/******************** INPROC COMMUNICATIONS ********************/
	/***************************************************************/
	// ucx_server_ctx_t context;
	// ucp_worker_h     ucp_data_worker;

	// ret = init_worker(ucp_context, &ucp_data_worker);
	// if (ret != 0) {
	//	perror("ERRIMSS_WORKER_INIT");
	//	pthread_exit(NULL);
	// }

	/* Initialize the server's context. */
	// context.conn_request = NULL;

	// status = start_server(ucp_worker, &context, &context.listener, NULL, 0);
	// status = start_server(ucp_worker, &context, &context.listener, NULL, bind_port + 1000);
	// if (status != UCS_OK) {
	//	perror("ERRIMSS_STAR_SERVER");
	//	pthread_exit(NULL);
	// }
	/*
	   while (context.conn_request == NULL) {
	   ucp_worker_progress(ucp_worker);
	   }
	   status = server_create_ep(ucp_data_worker, context.conn_request, &pub_ep);
	   if (status != UCS_OK) {
	   perror("ERRIMSS_SERVER_CREATE_EP");
	   pthread_exit(NULL);
	   }
	 */
	// Map tracking saved records.
	std::shared_ptr<map_records> map(new map_records(buffer_size * KB));

	int64_t data_reserved;
	// Pointer to the allocated buffer memory.
	char *buffer;
	// Size of the buffer involved.
	uint64_t size = (uint64_t)buffer_size * KB;
	// Check if the requested data is available in the current node.
	if ((data_reserved = memalloc(size, &buffer)) == -1)
		return -1;

	buffer_address = buffer;

	// Metadata bytes written into the buffer.
	uint64_t bytes_written = 0;

	if (args.type == TYPE_METADATA_SERVER)
	{
		if ((buffer_address = metadata_read(metadata_file, map.get(), buffer, &bytes_written)) == NULL)
			return -1;

		// Obtain the remaining free amount of data reserved to the buffer after the metadata read operation.
		data_reserved -= bytes_written;
	}

	// Buffer segment size assigned to each thread.
	buffer_segment = data_reserved / args.thread_pool;
	slog_info("buffer_segment=%ld", buffer_segment);

	// Initialize pool of threads.
	pthread_t threads[(args.thread_pool + 1)];
	// Thread arguments.
	p_argv arguments[(args.thread_pool + 1)];

	if (args.type == TYPE_DATA_SERVER)
		region_locks = (pthread_mutex_t *)calloc(args.thread_pool, sizeof(pthread_mutex_t));

	ucp_worker_threads = (ucp_worker_h *)malloc((args.thread_pool + 1) * sizeof(ucp_worker_h));
	local_addr = (ucp_address_t **)malloc((args.thread_pool + 1) * sizeof(ucp_address_t *));
	local_addr_len = (size_t *)malloc((args.thread_pool + 1) * sizeof(size_t));

	// Execute all threads.
	for (int32_t i = 0; i < (args.thread_pool + 1); i++)
	{
		// Add port number to thread arguments.
		arguments[i].ucp_context = ucp_context;
		arguments[i].blocksize = block_size;
		arguments[i].storage_size = max_storage_size;
		arguments[i].port = args.port;

		// Add the instance URI to the thread arguments.
		strcpy(arguments[i].my_uri, imss_uri);

		// Deploy all dispatcher + service threads.
		if (i == 0)
		{
			slog_debug("[SERVER] Creating dispatcher thread.");
			// Deploy a thread distributing incomming clients among all ports.
			if (pthread_create(&threads[i], NULL, dispatcher, (void *)&arguments[i]) == -1)
			{
				// Notify thread error deployment.
				perror("ERRIMSS_DISPATCHER_DEPLOY");
				return -1;
			}
		}
		else
		{
			ret = init_worker(ucp_context, &ucp_worker_threads[i]);
			if (ret != 0)
			{
				perror("ERRIMSS_WORKER_INIT");
			}

			arguments[i].ucp_worker = ucp_worker_threads[i];

			// status = ucp_worker_get_address(ucp_worker_threads[i], &local_addr[i], &local_addr_len[i]);
			ucp_worker_attr_t worker_attr;
			worker_attr.field_mask = UCP_WORKER_ATTR_FIELD_ADDRESS;
			status = ucp_worker_query(ucp_worker_threads[i], &worker_attr);
			local_addr_len[i] = worker_attr.address_length;
			local_addr[i] = worker_attr.address;

			// Add the reference to the map into the set of thread arguments.
			arguments[i].map = map;
			// Specify the address used by each thread to write inside the buffer.
			arguments[i].pt = (char *)((i - 1) * buffer_segment + buffer_address);

			// IMSS server.
			if (args.type == TYPE_DATA_SERVER)
			{
				slog_debug("[SERVER] Creating data thread.");
				if (pthread_create(&threads[i], NULL, srv_worker, (void *)&arguments[i]) == -1)
				{
					// Notify thread error deployment.
					perror("ERRIMSS_SRVWORKER_DEPLOY");
					return -1;
				}
			}
			// Metadata server.
			else
			{
				slog_debug("[SERVER] Creating metadata thread.");
				if (pthread_create(&threads[i], NULL, stat_worker, (void *)&arguments[i]) == -1)
				{
					// Notify thread error deployment.
					perror("ERRIMSS_STATWORKER_DEPLOY");
					return -1;
				}
			}
		}
	}

	// Notify to the metadata server the deployment of a new IMSS.
	if ((args.type == TYPE_DATA_SERVER) && !args.id && stat_port)
	{
		// Metadata structure containing the novel IMSS info.
		imss_info my_imss;

		strcpy(my_imss.uri_, imss_uri);
		my_imss.ips = (char **)calloc(num_servers, sizeof(char *));
		my_imss.num_storages = num_servers;
		my_imss.conn_port = aux_bind_port;
		my_imss.type = 'I'; // extremely important
		// FILE entity managing the IMSS deployfile.
		FILE *svr_nodes;

		if ((svr_nodes = fopen(deployfile, "r+")) == NULL)
		{
			perror("ERRIMSS_DEPLOYFILE_OPEN");
			return -1;
		}

		// Number of characters successfully read from the line.
		int32_t n_chars;
		for (int32_t i = 0; i < num_servers; i++)
		{
			// Allocate resources in the metadata structure so as to store the current IMSS's IP.
			(my_imss.ips)[i] = (char *)calloc(LINE_LENGTH, sizeof(char));
			size_t l_size = LINE_LENGTH;

			// Save IMSS metadata deployment.
			n_chars = getline(&((my_imss.ips)[i]), &l_size, svr_nodes);

			// Erase the new line character ('') from the string.
			((my_imss.ips)[i])[n_chars - 1] = '\0';
		}

		// Close the file.
		if (fclose(svr_nodes) != 0)
		{
			perror("ERRIMSS_DEPLOYFILE_CLOSE");
			return -1;
		}

		char key_plus_size[REQUEST_SIZE];
		uint32_t id = CLOSE_EP;
		// Send the created structure to the metadata server.
		sprintf(key_plus_size, "%" PRIu32 " SET %lu %s", id, (sizeof(imss_info) + my_imss.num_storages * LINE_LENGTH), my_imss.uri_);

		// status = ucp_ep_create(ucp_worker, &ep_params, &client_ep);

		if (send_req(ucp_worker, client_ep, req_addr, req_addr_len, key_plus_size) < 0)
		{
			perror("ERRIMSS_RLSIMSS_SENDADDR");
			return -1;
		}

		slog_debug("[SERVER] Creating IMSS_INFO at metadata server. ");
		// Send the new IMSS metadata structure to the metadata server entity.
		if (send_dynamic_stream(ucp_worker, client_ep, (char *)&my_imss, IMSS_INFO, attr.worker_uid) == -1)
			return -1;

		for (int32_t i = 0; i < num_servers; i++)
			free(my_imss.ips[i]);
		free(my_imss.ips);

		// ucp_ep_close_nb(client_ep, UCP_EP_CLOSE_MODE_FORCE);
	}

	// Wait for threads to finish.
	for (int32_t i = 0; i < (args.thread_pool + 1); i++)
	{
		// final deployment time.
		t = clock() - t;
		time_taken = ((double)t) / (CLOCKS_PER_SEC);
		// printf("[%c-server %d] Deployment time %f\n", args.type, args.id, time_taken);
		if (!args.id)
			printf("ServerID,'Deployment time (s)'\n");

		printf("%d,%f\n", args.id, time_taken);
		fflush(stdout);

		// fprintf(stderr, "Server init\n");

		// setenv("IMSS_INIT_SERVER", "1", 1);
		// char command[100];
		// strcpy(command,"export IMSS_INIT_SERVER=1");
		// int ret = system(command);
		// fprintf(stderr,"system status=%d\n", ret);
		// putenv("IMSS_INIT_SERVER=1");
		// printf("IMSS_INIT_SERVER=1\n");
		// fflush(stdout);
		if (pthread_join(threads[i], NULL) != 0)
		{
			perror("ERRIMSS_SRVTH_JOIN");
			return -1;
		}
	}

	// Write the metadata structures retrieved by the metadata server threads.
	if (args.type == TYPE_METADATA_SERVER)
	{
		if (metadata_write(metadata_file, buffer, map.get(), arguments, buffer_segment, bytes_written) == -1)

			return -1;

		// free(imss_uri);

		// Freeing all resources of the tree structure.
		g_node_traverse(tree_root, G_PRE_ORDER, G_TRAVERSE_ALL, -1, gnodetraverse, NULL);

		if (pthread_mutex_destroy(&tree_mut) != 0)
		{
			perror("ERRIMSS_TREEMUT_DESTROY");
			pthread_exit(NULL);
		}
	}
	else
		free(region_locks);

	// Close publisher socket.
	// ep_close(ucp_worker, pub_ep, UCP_EP_CLOSE_MODE_FORCE);

	// Free the memory buffer.
	free(buffer);
	// Free the publisher release address.
	return 0;
}
