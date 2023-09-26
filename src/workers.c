#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/sysinfo.h>
#include <signal.h>
#include "imss.h"
#include "comms.h"
#include "workers.h"
#include "directory.h"
#include "records.hpp"
#include "map_server_eps.hpp"
#include <sys/time.h>
// #include <inttypes.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <net/if.h>
#include <arpa/inet.h>

// Lock dealing when cleaning blocks
pthread_mutex_t mutex_garbage = PTHREAD_MUTEX_INITIALIZER;

// Initial buffer address.
char *buffer_address;
// Set of locks dealing with the memory buffer access.
pthread_mutex_t *region_locks;
// Segment size (amount of memory assigned to each thread).
uint64_t buffer_segment;

// Memory amount (in GB) assigned to the buffer process.
uint64_t buffer_KB;
// Flag stating that the previous parameter has been received.
int32_t size_received;
// Communication resources in order to retrieve the buffer_GB parameter.
pthread_mutex_t buff_size_mut;
pthread_cond_t buff_size_cond;
int32_t copied;

uint64_t BLOCK_SIZE; // In KB

StsHeader *mem_pool;

// URI of the attached deployment.
char att_imss_uri[URI_];

static long iov_cnt = 1;

// Map that stores server side endpoints
void *map_server_eps;

pthread_mutex_t tree_mut = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t mp = PTHREAD_MUTEX_INITIALIZER;

ucp_worker_h *ucp_worker_threads;
ucp_address_t **local_addr;
size_t *local_addr_len;

extern int IMSS_THREAD_POOL;

#define GARBAGE_COLLECTOR_PERIOD 120

// Thread method attending client read-write data requests.
void *srv_worker(void *th_argv)
{
	ucp_ep_params_t ep_params;

	ucp_am_handler_param_t param;
	ucs_status_t status;
	int ret = 0;
	p_argv *arguments = (p_argv *)th_argv;

	ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS |
						   UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE |
						   UCP_EP_PARAM_FIELD_ERR_HANDLER |
						   UCP_EP_PARAM_FIELD_USER_DATA;
	ep_params.err_mode = UCP_ERR_HANDLING_MODE_NONE;
	ep_params.err_handler.cb = err_cb_server;
	// ep_params.err_handler.arg = NULL;

	map_server_eps = map_server_eps_create();

	BLOCK_SIZE = arguments->blocksize * 1024;

	for (;;)
	{
		size_t peer_addr_len;
		ucp_address_t *peer_addr;
		ucs_status_t ep_status = UCS_OK;
		ucp_ep_h ep;
		struct ucx_context *request = NULL;
		char *req;
		ucp_tag_recv_info_t info_tag;
		ucp_tag_message_h msg_tag;
		msg_req_t *msg;
		ucp_request_param_t recv_param;

		clock_t t;
		double time_taken;
		t = clock();

		do
		{
			/* Progressing before probe to update the state */
			TIMING(ucp_worker_progress(arguments->ucp_worker), "[srv_worker]ucp_worker_progress", unsigned int);
			/* Probing incoming events in non-block mode */
			msg_tag = ucp_tag_probe_nb(arguments->ucp_worker, tag_req, tag_mask, 1, &info_tag);
		} while (msg_tag == NULL);

		slog_debug("[srv_worker] Message length=%ld bytes.", info_tag.length);
		msg = (msg_req_t *)malloc(info_tag.length);

		recv_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
								  UCP_OP_ATTR_FIELD_DATATYPE;

		recv_param.datatype = ucp_dt_make_contig(1);
		recv_param.cb.recv = recv_handler;

		request = (struct ucx_context *)TIMING(ucp_tag_msg_recv_nbx(arguments->ucp_worker, msg, info_tag.length, msg_tag, &recv_param), "[srv_worker]ucp_tag_msg_recv_nbx", ucs_status_ptr_t);

		status = TIMING(ucx_wait(arguments->ucp_worker, request, "receive", "srv_worker"), "[srv_worker]ucx_wait", ucs_status_t);

		peer_addr_len = msg->addr_len;
		peer_addr = (ucp_address *)malloc(peer_addr_len);
		req = msg->request;

		TIMING(memcpy(peer_addr, msg + 1, peer_addr_len), "[srv_worker]memcpy", void *);

		ucp_worker_address_attr_t attr;
		attr.field_mask = UCP_WORKER_ADDRESS_ATTR_FIELD_UID;
		ucp_worker_address_query(peer_addr, &attr);
		// slog_debug("[srv_worker_thread] Receiving request from %" PRIu64 ".", attr.worker_uid);

		//  look for this peer_addr in the map and get the ep
		ret = map_server_eps_search(map_server_eps, attr.worker_uid, &ep);
		// create ep if it's not in the map
		if (ret < 0)
		{
			// ucp_ep_h new_ep;
			ep_params.address = peer_addr;
			ep_params.user_data = &ep_status;
			// struct worker_info *worker_info = (struct worker_info*)malloc(sizeof(struct worker_info));
			// worker_info->worker_uid = attr.worker_uid;
			// worker_info->server_type = 'd';
			// ep_params.err_handler.arg = &worker_info;
			ep_params.err_handler.arg = &attr.worker_uid;

			status = ucp_ep_create(arguments->ucp_worker, &ep_params, &ep);
			// add ep to the map
			map_server_eps_put(map_server_eps, attr.worker_uid, ep);
		}
		else
		{
			slog_debug("\t[srv_worker]['%" PRIu64 "] Endpoint already exist'\n", attr.worker_uid);
			// fprintf(stderr, "\t[d]['%" PRIu64 "] Endpoint already exist'\n", attr.worker_uid);
		}

		arguments->peer_address = peer_addr;
		arguments->server_ep = ep;
		arguments->worker_uid = attr.worker_uid;

		char msg_[2024];
		sprintf(msg_, "[srv_worker] srv_worker_helper req %s", req);
		TIMING(srv_worker_helper(arguments, req), msg_, int);
		t = clock() - t;

		time_taken = ((double)t) / CLOCKS_PER_SEC; // in seconds
		slog_info("[srv_worker] Serving time %f s", time_taken);

		free(peer_addr);

		// fprintf(stderr, "\t[d]['%" PRIu64 "'] Ending srv worker\n", attr.worker_uid);

		// flush_ep(arguments->ucp_worker, ep);
		// ep_close(arguments->ucp_worker, arguments->server_ep, UCP_EP_CLOSE_MODE_FLUSH);
	}
}

int srv_worker_helper(p_argv *arguments, const char *req)
{
	// slog_init("workers", SLOG_INFO, 1, 0, 1, 1, 1);
	// std::unique_lock<std::mutex> lock(*mut2);

	ucs_status_t status;
	int ret = -1;

	// Cast from generic pointer type to p_argv struct type pointer.

	// Obtain the current map class element from the set of arguments.
	std::shared_ptr<map_records> map = arguments->map;

	// Resources specifying if the ZMQ_SNDMORE flag was set in the sender.
	int64_t more;
	size_t more_size = sizeof(more);

	// Code to be sent if the requested to-be-read key does not exist.
	char err_code[] = "$ERRIMSS_NO_KEY_AVAIL$";

	char mode[MODE_SIZE];

	slog_debug("[srv_worker_thread] Waiting for new request.");
	// Save the request to be served.
	// TIMING(ret = recv_data(arguments->ucp_worker, arguments->server_ep, req), "[srv_worker_thread] Save the request to be served");
	slog_debug("[srv_worker_thread] request to be served %s", req);

	// slog_info("********** %d",ret);

	// Elements conforming the request.
	uint32_t block_size_recv, block_offset;
	char uri_[URI_];
	size_t to_read = 0;

	sscanf(req, "%s %" PRIu32 " %" PRIu32 " %s %lu", mode, &block_size_recv, &block_offset, uri_, &to_read);

	if (!strcmp(mode, "GET"))
		more = GET_OP;
	else
		more = SET_OP;

	slog_debug("[srv_worker_thread] Request - mode '%s', block_size_recv '%" PRIu32 "', block_offset '%" PRIu32 "', uri_ '%s', more %ld", mode, block_size_recv, block_offset, uri_, more);

	// Create an std::string in order to be managed by the map structure.
	std::string key;
	key.assign((const char *)uri_);

	// Information associated to the arriving key.
	char *address_;
	uint64_t block_size_rtvd;

	// Differentiate between READ and WRITE operations.
	switch (more)
	{
	// No more messages will arrive to the socket.
	case READ_OP:
	{
		switch (block_size_recv)
		{
		case READ_OP:
		{
			int ret = TIMING(map->get(key, &address_, &block_size_rtvd), "[srv_worker_helper][READ_OP]map->get", int);
			// Check if there was an associated block to the key.
			if (ret == 0)
			{
				// Send the error code block.
				ret = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid);
				if (ret < 0)
				{
					perror("ERRIMSS_WORKER_SENDERR");
					return -1;
				}
			}
			else
			{
				// Send the requested block.
				// ret = TIMING(send_data(arguments->ucp_worker, arguments->server_ep, address_, block_size_rtvd, arguments->worker_uid), "[srv_worker_thread][READ_OP][READ_OP] Send the requested block");
				if (to_read <= 0)
				{
					to_read = block_size_rtvd;
				}
				slog_debug("[srv_worker_thread][READ_OP][READ_OP] Send the requested block with key=%s, block_offset=%ld, block_size_rtvd=%ld kb, to_read=%ld kb", key.c_str(), block_offset, block_size_rtvd / 1024, to_read / 1024);
				ret = TIMING(send_data(arguments->ucp_worker, arguments->server_ep, address_ + block_offset, to_read, arguments->worker_uid), "[srv_worker_helper][READ_OP]send_data", uint64_t);
				// fprintf(stderr,"\tblock_size_rtvd=%ld, address_=%s\n", block_size_rtvd, address_);
				if (ret < 0)
				{
					perror("ERRIMSS_WORKER_SENDBLOCK");
					return -1;
				}
			}
			break;
		}
		case RELEASE:
		{
			map_server_eps_erase(map_server_eps, arguments->worker_uid);
			slog_debug("[srv_worker_thread][READ_OP][RELEASE]");
			// return 0;
			break;
		}
		case RENAME_OP:
		{
			std::size_t found = key.find(',');
			slog_debug("[srv_worker_thread][RENAME_OP], key=%s, found=%d", key.c_str(), found);
			if (found != std::string::npos)
			{
				slog_debug("[srv_worker_thread][RENAME_OP], found != npos");
				std::string old_key = key.substr(0, found);
				std::string new_key = key.substr(found + 1, key.length());
				slog_debug("[srv_worker_thread][RENAME_OP], old_key=%s, new_key=%s", old_key.c_str(), new_key.c_str());
				// RENAME MAP
				map->cleaning_specific(new_key);
				int32_t result = map->rename_data_srv_worker(old_key, new_key);
				if (result == 0)
				{
					break;
				}
			}
			else
			{
				slog_debug("[srv_worker_thread][RENAME_OP], found == npos");
			}

			char release_msg[] = "RENAME\0";
			ret = TIMING(send_data(arguments->ucp_worker, arguments->server_ep, release_msg, RESPONSE_SIZE, arguments->worker_uid), "[srv_worker_thread][READ_OP][RENAME_OP] Send rename", int);
			if (ret < 0)
			{
				perror("ERRIMSS_PUBLISH_RENAMEMSG");
				return -1;
			}
			break;
		}
		case RENAME_DIR_DIR_OP:
		{
			// printf("SRV_WORKER RENAME_DIR_DIR_OP");
			std::size_t found = key.find(' ');
			if (found != std::string::npos)
			{
				std::string old_dir = key.substr(0, found);
				std::string rdir_dest = key.substr(found + 1, key.length());

				// RENAME MAP
				map->rename_data_dir_srv_worker(old_dir, rdir_dest);
			}

			char release_msg[] = "RENAME\0";
			ret = TIMING(send_data(arguments->ucp_worker, arguments->server_ep, release_msg, RESPONSE_SIZE, arguments->worker_uid), "[srv_worker_thread][READ_OP][RENAME_DIR_DIR_OP] Send rename", int);
			if (ret < 0)
			{
				perror("ERRIMSS_PUBLISH_RENAMEMSG");
				return 1;
			}
			break;
		}
		case READV: // Only 1 server work
		{
			// printf("READV CASE");
			std::size_t found = key.find('$');
			std::string path;
			if (found != std::string::npos)
			{
				path = key.substr(0, found + 1);
				// std::cout <<"path:" << path << '';
				key.erase(0, found + 1);
				std::size_t found = key.find(' ');
				int curr_blk = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				found = key.find(' ');
				int end_blk = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				found = key.find(' ');
				int blocksize = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				found = key.find(' ');
				int start_offset = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				found = key.find(' ');
				int64_t size = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				// Needed variables
				size_t byte_count = 0;
				int first = 0;
				int ds = 0;
				int64_t to_copy = 0;
				uint32_t filled = 0;
				size_t to_read = 0;

				int pos = path.find('$');
				std::string first_element = path.substr(0, pos + 1);
				first_element = first_element + std::to_string(0);
				// printf("first_element=%s",first_element.c_str());
				map->get(first_element, &address_, &block_size_rtvd);
				struct stat *stats = (struct stat *)address_;
				char *buf = (char *)malloc(size);

				while (curr_blk <= end_blk)
				{
					std::string element = path;
					element = element + std::to_string(curr_blk);
					// std::cout <<"SERVER READV element:" << element << '';
					if (map->get(element, &address_, &block_size_rtvd) == 0)
					{ // If dont exist
						// Send the error code block.
						// std::cout <<"SERVER READV NO EXISTE element:" << element << '';
						ret = TIMING(send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid), "[srv_worker_thread][READ_OP][READV] send_dynamic_stream", int);
						if (ret < 0)
						{
							perror("ERRIMSS_WORKER_SENDERR");
							return -1;
						}
					} // If was already stored:
					else
					{
						// First block case
						if (first == 0)
						{
							if (size < stats->st_size - start_offset)
							{
								// to_read = size;
								to_read = blocksize * KB - start_offset;
							}
							else
							{
								if (stats->st_size < blocksize * KB)
								{
									to_read = stats->st_size - start_offset;
								}
								else
								{
									to_read = blocksize * KB - start_offset;
								}
							}
							// Check if offset is bigger than filled, return 0 because is EOF case
							if (start_offset > stats->st_size)
								return 0;
							memcpy(buf, address_ + start_offset, to_read);
							byte_count += to_read;
							++first;

							// Middle block case
						}
						else if (curr_blk != end_blk)
						{
							memcpy(buf + byte_count, address_, blocksize * KB);
							byte_count += blocksize * KB;
							// End block case
						}
						else
						{

							// Read the minimum between end_offset and filled (read_ = min(end_offset, filled))
							int64_t pending = size - byte_count;
							memcpy(buf + byte_count, address_, pending);
							byte_count += pending;
						}
					}
					++curr_blk;
				}
				ret = TIMING(send_data(arguments->ucp_worker, arguments->server_ep, buf, size, arguments->worker_uid), "[srv_worker_thread][READ_OP][READV] send", int);
				// Send the requested block.
				if (ret < 0)
				{
					perror("ERRIMSS_WORKER_SENDBLOCK");
					return -1;
				}
			}
			break;
		}
		case SPLIT_READV:
		{
			// printf("SPLIT_READV CASE");
			slog_debug("key=%s", key.c_str());
			std::size_t found = key.find(' ');
			std::string path;
			if (found != std::string::npos)
			{
				path = key.substr(0, found);
				key.erase(0, found + 1);

				found = key.find(' ');
				int blocksize = stoi(key.substr(0, found)) * KB;
				key.erase(0, found + 1);

				found = key.find(' ');
				int start_offset = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				found = key.find(' ');
				int stats_size = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				int msg_size = stoi(key.substr(0, found));

				char *msg = (char *)calloc(msg_size, sizeof(char));

				ret = TIMING(recv_data(arguments->ucp_worker, arguments->server_ep, msg, arguments->worker_uid, 0), "[srv_worker_thread][READ_OP][SPLIT_READV] recv_data", size_t);

				// Send the requested block.
				if (ret < 0)
				{
					perror("ERRIMSS_WORKER_SENDBLOCK");
					return -1;
				}

				key = msg;
				found = key.find('$');
				int amount = stoi(key.substr(0, found));
				int size = amount * blocksize;
				key.erase(0, found + 1);

				slog_debug("msg=%s", key.c_str());
				slog_debug("msg_size=%d", msg_size);
				slog_debug("*path=%s", path.c_str());
				slog_debug("*blocksize=%d", blocksize);
				slog_debug("*start_offset=%d", start_offset);
				slog_debug("*size=%d", size);
				slog_debug("*amount=%d", amount);

				char *buf = (char *)malloc(size);
				// Needed variables
				size_t byte_count = 0;
				int first = 0;
				int ds = 0;
				int64_t to_copy = 0;
				uint32_t filled = 0;
				size_t to_read = 0;
				int curr_blk = 0;

				for (int i = 0; i < amount; i++)
				{
					// substract current block
					found = key.find('$');
					int curr_blk = stoi(key.substr(0, found));
					key.erase(0, found + 1);

					std::string element = path;
					element = element + '$' + std::to_string(curr_blk);
					if (map->get(element, &address_, &block_size_rtvd) == 0)
					{ // If dont exist
						// Send the error code block.
						if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid) < 0)
						{
							return -1;
							pthread_exit(NULL);
						}
					} // If was already stored:

					memcpy(buf + byte_count, address_, blocksize);
					byte_count += blocksize;
				}
				ret = TIMING(send_data(arguments->ucp_worker, arguments->server_ep, buf, byte_count, arguments->worker_uid), "[srv_worker_thread][READ_OP][READV] send buf", int);
				// Send the requested block.
				if (ret < 0)
				{
					perror("ERRIMSS_WORKER_SENDBLOCK");
					return -1;
				}
			}
			break;
		}
		case WHO:
		{
			// Provide the uri of this instance.
			ret = TIMING(send_data(arguments->ucp_worker, arguments->server_ep, arguments->my_uri, strlen(arguments->my_uri), arguments->worker_uid), ("[srv_worker_thread][READ_OP][WHO] send uri: %s", arguments->my_uri), int);
			if (ret < 0)
			{
				perror("ERRIMSS_WHOREQUEST");
				return -1;
			}
			break;
		}
		default:
			break;
		}
		break;
	}
		// More messages will arrive to the socket.
	case WRITE_OP:
	{
		// std::cout <<"WRITE_OP key:" << key << '';
		int op;
		std::size_t found = key.find(' ');
		std::size_t found2 = key.find("[OP]=");
		slog_debug("[srv_worker_thread][WRITE_OP] found=%d, found2=%d", found, found2);
		if (found2 != std::string::npos)
		{
			slog_debug("[srv_worker_thread][WRITE_OP] Entra en found2");
			op = stoi(key.substr(found2 + 5, (found - (found2 + 5))));
			key.erase(0, found + 1);
		}

		// if (found!=std::string::npos){
		if (found != std::string::npos && found2 == std::string::npos)
		{
			std::string path = key.substr(0, found);
			key.erase(0, found + 1);
			// std::cout <<"path:" << key << '';

			std::size_t found = key.find(' ');
			int curr_blk = stoi(key.substr(0, found));
			key.erase(0, found + 1);

			found = key.find(' ');
			int end_blk = stoi(key.substr(0, found));
			key.erase(0, found + 1);

			found = key.find(' ');
			int start_offset = stoi(key.substr(0, found));
			key.erase(0, found + 1);

			found = key.find(' ');
			int end_offset = stoi(key.substr(0, found));
			key.erase(0, found + 1);

			found = key.find(' ');
			int IMSS_DATA_BSIZE = stoi(key.substr(0, found));
			key.erase(0, found + 1);

			int size = stoi(key);

			char *buf = (char *)malloc(size);

			// Receive all blocks into the buffer.
			recv_data(arguments->ucp_worker, arguments->server_ep, buf, arguments->worker_uid, 0);

			// printf("WRITEV-buffer=%s",buf);
			int pos = path.find('$');
			std::string first_element = path.substr(0, pos + 1);
			first_element = first_element + "0";
			map->get(first_element, &address_, &block_size_rtvd);
			// imss_info * data = (imss_info *) address_;
			// printf("READ_OP SEND data->type=%c",data->type);
			struct stat *stats = (struct stat *)address_;

			// Needed variables
			size_t byte_count = 0;
			int first = 0;
			int ds = 0;
			int64_t to_copy = 0;
			uint32_t filled = 0;
			char *aux = (char *)malloc(IMSS_DATA_BSIZE);
			int count = 0;
			// For the rest of blocks
			while (curr_blk <= end_blk)
			{
				// printf("Nodename    - %s current_block=%d", detect.nodename, curr_blk);
				count = count + 1;
				// printf("count=%d",count);
				pos = path.find('$');
				std::string element = path.substr(0, pos + 1);
				element = element + std::to_string(curr_blk);
				// std::cout <<"element:" << element << '';

				// First fragmented block
				if (first == 0 && start_offset && stats->st_size != 0)
				{
					// Get previous block
					map->get(element, &aux, &block_size_rtvd); // path por curr_block
					// Bytes to write are the minimum between the size parameter and the remaining space in the block (BLOCKSIZE-start_offset)
					to_copy = (size < IMSS_DATA_BSIZE - start_offset) ? size : IMSS_DATA_BSIZE - start_offset;

					memcpy(aux + start_offset, buf + byte_count, to_copy);
				}
				// Last Block
				else if (curr_blk == end_blk)
				{
					if (end_offset != 0)
					{
						to_copy = end_offset;
					}
					else
					{
						to_copy = IMSS_DATA_BSIZE;
					}
					// Only if last block has contents
					if (curr_blk <= stats->st_blocks && start_offset)
					{
						map->get(element, &aux, &block_size_rtvd); // path por curr_block
					}
					else
					{
						memset(aux, 0, IMSS_DATA_BSIZE);
					}
					if (byte_count == size)
					{
						to_copy = 0;
					}
					// printf("curr_block=%d, end_block=%d, byte_count=%d",curr_blk, end_blk, byte_count);
					memcpy(aux, buf + byte_count, to_copy);
				}
				// middle block
				else
				{
					to_copy = IMSS_DATA_BSIZE;
					memcpy(aux, buf + byte_count, to_copy);
				}

				// Write and update variables
				if (!map->get(element, &address_, &block_size_rtvd))
				{
					map->put(element, aux, block_size_rtvd);

					// printf("Nodename    - %s after put", detect.nodename);
				}
				else
				{
					memcpy(address_, aux, block_size_rtvd);
				}
				// printf("currblock=%d, byte_count=%d",curr_blk, byte_count);
				byte_count += to_copy;
				++curr_blk;
				++first;
			}
			int16_t off = (end_blk * IMSS_DATA_BSIZE) - 1 - size;
			if (size + off > stats->st_size)
			{
				stats->st_size = size + off;
				stats->st_blocks = curr_blk - 1;
			}

			free(buf);
		}
		else if (found != std::string::npos && op == 2)
		{
			std::string path;
			std::size_t found = key.find(' ');
			// printf("Nodename	-%s SPLIT WRITEV",detect.nodename);

			path = key.substr(0, found);
			key.erase(0, found + 1);

			found = key.find(' ');
			int blocksize = stoi(key.substr(0, found)) * KB;
			key.erase(0, found + 1);

			found = key.find(' ');
			int start_offset = stoi(key.substr(0, found));
			key.erase(0, found + 1);

			found = key.find(' ');
			int stats_size = stoi(key.substr(0, found));
			key.erase(0, found + 1);

			found = key.find('$');
			int amount = stoi(key.substr(0, found));
			int size = amount * blocksize;
			key.erase(0, found + 1);

			slog_debug("amount=%d", amount);
			slog_debug("path=%s", path.c_str());
			slog_debug("blocksize=%d", blocksize);
			slog_debug("start_offset=%d", start_offset);
			slog_debug("size=%d", size);
			slog_debug("rest=%s", key.c_str());

			// Receive all blocks into the buffer.
			char *buf = (char *)malloc(size);
			// int size_recv = -1;
			ret = TIMING(recv_data(arguments->ucp_worker, arguments->server_ep, buf, arguments->worker_uid, 0), "[srv_worker_thread][WRITE_OP]", int);
			if (ret < 0)
			{
				perror("ERRIMSS_WRITE_OP_BUF");
				return -1;
			}

			// size_recv = size; // MIRAR
			int32_t insert_successful;

			// printf("Nodename	-%s size_recv=%d",detect.nodename,size_recv);
			// printf("Salida buf full=%c",buf[100]);

			int32_t byte_count = 0;
			for (int i = 0; i < amount; i++)
			{
				// substract current block
				found = key.find('$');
				int curr_blk = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				std::string element = path;
				element = element + '$' + std::to_string(curr_blk);
				// printf(" element=%s",element.c_str());

				if (map->get(element, &address_, &block_size_rtvd) == 0)
				{
					// If don't exist
					char *buffer = (char *)aligned_alloc(1024, blocksize);
					memcpy(buffer, buf + byte_count, blocksize);
					// printf("Salida buffer part=%c",buffer[100]);
					insert_successful = map->put(element, buffer, block_size_recv);
					if (insert_successful != 0)
					{
						perror("ERRIMSS_WORKER_MAPPUT");
						return -1;
					}
				}
				else
				{
					// If already exits
					memcpy(address_, buf + byte_count, blocksize);
					// printf("Alreadt exitsSalida buffer part=%c",buf[100]);
				}
				byte_count = byte_count + blocksize;
				// printf("Nodename	-%s byte_count=%d",detect.nodename,byte_count);
			}
		}
		else
		{
			slog_debug("[srv_worker_thread][WRITE_OP] WRITE NORMAL CASE. Size %ld, offset=%ld", block_size_recv, block_offset);
			// search for the block to know if it was previously stored.
			int ret = map->get(key, &address_, &block_size_rtvd);

			// if the block was not already stored:
			if (ret == 0)
			{
				// fprintf(stderr,"[srv_worker_thread][WRITE_OP] NO key find %s\n", key.c_str());
				slog_debug("[srv_worker_thread][WRITE_OP] NO key find %s", key.c_str());
				clock_t tp;
				tp = clock();
				char *buffer = (char *)StsQueue.pop(mem_pool);
				tp = clock() - tp;
				double time_taken2 = ((double)tp) / CLOCKS_PER_SEC; // in seconds
				// slog_info("[srv_worker_helper] pop time %f s", time_taken2);
				//  Receive the block into the buffer.
				// if (buffer == NULL)
				// char *buffer = (char *)malloc(block_size_recv);
				clock_t tr;
				// TIMING(recv_data(arguments->ucp_worker, arguments->server_ep, buffer, arguments->worker_uid, 1), "[srv_worker_thread][WRITE_OP] recv_data: Receive the block into the buffer.");
				recv_data(arguments->ucp_worker, arguments->server_ep, buffer + block_offset, arguments->worker_uid, 1);
				// sleep(5);
				struct stat *stats = (struct stat *)buffer;
				int32_t insert_successful;

				// Include the new record in the tracking structure.
				tr = clock();
				// fprintf(stderr,"[srv_worker_thread][WRITE_OP] ****[PUT]********* key=%s\n",  key.c_str());
				slog_debug("[srv_worker_thread][WRITE_OP] ****[PUT, block_size_recv=%ld, stats->st_size=%ld]********* key=%s", block_size_recv, stats->st_size, key.c_str());
				// TODO: should this be block_size_recv or a different size? block_size_recv might not be the full block size
				// insert_successful = map->put(key, buffer, block_size_recv);
				//  fprintf(stderr, "BLOCK_SIZE=%ld", BLOCK_SIZE);
				insert_successful = map->put(key, buffer, BLOCK_SIZE);
				slog_debug("[srv_worker_thread][WRITE_OP] insert_successful %d key=%s", insert_successful, key.c_str());
				// map->get(key, &address_, &block_size_rtvd);
				// fprintf(stderr,"****[PUT2]********* key=%s\n",  key.c_str());

				tr = clock() - tr;

				double time_taken = ((double)tr) / CLOCKS_PER_SEC; // in seconds
				// slog_info("[srv_worker_helper] put time %f s", time_taken);

				// fprintf(stderr, "-- %s\n", buffer);

				// Include the new record in the tracking structure.
				if (insert_successful != 0)
				{
					slog_error("[srv_worker_thread][WRITE_OP] ERRIMSS_WORKER_MAPPUT");
					perror("ERRIMSS_WORKER_MAPPUT");
					return -1;
				}

				// Update the pointer.
				arguments->pt += block_size_recv;
			}
			// if the block was already stored:
			else
			{
				// fprintf(stderr,"[srv_worker_thread][WRITE_OP] Key find %s\n", key.c_str());
				slog_debug("[srv_worker_thread][WRITE_OP] Key find %s", key.c_str());
				// Receive the block into the buffer.
				// fprintf(stderr, "---- %s\n", address_);
				std::size_t found = key.find("$0");
				if (found != std::string::npos)
				{
					slog_debug("[srv_worker_thread][WRITE_OP] Updating block $0 (%d)", block_size_rtvd);
					struct stat *old, *latest;

					// TODO: make sure this works
					char *buffer = (char *)malloc(block_size_recv);

					// TIMING(recv_data(arguments->ucp_worker, arguments->server_ep, buffer, arguments->worker_uid, 0), "[srv_worker_thread][WRITE_OP] recv_data Updating block $0");
					recv_data(arguments->ucp_worker, arguments->server_ep, buffer, arguments->worker_uid, 0);
					old = (struct stat *)address_;
					latest = (struct stat *)buffer;
					slog_debug("[srv_worker_thread] File size new %ld old %ld", latest->st_size, old->st_size);
					latest->st_size = std::max(latest->st_size, old->st_size);
					slog_debug("[srv_worker_thread] buffer: %ld", latest->st_size);

					// TODO: make sure this works
					memcpy(address_ + block_offset, buffer, block_size_recv);
					// TODO: should we update this block's size in the map?

					// slog_debug("address_=%x", address_);
					// free(buffer);
				}
				else
				{
					// TIMING(recv_data(arguments->ucp_worker, arguments->server_ep, address_, arguments->worker_uid, 1), ("[srv_worker_thread][WRITE_OP] recv_data Updated non 0 existing block"));
					slog_debug("[srv_worker_thread][WRITE_OP] Updated non 0 existing block, key.c_str(): %s", key.c_str());
					recv_data(arguments->ucp_worker, arguments->server_ep, address_ + block_offset, arguments->worker_uid, 1);
					// slog_debug("address_=%x", address_);
				}
			}
			break;
		}
	}
	default:
		break;
	}

	slog_debug("[srv_worker_thread] Terminated data helper");

	return 0;
}

// Thread method searching and cleaning nodes with st_nlink=0
void *garbage_collector(void *th_argv)
{
	// Obtain the current map class element from the set of arguments.
	map_records *map = (map_records *)th_argv;

	for (;;)
	{
		// Gnodetraverse_garbage_collector(map);//Future
		sleep(GARBAGE_COLLECTOR_PERIOD);
		pthread_mutex_lock(&mutex_garbage);
		map->cleaning();
		pthread_mutex_unlock(&mutex_garbage);
	}
	pthread_exit(NULL);
}

// Thread method attending client read-write metadata requests.
void *stat_worker(void *th_argv)
{
	ucp_am_handler_param_t param;
	ucs_status_t status;
	int ret = 0;

	p_argv *arguments = (p_argv *)th_argv;

	map_server_eps = map_server_eps_create();

	for (;;)
	{
		size_t peer_addr_len;
		ucp_address_t *peer_addr;
		ucs_status_t ep_status = UCS_OK;
		ucp_ep_h ep;
		struct ucx_context *request = NULL;
		char *req;
		ucp_tag_recv_info_t info_tag;
		ucp_tag_message_h msg_tag;
		msg_req_t *msg;
		ucp_request_param_t recv_param;

		do
		{
			/* Progressing before probe to update the state */
			ucp_worker_progress(arguments->ucp_worker);
			/* Probing incoming events in non-block mode */
			msg_tag = ucp_tag_probe_nb(arguments->ucp_worker, tag_req, tag_mask, 1, &info_tag);
		} while (msg_tag == NULL);

		msg = (msg_req_t *)malloc(info_tag.length); // Should the msg memory be free?
		memset(msg, 0, info_tag.length);

		recv_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
								  UCP_OP_ATTR_FIELD_DATATYPE |
								  UCP_OP_ATTR_FLAG_NO_IMM_CMPL;
		recv_param.datatype = ucp_dt_make_contig(1);
		recv_param.cb.recv = recv_handler;

		request = (struct ucx_context *)ucp_tag_msg_recv_nbx(arguments->ucp_worker, msg, info_tag.length, msg_tag, &recv_param);

		status = ucx_wait(arguments->ucp_worker, request, "receive", "stat_worker");

		peer_addr_len = msg->addr_len;
		peer_addr = (ucp_address *)malloc(peer_addr_len);
		req = msg->request;

		memcpy(peer_addr, msg + 1, peer_addr_len);

		ucp_worker_address_attr_t attr;
		attr.field_mask = UCP_WORKER_ADDRESS_ATTR_FIELD_UID;
		ucp_worker_address_query(peer_addr, &attr);
		slog_debug("[stat_worker] Receiving request from %" PRIu64 ".", attr.worker_uid);

		//  look for this peer_addr in the map and get the ep
		ret = map_server_eps_search(map_server_eps, attr.worker_uid, &ep);
		// create ep if it's not in the map
		if (ret < 0)
		{
			ucp_ep_params_t *ep_params;

			ep_params = (ucp_ep_params_t *)malloc(sizeof(ucp_ep_params_t));
			ep_params->field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS |
									UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE |
									UCP_EP_PARAM_FIELD_ERR_HANDLER |
									UCP_EP_PARAM_FIELD_USER_DATA;
			ep_params->err_mode = UCP_ERR_HANDLING_MODE_NONE;
			ep_params->err_handler.cb = err_cb_server;
			// ep_params->err_handler.arg = NULL;

			// struct worker_info *worker_info = (struct worker_info*)malloc(sizeof(struct worker_info));
			// worker_info->worker_uid = attr.worker_uid;
			// worker_info->server_type = 'm';
			// ep_params->err_handler.arg = &worker_info;
			ep_params->err_handler.arg = &attr.worker_uid;

			// ucp_ep_h new_ep;
			ep_params->address = peer_addr;
			ep_params->user_data = &ep_status;
			status = ucp_ep_create(arguments->ucp_worker, ep_params, &ep);
			// ucp_ep_print_info(ep, stderr);
			//  add ep to the map
			map_server_eps_put(map_server_eps, attr.worker_uid, ep);
		}
		else
		{
			// fprintf(stderr, "\t[m]['%" PRIu64 "'] Endpoint already exist\n", attr.worker_uid);
			slog_debug("\t[stat_worker]['%" PRIu64 "'] Endpoint already exist", attr.worker_uid);
		}

		arguments->peer_address = peer_addr;
		arguments->server_ep = ep;
		arguments->worker_uid = attr.worker_uid;
		// arguments->worker_uid = attr.worker_uid;
		stat_worker_helper(arguments, req);

		// ucp_ep_close_nb(ep, UCP_EP_CLOSE_MODE_FORCE);
		free(peer_addr);

		// ep_close(arguments->ucp_worker, arguments->server_ep, UCP_EP_CLOSE_MODE_FLUSH);
	}
}

int stat_worker_helper(p_argv *arguments, char *req)
{
	ucs_status_t status;
	int ret;

	// Obtain the current map class element from the set of arguments.
	std::shared_ptr<map_records> map = arguments->map;

	uint16_t current_offset = 0;

	// Resources specifying if the ZMQ_SNDMORE flag was set in the sender.
	int64_t more;
	size_t more_size = sizeof(more);

	// Code to be sent if the requested to-be-read key does not exist.
	char err_code[] = "$ERRIMSS_NO_KEY_AVAIL$";

	/*struct timeval start, end;
	  long delta_us;*/

	int client_id = 0;
	char mode[MODE_SIZE];

	// slog_debug("[STAT WORKER] Waiting for new request.");
	// Save the request to be served.
	// recv_data(arguments->ucp_worker, arguments->server_ep, req);
	// slog_info("[STAT WORKER] Request - '%s'", req);
	sscanf(req, "%" PRIu32 " %s", &client_id, mode);

	char *req_content = strstr(req, mode);
	req_content += 4;

	if (!strcmp(mode, "GET"))
		more = GET_OP;
	else
		more = SET_OP;

	// Expeted incomming message format: "SIZE_IN_KB KEY"
	int32_t req_size = strlen(req_content);

	char raw_msg[req_size + 1];
	memcpy((void *)raw_msg, req_content, req_size);
	raw_msg[req_size] = '\0';

	// printf("*********worker_metadata raw_msg %s",raw_msg);
	slog_info("[workers][stat_worker_helper] request received=%s", req);

	// Reference to the client request.
	char number[16];
	sscanf(raw_msg, "%s", number);
	int32_t number_length = (int32_t)strlen(number);
	// Elements conforming the request.
	char *uri_ = raw_msg + number_length + 1;
	uint64_t block_size_recv = (uint64_t)atoi(number);

	slog_info("[workers][stat_worker_helper] number=%s, number_length=%d uri=%s, block_size_recv=%ld", number, number_length, uri_, block_size_recv);

	// Create an std::string in order to be managed by the map structure.
	std::string key;
	key.assign((const char *)uri_);

	// Information associated to the arriving key.
	char *address_;
	uint64_t block_size_rtvd;
	// printf("stat_worker RECV more=%ld, blocksss=%ld",more, block_size_recv);
	// Differentiate between READ and WRITE operations.

	switch (more)
	{
	// No more messages will arrive to the socket.
	case GET_OP:
	{
		switch (block_size_recv)
		{
		case GETDIR:
		{
			// slog_fatal("stat_server GETDIR key=%s",key.c_str());
			char *buffer;
			int32_t numelems_indir;
			// Retrieve all elements inside the requested directory.
			pthread_mutex_lock(&tree_mut);
			buffer = GTree_getdir((char *)key.c_str(), &numelems_indir);
			pthread_mutex_unlock(&tree_mut);
			if (buffer == NULL)
			{
				if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid) < 0)
				{
					perror("ERRIMSS_STATWORKER_NODIR");
					return -1;
				}
				break;
			}

			// Send the serialized set of elements within the requested directory.
			msg_t m;
			m.size = numelems_indir * URI_;
			m.data = buffer;

			if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, (char *)&m, MSG, arguments->worker_uid) < 0)
			{
				perror("ERRIMSS_WORKER_SENDBLOCK");
				return -1;
			}

			free(buffer);
			break;
		}
		case READ_OP:
		{
			// printf("STAT_WORKER READ_OP");
			// Check if there was an associated block to the key.
			int err = map->get(key, &address_, &block_size_rtvd);
			slog_debug("[STAT WORKER] map->get (key %s, block_size_rtvd %ld) get res %d", key.c_str(), block_size_rtvd, err);

			if (err == 0)
			{
				// Send the error code block.
				if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid) < 0)
				{
					perror("ERRIMSS_WORKER_SENDERR");
					return -1;
				}
			}
			else
			{
				// dataset_info *dataset = (dataset_info*) address_;
				// printf("[STAT_SERVER] dataset.original=%s",dataset->original_name);
				// imss_info * data = (imss_info *) address_;
				// printf("READ_OP SEND data->type=%c",data->type);
				// Send the requested block.
				msg_t m;
				m.data = address_;
				m.size = block_size_rtvd;
				err = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, (char *)&m, MSG, arguments->worker_uid);
				if (err < 0)
				{
					perror("ERRIMSS_WORKER_SENDBLOCK");
					return -1;
				}
			}

			break;
		}

		case RELEASE:
		{
			slog_debug("[stat_worker_thread][READ_OP][RELEASE] Deleting endpoint with %" PRIu64 "", arguments->worker_uid);
			map_server_eps_erase(map_server_eps, arguments->worker_uid);
			// ucp_destroy(arguments->ucp_context);
			slog_debug("[stat_worker_thread][READ_OP][RELEASE] Endpoints deleted ");
			break;
			// return 0;
		}
		case DELETE_OP:
		{
			int32_t result = map->delete_metadata_stat_worker(key);
			slog_debug("[stat_worker_thread][READ_OP][DELETE_OP] delete_metadata_stat_worker=%d", result);
			GTree_delete((char *)key.c_str());

			char release_msg[] = "DELETE\0";

			if (send_data(arguments->ucp_worker, arguments->server_ep, release_msg, RESPONSE_SIZE, arguments->worker_uid) < 0)
			{
				perror("ERRIMSS_PUBLISH_DELETEMSG");
				return -1;
			}

			break;
		}
		case RENAME_OP:
		{
			std::size_t found = key.find(' ');
			if (found != std::string::npos)
			{
				std::string old_key = key.substr(0, found);
				std::string new_key = key.substr(found + 1, key.length());

				slog_debug("[stat_worker_helper][RENAME] old_key=%s, new_key=%s\n", old_key.c_str(), new_key.c_str());

				// RENAME MAP
				int32_t result = map->rename_metadata_stat_worker(old_key, new_key);
				if (result == 0)
				{
					// printf("0 elements rename from stat_worker");
					slog_debug("[stat_worker_helper][RENAME] 0 elements rename from stat_worker");
					break;
				}

				// RENAME TREE
				int ret = GTree_rename((char *)old_key.c_str(), (char *)new_key.c_str());
				slog_debug("[stat_worker_helper][RENAME] GTree_rename=%d", ret);
			}

			char release_msg[] = "RENAME\0";

			if (send_data(arguments->ucp_worker, arguments->server_ep, release_msg, RESPONSE_SIZE, arguments->worker_uid) < 0)
			{
				perror("ERRIMSS_PUBLISH_RENAMEMSG");
				return -1;
			}
			break;
		}
		case RENAME_DIR_DIR_OP:
		{
			std::size_t found = key.find(' ');
			if (found != std::string::npos)
			{
				std::string old_dir = key.substr(0, found);
				std::string rdir_dest = key.substr(found + 1, key.length());

				// RENAME MAP
				map->rename_metadata_dir_stat_worker(old_dir, rdir_dest);

				// RENAME TREE
				GTree_rename_dir_dir((char *)old_dir.c_str(), (char *)rdir_dest.c_str());
			}

			char release_msg[] = "RENAME\0";

			if (send_data(arguments->ucp_worker, arguments->server_ep, release_msg, RESPONSE_SIZE, arguments->worker_uid) < 0)
			{
				perror("ERRIMSS_PUBLISH_RENAMEMSG");
				return -1;
			}
			break;
		}
		default:
			break;
		}
		break;
	}
	// More messages will arrive to the socket.
	case SET_OP:
	{
		slog_debug("[STAT WORKER] Creating dataset %s.", key.c_str());
		pthread_mutex_lock(&mp);
		// If the record was not already stored, add the block.
		if (!map->get(key, &address_, &block_size_rtvd))
		{
			pthread_mutex_unlock(&mp);
			slog_debug("[STAT WORKER] Adding new block %s", &address_);
			// Receive the block into the buffer.
			char *buffer = (char *)malloc(block_size_recv);
			slog_debug("[STAT WORKER] Recv dynamic buffer size %ld", block_size_recv);
			recv_dynamic_stream(arguments->ucp_worker, arguments->server_ep, buffer, BUFFER, arguments->worker_uid);
			slog_debug("[STAT WORKER] END Recv dynamic");

			int32_t insert_successful;
			insert_successful = map->put(key, buffer, block_size_recv);
			slog_debug("[STAT WORKER] map->put (key %s) err %d", key.c_str(), insert_successful);

			if (insert_successful != 0)
			{
				perror("ERRIMSS_WORKER_MAPPUT");
				return -1;
			}

			// Insert the received uri into the directory tree.
			pthread_mutex_lock(&tree_mut);
			// slog_debug("[STAT WORKER] Inserting %s into directory tree", key.c_str());
			insert_successful = GTree_insert((char *)key.c_str());
			pthread_mutex_unlock(&tree_mut);

			if (insert_successful == -1)
			{
				perror("ERRIMSS_STATWORKER_GTREEINSERT");

				return -1;
			}
			// Update the pointer.
			arguments->pt += block_size_recv;
			slog_debug("[STAT WORKER] Dataset %s has been created.", key.c_str());
		}
		// If was already stored:
		else
		{
			pthread_mutex_unlock(&mp);
			// Follow a certain behavior if the received block was already stored.
			slog_debug("[STAT WORKER] LOCAL DATASET_UPDATE %ld", block_size_recv);
			switch (block_size_recv)
			{
			// Update where the blocks of a LOCAL dataset have been stored.
			case LOCAL_DATASET_UPDATE:
			{
				char data_ref[REQUEST_SIZE];
				if (recv_data(arguments->ucp_worker, arguments->server_ep, data_ref, arguments->worker_uid, 0) < 0)
				{
					return -1;
				}

				uint32_t data_size = RESPONSE_SIZE; // MIRAR

				// Value to be written in certain positions of the vector.
				uint16_t *update_value = (uint16_t *)(data_size + data_ref - 8);
				// Positions to be updated.
				uint32_t *update_positions = (uint32_t *)data_ref;

				// Set of positions that are going to be updated (those are just under the concerned dataset but not pointed by it).
				uint16_t *data_locations = (uint16_t *)(address_ + sizeof(dataset_info));

				// Number of positions to be updated.
				int32_t num_pos_toupdate = (data_size / sizeof(uint32_t)) - 2;

				// Perform the update operation.
				for (int32_t i = 0; i < num_pos_toupdate; i++)
					data_locations[update_positions[i]] = *update_value;

				// Answer the client with the update.
				slog_debug("[STAT_WORKER] Updating existing dataset %s.", key.c_str());
				char answer[] = "UPDATED!\0";
				if (send_data(arguments->ucp_worker, arguments->server_ep, answer, RESPONSE_SIZE, arguments->worker_uid) < 0)
				{
					perror("ERRIMSS_WORKER_DATALOCATANSWER2");
					return -1;
				}

				break;
			}

			default:
			{
				slog_debug("[STAT_WORKER] Updating existing dataset %s.", key.c_str());
				// Clear the corresponding memory region.
				char *buffer = (char *)malloc(block_size_recv);
				// Receive the block into the buffer.
				recv_dynamic_stream(arguments->ucp_worker, arguments->server_ep, buffer, BUFFER, arguments->worker_uid);
				free(buffer);
				slog_debug("[STAT_WORKER] End Updating existing dataset 2222 %s.", key.c_str());
				break;
			}
			}
		}
		break;
	}
	default:
		break;
	}

	slog_debug("[srv_worker_thread] Terminated meta helper");

	return 0;
}

// Server dispatcher thread method.
void *srv_attached_dispatcher(void *th_argv)
{
	// Cast from generic pointer type to p_argv struct type pointer.
	p_argv *arguments = (p_argv *)th_argv;

	ucx_server_ctx_t context;
	ucp_worker_h ucp_data_worker;
	ucp_am_handler_param_t param;
	ucp_ep_h server_ep;
	ucs_status_t status;
	int ret;

	// Variable specifying the ID that will be granted to the next client.
	uint32_t client_id_ = 0;
	char req[256];

	ret = init_worker(arguments->ucp_context, &ucp_data_worker);
	if (ret != 0)
	{
		perror("ERRIMSS_INIT_WORKER");
		pthread_exit(NULL);
	}

	/* Initialize the server's context. */
	context.conn_request = StsQueue.create();
	// status = start_server(arguments->ucp_worker, &context, &context.listener, NULL, arguments->port);
	// if (status != UCS_OK)
	//{
	//	perror("ERRIMSS_STAR_SERVER");
	//	pthread_exit(NULL);
	// }

	for (;;)
	{
		ucp_conn_request_h conn_req;
		slog_debug("[DATA DISPATCHER] Waiting for connection requests.");

		while (StsQueue.size(context.conn_request) == 0)
		{
			ucp_worker_progress(arguments->ucp_worker);
		}

		conn_req = (ucp_conn_request_h)StsQueue.pop(context.conn_request);

		status = server_create_ep(ucp_data_worker, conn_req, &server_ep);
		if (status != UCS_OK)
		{
			perror("ERRIMSS_SERVER_CREATE_EP");
			// ep_flush(server_ep, ucp_data_worker);
			ep_close(ucp_data_worker, server_ep, UCP_EP_CLOSE_MODE_FLUSH);
			pthread_exit(NULL);
		}

		// Save the identity of the requesting client.
		char mode[MODE_SIZE];
		recv_data(ucp_data_worker, server_ep, req, arguments->worker_uid, 0);

		sscanf(req, "%" PRIu32 " %s", &client_id_, mode);
		char *req_content = strstr(req, mode);
		req_content += 4;

		uint32_t c_id = client_id_;

		// Check if the client is requesting connection resources.
		if (!strncmp(req_content, "HELLO!", 6))
		{
			if (strncmp(req_content, "HELLO!JOIN", 10) != 0)
			{
				// Retrieve the buffer size that will be asigned to the current server process.
				char buff[6];
				sscanf(req, "%s %ld %s", buff, &buffer_KB, att_imss_uri);
				strcpy(arguments->my_uri, att_imss_uri);

				// printf("MU URI: %s", att_imss_uri);

				// Notify that the value has been received.
				pthread_mutex_lock(&buff_size_mut);
				copied = 1;
				pthread_cond_signal(&buff_size_cond);
				pthread_mutex_unlock(&buff_size_mut);
			}

			// Message containing the client's communication ID plus its connection port.
			char response_[RESPONSE_SIZE];
			memset(response_, '\0', RESPONSE_SIZE);
			// Port that the new client will be forwarded to.
			int32_t port_ = arguments->port + 1 + (client_id_ % IMSS_THREAD_POOL);
			// Wrap the previous info into the ZMQ message.
			sprintf(response_, "%d%c%d", port_, '-', client_id_++);

			// Send communication specifications.
			if (send_data(ucp_data_worker, server_ep, response_, RESPONSE_SIZE, arguments->worker_uid) < 0)
			{
				perror("ERRIMSS_SRVDISP_SENDBLOCK");
				// ep_flush(server_ep, ucp_data_worker);
				ep_close(ucp_data_worker, server_ep, UCP_EP_CLOSE_MODE_FLUSH);
				pthread_exit(NULL);
			}

			slog_debug("[DATA DISPATCHER] Replied client %s.", response_);
			continue;
		}
		// Check if someone is requesting identity resources.
		else if (*((int32_t *)req) == WHO) // MIRAR
		{
			// Provide the uri of this instance.
			if (send_data(ucp_data_worker, server_ep, arguments->my_uri, RESPONSE_SIZE, arguments->worker_uid) < 0) // MIRAR
			{
				perror("ERRIMSS_WHOREQUEST");
				// ep_flush(server_ep, ucp_data_worker);
				ep_close(ucp_data_worker, server_ep, UCP_EP_CLOSE_MODE_FLUSH);
				pthread_exit(NULL);
			}
		}
		// context.conn_request = NULL;
		ep_close(ucp_data_worker, server_ep, UCP_EP_CLOSE_MODE_FLUSH);
	}
	pthread_exit(NULL);
}

// Metadata dispatcher thread method.
void *dispatcher(void *th_argv)
{

	// Cast from generic pointer type to p_argv struct type pointer.
	p_argv *arguments = (p_argv *)th_argv;

	uint32_t client_id_ = 0;
	char req[REQUEST_SIZE];

	int ret;
	int sockfd = -1;
	int listenfd = -1;
	int optval = 1;
	char service[8];
	struct addrinfo hints, *res, *t;

	snprintf(service, sizeof(service), "%ld", arguments->port);
	memset(&hints, 0, sizeof(hints));
	hints.ai_flags = AI_PASSIVE;
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;
	int client = 0;

	ret = getaddrinfo(NULL, service, &hints, &res);

	for (t = res; t != NULL; t = t->ai_next)
	{
		sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
		if (sockfd < 0)
		{
			continue;
		}

		ret = setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
		ret = bind(sockfd, t->ai_addr, t->ai_addrlen);
		if (ret < 0)
		{

			// perror("Dispatcher Server");
			perror("ERRIMSS_DISPATCHER_DEPLOY");
			exit(-1);
		}
		else if (ret == 0)
		{
			ret = listen(sockfd, 0);
			/* Accept next connection */
			listenfd = sockfd;
			while (1)
			{
				ucs_status_t status;
				char mode[MODE_SIZE];

				slog_debug("[DISPATCHER] Waiting for connection requests.");
				// fprintf(stderr, "[DISPATCHER] Waiting for connection requests.\n");
				sockfd = accept(listenfd, NULL, NULL);
				ret = recv(sockfd, req, REQUEST_SIZE, MSG_WAITALL);

				sscanf(req, "%" PRIu32 " %s", &client_id_, mode);

				char *req_content = strstr(req, mode);
				req_content += 4;

				// Check if the client is requesting connection resources.
				if (!strncmp(req_content, "HELLO!", 6))
				{
					ret = send(sockfd, &local_addr_len[(client % IMSS_THREAD_POOL) + 1], sizeof(local_addr_len[(client % IMSS_THREAD_POOL) + 1]), 0);
					ret = send(sockfd, local_addr[(client % IMSS_THREAD_POOL) + 1], local_addr_len[(client % IMSS_THREAD_POOL) + 1], 0);
					client++;
					slog_debug("[DISPATCHER] Replied client.");
				}
				else if (!strncmp(req_content, "MAIN!", 5))
				{
					ret = send(sockfd, &local_addr_len[1], sizeof(local_addr_len[1]), 0);
					ret = send(sockfd, local_addr[1], local_addr_len[1], 0);
				}
				// Check if someone is requesting identity resources.
				else if (*((int32_t *)req) == WHO)
				{
					ret = send(sockfd, &local_addr_len[client], sizeof(local_addr_len[client]), 0);
					ret = send(sockfd, local_addr[client], local_addr_len[client], 0);
					slog_debug("[DISPATCHER] Replied client %s.", arguments->my_uri);
				}

				// MIRAR ucp_worker_release_address(ucp_worker_threads[client_id_ % IMSS_THREAD_POOL], local_addr);
				close(sockfd);
			}
		}
	}
	close(listenfd);

	pthread_exit(NULL);
}
