#include <iostream>
#include <vector>
#include <cstddef>
#include <cstring>
#include <sys/stat.h>
#include <fcntl.h>
#include <map>

#include <ucp/api/ucp.h>
#include "map_server_eps.hpp"

void *map_server_eps_create()
{
	return reinterpret_cast<void *>(new map_server_eps_t);
}

void map_server_eps_put(void *map, uint64_t uuid, ucp_ep_h ep, char server_type)
{
	map_server_eps_t *m = reinterpret_cast<map_server_eps_t *>(map);
	m->insert(std::pair<uint64_t, ucp_ep_h>(uuid, ep));

	fprintf(stderr, "\t[%c]['%" PRIu64 "'] Adding new connection, #%ld\n", server_type, uuid, m->size());
}

void map_server_eps_erase(void *map, uint64_t uuid, char server_type)
{
	map_server_eps_t *m = reinterpret_cast<map_server_eps_t *>(map);
	// Count the number of elements in the map
	size_t prev_elements = m->size();

	auto search = m->find(uuid);

	// TODO
	//  close ep if found
	if (search != m->end())
	{
		// ucp_ep_flush(search->second);
		ucp_ep_close_nb(search->second, UCP_EP_CLOSE_MODE_FLUSH);
	}
	m->erase(uuid);
	size_t after_elements = m->size();

	fprintf(stderr, "\t[%c]['%" PRIu64 "'] Deleting connection, from %ld to %ld\n", server_type, uuid, prev_elements, after_elements);
}

int map_server_eps_search(void *map, uint64_t uuid, ucp_ep_h *ep)
{
	map_server_eps_t *m = reinterpret_cast<map_server_eps_t *>(map);
	auto search = m->find(uuid);

	if (search != m->end())
	{
		*ep = (search->second);
		return 1;
	}
	else
	{
		return -1;
	}
}
