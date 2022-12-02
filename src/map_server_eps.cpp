#include <iostream>
#include <vector>
#include <cstddef>
#include <cstring>
#include <sys/stat.h>
#include <fcntl.h>
#include <map>

#include <ucp/api/ucp.h>
#include "map_server_eps.hpp"


void* map_server_eps_create() {
	return reinterpret_cast<void*> (new map_server_eps_t);
}


void map_server_eps_put(void * map, uint64_t uuid, ucp_ep_h ep) {
	map_server_eps_t * m = reinterpret_cast<map_server_eps_t*> (map);
	m->insert(std::pair<uint64_t, ucp_ep_h>(uuid,ep));
}


void map_server_eps_erase(void* map, uint64_t uuid) {
	map_server_eps_t * m = reinterpret_cast<map_server_eps_t*> (map);
	auto search = m->find(uuid);

	//TODO
	// close ep if found
	if (search != m->end()) {
		ucp_ep_close_nb(search->second, UCP_EP_CLOSE_MODE_FORCE);
	} 
	m->erase(uuid);
}


int map_server_eps_search(void * map, uint64_t uuid, ucp_ep_h *ep) {
	map_server_eps_t * m = reinterpret_cast<map_server_eps_t*> (map);
	auto search = m->find(uuid);

	if (search != m->end()) {
		*ep = (search->second);
		return 1;
	} else {
		return -1;
	}
}

