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


void map_server_eps_put(void * map, ucp_address_t *peer_address, ucp_ep_h ep) {
	map_server_eps_t * m = reinterpret_cast<map_server_eps_t*> (map);
	m->insert(std::pair<ucp_address_t*, ucp_ep_h>(peer_address,ep));
}


void map_server_eps_erase(void* map, ucp_address_t *peer_address) {
	map_server_eps_t * m = reinterpret_cast<map_server_eps_t*> (map);
	auto search = m->find(peer_address);

	//TODO
	// close ep if found
	if (search != m->end()) {
		ucp_ep_close_nb(search->second, UCP_EP_CLOSE_MODE_FORCE);
	} 
	m->erase(peer_address);
}


int map_server_eps_search(void * map, ucp_address_t *peer_address, ucp_ep_h *ep) {
	map_server_eps_t * m = reinterpret_cast<map_server_eps_t*> (map);
	auto search = m->find(peer_address);

	if (search != m->end()) {
		*ep = (search->second);
		return 1;
	} else {
		return -1;
	}
}
