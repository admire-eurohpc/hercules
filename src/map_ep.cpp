#include <map>
#include <iostream>
#include <vector>
#include <cstddef>
#include <cstring>
#include <sys/stat.h>
#include <fcntl.h>

#include <ucp/api/ucp.h>
#include "queue.h"
#include "map_ep.hpp"

void* map_ep_create() {
	return reinterpret_cast<void*> (new map_ep_t);
}

void map_ep_put(void* map, ucp_ep_h ep,  StsHeader * req_queue) {
	map_ep_t * m = reinterpret_cast<map_ep_t*> (map);
	m->insert(std::pair<ucp_ep_h, StsHeader*>(ep,req_queue));
}

void map_ep_erase(void* map, ucp_ep_h ep) {
	map_ep_t * m = reinterpret_cast<map_ep_t*> (map);
	auto search = m->find(ep);

	if (search != m->end()) {
		StsQueue.destroy(search->second);
	} 
	m->erase(ep);
}

int map_ep_search(void* map, const ucp_ep_h ep, StsHeader ** req_queue) {
	map_ep_t * m = reinterpret_cast<map_ep_t*> (map);
	auto search = m->find(ep);

	if (search != m->end()) {
		*req_queue = (search->second);
		return 1;
	} else {
		return -1;
	}
}
