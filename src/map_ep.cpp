#include <map>
#include <iostream>
#include <vector>
#include <cstddef>
#include <cstring>
#include <sys/stat.h>
#include <fcntl.h>

#include <ucp/api/ucp.h>

typedef std::map<ucp_ep_h, StsHeader> map_ep_t;

extern "C" {

void* map_ep_create() {
  return reinterpret_cast<void*> (new map_ep_t);
}

void map_ep_put(void* map, ucp_ep_h ep, StsHeader queue ) {
  Map* m = reinterpret_cast<map_ep_t*> (map);
  m->insert(std::pair<ucp_ep_h, ucx_async_t>(ep,queue));
}

void map_erase(void* map, ucp_ep_h ep) {
  map_ep_t * m = reinterpret_cast<map_ep_t *> (map);
  auto search = m->find(ep);
     
  if (search != m->end()) {
	    StsHeader.destroy(search->second.pending);
  } 
  m->erase(ep);
}

int map_search(void* map, const ucp_ep_h ep, StsHeader * queue) {
    map_ep_t * m = reinterpret_cast<map_ep_t *> (map);
    auto search = m->find(ep);
     
    if (search != m->end()) {
        *queue = search->second;
	      return 1;
    } else {
        return -1;
    }
}

} // extern "C"
