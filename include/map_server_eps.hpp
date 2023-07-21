#ifndef MAP_SERVER_EPS_H
#define MAP_SERVER_EPS_H

#include <map>
#include <ucp/api/ucp.h>
#include <inttypes.h>


typedef std::map<uint64_t, ucp_ep_h> map_server_eps_t;


void *  map_server_eps_create();
void    map_server_eps_put(void * map, uint64_t uuid, ucp_ep_h ep);
void    map_server_eps_erase(void * map, uint64_t uuid);
int     map_server_eps_search(void * map, uint64_t uuid, ucp_ep_h *ep);

#endif
