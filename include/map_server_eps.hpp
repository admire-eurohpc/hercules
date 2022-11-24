#ifndef MAP_SERVER_EPS_H
#define MAP_SERVER_EPS_H

#include <map>
#include <ucp/api/ucp.h>


typedef std::map<ucp_address_t*, ucp_ep_h> map_server_eps_t;


void *  map_server_eps_create();
void    map_server_eps_put(void * map, ucp_address_t *peer_address, ucp_ep_h ep);
void    map_server_eps_erase(void * map, ucp_address_t *peer_address);
int     map_server_eps_search(void * map, ucp_address_t *peer_address, ucp_ep_h *ep);

#endif
