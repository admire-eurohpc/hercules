#ifndef MAP_EP_H
#define MAP_EP_H

#include <map>
#include <ucp/api/ucp.h>
#include "queue.h"


typedef std::map<ucp_ep_h, StsHeader*> map_ep_t;


void *  map_ep_create();
void    map_ep_put(void * map, ucp_ep_h ep, StsHeader * req_queue);
void    map_ep_erase(void * map, ucp_ep_h ep);
int     map_ep_search(void * map, const ucp_ep_h ep, StsHeader ** req_queue);

#endif
