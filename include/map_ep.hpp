#ifndef H_MAP
#define H_MAP

#include <sys/stat.h>
#include <ucp/api/ucp.h>
#include "queue.h"
#include "comms.h"


void *  map_ep_create();
void    map_ep_put(void * map, ucp_ep_h ep, StsHeader * req_queue);
void    map_ep_erase(void * map, ucp_ep_h ep);
int     map_ep_search(void * map, const ucp_ep_h ep, StsHeader ** req_queue);

#endif
