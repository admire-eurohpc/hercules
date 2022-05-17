#ifndef H_MAP_FD
#define H_MAP_FD

#include <sys/stat.h>
void* map_fd_create();
void map_fd_put(void* map, char* k, int v, int p);
void map_fd_update_value(void* map, char* k, int v, int p);
void map_fd_erase(void* map, char* k);
int map_fd_search(void* map,const char* k, int *v, int *p);
int map_fd_search_by_val_close(void* map, int v);
int map_fd_search_by_val(void* map, char* path, int v);
int map_fd_rename(void* map, const char * oldname, const char * newname);
#endif

 