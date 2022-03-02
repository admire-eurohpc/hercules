#ifndef H_MAP
#define H_MAP

#include <sys/stat.h>
//structura con int, stat y char * malloc
void* map_create();
void map_put(void* map, char* k, int v, struct stat stat, char * aux);
void map_erase(void* map, char* k);
int map_search(void* map, const char* k, int *v, struct stat *stat, char** aux);
int map_rename(void* map, const char * oldname, const char * newname);
void map_update(void* map, char* k, int v, struct stat stat);
int map_rename_dir_dir(void* map, const char * old_dir, const char * rdir_dest);
#endif

