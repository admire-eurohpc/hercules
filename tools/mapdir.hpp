#ifndef H_MAP_DIR
#define H_MAP_DIR

#include <sys/stat.h>
#include <dirent.h>

void *map_dir_create();
void map_dir_put(void *map, DIR *dir, int v);
void map_dir_erase(void *map, DIR *dir);
int map_dir_search(void *map, DIR *dir, int *v);

#endif
