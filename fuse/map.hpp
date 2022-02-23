#ifndef H_MAP
#define H_MAP

void* map_create();
void map_put(void* map, char* k, int v);
void map_erase(void* map, char* k);
int map_search(void* map, const char* k, int *v);

#endif

