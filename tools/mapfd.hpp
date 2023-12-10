#ifndef H_MAP_FD
#define H_MAP_FD

// #include <sys/stat.h>
void *map_fd_create();
int map_fd_put(void *map, const char *pathname, const int fd, unsigned long offset);
void map_fd_update_value(void *map, const char *pathname, const int fd, unsigned long offset);
void map_fd_update_fd(void *map, const char *pathname, const int fd, const int new_fd, unsigned long offset);
void map_fd_erase(void *map, const int fd);
int map_fd_search(void *map, const char *pathname, const int fd, unsigned long *offset);
int map_fd_search_by_pathname(void *map, const char *pathname, int *fd, long *offset);
int map_fd_erase_by_pathname(void *map, const char *pathname);
int map_fd_search_by_val_close(void *map, int fd);
char *map_fd_search_by_val(void *map, const int fd);
#endif
