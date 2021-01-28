#ifndef IMSS_DIRECTORY
#define IMSS_DIRECTORY

#include <glib.h>

//Wrapper to the GTree_search_ function that compares if the parent node is requested.
int32_t GTree_search(GNode * parent_node, char * desired_data, GNode ** found_node);

//Method retrieving a buffer with all the files within a directory.
char * GTree_getdir(char * desired_dir, int32_t * numdir_elems);

//Method inserting a new path.
int32_t GTree_insert(char * desired_data);

//Method that will be called for each tree node freeing the associated data element.
int32_t gnodetraverse (GNode * node, void * data);

#endif
