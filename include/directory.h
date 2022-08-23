#ifndef IMSS_DIRECTORY
#define IMSS_DIRECTORY

#include <glib.h>

//Wrapper to the GTree_search_ function that compares if the parent node is requested.
int32_t GTree_search(GNode * parent_node, char * desired_data, GNode ** found_node);

//Method retrieving a buffer with all the files within a directory.
char *  GTree_getdir(char * desired_dir, int32_t * numdir_elems);

//Method deleting a new path.
int32_t GTree_delete(char * desired_data);

//Method renaming a new path.
int32_t GTree_rename(char * old_desired_data,char * new_desired_data);

//Method serializing the number of elements within a directory into a buffer.
int32_t
serialize_dir(GNode * 	visited_node,uint32_t 	num_nodes,char ** 	buffer);

//Method serializing the number of childrens within a directory into a buffer.
int32_t
serialize_dir_childrens(GNode * 	visited_node,uint32_t 	num_children,char ** 	buffer);

//Method renaming dir to dir.
int32_t
GTree_rename_dir_dir(char * old_dir,char * rdir_dest);

//Method inserting a new path.
int32_t GTree_insert(char * desired_data);

//Method that will be called for each tree node freeing the associated data element.
int32_t gnodetraverse (GNode * node, void * data);

int32_t
gnodetraverse_find (GNode * 	node,
	       void * 	desired_data);
#endif
