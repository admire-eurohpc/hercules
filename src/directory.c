#include <imss.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <directory.h>
#include <stdio.h>
#include "records.hpp"

//Pointer to the tree's root node.
GNode * tree_root;

//Method searching for a certain data node.
int32_t
GTree_search_(GNode * 	parent_node,
	      char * 	desired_data,
	      GNode ** 	found_node)
{
	//Number of children of the current parent_node.
	uint32_t num_children = g_node_n_children(parent_node);

	GNode * child = parent_node->children;

	*found_node = parent_node;

	//Search for the requested data within the children of the current node.
	for (int32_t i = 0; i < num_children; i++)
	{
		//Search for a directory antecesor of the desired node.

		//HAVE TO CHECK IF IT IS A DIRECTORY OR A FILE
		//For this i check if it has at the end /
		
		if(desired_data[strlen(desired_data)-1]=='/'&&!strncmp((char *) child->data, desired_data, strlen((char *) child->data))){
			//Check if the compared node is the requested one.
			int a = 1;
			if (!strcmp((char *) child->data, desired_data))
			{
				*found_node = child;
				
				//The desired data was found.
				return 1;
			}
			else{
				//Check within the new node.
				return GTree_search_(child, desired_data, found_node);
			}
		}else if(desired_data[strlen(desired_data)-1]!='/'&&!strncmp((char *) child->data, desired_data, strlen((char *) child->data))){
			//Check if the compared node is the requested one.
			if (!strcmp((char *) child->data, desired_data))
			{
				*found_node = child;

				//The desired data was found.
				return 1;
			}
			else{
				//SPECIAL CASE EXAMPLE PRUEBA_1 CREATED AND WANT TO ADD PRUEBA_11
				//CHECK THE NUMBERS OF '/' IN THE PATHS TO SEE IF WE ARRIVE TO THE DIRECTORY
				int amount = 0;
				for (int32_t j = 0; j < strlen(desired_data)-1; j++)
				{
					if(desired_data[j]=='/'){
						amount = amount + 1;
					}
				}
				int amount_child = 0;
				char * path_child = (char *)child->data;
				for (int32_t j = 0; j < strlen(path_child)-1; j++)
				{
					if(path_child[j]=='/'){
						amount_child = amount_child + 1;
					}
				}

				if(amount==amount_child){
					//Move on to the following child.
					child = child->next;
					continue;
				}

				//Check within the new node.
				return GTree_search_(child, desired_data, found_node);
			}
		}

		//Move on to the following child.
		child = child->next;
	}

	return 0;
}

//Wrapper to the GTree_search_ function that compares if the parent node is requested.
int32_t
GTree_search(GNode * 	parent_node,
	     char * 	desired_data,
	     GNode ** 	found_node)
{
	//Check if the desired data was contained by the provided node.
	if (!strcmp((char *) parent_node->data, desired_data))
	{
		*found_node = parent_node;

		return 1;
	}

	return GTree_search_(parent_node, desired_data, found_node);
}

//Method renaming a new path.
int32_t
GTree_rename(char * old_desired_data,char * new_desired_data)
{
	//Closest node to the one requested (or even the requested one itself).
	GNode * closest_node;

	//Check if the node has been already inserted.
	if (GTree_search(tree_root, old_desired_data, &closest_node)==1){
		printf("TREE Exist found:%s\n",old_desired_data);
		if(strcmp(old_desired_data,(char *)closest_node->data)==0){
			printf("TREE Data rename Tree Before: %s\n",closest_node->data);
			//memcpy(closest_node->data,new_desired_data,strlen(new_desired_data)+1);
			g_node_destroy(closest_node);
			GTree_insert(new_desired_data);

			//printf("TREE Data rename Tree After: %s\n",closest_node->data);


		}
		
	}else{
		printf("Rename Error not found:%s\n",old_desired_data);
		return 0;
	}
		
	return 1;
}

//Method renaming dir to dir.
int32_t
GTree_rename_dir_dir(char * old_dir,char * rdir_dest)
{	
	printf("TREE DIR TO DIR RENAME\n");
	//Node whose elements must be retrieved.
	GNode * dir_node;
	//Check if the node has been already inserted.
	if (GTree_search(tree_root, old_dir, &dir_node)==1){
	
		uint32_t num_elements_indir = g_node_n_nodes (dir_node, G_TRAVERSE_ALL)-1;
		uint32_t num_elements_indir_childrens = g_node_n_children (dir_node);
		printf("DIR_NUM_ELEMENTS=%d\n",num_elements_indir+1);
		char * dir_elements = (char *) malloc((num_elements_indir+1)*URI_);
		char * aux_dir_elem = dir_elements;
		serialize_dir(dir_node,num_elements_indir_childrens,&aux_dir_elem);
		
		char * aux = dir_elements;
		for(int i=0;i<num_elements_indir+1;i++){
			
			if(strstr(aux, old_dir) != NULL) {
				char * path = aux;
				
				size_t len = strlen(old_dir);
				if (len > 0) {
					char *p = path;
					while ((p = strstr(p, old_dir)) != NULL) {
						memmove(p, p + len, strlen(p + len) + 1);
					}
				}
				char * new_path = (char *) malloc(256); 
				strcpy(new_path, rdir_dest);
				strcat(new_path,"/");
				strcat(new_path,path);
				printf("inserting=%s\n",new_path);	
				GTree_insert(new_path);

			}
			aux += URI_;
		}
		g_node_destroy(dir_node);

	}
	return 0;
}

//Method deleting a new path.
int32_t
GTree_delete(char * desired_data)
{
	//Closest node to the one requested (or even the requested one itself).
	GNode * closest_node;

	//Check if the node has been already inserted.
	if (GTree_search(tree_root, desired_data, &closest_node)==1){
		//printf("Entro a borrar: %s\n",closest_node->data);
		if(strcmp(desired_data,(char *)closest_node->data)==0){
			//printf("g_node_destroy: %s\n",closest_node->data);
			g_node_destroy(closest_node);//Delete Node
		}
		
	}else{
		//printf("Delete Error not found:\n");
		return 0;
	}

		

	return 1;
}

//Method inserting a new path.
int32_t
GTree_insert(char * desired_data)
{
	//Closest node to the one requested (or even the requested one itself).
	GNode * closest_node;

	//Check if the node has been already inserted.
	if (GTree_search(tree_root, desired_data, &closest_node))

		return 0;

	//Length of the found uri. An additional unit is added in order to avoid the first '/' encountered.
	int32_t closest_data_length = strlen((char *) closest_node->data) + 1;

	//Number of characters that the desired string has more than the found one.
	int32_t more_chars = strlen(desired_data) - closest_data_length;

	//Special case: insertion of a one character length file in the root directory.
	if (!more_chars && (closest_data_length == 2))
	{
		more_chars = 1;

		closest_data_length--;
	}

	//Search for the '/' characters within the additional ones.
	for (int32_t i = 0; i < more_chars; i++)
	{
		int32_t new_position = closest_data_length + i;

		if ((desired_data[new_position] == '/') || (i == (more_chars-1)))
		{
			if (i == (more_chars-1))
				new_position++;

            //if (i == 0 && desired_data[new_position+1] == '/')
       
			//String that will be introduced as a new node.
			char * new_data = (char *) malloc(new_position+1);
			strcpy(new_data, desired_data);
			//New node to be introduced.
			//printf("new_node=%s\n",new_data);
			GNode * new_node = g_node_new((void *) new_data);

			//Introduce it as a child of the closest one found.
			g_node_append(closest_node, new_node);

			closest_node = new_node;
		}
	}

	return 1;
}

//Method serializing the number of childrens within a directory into a buffer.
int32_t
serialize_dir_childrens(GNode * 	visited_node,
	      uint32_t 	num_children,
	      char ** 	buffer)
{
	//Add the concerned uri into the buffer.
	memcpy(*buffer, (char *) visited_node->data, URI_);
	*buffer += URI_;

	GNode * child = visited_node->children;
	//printf("node=%s  num_children=%d\n",(char *) visited_node->data,num_children);
	for (int32_t i = 0; i < num_children; i++)
	{ 
		//Number of children of the current child node.
		
		//uint32_t num_grandchildren = g_node_n_children(child);

		//If the child is a leaf one, just store the corresponding info.
		/*if (!num_grandchildren)
		{*/
			//Add the child's uri to the buffer.
			printf("serialize_dir_childrens add:%s\n",child->data);
			memcpy(*buffer, (char *) child->data, URI_);
			*buffer += URI_;
		/*}
		else

			serialize_dir(child, num_grandchildren, buffer);*/

		child = child->next;
	}

	return 0;
}

//Method serializing the number of elements within a directory into a buffer.
int32_t
serialize_dir(GNode * 	visited_node,
	      uint32_t 	num_nodes,
	      char ** 	buffer)
{
	//Add the concerned uri into the buffer.
	printf("First serialize_dir add:%s\n",visited_node->data);
	memcpy(*buffer, (char *) visited_node->data, URI_);
	*buffer += URI_;

	GNode * child = visited_node->children;
	//printf("node=%s  num_nodes=%d\n",(char *) visited_node->data,num_nodes);
	for (int32_t i = 0; i < num_nodes; i++)
	{ 
		//Number of children of the current child node.
		
		uint32_t num_grandchildren = g_node_n_children(child);

		//If the child is a leaf one, just store the corresponding info.
		if (!num_grandchildren)
		{
			//Add the child's uri to the buffer.
			printf("Next serialize_dir add:%s\n",child->data);
			memcpy(*buffer, (char *) child->data, URI_);
			*buffer += URI_;
		}
		else{
			serialize_dir(child, num_grandchildren, buffer);
		}
		child = child->next;
	}

	return 0;
}

/**********************************************************/
//WARNING: this function reserves memory that must be freed.
/**********************************************************/

//Method retrieving a buffer with all the files within a directory.
char *
GTree_getdir(char * 	desired_dir,
	     int32_t * 	numdir_elems)
{
	//Node whose elements must be retrieved.
	GNode * dir_node;

	//Check if the node is inserted.
	if (!GTree_search(tree_root, desired_dir, &dir_node))

		return NULL;

	//Number of elements contained by the concerned directory.
	//uint32_t num_elements_indir = g_node_n_nodes (dir_node, G_TRAVERSE_ALL);

	//*numdir_elems = num_elements_indir;


	//Number of children of the directory node.
	uint32_t num_children = g_node_n_children(dir_node);
	*numdir_elems = num_children +1;//+1 because of the actual directory + childrens

	//Buffer containing the whole set of elements within a certain directory.
	//char * dir_elements = (char *) malloc(sizeof(char)*num_elements_indir*URI_);
	char * dir_elements = (char *) malloc((num_children+1)*URI_);
	char * aux_dir_elem = dir_elements;

	

	//Call the serialization function storing all dir elements in the buffer.
	serialize_dir_childrens(dir_node, num_children, &aux_dir_elem);

	return dir_elements;
}

//Method that will be called for each tree node freeing the associated data element.
int32_t
gnodetraverse (GNode * 	node,
	       void * 	data)
{
	free(node->data);

	return 0;
}

//Method that will be called for each tree node freeing if st_nlink=0 the associated data element.
int32_t
gnodetraverse_garbage_collector (GNode * 	node,//dont delete maybe usefull in the future
	       void * 	data)
{
	char * key = (char *) data;
	//Check if there was an associated block to the key.
	/*if (!(map->get(key, &address_, &block_size_rtvd)))
	{
		printf("ERROR NO BLOCK");
	}*/
	//g_tree_destroy
	//free(node->data);

	return 0;
}

//Method that will be called for each tree node..
int32_t
Gnodetraverse_garbage_collector (map_records * map)//dont delete maybe usefull in the future
{
	//Freeing all resources of the tree structure.
	g_node_traverse(tree_root, G_PRE_ORDER, G_TRAVERSE_ALL, -1, gnodetraverse_garbage_collector, NULL);
	

	return 0;
}



	

