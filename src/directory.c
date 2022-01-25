#include <imss.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <directory.h>
#include <stdio.h>

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
		//if (!strncmp((char *) child->data, desired_data, strlen((char *) child->data)))
		if (!strcmp((char *) child->data, desired_data))
		{
			//Check if the compared node is the requested one.
			if (!strcmp((char *) child->data, desired_data))
			{
				*found_node = child;

				//The desired data was found.
				return 1;
			}
			else
				//Check within the new node.
				return GTree_search_(child, desired_data, found_node);
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
			GNode * new_node = g_node_new((void *) new_data);

			//Introduce it as a child of the closest one found.
			g_node_append(closest_node, new_node);

			closest_node = new_node;
		}
	}

	return 1;
}

//Method serializing the number of elements within a directory into a buffer.
int32_t
serialize_dir(GNode * 	visited_node,
	      uint32_t 	num_children,
	      char ** 	buffer)
{
	//Add the concerned uri into the buffer.
	memcpy(*buffer, (char *) visited_node->data, URI_);
	*buffer += URI_;

	GNode * child = visited_node->children;

	for (int32_t i = 0; i < num_children; i++)
	{
		//Number of children of the current child node.
		uint32_t num_grandchildren = g_node_n_children(child);

		//If the child is a leaf one, just store the corresponding info.
		if (!num_grandchildren)
		{
			//Add the child's uri to the buffer.
			memcpy(*buffer, (char *) child->data, URI_);
			*buffer += URI_;
		}
		else

			serialize_dir(child, num_grandchildren, buffer);

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
	uint32_t num_elements_indir = g_node_n_nodes (dir_node, G_TRAVERSE_ALL);

	*numdir_elems = num_elements_indir;

	//Buffer containing the whole set of elements within a certain directory.
	char * dir_elements = (char *) malloc(sizeof(char)*num_elements_indir*URI_);
	char * aux_dir_elem = dir_elements;

	//Number of children of the directory node.
	uint32_t num_children = g_node_n_children(dir_node);

	//Call the serialization function storing all dir elements in the buffer.
	serialize_dir(dir_node, num_children, &aux_dir_elem);

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

