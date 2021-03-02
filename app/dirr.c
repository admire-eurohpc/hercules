#include <stdio.h>
#include <stdlib.h>
#include <sys/signal.h>
#include "directory.h"

#define URI	256

int		num_nodes;
extern GNode * 	tree_root;
char ** 	uris;

void release(int signal)
{
	g_node_traverse(tree_root, G_PRE_ORDER, G_TRAVERSE_ALL, -1, gnodetraverse, NULL);

	free(uris);

	exit(0);
}

int main ()
{
	//Create the tree_root node.
	char * root_data = (char *) malloc(1);
	root_data[0] = '/';
	tree_root = g_node_new((void *) root_data);

	printf("Number of nodes to be introduced into the tree: ");
	scanf("%d", &num_nodes);

	uris = (char **) malloc(sizeof(char *) * num_nodes);

	for (int i = 0; i < num_nodes; i++)
	{
		uris[i] = (char *) malloc(sizeof(char) * URI);
		memset(uris[i], 0, URI);

		printf("Introduce element number %d: ", i);
		scanf("%s", uris[i]);
	}

	signal(SIGINT, release);

	printf("\nElements introduced:\n");
	for (int i = 0; i < num_nodes; i++)
	{
		GTree_insert(uris[i]);
		printf("%d. %s\n", i, uris[i]);
	}
	printf("\n");

	while (1)
	{
		char request[URI];
		printf("\nREQUEST AN ELEMENT: ");
		scanf("%s", &request);
		printf("Element requested: %s\n", request);

		int num_dir_elements;
		char * dir_elements = GTree_getdir(request, &num_dir_elements);

		printf("%d elements found inside it:\n", num_dir_elements);
		for (int i = 0; i < num_dir_elements; i++)

			printf("%d. %s\n", i+1, dir_elements+URI*i);

		free(dir_elements);
	}

	return 0;
}
