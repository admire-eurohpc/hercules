#include <zmq.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include "comms.h"

int32_t main(int32_t argc, char **argv)
{
	void * context;
	//ZeroMQ context intialization.
	if (!(context = comm_ctx_new()))
	{
		perror("ERRIMSSRLS_CTX_CREATE");
		return -1;
	}

	//Sockets dealing with each endpoint provided.
	void ** sockets;
	sockets = (void **) malloc((argc-1) * sizeof(void *));

	//Send a release message to every provided endpoint.
	for (int32_t i = 1; i < argc; i++)
	{
		//Create the actual ZMQ socket.
		if ((sockets[i-1] = comm_socket(context, ZMQ_DEALER)) == NULL)
		{
			perror("ERRIMSSRLS_SOCKET_CRT");
			return -1;
		}

		//Set communication id.
		//if (zmq_setsockopt(sockets[i-1], ZMQ_IDENTITY, &i, sizeof(int32_t)) == -1)
		//{
		//	perror("ERRIMSSRLS_SOCKET_IDEN");
		//	return -1;
		//}

		//Connect to the specified endpoint.
		if (comm_connect(sockets[i-1], (const char *) argv[i]) == -1)
		{
			perror("ERRIMSSRLS_SOCKET_CONNECT");
			return -1;
		}

		char release_msg[] = "2 RELEASE\0";

		if (comm_send(sockets[i-1], release_msg, strlen(release_msg), 0) < 0)
		{
			perror("ERRIMSSRLS_SEND_RELEASEMSG");
			return -1;
		}
	}

	//Close the previous DEALER sockets.
	for (int32_t i = 1; i < argc; i++)
	{
		if (comm_close(sockets[i-1]) == -1)
		{
			perror("ERRIMSSRLS_SCKT_CLOSE");
			return -1;
		}
	}

	if (comm_ctx_destroy(context) == -1)
	{
		perror("ERRIMSSRLS_CTX_DSTRY");
		return -1;
	}

	return 0;
}
