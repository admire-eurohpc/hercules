#include <mpi.h>
#include <zmq.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>


int32_t main(int32_t argc, char **argv)
{
	MPI_Init(&argc, &argv);

	//Port that will be used to perform the release operation.
	uint16_t bind_port = atoi(argv[1]);

	void * context;
	//ZeroMQ context intialization.
	if (!(context = zmq_ctx_new()))
	{
		perror("ERRIMSSRLS_CTX_CREATE");
		return -1;
	}

	void * publisher;
	//Publisher socket creation.
	if (!(publisher = zmq_socket (context, ZMQ_PUB)))
	{
		perror("ERRIMSSRLS_CRT_PUB");
		return -1;
	}

	char bind_addr[16];
	sprintf(bind_addr, "tcp://*:%d", bind_port);

	//Bind the previous socket to the specified machine port.
	if (zmq_bind(publisher, bind_addr) == -1)
	{
		perror("ERRIMSSRLS_SOCK_BIND");
		return -1;
	}


	/*
		The following sleep is a crucial one in the current architecture:

		... there's an important point to note here, for PUB/SUB - even if you connect() first
		with your subscriber, that connection doesn't actually occur until after the publisher
		has bind()-ed, so if you attempt to send messages with your publisher without waiting
		for your subscriber to finish its connection, those messages will never make it to your
		subscriber.

		SOURCE: https://stackoverflow.com/questions/33254975/pub-sub-can-i-connect-before-i-bind
	*/


	sleep(5);

	//Loop publishing release requests.
	for (int i = 2; i < argc; i++)
	{
		zmq_msg_t msg;
		//Size fo the message to be sent.
		int32_t msg_size = strlen(argv[i]) + 4;
		//Initialize size of the message.
		zmq_msg_init_size (&msg, msg_size);
		//Message content. The topic is specified in the beginning of the message.
		sprintf((char *) zmq_msg_data(&msg), "%s REL%c", argv[i], '\0'); 

		if (zmq_msg_send(&msg, publisher, 0) == -1)
		{
			perror("ERRIMSSRLS_PUBLISH");
			return -1;
		}

		if (zmq_msg_close(&msg) == -1)
		{
			perror("ERRIMSSRLS_CLS_MSG");
			return -1;
		}
	}

	//Close the publisher socket.
	if (zmq_close(publisher) == -1)
	{
		perror("ERRIMSSRLS_CLS_PUB");
		return -1;
	}

	//Close context holding the publisher socket.
	if (zmq_ctx_destroy(context) == -1)
	{
		perror("ERRIMSSRLS_CTX_DSTRY");
		return -1;
	}

	MPI_Finalize();	

	return 0;
}
