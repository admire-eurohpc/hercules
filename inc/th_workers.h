#ifndef WORKER_H
#define WORKER_H

#define READ_OP		0
#define WRITE_OP	1

#define THREAD_POOL	32

//Thread method attending a client.
void * worker (void * th_argv);

//Dispatcher thread method distributing clients among the pool of threads.
void * dispatcher (void * th_argv);

#endif
