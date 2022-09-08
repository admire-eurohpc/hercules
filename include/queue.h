#ifndef STS_QUEUE_H
#define STS_QUEUE_H

#include <pthread.h>

typedef struct StsHeader StsHeader;

typedef struct StsElement {
  void *next;
  void *value;
} StsElement;

struct StsHeader {
  StsElement *head;
  StsElement *tail;
  int size;
  pthread_mutex_t *mutex;
};


typedef struct {
  StsHeader* (* const create)();
  void (* const destroy)(StsHeader *handle);
  int (* const size)(StsHeader *handle);
  void (* const push)(StsHeader *handle, void *elem);
  void* (* const pop)(StsHeader *handle);
} _StsQueue;

extern _StsQueue const StsQueue;

#endif
