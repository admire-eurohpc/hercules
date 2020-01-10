#ifndef MEMALLOC_
#define MEMALLOC_

// Method allocating a certain memory region to a buffer process (requested memory in bytes).
//int64_t memalloc (int64_t req_size, int32_t block_size, unsigned char ** reference);
int64_t memalloc (int64_t req_size, unsigned char ** reference);

#endif
