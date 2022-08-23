/**
* Copyright (C) Mellanox Technologies Ltd. 2001-2016.  ALL RIGHTS RESERVED.
*
* See file LICENSE for terms.
*/

#ifndef UCX_HELLO_WORLD_H
#define UCX_HELLO_WORLD_H

#include <ucs/memory/memory_type.h>

#include <sys/poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <netdb.h>

#ifdef HAVE_CUDA
#  include <cuda.h>
#  include <cuda_runtime.h>
#endif


#define CHKERR_ACTION(_cond, _msg, _action) \
    do { \
        if (_cond) { \
            fprintf(stderr, "Failed to %s\n", _msg); \
            _action; \
        } \
    } while (0)


#define CHKERR_JUMP(_cond, _msg, _label) \
    CHKERR_ACTION(_cond, _msg, goto _label)


#define CHKERR_JUMP_RETVAL(_cond, _msg, _label, _retval) \
    do { \
        if (_cond) { \
            fprintf(stderr, "Failed to %s, return value %d\n", _msg, _retval); \
            goto _label; \
        } \
    } while (0)


static ucs_memory_type_t test_mem_type = UCS_MEMORY_TYPE_HOST;


#define CUDA_FUNC(_func)                                   \
    do {                                                   \
        cudaError_t _result = (_func);                     \
        if (cudaSuccess != _result) {                      \
            fprintf(stderr, "%s failed: %s\n",             \
                    #_func, cudaGetErrorString(_result));  \
        }                                                  \
    } while(0)



#endif /* UCX_HELLO_WORLD_H */
