#ifndef LIBSTAT_
#define LIBSTAT_

#include "workers.h"

#define OFFSET	8

//Method retrieving the size of the current metadata file.
int64_t metadata_size(char * metadata_file);

//Method retrieving the set of dataset metadata structures stored in a metadata file.
char * metadata_read(char * metadata_file, map_records * map, char * buffer, uint64_t * bytes_written);

//Method storing the set of dataset metadata structures into a file.
int32_t metadata_write(char * metadata_file, char * buffer, map_records * map, p_argv * regions, int64_t segment_size, uint64_t read_metadata);

#endif
