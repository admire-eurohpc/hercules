#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "stat.h"
#include "imss.h"



//Method retrieving the set of dataset metadata structures stored in a metadata file.
unsigned char *
metadata_read(char * 		metadata_file,
	      map_records * 	map,
	      unsigned char * 	buffer,
	      uint64_t * 	bytes_written)
{
	//FILE entity managing the metadata file.
	FILE * meta_file = fopen(metadata_file, "r");

	//STAT structure retrieving the metadata file's information.
	struct stat meta_inf;
	if (stat(metadata_file, &meta_inf) == -1)
	{
		perror("ERRIMSS_METADATAREAD_GETMETAINF");
		return NULL;
	}

	//Total size of the metadata file.
	int64_t metadata_info = meta_inf.st_size;

	//If there was no metadata stored in the file, return the reference provided.
	if (!metadata_info)

		return buffer;

	//Number of structures stored in the file.
	int32_t num_elements;

	if (fread((char *) &num_elements, sizeof(int32_t), 1, meta_file) < 1)
	{
		//Check if an error took place during the retrieval.
		if (ferror(meta_file) != 0)
		{
			perror("ERRIMSS_METADATAREAD_NUMELEMS");
			return NULL;
		}
	}

	//Move the pointer to the begining of the sequence of structures to be read.
	fseek(meta_file, OFFSET, SEEK_SET);

	//Update the number of bytes to be read.
	metadata_info -= OFFSET;

	//Read into the buffer the remaining chunk of metadata bytes.
	if ((*bytes_written = fread((void *) buffer, metadata_info, 1, meta_file)) < metadata_info)
	{
		//Check if an error took place during the retrieval.
		if (ferror(meta_file) != 0)
		{
			perror("ERRIMSS_METADATAREAD_READ");
			return NULL;
		}
	}

	//Store the whole set of elements retrieved into the map.
	for (int32_t i = 0; i < num_elements; i++)
	{
		//Resource URI. Will be used as identifier within the map structure.
		char identifier[URI_];
		memcpy((void *) identifier, buffer, URI_);
		std::string key;
		key.assign((char *) identifier);

		//Character specifying the type of structure that will be managed.
		char struct_type = *(buffer + URI_);

		//Operate depending to the type of structure to be sealt with.
		switch (struct_type)
		{
			//Add the corresponding structure to the provided map.
			case 'I':
			{
				imss_info * imss_struct = (imss_info *) buffer;

				//Size of the current IMSS structure.
				uint64_t struct_size = sizeof(imss_info) + (imss_struct->num_storages * LINE_LENGTH);

				map->put(key, buffer, struct_size);

				buffer += struct_size;

				break;
			}
			case 'D':
			{
				map->put(key, buffer, sizeof(dataset_info));

				buffer += sizeof(dataset_info);

				break;
			}
			case 'L':
			{
				dataset_info * dataset_struct = (dataset_info *) buffer;

				//Size of the current dataset structure.
				uint64_t struct_size = sizeof(dataset_info) + (dataset_struct->num_data_elem * sizeof(uint16_t));

				map->put(key, buffer, struct_size);

				buffer += struct_size;

				break;
			}
		}
	}

	fclose(meta_file);

	return buffer;
}

//Method storing the set of dataset metadata structures into a file.
int32_t
metadata_write(char * 		metadata_file,
	       unsigned char * 	buffer,
	       map_records * 	map,
	       p_argv *		regions,
	       int64_t 		segment_size,
	       uint64_t 	read_metadata)
{
	//FILE entity managing the metadata file.
	FILE * meta_file = fopen(metadata_file, "w+");

	//Retrieve the number of metadata structures stored by the metadata server.
	int32_t num_elements = map->size();

	//Write the number of structures that will be written into the file.
	if (fwrite((void *) &num_elements, sizeof(int32_t), 1, meta_file) < 1)
	{
		perror("ERRIMSS_METADATAWRITE_NUMSTRUCTS");
		return -1;
	}

	//Move the file pointer OFFSET bytes from the begining in order to start writing structures.
	fseek(meta_file, OFFSET, SEEK_SET);

	//Write the metadata chunck stored by each metadata server thread.
	for (uint32_t i = 0; i < THREAD_POOL; i++)
	{
		//Write the metadata info written into the buffer through a previous invocation of the metadata_read function.
		if (!i)
		{
			//If nothing was written in a previous execution context, continue.
			if (!read_metadata)

				continue;

			//Write the previous memory region.
			if (fwrite((void *) buffer, sizeof(char), read_metadata, meta_file) < 1)
			{
				perror("ERRIMSS_METADATAWRITE_METACHUNK");
				return -1;
			}

			buffer += read_metadata;
		}
		//Write the metadata info stored by each metadata server thread.
		else
		{
			//Number of bytes to be written.
			uint32_t bytes_to_write = regions[i].pt - buffer;

			//If no metadata information was written by the previous thread, continue.
			if (!bytes_to_write)
			{
				buffer += segment_size;
				continue;
			}

			//Write the previous memory region.
			if (fwrite((void *) buffer, sizeof(char), bytes_to_write, meta_file) < 1)
			{
				perror("ERRIMSS_METADATAWRITE_THREADCHUNK");
				return -1;
			}

			//Memory address where the next thread started storing structures.
			buffer += segment_size;
		}
	}

	fclose(meta_file);

	return 0;
}

