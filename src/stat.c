#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "stat.h"
#include "imss_data.h"


//Method retrieving the size of the current metadata file.
int64_t metadata_size(char * metadata_file)
{
	//STAT structure retrieving the metadata file's information.
	struct stat meta_inf;

	if (stat(metadata_file, &meta_inf) == -1)
	{
		perror("ERRIMSS_STAT_GET_META_INF");
		return -1;
	}

	//Return the file's size.
	return meta_inf.st_size;
}

//Method retrieving the set of dataset metadata structures stored in a metadata file.
unsigned char * metadata_read(char * metadata_file, map_records * map, unsigned char * buffer, uint64_t * bytes_written)
{
	//Open the file in write mode in order to create it if it does not exist.
	FILE * fp = fopen(metadata_file, "a"); fclose(fp);

	//FILE entity managing the metadata file.
	FILE * meta_file = fopen(metadata_file, "r");

	//Obtain the AMOUNT OF INFORMATION to be read from the metadata file.
	int64_t data_to_read;

	if ((data_to_read = metadata_size(metadata_file)) == -1)

		return NULL;

	if (!data_to_read)

		return buffer;

	//Obtain the NUMBER OF ELEMENTS to be read from the file.
	uint64_t elements_to_read = data_to_read / sizeof(dataset_info);

	//Number of dataset_info elements that will have been read from the file.
	uint64_t num_elem_read;
	
	//Retrieve the whole set of metadata structures into the buffer.
	if ((num_elem_read = fread((void *) buffer, sizeof(dataset_info), elements_to_read, meta_file)) < elements_to_read)
	{
		//Check if an error took place during the retrieval.
		if (ferror(meta_file) != 0)
		{
			perror("ERRIMSS_STAT_READ_METAFILE");
			return NULL;
		}
	}

	*(bytes_written) = num_elem_read * sizeof(dataset_info);

	//Store the whole set of elements retrieved into the map.
	for (uint64_t i = 0; i < num_elem_read; i++)
	{
		//Memory address of the current dataset_info struct to be written.
		unsigned char * dataset_addr_ = (buffer + i*sizeof(dataset_info));

		//Obtain the dataset key to be added to the map.
		char name_[URI_];
		memcpy(name_, dataset_addr_, URI_);

		//Add the previous key to the map.
		std::string key;
		key.assign((char *) name_);
		map->put(key, dataset_addr_, sizeof(dataset_info));
	}

	fclose(meta_file);

	return (num_elem_read * sizeof(dataset_info) + buffer);
}

//Method storing the set of dataset metadata structures into a file.
int32_t metadata_write(char * metadata_file, map_records * map, unsigned char * buffer)
{
	//TODO: file must opened in APPEND mode (a || a+ ??).
	//FILE entity managing the metadata file.
	FILE * meta_file = fopen(metadata_file, "w+");

	//Retrieve the number of metadata structures.
	int32_t num_elements = map->size();

	//Move the file pointer in order to write the sequence of metadata structures afterwards.
	fseek(meta_file, num_elements+1, SEEK_SET);

	//String containing the sequence of metadata structures.
	char metastruct_seq[num_elements+1];
	metastruct_seq[num_elements] = '\0';

	//Map iterator that will be traversing the redords map.
	std::map <std::string, std::pair<unsigned char *, uint64_t>>::iterator it;

	int32_t counter = 0;

	//Traverse the whole map writing each metadata record into the file.
	for (it = map->begin(); it != map->end(); it++)
	{
		//Specify which structure is to be written.

		if (it->second.second == sizeof(imss_info))

			metastruct_seq[counter] = 'I';

		else if (it->second.second == sizeof(dataset_info))

			metastruct_seq[counter] = 'D';

		if (fwrite((void *) it->second.first, it->second.second, 1, meta_file) < 1)
		{
			perror("ERRIMSS_STAT_WRITE_METAFILE");
			return -1;
		}

		counter++;
	}

	//Move the file pointer to the begining of the file in order to write the metadata structure sequence.
	fseek(meta_file, 0, SEEK_SET);

	//Write the metadata structure sequence into the file.
	fwrite(metastruct_seq, 1, strlen(metastruct_seq)+1, meta_file);
	
	fclose(meta_file);

	return 0;
}
