#ifndef MAP_RECORDS
#define MAP_RECORDS

#include <map>
#include <mutex>
#include <string>
#include <utility>
#include <iostream>
#include <cassert>
#include <string.h>
#include <stdio.h>
#include <vector>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/utsname.h>

// Structure storing all information related to a certain IMSS.
typedef struct
{

	// IMSS URI.
	char uri_[256];
	// Byte specifying the type of structure.
	char type; // = 'I';
	// Set of ips comforming the IMSS.
	char **ips;
	// Number of IMSS servers.
	int32_t num_storages;
	// Server's dispatcher thread connection port.
	uint16_t conn_port;
} imss_info_;

// In-memory structure storing key-address couples.

class map_records
{
public:
	map_records(const map_records &r);
	map_records() = delete;
	map_records(const uint64_t nsize);
	void set_size(const uint64_t nsize);
	int64_t get_size();
	void print_map();
	// const char *get_head_element();
	std::string get_head_element();
	int32_t erase_head_element();
	// Used in stat_worker threads
	// Method deleting a record.
	int32_t delete_metadata_stat_worker(std::string key)
	{
		return buffer.erase(key);
	}

	// Method storing a new record.
	int32_t put(std::string key, void *address, uint64_t length);
	// Method retrieving the address associated to a certain record.
	int32_t get(std::string key, void **add_, uint64_t *size_);
	// Method renaming from stat_worker
	int32_t rename_metadata_stat_worker(std::string old_key, std::string new_key);
	// Method renaming from srv_worker
	int32_t rename_data_srv_worker(std::string old_key, std::string new_key);
	// Method renaming from srv_worker
	int32_t rename_data_dir_srv_worker(std::string old_dir, std::string rdir_dest);
	// Method renaming from stat_worker
	int32_t rename_metadata_dir_stat_worker(std::string old_dir, std::string rdir_dest);
	// Used in str_worker threads
	// Method retrieving the address associated to a certain record.
	int32_t cleaning();
	int32_t cleaning_specific(std::string new_key);

	// Method retrieving a map::begin iterator referencing the first element in the map container.
	std::map<std::string, std::pair<void *, uint64_t>>::iterator begin()
	{
		return buffer.begin();
	}
	// Method retrieving a reference to the end of the map.
	std::map<std::string, std::pair<void *, uint64_t>>::iterator end()
	{
		return buffer.end();
	}

	// Method retrieving the number of elements within the map.
	int32_t size()
	{
		return buffer.size();
	}

private:
	// Map structure tracking stored records (by default sorts keys with '<' op).
	// <key(file uri), <data, lenght>>
	std::map<std::string, std::pair<void *, uint64_t>> buffer;
	// Mutex restricting access to structure.
	uint64_t total_size;
	uint64_t quantity_occupied = 0;
	std::mutex *mut;
};

#endif
