#include <map>
#include <iostream>
#include <vector>
#include <cstddef>
#include <cstring>
#include <sys/stat.h>
#include <fcntl.h>
#include <mutex>

#include <dirent.h>

// to manage logs.
#include "slog.h"

using std::string;
typedef std::map<::DIR*, int> Map;
std::mutex fdlock;

extern "C"
{

	void *map_dir_create()
	{
		return reinterpret_cast<void *>(new Map);
	}

	void map_dir_put(void *map, DIR* dir, int v)
	{
		std::unique_lock<std::mutex> lck(fdlock);
		Map *m = reinterpret_cast<Map *>(map);
		// std::pair<int, int> value(v, p);
		m->insert({dir, v});
	}

	// void map_dir_update_value(void *map, char *k, int v, unsigned long p)
	// {
	// 	std::unique_lock<std::mutex> lck(fdlock);
	// 	Map *m = reinterpret_cast<Map *>(map);
	// 	auto search = m->find(std::string(k));

	// 	if (search != m->end())
	// 	{
	// 		search->second.first = v;
	// 		search->second.second = p;
	// 	}
	// 	else
	// 	{
	// 		fprintf(stderr, "Map not updated, key=%s", k);
	// 	}
	// }

	void map_dir_erase(void *map, DIR *dir)
	{
		std::unique_lock<std::mutex> lck(fdlock);
		Map *m = reinterpret_cast<Map *>(map);
		m->erase(dir);
	}

	int map_dir_search(void *map, DIR* dir, int *v)
	{
		// lock this function.
		std::unique_lock<std::mutex> lck(fdlock);
		Map *m = reinterpret_cast<Map *>(map);
		// looking for the value with key "k".
		auto search = m->find(dir);
		// if the key exists:
		if (search != m->end())
		{
			// get the pair values (refered as *v=first and *p=second) from the value (refered as second) of the map (refered as search).
			// fprintf(stderr, "[map_dir_search] fd with key %s has been found", k);
			// slog_debug("[map_dir_search] fd with key %s has been found", k);
			*v = search->second;
			// *p = search->second.second;
			return 1;
		}
		else
		{
			// nothing to do.
			return -1;
		}
	}

	// int map_dir_search_by_val(void *map, char *path, int v)
	// {
	// 	std::unique_lock<std::mutex> lck(fdlock);
	// 	Map *m = reinterpret_cast<Map *>(map);
	// 	// Traverse the map
	// 	for (auto &it : *m)
	// 	{
	// 		// If mapped value is K,
	// 		// then print the key value
	// 		if (it.second.first == v)
	// 		{
	// 			strcpy(path, (char *)it.first.c_str());
	// 			return 1;
	// 		}
	// 	}
	// 	return 0;
	// }

	// int map_dir_search_by_val_close(void *map, int v)
	// {
	// 	std::unique_lock<std::mutex> lck(fdlock);
	// 	Map *m = reinterpret_cast<Map *>(map);
	// 	// Traverse the map
	// 	string remove = "";

	// 	for (auto &it : *m)
	// 	{
	// 		if (it.second.first == v)
	// 		{
	// 			remove = it.first;
	// 		}
	// 	}

	// 	if (remove != "")
	// 	{
	// 		m->erase(remove);
	// 		return 1;
	// 	}
	// 	return 0;
	// }

	// int map_dir_rename(void *map, const char *oldname, const char *newname)
	// {
	// 	std::unique_lock<std::mutex> lck(fdlock);
	// 	Map *m = reinterpret_cast<Map *>(map);
	// 	auto node = m->extract(oldname);
	// 	node.key() = newname;
	// 	m->insert(std::move(node));

	// 	return 1;
	// }

} // extern "C"
