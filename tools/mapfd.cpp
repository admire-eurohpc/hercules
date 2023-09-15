#include <map>
#include <iostream>
#include <vector>
#include <cstddef>
#include <cstring>
#include <sys/stat.h>
#include <fcntl.h>
#include <mutex>

// to manage logs.
#include "slog.h"

using std::string;
// typedef std::map<std::string, std::pair<int, long>> Map;
typedef std::map<int, std::pair<std::string, long>> Map;

std::mutex fdlock;

extern "C"
{

	void *map_fd_create()
	{
		return reinterpret_cast<void *>(new Map);
	}

	int map_fd_put(void *map, const char *pathname, const int fd, unsigned long offset)
	{
		std::unique_lock<std::mutex> lck(fdlock);
		Map *m = reinterpret_cast<Map *>(map);
		std::pair<std::string, int> value(pathname, offset);
		std::pair<std::map<int, std::pair<std::string, long>>::iterator, bool> ret;
		ret = m->insert({fd, value});
		if(ret.second==false) {
			return -1;
		} else {
			return 1;
		}
	}

	void map_fd_update_value(void *map, const char *pathname, const int fd, unsigned long offset)
	{
		std::unique_lock<std::mutex> lck(fdlock);
		Map *m = reinterpret_cast<Map *>(map);
		auto search = m->find(fd);

		if (search != m->end())
		{
			search->second.first = pathname;
			search->second.second = offset;
		}
		// else
		// {
		// 	fprintf(stderr, "Map not updated, fd=%d", fd);
		// }
	}

	void map_fd_update_fd(void *map, const char *pathname, const int fd, const int new_fd, unsigned long offset)
	{

		std::unique_lock<std::mutex> lck(fdlock);
		Map *m = reinterpret_cast<Map *>(map);
		auto node = m->extract(fd);
		node.key() = new_fd;
		m->insert(std::move(node));
	}

	void map_fd_erase(void *map, const int fd)
	// void map_fd_erase(void *map, char *k)
	{
		std::unique_lock<std::mutex> lck(fdlock);
		Map *m = reinterpret_cast<Map *>(map);
		m->erase(fd);
		// m->erase(std::string(k));
	}

	int map_fd_erase_by_pathname(void *map, const char *pathname)
	{
		std::unique_lock<std::mutex> lck(fdlock);
		Map *m = reinterpret_cast<Map *>(map);

		for (auto &it : *m)
		{
			const char *val = it.second.first.c_str();
			if (!strncmp(val, pathname, strlen(val)))
			{
				int fd = it.first;
				m->erase(fd);
				return 1;
			}
		}
		return -1;
	}

	int map_fd_search(void *map, char *pathname, const int fd, unsigned long *offset)
	{
		// lock this function.
		std::unique_lock<std::mutex> lck(fdlock);
		Map *m = reinterpret_cast<Map *>(map);
		// looking for the value with key "fd".
		auto search = m->find(fd);
		// if the key exists:
		if (search != m->end())
		{
			*offset = search->second.second;
			return 1;
		}
		else
		{
			// nothing to do.
			return -1;
		}
	}

	char *map_fd_search_by_val(void *map, const int fd)
	{
		std::unique_lock<std::mutex> lck(fdlock);
		Map *m = reinterpret_cast<Map *>(map);

		auto search = m->find(fd);

		if (search != m->end())
		{
			// when "fd" exists, return the pathname.
			return (char *)search->second.first.c_str();
		}
		else
		{
			return NULL;
		}
	}

	int map_fd_search_by_val_close(void *map, int fd)
	{
		std::unique_lock<std::mutex> lck(fdlock);
		Map *m = reinterpret_cast<Map *>(map);
		// Traverse the map
		int remove = -1;

		for (auto &it : *m)
		{
			if (it.first == fd)
			{
				remove = it.first;
			}
		}

		if (remove != -1)
		{
			m->erase(remove);
			return 1;
		}
		return 0;
	}

	int map_fd_search_by_pathname(void *map, const char *pathname, int *fd, long *offset)
	{
		std::unique_lock<std::mutex> lck(fdlock);
		Map *m = reinterpret_cast<Map *>(map);
		// Traverse the map

		for (auto &it : *m)
		{
			const char *val = it.second.first.c_str();
			if (!strncmp(val, pathname, strlen(val)))
			{
				*fd = it.first;
				*offset = it.second.second;
				return 1;
			}
		}
		return -1;
	}

} // extern "C"
