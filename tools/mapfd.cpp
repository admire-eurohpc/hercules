#include <map>
#include <iostream>
#include <vector>
#include <cstddef>
#include <cstring>
#include <sys/stat.h>
#include <fcntl.h>
#include <mutex>

using std::string;
typedef std::map<std::string,std::pair< int, long>> Map;
std::mutex fdlock;

extern "C" {

	void* map_fd_create() {
		return reinterpret_cast<void*> (new Map);
	}

	void map_fd_put(void* map, char* k, int v, unsigned long p) {
		std::unique_lock<std::mutex> lck (fdlock);
		Map* m = reinterpret_cast<Map*> (map);
		std::pair<int, int> value(v, p);
		m->insert({k, value});
	}

	void map_fd_update_value(void* map, char* k, int v, unsigned long p) {
		std::unique_lock<std::mutex> lck (fdlock);
		Map* m = reinterpret_cast<Map*> (map);
		auto search = m->find(std::string(k));

		if (search != m->end()) {
			search->second.first = v;
			search->second.second = p;
		}
	}

	void map_fd_erase(void* map, char* k) {
		std::unique_lock<std::mutex> lck (fdlock);  
		Map* m = reinterpret_cast<Map*> (map);
		m->erase(std::string(k));
	}

	int map_fd_search(void* map, const char* k, int *v,  unsigned long *p) {
		std::unique_lock<std::mutex> lck (fdlock);
		Map* m = reinterpret_cast<Map*> (map);
		auto search = m->find(std::string(k));

		if (search != m->end()) {
			*v =  search->second.first;
			*p = search->second.second; 
			return 1;
		} else {
			return -1;
		} 
	}

	int map_fd_search_by_val(void* map, char* path, int v) {
		std::unique_lock<std::mutex> lck (fdlock);
		Map* m = reinterpret_cast<Map*> (map);
		// Traverse the map
		for (auto& it : *m) {
			// If mapped value is K,
			// then print the key value
			if (it.second.first == v) {
				strcpy(path,(char*)it.first.c_str());
				return 1;
			}
		}
		return 0;
	}

	int map_fd_search_by_val_close(void* map, int v) {
		std::unique_lock<std::mutex> lck (fdlock);
		Map* m = reinterpret_cast<Map*> (map);
		// Traverse the map
		string remove = "";

		for (auto& it : *m) {
			if (it.second.first == v) {
				remove=it.first;
			}
		}

		if(remove != ""){
			m->erase(remove);
			return 1;
		}
		return 0;
	}

	int map_fd_rename(void* map, const char * oldname, const char * newname) {
		std::unique_lock<std::mutex> lck (fdlock);
		Map* m = reinterpret_cast<Map*> (map);
		auto node = m->extract(oldname);
		node.key() = newname;
		m->insert(std::move(node));

		return 1;
	}


} // extern "C"
