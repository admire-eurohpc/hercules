#include <map>

typedef std::map<std::string, int> Map;

extern "C" {

void* map_create() {
  return reinterpret_cast<void*> (new Map);
}

void map_put(void* map, char* k, int v) {
  Map* m = reinterpret_cast<Map*> (map);
  m->insert(std::pair<std::string, int>(std::string(k), v));
}

void map_erase(void* map, char* k) {
  Map* m = reinterpret_cast<Map*> (map);
  m->erase(std::string(k));
}

int map_search(void* map, char* k, int *v) {
    Map* m = reinterpret_cast<Map*> (map);
    auto search = m->find(std::string(k));
     
    if (search != m->end()) {
        *v =  search->second;
	return 1;
    } else {
        return -1;
    }
}
// etc...

} // extern "C"
