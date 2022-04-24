#include <map>
#include <iostream>
#include <vector>
#include <cstddef>
#include <cstring>
#include <sys/stat.h>
#include <fcntl.h>

extern uint64_t IMSS_BLKSIZE;
#define KB 1024

using std::string;
typedef std::map<std::string, struct elements> Map;

struct elements {
  int fd;
  struct stat stat;
  char * aux;
}elements;

extern "C" {

void* map_create() {
  return reinterpret_cast<void*> (new Map);
}

void map_put(void* map, char* k, int v, struct stat stat, char * aux) {
  
  Map* m = reinterpret_cast<Map*> (map);
  //char *aux = (char*) malloc(IMSS_BLKSIZE*KB);
  //printf("insert aux %p\n", aux);
  struct elements p = {v,stat,aux};
  //printf("insert aux %p\n", p.aux);
  m->insert(std::pair<std::string, struct elements>(std::string(k),p));
}

void map_update(void* map, char* k, int v, struct stat stat) {
  Map* m = reinterpret_cast<Map*> (map);
  auto search = m->find(std::string(k));
  search->second.stat  = stat;
}

void map_erase(void* map, char* k) {
  Map* m = reinterpret_cast<Map*> (map);
  auto search = m->find(std::string(k));
     
    if (search != m->end()) {
        free(search->second.aux);
    } 
  m->erase(std::string(k));
}

int map_search(void* map, const char* k, int *v, struct stat *stat, char ** aux) {
    Map* m = reinterpret_cast<Map*> (map);
    auto search = m->find(std::string(k));
     
    if (search != m->end()) {
        *v =  search->second.fd;
        *stat = search->second.stat;
        //printf("map_search: %p\n", search->second.aux);
        *aux = search->second.aux;
        //printf("map_search aux: %p\n", *aux);
	      return 1;
    } else {
        return -1;
    }
}

int map_rename(void* map, const char * oldname, const char * newname) {
    Map* m = reinterpret_cast<Map*> (map);
    auto node = m->extract(oldname);
		node.key() = newname;
		m->insert(std::move(node));

    return 1;
}

int map_rename_dir_dir(void* map, const char * old_dir, const char * rdir_dest) {
    Map* m = reinterpret_cast<Map*> (map);
    
    std::vector<string> vec;

    for(auto it = m->cbegin(); it != m->cend(); ++it){
      string key = it->first;

      int found = key.find(old_dir);
      if (found!=std::string::npos){
        vec.insert(vec.begin(),key);
      }
    }

    std::vector<string>::iterator i;
			for (i=vec.begin(); i<vec.end(); i++){
        string key = *i;
        key.erase(0,strlen(old_dir)-1);
        
        string new_path=rdir_dest;
        new_path.append(key);
      
        auto node = m->extract(*i);
        node.key() = new_path;
        m->insert(std::move(node));

      }
    

    return 1;
}

} // extern "C"
