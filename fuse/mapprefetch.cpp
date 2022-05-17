#include <map>
#include <iostream>
#include <vector>
#include <cstddef>
#include <cstring>
#include <sys/stat.h>
#include <fcntl.h>
#include <mutex>

extern uint64_t IMSS_BLKSIZE;
#define KB 1024

std::mutex mtx;
struct PrefetchItem {
 int32_t first_block; 
 int32_t last_block;
 char * buf;
};

using std::string;
typedef std::map<std::string, struct PrefetchItem> MapPrefetch;

extern "C" {

void* map_create_prefetch() {
  return reinterpret_cast<void*> (new MapPrefetch);
}


char * map_get_buffer_prefetch (void* map, char * path, int * first_block, int * last_block) {
// critical section (exclusive access to std::cout signaled by lifetime of lck):
  std::unique_lock<std::mutex> lck (mtx);
 MapPrefetch* m = reinterpret_cast<MapPrefetch*> (map);

   auto search = m->find(std::string(path));
     
  if (search != m->end()) {
    *(first_block) = search->second.first_block;
    *(last_block) = search->second.last_block;
      return search->second.buf;
  }
  return NULL;
}

void map_update_prefetch(void* map, char * path,   int first_block,  int last_block) {
  // critical section (exclusive access to std::cout signaled by lifetime of lck):
  std::unique_lock<std::mutex> lck (mtx);
  MapPrefetch* m = reinterpret_cast<MapPrefetch*> (map);

   auto search = m->find(std::string(path));
  if (search != m->end()) {
     search->second.first_block = first_block;
     search->second.last_block = last_block;
  }
}


void map_release_prefetch(void* map, const char * path) {
  // critical section (exclusive access to std::cout signaled by lifetime of lck):
  std::unique_lock<std::mutex> lck (mtx);
  MapPrefetch* m = reinterpret_cast<MapPrefetch*> (map);
   auto search = m->find(std::string(path));
     
  if (search != m->end()) {
     free(search->second.buf);;
  } 

  m->erase(path);
}

void map_init_prefetch(void* map, char * path, char * buff) {
 // critical section (exclusive access to std::cout signaled by lifetime of lck):
  std::unique_lock<std::mutex> lck (mtx);
  MapPrefetch* m = reinterpret_cast<MapPrefetch*> (map);  

  struct  PrefetchItem  pitem;
  pitem.first_block = 0;
  pitem.last_block = 0;
  pitem.buf = buff;

  m->insert(std::pair<std::string, struct PrefetchItem>(std::string(path),  pitem));
}

int map_rename_prefetch(void* map, const char * oldname, const char * newname) {
    // critical section (exclusive access to std::cout signaled by lifetime of lck):
  std::unique_lock<std::mutex> lck (mtx);
    MapPrefetch* m = reinterpret_cast<MapPrefetch*> (map);
    auto node = m->extract(oldname);
		node.key() = newname;
		m->insert(std::move(node));

    return 1;
}

int map_rename_dir_dir_prefetch(void* map, const char * old_dir, const char * rdir_dest) {
    // critical section (exclusive access to std::cout signaled by lifetime of lck):
    std::unique_lock<std::mutex> lck (mtx);
    MapPrefetch* m = reinterpret_cast<MapPrefetch*> (map);

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
