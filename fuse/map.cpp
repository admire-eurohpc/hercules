#include <map>
#include <iostream>
#include <vector>
#include <cstddef>
#include <cstring>

using std::string;
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

int map_search(void* map, const char* k, int *v) {
    Map* m = reinterpret_cast<Map*> (map);
    auto search = m->find(std::string(k));
     
    if (search != m->end()) {
        *v =  search->second;
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
      //std::cout << "Key.it=" << it->first << "\n";

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
        //std::cout << " new_path: " << new_path<< '\n';
      
        auto node = m->extract(*i);
        node.key() = new_path;
        m->insert(std::move(node));

      }
    

    return 1;
}

} // extern "C"
