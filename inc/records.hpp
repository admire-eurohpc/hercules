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
#include <fcntl.h>

using std::map;
using std::pair;
using std::make_pair;
using std::string;

//Structure storing all information related to a certain IMSS.
typedef struct {

	//IMSS URI.
	char uri_[256];
	//Byte specifying the type of structure.
	char type;// = 'I';
	//Set of ips comforming the IMSS.
	char ** ips;
	//Number of IMSS servers.
	int32_t num_storages;
	//Server's dispatcher thread connection port.
	uint16_t conn_port;

} imss_info_;

//In-memory structure storing key-address couples.
class map_records
{
	public:
	   
		map_records()  {
			total_size = 0;
		}
	
		map_records(uint64_t nsize)  {
			total_size = nsize;
		}


		void set_size(uint64_t nsize)  {
			total_size = nsize;
		}

		//Used in stat_worker threads
		//Method deleting a record.
		int32_t delete_metadata_stat_worker(std::string key)
		{
			return buffer.erase(key);
		}
		
		//Method storing a new record.
		int32_t put(std::string key, unsigned char * address, uint64_t length)
		{
			//Construct a pair object storing the couple of values associated to a key.
			std::pair<unsigned char *, uint64_t> value(address, length);
			//Block the access to the map structure.
			std::unique_lock<std::mutex> lock(mut);
			//Add a new couple to the map.
			if (quantity_occupied + length > total_size) { //out of space
			  fprintf(stderr, "[Map record] Out of space  %ld/%ld.\n",quantity_occupied + length, total_size);			  
			  return -1;
			}
			quantity_occupied = quantity_occupied + length;
			buffer.insert({key, value});

			return 0;
		}

		//Method retrieving the address associated to a certain record.
		int32_t get(std::string key, unsigned char ** add_, uint64_t * size_)
		{
			//Map iterator that will be searching for the key.
			std::map <std::string, std::pair<unsigned char *, uint64_t>>::iterator it;
			//Block the access to the map structure.
			std::unique_lock<std::mutex> lock(mut);

			//Search for the address related to the key.
			it = buffer.find(key);
			//Check if the value did exist within the map.
			if(it == buffer.end()){
				return 0;
			}

			//Assign the values obtained to the provided references.
			
			*(add_) = it->second.first;
			*(size_) = it->second.second;

			//Return the address associated to the record.
			return 1;
		}

		
		//Method renaming from stat_worker
		int32_t rename_metadata_stat_worker(std::string old_key, std::string new_key)
		{
			//Map iterator that will be searching for the key.
			std::map <std::string, std::pair<unsigned char *, uint64_t>>::iterator it;
			//Block the access to the map structure.
			std::unique_lock<std::mutex> lock(mut);
			
			//Search for the address related to the key.
			it = buffer.find(old_key);
			//Check if the value did exist within the map.
			if(it == buffer.end()){
				return 0;
			}else{
				
				imss_info_ * data = (imss_info_*) it->second.first;
				strcpy(data->uri_,new_key.c_str());
				auto node = buffer.extract(old_key);
				node.key()=new_key;
				buffer.insert(std::move(node));
				
			}

			//Return the address associated to the record.
			return 1;
		}

		//Method renaming from srv_worker
		int32_t rename_data_srv_worker(std::string old_key, std::string new_key)
		{
			//Map iterator that will be searching for the key.
			std::map <std::string, std::pair<unsigned char *, uint64_t>>::iterator it;
			//Block the access to the map structure.
			std::unique_lock<std::mutex> lock(mut);
			
			//save partners for later deletion and new insertion of news paths
			std::vector<string> vec;
			
			for(const auto & it : buffer) {
				string key = it.first;
				
				int pos = key.find('$');
				string path = key.substr(0,pos);
				string block = key.substr(pos,key.length()+1);
				
				if(path.compare(old_key) == 0){
					vec.insert(vec.begin(),key);
				}
			}

			std::vector<string>::iterator i;
			for (i=vec.begin(); i<vec.end(); i++){
				auto item = buffer.find(*i);

				string key = *i;
				int pos = key.find('$');
				string block = key.substr(pos,key.length()+1);

				
				string new_path=new_key+block;
				auto node = buffer.extract(key);
				node.key()=new_path;
				buffer.insert(std::move(node));				
			}

			//Return the address associated to the record.
			return 1;
		}

		//Method renaming from srv_worker
		int32_t rename_data_dir_dir_srv_worker(std::string old_dir, std::string rdir_dest)
		{
			//Map iterator that will be searching for the key.
			std::map <std::string, std::pair<unsigned char *, uint64_t>>::iterator it;
			//Block the access to the map structure.
			std::unique_lock<std::mutex> lock(mut);
			std::vector<string> vec;
			
			for(const auto & it : buffer) {
				string key = it.first;
				int found = key.find(old_dir);
				if (found!=std::string::npos){
					vec.insert(vec.begin(),key);

					key.erase(0,old_dir.length()-1);
					
					string new_path=rdir_dest;
					new_path.append(key);
				}
			}

			std::vector<string>::iterator i;
			for (i=vec.begin(); i<vec.end(); i++){
				string key = *i;
				key.erase(0,old_dir.length()-1);
				
				string new_path=rdir_dest;
				new_path.append(key);
		
				auto node = buffer.extract(*i);
				node.key() = new_path;
				buffer.insert(std::move(node));

			}

			return 1;
		}

		//Method renaming from stat_worker
		int32_t rename_metadata_dir_dir_stat_worker(std::string old_dir, std::string rdir_dest)
		{
			//Map iterator that will be searching for the key.
			std::map <std::string, std::pair<unsigned char *, uint64_t>>::iterator it;
			//Block the access to the map structure.
			std::unique_lock<std::mutex> lock(mut);
			std::vector<string> vec;
			
			for(const auto & it : buffer) {
				string key = it.first;
				int found = key.find(old_dir);
				if (found!=std::string::npos){
					vec.insert(vec.begin(),key);

					key.erase(0,old_dir.length()-1);
					
					string new_path=rdir_dest;
					new_path.append(key);
					
					imss_info_ * data = (imss_info_*) it.second.first;
					strcpy(data->uri_,new_path.c_str());

				}
			}

			std::vector<string>::iterator i;
			for (i=vec.begin(); i<vec.end(); i++){
			string key = *i;
			key.erase(0,old_dir.length()-1);
			
			string new_path=rdir_dest;
			new_path.append(key);
		
			auto node = buffer.extract(*i);
			node.key() = new_path;
			buffer.insert(std::move(node));

			}


			return 1;
		}


		//Used in str_worker threads
		//Method retrieving the address associated to a certain record.
		int32_t cleaning()
		{
			std::vector<string> vec;

			for(const auto & it : buffer) {
				string key = it.first;
				int found = key.find("$0");
			
				if(found != std::string::npos){
		
					//comprobar la estructura st_ulink
					struct stat * st_p = (struct stat *) it.second.first;
					//std::cout << key << "stlink:" <<st_p->st_nlink<<'\n';
					if(st_p->st_nlink == 0){
						
						//borrar todos los bloques con mismo path/key
						for(const auto & it2 : buffer){
							
							string partner_key = it2.first;
							if(partner_key.compare(key) != 0){//para no borrar el actual con el que estoy comparando
								int pos = key.find('$');
								string path = key.substr(0,pos);
								
								int pos_partner = partner_key.find('$');
								string partner_path = partner_key.substr(0,pos_partner);
								
								int found_partner = partner_path.compare(path);
								if(found_partner == 0){
									vec.insert(vec.begin(),partner_key);
								}
							}
						}
						vec.insert(vec.begin(),key);
					}	    
					
				}
				
			}

			//Block the access to the map structure.
			std::unique_lock<std::mutex> lock(mut);
			std::vector<string>::iterator i;
			for (i=vec.begin(); i<vec.end(); i++){
				//std::cout << "Garbage Collector: Deleting " << *i << "\n";
				auto item = buffer.find(*i);
				free(item->second.first);
				buffer.erase (*i);
				
			}
			

			
			/*for(const auto & it : buffer){
				string key = it.first;
				std::cout <<"Garbage Collector: Exist " << key << '\n';
			}*/
			return 0;
		}

		int32_t cleaning_specific(std::string new_key)
		{
			std::vector<string> vec;

		
					
				
				//borrar todos los bloques con mismo path/key
				for(const auto & it2 : buffer){
					
					string partner_key = it2.first;

						
						int pos_partner = partner_key.find('$');
						string partner_path = partner_key.substr(0,pos_partner);
						
						int found_partner = partner_path.compare(new_key);
						if(found_partner == 0){
							vec.insert(vec.begin(),partner_key);
						}
					
				}
				    

			//Block the access to the map structure.
			std::unique_lock<std::mutex> lock(mut);
			std::vector<string>::iterator i;
			for (i=vec.begin(); i<vec.end(); i++){
				//std::cout << "Garbage Collector: Deleting " << *i << "\n";
				auto item = buffer.find(*i);
				free(item->second.first);
				buffer.erase (*i);
				
			}
			

			
			/*for(const auto & it : buffer){
				string key = it.first;
				std::cout <<"Garbage Collector: Exist " << key << '\n';
			}*/
			return 0;
		}

		//Method retrieving a map::begin iterator referencing the first element in the map container.
		std::map <std::string, std::pair<unsigned char *, uint64_t>>::iterator begin()
		{
			return buffer.begin();
		}

		//Method retrieving a reference to the end of the map.
		std::map <std::string, std::pair<unsigned char *, uint64_t>>::iterator end()
		{
			return buffer.end();
		}

		//Method retrieving the number of elements within the map.
		int32_t size()
		{
			return buffer.size();
		}

	private:

		//Map structure tracking stored records (by default sorts keys with '<' op).
		std::map <std::string, std::pair<unsigned char *, uint64_t>> buffer;
		//Mutex restricting access to structure.
        uint64_t total_size;
		uint64_t quantity_occupied = 0;
		std::mutex mut;
};

#endif
