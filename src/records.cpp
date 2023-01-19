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
#include "records.hpp"
#include "imss.h"
#include "queue.h"

using std::map;
using std::pair;
using std::make_pair;
using std::string;

extern StsHeader * mem_pool;

map_records::map_records(const uint64_t nsize)  {
	total_size = nsize;
	mut = new std::mutex();
}

void map_records::set_size(const uint64_t nsize)  {
	total_size = nsize;
}

int64_t map_records::get_size()  {
	return total_size;
}

//Method storing a new record.
int32_t map_records::put(std::string key, char * address, uint64_t length)
{
	//Construct a pair object storing the couple of values associated to a key.
	std::pair<char *, uint64_t> value(address, length);
	//Block the access to the map structure.
	std::unique_lock<std::mutex> lock(*mut);
	//Add a new couple to the map.
	//printf("total_size=%ld, quantity_occupied=%ld\n",total_size, quantity_occupied);
	if (quantity_occupied + length > total_size && total_size>0) { //out of space
		fprintf(stderr, "[Map record] Out of space  %ld/%ld.\n",quantity_occupied + length, total_size);			  
		return -1;
	}


	//struct utsname detect;
	//uname(&detect);
	//DPRINT("Nodename    - %s add in map=%s\n", detect.nodename, key.c_str());
	//printf("quantity=%ld total size=%ld\n",quantity_occupied, total_size);
	quantity_occupied = quantity_occupied + length;
	buffer.insert({key, value});
	return 0;
}

//Method retrieving the address associated to a certain record.
int32_t map_records::get(std::string key, char ** add_, uint64_t * size_)
{

	// fprintf(stderr, "GET KEY=%s\n",key.c_str());
	//Map iterator that will be searching for the key.
	std::map <std::string, std::pair<char *, uint64_t>>::iterator it;
	//Block the access to the map structure.
	std::unique_lock<std::mutex> lock(*mut);

	// struct utsname detect;
	// uname(&detect);

	if (buffer.empty())
		return 0;

	//Search for the address related to the key.
	it = buffer.find(key);
	//Check if the value did exist within the map.
	if(it == buffer.end()){
		// fprintf(stderr,"Nodename-%s NO EXIST=%s\n",detect.nodename, key.c_str());
		// fprintf(stderr,"NO EXIST=%s\n", key.c_str());
		return 0;
	}

	// fprintf(stderr,"GET-%s \n", key.c_str());
	// fprintf(stderr,"Nodename    - %s	GET-%s \n", detect.nodename, key.c_str());

	//Assign the values obtained to the provided references.
	//std::cout <<"Exist " << key << '\n';
	*(add_) = it->second.first;
	*(size_) = it->second.second;

	//Return the address associated to the record.
	return 1;
}


//Method renaming from stat_worker
int32_t map_records::rename_metadata_stat_worker(std::string old_key, std::string new_key)
{
	//Map iterator that will be searching for the key.
	std::map <std::string, std::pair<char *, uint64_t>>::iterator it;
	//Block the access to the map structure.
	std::unique_lock<std::mutex> lock(*mut);

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
int32_t map_records::rename_data_srv_worker(std::string old_key, std::string new_key)
{
	//Map iterator that will be searching for the key.
	std::map <std::string, std::pair<char *, uint64_t>>::iterator it;
	//Block the access to the map structure.
	std::unique_lock<std::mutex> lock(*mut);

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
int32_t map_records::rename_data_dir_srv_worker(std::string old_dir, std::string rdir_dest)
{
	//printf("rename data_dir_dir_srv_worker old_dir=%s dir_dest=%s\n",old_dir.c_str(), rdir_dest.c_str());
	//Map iterator that will be searching for the key.
	std::map <std::string, std::pair<char *, uint64_t>>::iterator it;
	//Block the access to the map structure.
	std::unique_lock<std::mutex> lock(*mut);
	std::vector<string> vec;

	struct utsname detect;
	uname(&detect);

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
		//printf("Nodename    - %s	Rename modify original=%s\n",detect.nodename,key.c_str());
		key.erase(0,old_dir.length()-1);

		string new_path=rdir_dest;
		new_path.append(key);

		auto node = buffer.extract(*i);
		//printf("Nodename    - %s	Rename new=%s\n",detect.nodename, new_path.c_str());
		node.key() = new_path;
		buffer.insert(std::move(node));

	}

	return 1;
}

//Method renaming from stat_worker
int32_t map_records::rename_metadata_dir_stat_worker(std::string old_dir, std::string rdir_dest)
{
	//printf("rename_metadata_dir_dir_stat_worker\n");
	//Map iterator that will be searching for the key.
	std::map <std::string, std::pair<char *, uint64_t>>::iterator it;
	//Block the access to the map structure.
	std::unique_lock<std::mutex> lock(*mut);
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
int32_t map_records::cleaning()
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
	std::unique_lock<std::mutex> lock(*mut);
	std::vector<string>::iterator i;
	for (i=vec.begin(); i<vec.end(); i++){
		//std::cout << "Garbage Collector: Deleting " << *i << "\n";
		auto item = buffer.find(*i);
		StsQueue.push(mem_pool, item->second.first);
		// free(item->second.first);
		buffer.erase (*i);

	}



	/*for(const auto & it : buffer){
	  string key = it.first;
	  std::cout <<"Garbage Collector: Exist " << key << '\n';
	  }*/
	return 0;
}

int32_t map_records::cleaning_specific(std::string new_key)
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
	std::unique_lock<std::mutex> lock(*mut);
	std::vector<string>::iterator i;
	for (i=vec.begin(); i<vec.end(); i++){
		//std::cout << "Garbage Collector: Deleting " << *i << "\n";
		auto item = buffer.find(*i);
		StsQueue.push(mem_pool, item->second.first);
		// free(item->second.first);
		buffer.erase (*i);

	}

	/*for(const auto & it : buffer){
	  string key = it.first;
	  std::cout <<"Garbage Collector: Exist " << key << '\n';
	  }*/
	return 0;
}


