#ifndef MAP_RECORDS
#define MAP_RECORDS

#include <map>
#include <mutex>
#include <string>
#include <utility>

//In-memory structure storing key-address couples.
class map_records
{
	public:

		map_records() {}

		//Method storing a new record.
		int32_t put(std::string key, unsigned char * address, uint64_t length)
		{
			//Construct a pair object storing the couple of values associated to a key.
			std::pair<unsigned char *, uint64_t> value(address, length);
			//Block the access to the map structure.
			std::unique_lock<std::mutex> lock(mut);
			//Add a new couple to the map.
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
			if (it == buffer.end())

				return 0;

			//Assign the values obtained to the provided references.
			*(add_) = it->second.first;
			*(size_) = it->second.second;

			//Return the address associated to the record.
			return 1;
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
		std::mutex mut;
};

#endif

