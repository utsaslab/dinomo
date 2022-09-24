#ifndef KVS_INCLUDE_TYPES_HPP_
#define KVS_INCLUDE_TYPES_HPP_

#include "kvs_threads.hpp"
#include "types.hpp"
#include <chrono>

using StorageStats = map<Address, map<unsigned, unsigned long long>>;

using CacheStats = map<Address, map<unsigned, uint64_t>>;

using KvsStats = map<Address, map<unsigned, double>>;

using OccupancyStats = map<Address, map<unsigned, pair<double, unsigned>>>;

using AccessStats = map<Address, map<unsigned, unsigned>>;

using TimePoint = std::chrono::time_point<std::chrono::system_clock>;

using ServerThreadList = vector<ServerThread>;

using ServerThreadSet = std::unordered_set<ServerThread, ThreadHash>;

#endif // KVS_INCLUDE_TYPES_HPP_