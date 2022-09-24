#ifndef KVS_INCLUDE_MONITOR_MONITORING_HANDLERS_HPP_
#define KVS_INCLUDE_MONITOR_MONITORING_HANDLERS_HPP_

#include "hash_ring.hpp"
#include "metadata.pb.h"

void membership_handler(logger log, string &serialized,
                        GlobalRingMap &global_hash_rings,
                        unsigned &new_memory_count, unsigned &new_storage_count,
                        TimePoint &grace_start, vector<Address> &routing_ips,
                        StorageStats &memory_consumption, StorageStats &storage_consumption,
                        OccupancyStats &memory_occupancy,
                        OccupancyStats &storage_occupancy,
                        map<Key, map<Address, unsigned>> &key_access_frequency);

void depart_done_handler(logger log, string &serialized,
                         map<Address, unsigned> &departing_node_map,
                         Address management_ip, bool &removing_memory_node,
                         bool &removing_storage_node, SocketCache &pushers,
                         TimePoint &grace_start);

void feedback_handler(
    logger log, string &serialized,
    map<string, std::tuple<double, double, double, double, double, unsigned>> &user_latency,
    map<string, double> &user_throughput,
    map<Key, std::pair<double, unsigned>> &latency_miss_ratio_map,
    unsigned BenchmarkNodeCount, unsigned BenchmarkThreadCount);

#endif // KVS_INCLUDE_MONITOR_MONITORING_HANDLERS_HPP_