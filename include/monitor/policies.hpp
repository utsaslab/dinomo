#ifndef KVS_INCLUDE_MONITOR_POLICIES_HPP_
#define KVS_INCLUDE_MONITOR_POLICIES_HPP_

#include "hash_ring.hpp"

extern bool kEnableTiering;
extern bool kEnableElasticity;
extern bool kEnableSelectiveRep;
extern bool kUsingAvgLatency;

void storage_policy(logger log, GlobalRingMap &global_hash_rings,
                    TimePoint &grace_start, SummaryStats &ss,
                    unsigned &memory_node_count, unsigned &storage_node_count,
                    unsigned &new_memory_count, unsigned &new_storage_count,
                    bool &removing_storage_node, Address management_ip,
                    MonitoringThread &mt,
                    map<Address, unsigned> &departing_node_map,
                    SocketCache &pushers);

void movement_policy(logger log, GlobalRingMap &global_hash_rings,
                     LocalRingMap &local_hash_rings, TimePoint &grace_start,
                     SummaryStats &ss, unsigned &memory_node_count,
                     unsigned &storage_node_count, unsigned &new_memory_count,
                     unsigned &new_storage_count, Address management_ip,
                     map<Key, KeyReplication> &key_replication_map,
                     map<Key, unsigned> &key_access_summary,
                     map<Key, unsigned> &key_size, MonitoringThread &mt,
                     SocketCache &pushers, zmq::socket_t &response_puller,
                     vector<Address> &routing_ips, unsigned &rid);

void slo_policy(logger log, GlobalRingMap &global_hash_rings,
                LocalRingMap &local_hash_rings, TimePoint &grace_start,
                SummaryStats &ss, unsigned &memory_node_count,
                unsigned &new_memory_count, bool &removing_memory_node,
                Address management_ip,
                map<Key, KeyReplication> &key_replication_map,
                map<Key, unsigned> &key_access_summary, MonitoringThread &mt,
                map<Address, unsigned> &departing_node_map,
                SocketCache &pushers, zmq::socket_t &response_puller,
                vector<Address> &routing_ips, unsigned &rid,
                map<Key, std::pair<double, unsigned>> &latency_miss_ratio_map);

#endif // KVS_INCLUDE_MONITOR_POLICIES_HPP_