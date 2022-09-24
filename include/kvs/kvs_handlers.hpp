#ifndef INCLUDE_KVS_KVS_HANDLERS_HPP_
#define INCLUDE_KVS_KVS_HANDLERS_HPP_

#include "hash_ring.hpp"
#include "metadata.pb.h"
#include "requests.hpp"
#include "server_utils.hpp"

void node_join_handler(unsigned thread_id, unsigned &seed, Address public_ip,
                       Address private_ip, logger log, string &serialized,
                       GlobalRingMap &global_hash_rings,
                       LocalRingMap &local_hash_rings,
#ifdef SHARED_NOTHING
                       set<Key> &stored_key_map,
#else
                       map<Key, KeyProperty> &stored_key_map,
#endif
                       map<Key, KeyReplication> &key_replication_map,
                       map<Key, vector<PendingRequest> > &batching_requests,
                       set<Key> &join_remove_set, SocketCache &pushers,
                       ServerThread &wt, AddressKeysetMap &join_gossip_map,
                       int self_join_count, SerializerMap &serializers,
                       zmq::socket_t &response_puller,
                       vector<zmq::pollitem_t> &failover_pollitems);

void node_depart_handler(unsigned thread_id, Address public_ip,
                         Address private_ip, GlobalRingMap &global_hash_rings,
                         logger log, string &serialized, SocketCache &pushers);

void self_depart_handler(unsigned thread_id, unsigned &seed, Address public_ip,
                         Address private_ip, logger log, string &serialized,
                         GlobalRingMap &global_hash_rings,
                         LocalRingMap &local_hash_rings,
#ifdef SHARED_NOTHING
                         set<Key> &stored_key_map,
#else
                         map<Key, KeyProperty> &stored_key_map,
#endif
                         map<Key, KeyReplication> &key_replication_map,
                         vector<Address> &routing_ips,
                         vector<Address> &monitoring_ips, ServerThread &wt,
                         SocketCache &pushers, SerializerMap &serializers,
                         map<Key, vector<PendingRequest> > &pending_requests,
                         map<Key, vector<PendingRequest> > &batching_requests,
                         vector<zmq::pollitem_t> &drain_pollitems,
                         zmq::socket_t &request_puller);

void user_request_handler(
    unsigned &access_count, unsigned &seed, string &serialized, logger log,
    GlobalRingMap &global_hash_rings, LocalRingMap &local_hash_rings,
    map<Key, vector<PendingRequest>> &pending_requests,
    map<Key, std::multiset<TimePoint>> &key_access_tracker,
#ifdef SHARED_NOTHING
    set<Key> &stored_key_map,
#else
    map<Key, KeyProperty> &stored_key_map,
#endif
    map<Key, KeyReplication> &key_replication_map, set<Key> &local_changeset,
    ServerThread &wt, SerializerMap &serializers, SocketCache &pushers,
    map<Key, vector<PendingRequest>> &batching_requests, bool batching);

void gossip_handler(unsigned &seed, string &serialized,
                    GlobalRingMap &global_hash_rings,
                    LocalRingMap &local_hash_rings,
                    map<Key, vector<PendingGossip>> &pending_gossip,
#ifdef SHARED_NOTHING
                    set<Key> &stored_key_map,
#else
                    map<Key, KeyProperty> &stored_key_map,
#endif
                    map<Key, KeyReplication> &key_replication_map,
                    ServerThread &wt, SerializerMap &serializers,
                    SocketCache &pushers, logger log,
                    map<Key, vector<PendingRequest> > &batching_requests,
                    bool batching);

void replication_response_handler(
    unsigned &seed, unsigned &access_count, logger log, string &serialized,
    GlobalRingMap &global_hash_rings, LocalRingMap &local_hash_rings,
    map<Key, vector<PendingRequest>> &pending_requests,
    map<Key, vector<PendingGossip>> &pending_gossip,
    map<Key, std::multiset<TimePoint>> &key_access_tracker,
#ifdef SHARED_NOTHING
    set<Key> &stored_key_map,
#else
    map<Key, KeyProperty> &stored_key_map,
#endif
    map<Key, KeyReplication> &key_replication_map, set<Key> &local_changeset,
    ServerThread &wt, SerializerMap &serializers, SocketCache &pushers,
    map<Key, vector<PendingRequest>> &batching_requests, bool batching);

void replication_change_handler(
    Address public_ip, Address private_ip, unsigned thread_id, unsigned &seed,
    logger log, string &serialized, GlobalRingMap &global_hash_rings,
#ifdef SHARED_NOTHING
    LocalRingMap &local_hash_rings, set<Key> &stored_key_map,
#else
    LocalRingMap &local_hash_rings, map<Key, KeyProperty> &stored_key_map,
#endif
    map<Key, KeyReplication> &key_replication_map, set<Key> &local_changeset,
    ServerThread &wt, SerializerMap &serializers, SocketCache &pushers,
    zmq::socket_t &response_puller);

// Postcondition:
// cache_ip_to_keys, key_to_cache_ips are both updated
// with the IPs and their fresh list of repsonsible keys
// in the serialized response.
void cache_ip_response_handler(string &serialized,
                               map<Address, set<Key>> &cache_ip_to_keys,
                               map<Key, set<Address>> &key_to_cache_ips);

void management_node_response_handler(string &serialized,
                                      set<Address> &extant_caches,
                                      map<Address, set<Key>> &cache_ip_to_keys,
                                      map<Key, set<Address>> &key_to_cache_ips,
                                      GlobalRingMap &global_hash_rings,
                                      LocalRingMap &local_hash_rings,
                                      SocketCache &pushers, ServerThread &wt,
                                      unsigned &rid);

#ifdef SHARED_NOTHING
void send_gossip(AddressKeysetMap &addr_keyset_map, SocketCache &pushers,
                 SerializerMap &serializers,
                 set<Key> &stored_key_map);
#else
void send_gossip(AddressKeysetMap &addr_keyset_map, SocketCache &pushers,
                 SerializerMap &serializers,
                 map<Key, KeyProperty> &stored_key_map);
#endif

#ifdef SHARED_NOTHING
void send_failover_gossip(AddressKeysetMap &addr_keyset_map, SocketCache &pushers);
#endif

#ifndef ENABLE_DINOMO_KVS
std::pair<string, AnnaError> process_get(const Key &key,
                                         Serializer *serializer);

void process_put(const Key &key, LatticeType lattice_type,
                 const string &payload, Serializer *serializer,
                 map<Key, KeyProperty> &stored_key_map);
#else
void failover_handler(string &serialized, SerializerMap &serializers, 
        logger log);

#ifdef SHARED_NOTHING
void response_batching_requests(SocketCache &pushers, logger log,
        map<Key, vector<PendingRequest>> &batching_requests,
        set<Key> &stored_key_map);
#else
void response_batching_requests(SocketCache &pushers, logger log,
        map<Key, vector<PendingRequest>> &batching_requests,
        map<Key, KeyProperty> &stored_key_map);
#endif

std::pair<string, AnnaError> process_get(const Key &key,
                                         Serializer *serializer);

std::pair<string, AnnaError> process_get_replicated(const Key &key,
                                         Serializer *serializer);

std::pair<string, AnnaError> process_get_meta(const Key &key,
                                         Serializer *serializer);

unsigned process_put(const Key &key, LatticeType lattice_type,
                 const string &payload, Serializer *serializer);

void process_put_replicated(const Key &key, LatticeType lattice_type,
                 const string &payload, Serializer *serializer);

void process_put_meta(const Key &key, LatticeType lattice_type,
                 const string &payload, Serializer *serializer);

unsigned process_update(const Key &key, LatticeType lattice_type,
                 const string &payload, Serializer *serializer);

void process_update_replicated(const Key &key, LatticeType lattice_type,
                 const string &payload, Serializer *serializer);

void process_merge(Serializer *serializer, bool clear_cache);

void process_flush(Serializer *serializer);

void process_failover(Serializer *serializer, int failed_node_rank);

void process_invalidate(const Key &key, Serializer *serializer);

void process_switch_to_replication(const Key &key, Serializer *serializer);

void process_revert_to_non_replication(const Key &key, Serializer *serializer);
#endif
bool is_primary_replica(const Key &key,
                        map<Key, KeyReplication> &key_replication_map,
                        GlobalRingMap &global_hash_rings,
                        LocalRingMap &local_hash_rings, const ServerThread &st);
#endif // INCLUDE_KVS_KVS_HANDLERS_HPP_