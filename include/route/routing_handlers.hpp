#ifndef INCLUDE_ROUTE_ROUTING_HANDLERS_HPP_
#define INCLUDE_ROUTE_ROUTING_HANDLERS_HPP_

#include "hash_ring.hpp"
#include "metadata.pb.h"

string seed_handler(logger log, GlobalRingMap &global_hash_rings);

void membership_handler(logger log, string &serialized, SocketCache &pushers,
                        GlobalRingMap &global_hash_rings, unsigned thread_id,
                        Address ip);

void replication_response_handler(
    logger log, string &serialized, SocketCache &pushers, RoutingThread &rt,
    GlobalRingMap &global_hash_rings, LocalRingMap &local_hash_rings,
    map<Key, KeyReplication> &key_replication_map,
    map<Key, vector<pair<Address, string>>> &pending_requests, unsigned &seed);

void replication_change_handler(logger log, string &serialized,
                                SocketCache &pushers,
                                map<Key, KeyReplication> &key_replication_map,
                                unsigned thread_id, Address ip);

void address_handler(logger log, string &serialized, SocketCache &pushers,
                     RoutingThread &rt, GlobalRingMap &global_hash_rings,
                     LocalRingMap &local_hash_rings,
                     map<Key, KeyReplication> &key_replication_map,
                     map<Key, vector<pair<Address, string>>> &pending_requests,
                     unsigned &seed);

#endif // INCLUDE_ROUTE_ROUTING_HANDLERS_HPP_
