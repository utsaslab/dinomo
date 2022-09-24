#ifndef KVS_INCLUDE_KVS_COMMON_HPP_
#define KVS_INCLUDE_KVS_COMMON_HPP_

#include "kvs_types.hpp"
#include "metadata.pb.h"

const unsigned kMetadataReplicationFactor = 1;
const unsigned kMetadataLocalReplicationFactor = 1;

const unsigned kVirtualThreadNum = 3000;

const vector<Tier> kAllTiers = {
    Tier::MEMORY,
    Tier::STORAGE};

extern unsigned kSloWorst;

// run-time constants
extern Tier kSelfTier;
extern vector<Tier> kSelfTierIdVector;

extern uint64_t kMemoryNodeCapacity;
extern uint64_t kStorageNodeCapacity;

// the number of threads running in this executable
extern unsigned kThreadNum;
extern unsigned kMemoryThreadCount;
extern unsigned kStorageThreadCount;
extern unsigned kRoutingThreadCount;

extern unsigned kDefaultGlobalMemoryReplication;
extern unsigned kDefaultGlobalStorageReplication;
extern unsigned kDefaultLocalReplication;
extern unsigned kMinimumReplicaNumber;

inline void prepare_get_tuple(KeyRequest &req, Key key,
                              LatticeType lattice_type) {
  KeyTuple *tp = req.add_tuples();
  tp->set_key(std::move(key));
  tp->set_lattice_type(std::move(lattice_type));
}

inline void prepare_put_tuple(KeyRequest &req, Key key,
                              LatticeType lattice_type, string payload) {
  KeyTuple *tp = req.add_tuples();
  tp->set_key(std::move(key));
  tp->set_lattice_type(std::move(lattice_type));
  tp->set_payload(std::move(payload));
}

#endif // KVS_INCLUDE_KVS_COMMON_HPP_
