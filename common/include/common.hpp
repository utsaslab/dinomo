#ifndef INCLUDE_COMMON_HPP_
#define INCLUDE_COMMON_HPP_

#include <algorithm>
#include <sstream>

#include "anna.pb.h"
#include "lattices/lww_pair_lattice.hpp"
#include "lattices/multi_key_causal_lattice.hpp"
#include "lattices/priority_lattice.hpp"
#include "lattices/single_key_causal_lattice.hpp"
#include "types.hpp"
#include "zmq/socket_cache.hpp"
#include "zmq/zmq_util.hpp"

enum UserMetadataType { cache_ip };

const string kMetadataIdentifier = "ANNA_METADATA";
const string kMetadataDelimiter = "|";
const char kMetadataDelimiterChar = '|';
const string kMetadataTypeCacheIP = "cache_ip";
const unsigned kMaxSocketNumber = 10000;

inline void split(const string& s, char delim, vector<string>& elems) {
  std::stringstream ss(s);
  string item;

  while (std::getline(ss, item, delim)) {
    elems.push_back(item);
  }
}

// form the timestamp given a time and a thread id
inline unsigned long long get_time() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

inline unsigned long long generate_timestamp(const unsigned& id) {
  unsigned pow = 10;
  auto time = get_time();
  while (id >= pow) pow *= 10;
  return time * pow + id;
}

// This version of the function should only be called with
// certain types of MetadataType,
// so if it's called with something else, we return
// an empty string.
// TODO: There should probably be a less silent error check.
inline Key get_user_metadata_key(string data_key, UserMetadataType type) {
  if (type == UserMetadataType::cache_ip) {
    return kMetadataIdentifier + kMetadataDelimiter + kMetadataTypeCacheIP +
           kMetadataDelimiter + data_key;
  }
  return "";
}

// Inverse of get_user_metadata_key, returning just the key itself.
// TODO: same problem as get_user_metadata_key with the metadata types.
inline Key get_key_from_user_metadata(Key metadata_key) {
  string::size_type n_id;
  string::size_type n_type;
  // Find the first delimiter; this skips over the metadata identifier.
  n_id = metadata_key.find(kMetadataDelimiter);
  // Find the second delimiter; this skips over the metadata type.
  n_type = metadata_key.find(kMetadataDelimiter, n_id + 1);
  string metadata_type = metadata_key.substr(n_id + 1, n_type - (n_id + 1));
  if (metadata_type == kMetadataTypeCacheIP) {
    return metadata_key.substr(n_type + 1);
  }

  return "";
}

inline string serialize(const LWWPairLattice<string>& l) {
  LWWValue lww_value;
  lww_value.set_timestamp(l.reveal().timestamp);
  lww_value.set_value(l.reveal().value);

  string serialized;
  lww_value.SerializeToString(&serialized);
  return serialized;
}

inline string serialize(const unsigned long long& timestamp,
                        const string& value) {
  LWWValue lww_value;
  lww_value.set_timestamp(timestamp);
  lww_value.set_value(value);

  string serialized;
  lww_value.SerializeToString(&serialized);
  return serialized;
}

inline string serialize(const SetLattice<string>& l) {
  SetValue set_value;
  for (const string& val : l.reveal()) {
    set_value.add_values(val);
  }

  string serialized;
  set_value.SerializeToString(&serialized);
  return serialized;
}

inline string serialize(const OrderedSetLattice<string>& l) {
  // We will just serialize ordered sets as regular sets for now;
  // order in serialization helps with performance
  // but is not necessary for correctness.
  SetValue set_value;
  for (const string& val : l.reveal()) {
    set_value.add_values(val);
  }

  string serialized;
  set_value.SerializeToString(&serialized);
  return serialized;
}

inline string serialize(const set<string>& set) {
  SetValue set_value;
  for (const string& val : set) {
    set_value.add_values(val);
  }

  string serialized;
  set_value.SerializeToString(&serialized);
  return serialized;
}

inline string serialize(const SingleKeyCausalLattice<SetLattice<string>>& l) {
  SingleKeyCausalValue sk_causal_value;
  auto ptr = sk_causal_value.mutable_vector_clock();
  // serialize vector clock
  for (const auto& pair : l.reveal().vector_clock.reveal()) {
    (*ptr)[pair.first] = pair.second.reveal();
  }
  // serialize values
  for (const string& val : l.reveal().value.reveal()) {
    sk_causal_value.add_values(val);
  }

  string serialized;
  sk_causal_value.SerializeToString(&serialized);
  return serialized;
}

inline string serialize(const MultiKeyCausalLattice<SetLattice<string>>& l) {
  MultiKeyCausalValue mk_causal_value;
  auto ptr = mk_causal_value.mutable_vector_clock();
  // serialize vector clock
  for (const auto& pair : l.reveal().vector_clock.reveal()) {
    (*ptr)[pair.first] = pair.second.reveal();
  }

  // serialize dependencies
  for (const auto& pair : l.reveal().dependencies.reveal()) {
    auto dep = mk_causal_value.add_dependencies();
    dep->set_key(pair.first);
    auto vc_ptr = dep->mutable_vector_clock();
    for (const auto& vc_pair : pair.second.reveal()) {
      (*vc_ptr)[vc_pair.first] = vc_pair.second.reveal();
    }
  }

  // serialize values
  for (const string& val : l.reveal().value.reveal()) {
    mk_causal_value.add_values(val);
  }

  string serialized;
  mk_causal_value.SerializeToString(&serialized);
  return serialized;
}

inline string serialize(const PriorityLattice<double, string>& l) {
  PriorityValue priority_value;
  priority_value.set_priority(l.reveal().priority);
  priority_value.set_value(l.reveal().value);

  string serialized;
  priority_value.SerializeToString(&serialized);
  return serialized;
}

inline LWWPairLattice<string> deserialize_lww(const string& serialized) {
  LWWValue lww;
  lww.ParseFromString(serialized);

  return LWWPairLattice<string>(
      TimestampValuePair<string>(lww.timestamp(), lww.value()));
}

inline SetLattice<string> deserialize_set(const string& serialized) {
  SetValue s;
  s.ParseFromString(serialized);

  set<string> result;

  for (const string& value : s.values()) {
    result.insert(value);
  }

  return SetLattice<string>(result);
}

inline OrderedSetLattice<string> deserialize_ordered_set(
    const string& serialized) {
  SetValue s;
  s.ParseFromString(serialized);
  ordered_set<string> result;
  for (const string& value : s.values()) {
    result.insert(value);
  }
  return OrderedSetLattice<string>(result);
}

inline SingleKeyCausalValue deserialize_causal(const string& serialized) {
  SingleKeyCausalValue causal;
  causal.ParseFromString(serialized);

  return causal;
}

inline MultiKeyCausalValue deserialize_multi_key_causal(
    const string& serialized) {
  MultiKeyCausalValue mk_causal;
  mk_causal.ParseFromString(serialized);

  return mk_causal;
}

inline PriorityLattice<double, string> deserialize_priority(
    const string& serialized) {
  PriorityValue pv;
  pv.ParseFromString(serialized);
  return PriorityLattice<double, string>(
      PriorityValuePair<double, string>(pv.priority(), pv.value()));
}

inline VectorClockValuePair<SetLattice<string>> to_vector_clock_value_pair(
    const SingleKeyCausalValue& cv) {
  VectorClockValuePair<SetLattice<string>> p;
  for (const auto& pair : cv.vector_clock()) {
    p.vector_clock.insert(pair.first, pair.second);
  }
  for (auto& val : cv.values()) {
    p.value.insert(std::move(val));
  }
  return p;
}

inline MultiKeyCausalPayload<SetLattice<string>> to_multi_key_causal_payload(
    const MultiKeyCausalValue& mkcv) {
  MultiKeyCausalPayload<SetLattice<string>> p;

  for (const auto& pair : mkcv.vector_clock()) {
    p.vector_clock.insert(pair.first, pair.second);
  }

  for (const auto& dep : mkcv.dependencies()) {
    VectorClock vc;
    for (const auto& pair : dep.vector_clock()) {
      vc.insert(pair.first, pair.second);
    }

    p.dependencies.insert(dep.key(), vc);
  }

  for (auto& val : mkcv.values()) {
    p.value.insert(std::move(val));
  }

  return p;
}

struct lattice_type_hash {
  std::size_t operator()(const LatticeType& lt) const {
    return std::hash<string>()(LatticeType_Name(lt));
  }
};

#endif  // INCLUDE_COMMON_HPP_
