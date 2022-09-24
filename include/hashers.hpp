#ifndef KVS_INCLUDE_HASHERS_HPP_
#define KVS_INCLUDE_HASHERS_HPP_

#include "kvs_threads.hpp"
#include <vector>

struct GlobalHasher {
  uint32_t operator()(const ServerThread &th) {
    // prepend a string to make the hash value different than
    // what it would be on the naked input
    return std::hash<string>{}("GLOBAL" + th.virtual_id());
  }

  uint32_t operator()(const Key &key) {
    // prepend a string to make the hash value different than
    // what it would be on the naked input
    return std::hash<string>{}("GLOBAL" + key);
  }

  typedef uint32_t ResultType;
};

struct LocalHasher {
  typedef std::hash<string>::result_type ResultType;

  ResultType operator()(const ServerThread &th) {
    return std::hash<string>{}(std::to_string(th.tid()) + "_" +
                               std::to_string(th.virtual_num()));
  }

  ResultType operator()(const Key &key) { return std::hash<string>{}(key); }
};

#endif // KVS_INCLUDE_HASHERS_HPP_