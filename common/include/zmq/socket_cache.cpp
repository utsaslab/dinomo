#include "socket_cache.hpp"

#include <utility>

zmq::socket_t& SocketCache::At(const Address& addr) {
  auto iter = cache_.find(addr);
  if (iter != cache_.end()) {
    return iter->second;
  }

  zmq::socket_t socket(*context_, type_);
  int hwm_out_msg = 0;
  socket.setsockopt(ZMQ_SNDHWM, &hwm_out_msg, sizeof(hwm_out_msg));
  socket.connect(addr);
  auto p = cache_.insert(std::make_pair(addr, std::move(socket)));

  return p.first->second;
}

zmq::socket_t& SocketCache::operator[](const Address& addr) { return At(addr); }

void SocketCache::clear_cache() { cache_.clear(); }
