#ifndef SRC_INCLUDE_ZMQ_ZMQ_UTIL_HPP_
#define SRC_INCLUDE_ZMQ_ZMQ_UTIL_HPP_

#include <cstring>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "types.hpp"
#include "zmq.hpp"

class ZmqUtilInterface {
 public:
  // Converts the data within a `zmq::message_t` into a string.
  string message_to_string(const zmq::message_t& message);
  // Converts a string into a `zmq::message_t`.
  zmq::message_t string_to_message(const string& s);
  // `send` a string over the socket.
  virtual void send_string(const string& s, zmq::socket_t* socket) = 0;
  // `recv` a string over the socket.
  virtual string recv_string(zmq::socket_t* socket) = 0;
  // `poll` is a wrapper around `zmq::poll` that takes a vector instead of a
  // pointer and a size.
  virtual int poll(long timeout, vector<zmq::pollitem_t>* items) = 0;
};

class ZmqUtil : public ZmqUtilInterface {
 public:
  virtual void send_string(const string& s, zmq::socket_t* socket);
  virtual string recv_string(zmq::socket_t* socket);
  virtual int poll(long timeout, vector<zmq::pollitem_t>* items);
};

extern ZmqUtilInterface* kZmqUtil;

#endif  // SRC_INCLUDE_ZMQ_ZMQ_UTIL_HPP_