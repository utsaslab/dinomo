#include "zmq_util.hpp"

#include <iomanip>
#include <ios>

string ZmqUtilInterface::message_to_string(const zmq::message_t& message) {
  return string(static_cast<const char*>(message.data()), message.size());
}

zmq::message_t ZmqUtilInterface::string_to_message(const string& s) {
  zmq::message_t msg(s.size());
  memcpy(msg.data(), s.c_str(), s.size());
  return msg;
}

void ZmqUtil::send_string(const string& s, zmq::socket_t* socket) {
  socket->send(string_to_message(s));
}

string ZmqUtil::recv_string(zmq::socket_t* socket) {
  zmq::message_t message;
  socket->recv(&message);
  return message_to_string(message);
}

int ZmqUtil::poll(long timeout, vector<zmq::pollitem_t>* items) {
  return zmq::poll(items->data(), items->size(), timeout);
}
