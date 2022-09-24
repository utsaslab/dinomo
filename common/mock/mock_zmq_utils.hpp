//  Copyright 2019 U.C. Berkeley RISE Lab
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

#ifndef MOCK_MOCK_ZMQ_UTILS_HPP_
#define MOCK_MOCK_ZMQ_UTILS_HPP_

#include "zmq/zmq_util.hpp"

class MockZmqUtil : public ZmqUtilInterface {
 public:
  vector<string> sent_messages;

  virtual void send_string(const string &s, zmq::socket_t *socket);
  virtual string recv_string(zmq::socket_t *socket);
  virtual int poll(long timeout, vector<zmq::pollitem_t> *items);
};

#endif  // MOCK_MOCK_ZMQ_UTILS_HPP_
