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

#include "mock_zmq_utils.hpp"

void MockZmqUtil::send_string(const string &s, zmq::socket_t *socket) {
  sent_messages.push_back(s);
}

string MockZmqUtil::recv_string(zmq::socket_t *socket) { return ""; }

int MockZmqUtil::poll(long timeout, vector<zmq::pollitem_t> *items) {
  return 0;
}
