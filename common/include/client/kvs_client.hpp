#ifndef INCLUDE_ASYNC_CLIENT_HPP_
#define INCLUDE_ASYNC_CLIENT_HPP_

#include "anna.pb.h"
#include "common.hpp"
#include "requests.hpp"
#include "threads.hpp"
#include "types.hpp"
#ifdef BENCH_CACHE_ROUTING
#include "hash_ring.hpp"
#include "metadata.pb.h"
#endif

using TimePoint = std::chrono::time_point<std::chrono::system_clock>;

struct PendingRequest {
  TimePoint tp_;
  Address worker_addr_;
  KeyRequest request_;
};

class KvsClientInterface {
 public:
  virtual string put_async(const Key& key, const string& payload,
                           LatticeType lattice_type) = 0;
  virtual string update_async(const Key& key, const string& payload,
                           LatticeType lattice_type) = 0;
  virtual void get_async(const Key& key) = 0;
  virtual vector<KeyResponse> receive_async() = 0;
  virtual vector<KeyResponse> receive_async2(double &key_latency) = 0;
  virtual zmq::context_t* get_context() = 0;
};

class KvsClient : public KvsClientInterface {
 public:
  /**
   * @addrs A vector of routing addresses.
   * @routing_thread_count The number of thread sone ach routing node
   * @ip My node's IP address
   * @tid My client's thread ID
   * @timeout Length of request timeouts in ms
   */
  KvsClient(vector<UserRoutingThread> routing_threads, string ip,
            unsigned tid = 0, unsigned timeout = 10000, bool kEnableReconfig = true,
            int kRespPullerHWM = 0, int kRespPullerBacklog = 100) :
      routing_threads_(routing_threads),
      ut_(UserThread(ip, tid)),
      context_(zmq::context_t(1)),
      socket_cache_(SocketCache(&context_, ZMQ_PUSH)),
      key_address_puller_(zmq::socket_t(context_, ZMQ_PULL)),
      response_puller_(zmq::socket_t(context_, ZMQ_PULL)),
      log_(spdlog::basic_logger_mt(std::string("client_log") + std::to_string(tid), std::string("client_log") + std::to_string(tid) + std::string(".txt"), true)),
      timeout_(timeout), enableReconfig(kEnableReconfig) {
    // initialize logger
    log_->flush_on(spdlog::level::info);

#ifdef ENABLE_CLOVER_KVS
    log_->info("Clover client.");
#elif SHARED_NOTHING
    log_->info("Dinomo-N client.");
#else
    log_->info("Dinomo client.");
#endif

    std::hash<string> hasher;
    seed_ = time(NULL);
    seed_ += hasher(ip);
    seed_ += tid;
    log_->info("Random seed is {}.", seed_);

    // bind the two sockets we listen on
    key_address_puller_.bind(ut_.key_address_bind_address());
    response_puller_.setsockopt(ZMQ_RCVHWM, &kRespPullerHWM, sizeof(kRespPullerHWM));
    response_puller_.setsockopt(ZMQ_BACKLOG, &kRespPullerBacklog, sizeof(kRespPullerBacklog));
    response_puller_.bind(ut_.response_bind_address());

    pollitems_ = {
        {static_cast<void*>(key_address_puller_), 0, ZMQ_POLLIN, 0},
        {static_cast<void*>(response_puller_), 0, ZMQ_POLLIN, 0},
    };

    // set the request ID to 0
    rid_ = 0;

#ifdef BENCH_CACHE_ROUTING
    global_hash_rings.clear();
    local_hash_rings.clear();
    key_replication_map.clear();
    pending_request_map_.clear();
    waiting_new_membership = false;
#endif
  }

  ~KvsClient() {}

 public:
  /**
   * Issue an async PUT/INSERT request to the KVS for a certain lattice typed value.
   */
  string put_async(const Key& key, const string& payload,
                   LatticeType lattice_type) {
    KeyRequest request;
    KeyTuple* tuple = prepare_data_request(request, key);
    request.set_type(RequestType::PUT);
    tuple->set_lattice_type(lattice_type);
    tuple->set_payload(payload);

    try_request(request);
    return request.request_id();
  }

  /**
   * Issue an async UPDATE request to the KVS for a certain lattice typed value.
   */
  string update_async(const Key& key, const string& payload,
                   LatticeType lattice_type) {
    KeyRequest request;
    KeyTuple* tuple = prepare_data_request(request, key);
    request.set_type(RequestType::UPDATE);
    tuple->set_lattice_type(lattice_type);
    tuple->set_payload(payload);

    try_request(request);
    return request.request_id();
  }

  /**
   * Issue an async GET/READ request to the KVS.
   */
#ifdef SINGLE_OUTSTANDING
  void get_async(const Key& key) {
    // we issue GET only when it is not in the pending map
    if (pending_get_response_map_.find(key) ==
        pending_get_response_map_.end()) {
      KeyRequest request;
      prepare_data_request(request, key);
      request.set_type(RequestType::GET);

      try_request(request);
    }
  }
#else
  void get_async(const Key& key) {
      KeyRequest request;
      prepare_data_request(request, key);
      request.set_type(RequestType::GET);

      try_request(request);
  }
#endif

  vector<KeyResponse> receive_async() {
    vector<KeyResponse> result;
    kZmqUtil->poll(0, &pollitems_);

    if (pollitems_[0].revents & ZMQ_POLLIN) {
#ifndef BENCH_CACHE_ROUTING
      string serialized = kZmqUtil->recv_string(&key_address_puller_);
      KeyAddressResponse response;
      response.ParseFromString(serialized);
      Key key = response.addresses(0).key();

      if (pending_request_map_.find(key) != pending_request_map_.end()) {
        if (response.error() == AnnaError::NO_SERVERS) {
          log_->error(
              "No servers have joined the cluster yet. Retrying request.");
          pending_request_map_[key].first = std::chrono::system_clock::now();

          query_routing_async(key);
        } else {
          // populate cache
          for (const Address& ip : response.addresses(0).ips()) {
            key_address_cache_[key].insert(ip);
          }

          // handle stuff in pending request map
          for (auto& req : pending_request_map_[key].second) {
            try_request(req);
          }

          // GC the pending request map
          pending_request_map_.erase(key);
        }
      }
#else
      string serialized = kZmqUtil->recv_string(&key_address_puller_);
      ClusterMembership membership;
      membership.ParseFromString(serialized);

      waiting_new_membership = false;

      if (!pending_request_map_.empty()) {
          global_hash_rings.clear();
          local_hash_rings.clear();
          key_address_cache_.clear();
#ifdef ENABLE_CLOVER_KVS
          threads.clear();
#endif

          for (const auto &tier : membership.tiers())
          {
              Tier id = tier.tier_id();

              for (const auto server : tier.servers())
              {
                  global_hash_rings[id].insert(server.public_ip(), server.private_ip(), 0, 0);
#ifdef ENABLE_CLOVER_KVS
                  for (const auto &pair : kTierMetadata)
                  {
                      TierMetadata tier = pair.second;
                      for (unsigned tid = 0; tid < tier.thread_number_; tid++)
                      {
                          threads.insert(ServerThread(server.public_ip(), server.private_ip(), tid).key_request_connect_address());
                      }
                  }
#endif
              }
          }

          int num_servers = 0;
          for (const auto &pair : global_hash_rings)
          {
              num_servers += pair.second.size();
          }

          if (num_servers == 0) {
              log_->error("No servers have joined the cluster yet. Retrying request.");
              query_routing_async("membership");
          } else {
#ifdef ENABLE_CLOVER_KVS
              log_->info("{} servers are active in the cluster ({}).", num_servers, threads.size());
#else
              log_->info("{} servers are active in the cluster.", num_servers);
#endif
              for (const auto &pair : kTierMetadata)
              {
                  TierMetadata tier = pair.second;
                  for (unsigned tid = 0; tid < tier.thread_number_; tid++)
                  {
                      local_hash_rings[tier.id_].insert(ut_.ip(), ut_.ip(), 0, tid);
                  }
              }

              for (const auto &pair : pending_request_map_) {
                  for (auto &req : pending_request_map_[pair.first].second) {
                      try_request(req);
                  }
              }

              pending_request_map_.clear();
          }
      }
#endif
    }

    if (pollitems_[1].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&response_puller_);
      KeyResponse response;
      response.ParseFromString(serialized);
      Key key = response.tuples(0).key();

      if (response.type() == RequestType::GET) {
#ifdef SINGLE_OUTSTANDING
        if (pending_get_response_map_.find(key) !=
            pending_get_response_map_.end()) {
          if (check_tuple(response.tuples(0))) {
            // error no == 2, so re-issue request
            pending_get_response_map_[key].tp_ =
                std::chrono::system_clock::now();
#ifndef BENCH_CACHE_ROUTING
            try_request(pending_get_response_map_[key].request_);
#else
            if (pending_request_map_.find(key) == pending_request_map_.end()) {
                pending_request_map_[key].first = std::chrono::system_clock::now();
            }
            pending_request_map_[key].second.push_back(pending_get_response_map_[key].request_);
#endif
          } else {
            // error no == 0 or 1
            result.push_back(response);
            pending_get_response_map_.erase(key);
          }
        }
#else
        if (pending_get_response_map_.find(key) !=
                pending_get_response_map_.end() &&
                pending_get_response_map_[key].find(response.response_id()) !=
                pending_get_response_map_[key].end()) {
            if (check_tuple(response.tuples(0))) {
                // error no == 2, so re-issue request
                pending_get_response_map_[key][response.response_id()].tp_ =
                    std::chrono::system_clock::now();
#ifndef BENCH_CACHE_ROUTING
                try_request(pending_get_response_map_[key][response.response_id()].request_);
#else
                if (pending_request_map_.find(key) == pending_request_map_.end()) {
                    pending_request_map_[key].first = std::chrono::system_clock::now();
                }
                pending_request_map_[key].second.push_back(pending_get_response_map_[key][response.response_id()].request_);
#endif
            } else {
                // error no == 0 or 1
                result.push_back(response);
                pending_get_response_map_[key].erase(response.response_id());

                if (pending_get_response_map_[key].size() == 0) {
                    pending_get_response_map_.erase(key);
                }
            }
        }
#endif
      } else if (response.type() == RequestType::UPDATE) {
        if (pending_update_response_map_.find(key) !=
                pending_update_response_map_.end() &&
            pending_update_response_map_[key].find(response.response_id()) !=
                pending_update_response_map_[key].end()) {
          if (check_tuple(response.tuples(0))) {
            // error no == 2, so re-issue request
            pending_update_response_map_[key][response.response_id()].tp_ =
                std::chrono::system_clock::now();
#ifndef BENCH_CACHE_ROUTING
            try_request(pending_update_response_map_[key][response.response_id()]
                            .request_);
#else
            if (pending_request_map_.find(key) == pending_request_map_.end()) {
                pending_request_map_[key].first = std::chrono::system_clock::now();
            }
            pending_request_map_[key].second.push_back(pending_update_response_map_[key][response.response_id()].request_);
#endif
          } else {
            // error no == 0
            result.push_back(response);
            pending_update_response_map_[key].erase(response.response_id());

            if (pending_update_response_map_[key].size() == 0) {
              pending_update_response_map_.erase(key);
            }
          }
        }
      } else {
        if (pending_put_response_map_.find(key) !=
                pending_put_response_map_.end() &&
            pending_put_response_map_[key].find(response.response_id()) !=
                pending_put_response_map_[key].end()) {
          if (check_tuple(response.tuples(0))) {
            // error no == 2, so re-issue request
            pending_put_response_map_[key][response.response_id()].tp_ =
                std::chrono::system_clock::now();
#ifndef BENCH_CACHE_ROUTING
            try_request(pending_put_response_map_[key][response.response_id()]
                            .request_);
#else
            if (pending_request_map_.find(key) == pending_request_map_.end()) {
                pending_request_map_[key].first = std::chrono::system_clock::now();
            }
            pending_request_map_[key].second.push_back(pending_put_response_map_[key][response.response_id()].request_);
#endif
          } else {
            // error no == 0
            result.push_back(response);
            pending_put_response_map_[key].erase(response.response_id());

            if (pending_put_response_map_[key].size() == 0) {
              pending_put_response_map_.erase(key);
            }
          }
        }
      }
    }

    if (enableReconfig) {
        // Retry the pending request map
        for (const auto& pair : pending_request_map_) {
            if (std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now() - pair.second.first)
                    .count() > timeout_) {
                log_->error("Time out in receiving routing info");
                // query to the routing tier timed out
                pending_request_map_[pair.first].first = std::chrono::system_clock::now();
                query_routing_async(pair.first);
            }
        }

#ifdef SINGLE_OUTSTANDING
        // Retry the pending get response map
        set<Key> to_remove_get;
        for (const auto& pair : pending_get_response_map_) {
            if (std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now() - pair.second.tp_)
                    .count() > timeout_) {
                log_->error("Time out in receiving get request response");
                invalidate_cache_for_worker(pair.second.worker_addr_);
                to_remove_get.insert(pair.first);
#ifndef BENCH_CACHE_ROUTING
                try_request(pending_get_response_map_[pair.first].request_);
#else
                if (pending_request_map_.find(pair.first) == pending_request_map_.end()) {
                    pending_request_map_[pair.first].first = std::chrono::system_clock::now();
                }
                pending_request_map_[pair.first].second.push_back(pending_get_response_map_[pair.first].request_);
#endif
            }
        }

        for (const Key& key : to_remove_get) {
            pending_get_response_map_.erase(key);
        }
#else
        // Retry the pending get response map
        map<Key, set<string>> to_remove_get;
        for (const auto& key_map_pair : pending_get_response_map_) {
            for (const auto& id_map_pair :
                    pending_get_response_map_[key_map_pair.first]) {
                if (std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now() -
                            pending_get_response_map_[key_map_pair.first][id_map_pair.first]
                            .tp_)
                        .count() > timeout_) {
                    log_->error("Time out in receiving get request response");
                    invalidate_cache_for_worker(id_map_pair.second.worker_addr_);
                    to_remove_get[key_map_pair.first].insert(id_map_pair.first);
#ifndef BENCH_CACHE_ROUTING
                    try_request(pending_get_response_map_[key_map_pair.first][id_map_pair.first].request_);
#else
                    if (pending_request_map_.find(key_map_pair.first) == pending_request_map_.end()) {
                        pending_request_map_[key_map_pair.first].first = std::chrono::system_clock::now();
                    }
                    pending_request_map_[key_map_pair.first].second.push_back(pending_get_response_map_[key_map_pair.first][id_map_pair.first].request_);
#endif
                }
            }
        }

        for (const auto& key_set_pair : to_remove_get) {
            for (const auto& id : key_set_pair.second) {
                pending_get_response_map_[key_set_pair.first].erase(id);
            }
        }
#endif

        // Retry the pending put response map
        map<Key, set<string>> to_remove_put;
        for (const auto& key_map_pair : pending_put_response_map_) {
            for (const auto& id_map_pair :
                    pending_put_response_map_[key_map_pair.first]) {
                if (std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now() -
                            pending_put_response_map_[key_map_pair.first][id_map_pair.first]
                            .tp_)
                        .count() > timeout_) {
                    log_->error("Time out in receiving put request response");
                    invalidate_cache_for_worker(id_map_pair.second.worker_addr_);
                    to_remove_put[key_map_pair.first].insert(id_map_pair.first);
#ifndef BENCH_CACHE_ROUTING
                    try_request(pending_put_response_map_[key_map_pair.first][id_map_pair.first].request_);
#else
                    if (pending_request_map_.find(key_map_pair.first) == pending_request_map_.end()) {
                        pending_request_map_[key_map_pair.first].first = std::chrono::system_clock::now();
                    }
                    pending_request_map_[key_map_pair.first].second.push_back(pending_put_response_map_[key_map_pair.first][id_map_pair.first].request_);
#endif
                }
            }
        }

        for (const auto& key_set_pair : to_remove_put) {
            for (const auto& id : key_set_pair.second) {
                pending_put_response_map_[key_set_pair.first].erase(id);
            }
        }

        // Retry the pending update response map
        map<Key, set<string>> to_remove_update;
        for (const auto& key_map_pair : pending_update_response_map_) {
            for (const auto& id_map_pair :
                    pending_update_response_map_[key_map_pair.first]) {
                if (std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now() -
                            pending_update_response_map_[key_map_pair.first][id_map_pair.first]
                            .tp_)
                        .count() > timeout_) {
                    log_->error("Time out in receiving update request response");
                    invalidate_cache_for_worker(id_map_pair.second.worker_addr_);
                    to_remove_update[key_map_pair.first].insert(id_map_pair.first);
#ifndef BENCH_CACHE_ROUTING
                    try_request(pending_update_response_map_[key_map_pair.first][id_map_pair.first].request_);
#else
                    if (pending_request_map_.find(key_map_pair.first) == pending_request_map_.end()) {
                        pending_request_map_[key_map_pair.first].first = std::chrono::system_clock::now();
                    }
                    pending_request_map_[key_map_pair.first].second.push_back(pending_update_response_map_[key_map_pair.first][id_map_pair.first].request_);
#endif
                }
            }
        }

        for (const auto& key_set_pair : to_remove_update) {
            for (const auto& id : key_set_pair.second) {
                pending_update_response_map_[key_set_pair.first].erase(id);
            }
        }
    }

    return result;
  }

  vector<KeyResponse> receive_async2(double &key_latency) {
    vector<KeyResponse> result;
    kZmqUtil->poll(0, &pollitems_);

    if (pollitems_[0].revents & ZMQ_POLLIN) {
#ifndef BENCH_CACHE_ROUTING
      string serialized = kZmqUtil->recv_string(&key_address_puller_);
      KeyAddressResponse response;
      response.ParseFromString(serialized);
      Key key = response.addresses(0).key();

      if (pending_request_map_.find(key) != pending_request_map_.end()) {
        if (response.error() == AnnaError::NO_SERVERS) {
          log_->error(
              "No servers have joined the cluster yet. Retrying request.");
          pending_request_map_[key].first = std::chrono::system_clock::now();

          query_routing_async(key);
        } else {
          // populate cache
          for (const Address& ip : response.addresses(0).ips()) {
            key_address_cache_[key].insert(ip);
          }

          // handle stuff in pending request map
          for (auto& req : pending_request_map_[key].second) {
            try_request(req);
          }

          // GC the pending request map
          pending_request_map_.erase(key);
        }
      }
#else
      string serialized = kZmqUtil->recv_string(&key_address_puller_);
      ClusterMembership membership;
      membership.ParseFromString(serialized);

      waiting_new_membership = false;

      if (!pending_request_map_.empty()) {
          global_hash_rings.clear();
          local_hash_rings.clear();
          key_address_cache_.clear();
#ifdef ENABLE_CLOVER_KVS
          threads.clear();
#endif

          for (const auto &tier : membership.tiers())
          {
              Tier id = tier.tier_id();

              for (const auto server : tier.servers())
              {
                  global_hash_rings[id].insert(server.public_ip(), server.private_ip(), 0, 0);
#ifdef ENABLE_CLOVER_KVS
                  for (const auto &pair : kTierMetadata)
                  {
                      TierMetadata tier = pair.second;
                      for (unsigned tid = 0; tid < tier.thread_number_; tid++)
                      {
                          threads.insert(ServerThread(server.public_ip(), server.private_ip(), tid).key_request_connect_address());
                      }
                  }
#endif
              }
          }

          int num_servers = 0;
          for (const auto &pair : global_hash_rings)
          {
              num_servers += pair.second.size();
          }

          if (num_servers == 0) {
              log_->error("No servers have joined the cluster yet. Retrying request.");
              query_routing_async("membership");
          } else {
#ifdef ENABLE_CLOVER_KVS
              log_->info("{} servers are active in the cluster ({}).", num_servers, threads.size());
#else
              log_->info("{} servers are active in the cluster.", num_servers);
#endif
              for (const auto &pair : kTierMetadata)
              {
                  TierMetadata tier = pair.second;
                  for (unsigned tid = 0; tid < tier.thread_number_; tid++)
                  {
                      local_hash_rings[tier.id_].insert(ut_.ip(), ut_.ip(), 0, tid);
                  }
              }

              for (const auto &pair : pending_request_map_) {
                  for (auto &req : pending_request_map_[pair.first].second) {
                      try_request(req);
                  }
              }

              pending_request_map_.clear();
          }
      }
#endif
    }

    if (pollitems_[1].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&response_puller_);
      KeyResponse response;
      response.ParseFromString(serialized);
      Key key = response.tuples(0).key();

      if (response.type() == RequestType::GET) {
#ifdef SINGLE_OUTSTANDING
        if (pending_get_response_map_.find(key) !=
            pending_get_response_map_.end()) {
          if (check_tuple(response.tuples(0))) {
            // error no == 2, so re-issue request
            pending_get_response_map_[key].tp_ =
                std::chrono::system_clock::now();
#ifndef BENCH_CACHE_ROUTING
            try_request(pending_get_response_map_[key].request_);
#else
            if (pending_request_map_.find(key) == pending_request_map_.end()) {
                pending_request_map_[key].first = std::chrono::system_clock::now();
            }
            pending_request_map_[key].second.push_back(pending_get_response_map_[key].request_);
#endif
          } else {
            // error no == 0 or 1
            result.push_back(response);
            pending_get_response_map_.erase(key);
          }
        }
#else
        if (pending_get_response_map_.find(key) !=
                pending_get_response_map_.end() &&
                pending_get_response_map_[key].find(response.response_id()) !=
                pending_get_response_map_[key].end()) {
            if (check_tuple(response.tuples(0))) {
                // error no == 2, so re-issue request
                pending_get_response_map_[key][response.response_id()].tp_ =
                    std::chrono::system_clock::now();
#ifndef BENCH_CACHE_ROUTING
                try_request(pending_get_response_map_[key][response.response_id()]
                        .request_);
#else
                if (pending_request_map_.find(key) == pending_request_map_.end()) {
                    pending_request_map_[key].first = std::chrono::system_clock::now();
                }
                pending_request_map_[key].second.push_back(pending_get_response_map_[key][response.response_id()].request_);
#endif
            } else {
                // error no == 0 or 1
                result.push_back(response);
                key_latency = (double)std::chrono::duration_cast<std::chrono::microseconds> (
                        std::chrono::system_clock::now() -
                        pending_get_response_map_[key][response.response_id()].tp_).count();

                pending_get_response_map_[key].erase(response.response_id());
                if (pending_get_response_map_[key].size() == 0) {
                    pending_get_response_map_.erase(key);
                }
            }
        }
#endif
      } else if (response.type() == RequestType::UPDATE) {
        if (pending_update_response_map_.find(key) !=
                pending_update_response_map_.end() &&
            pending_update_response_map_[key].find(response.response_id()) !=
                pending_update_response_map_[key].end()) {
          if (check_tuple(response.tuples(0))) {
            // error no == 2, so re-issue request
            pending_update_response_map_[key][response.response_id()].tp_ =
                std::chrono::system_clock::now();
#ifndef BENCH_CACHE_ROUTING
            try_request(pending_update_response_map_[key][response.response_id()]
                            .request_);
#else
            if (pending_request_map_.find(key) == pending_request_map_.end()) {
                pending_request_map_[key].first = std::chrono::system_clock::now();
            }
            pending_request_map_[key].second.push_back(pending_update_response_map_[key][response.response_id()].request_);
#endif
          } else {
            // error no == 0
            result.push_back(response);
            key_latency = (double)std::chrono::duration_cast<std::chrono::microseconds> (
                    std::chrono::system_clock::now() -
                    pending_update_response_map_[key][response.response_id()].tp_).count();

            pending_update_response_map_[key].erase(response.response_id());
            if (pending_update_response_map_[key].size() == 0) {
              pending_update_response_map_.erase(key);
            }
          }
        }
      } else {
        if (pending_put_response_map_.find(key) !=
                pending_put_response_map_.end() &&
            pending_put_response_map_[key].find(response.response_id()) !=
                pending_put_response_map_[key].end()) {
          if (check_tuple(response.tuples(0))) {
            // error no == 2, so re-issue request
            pending_put_response_map_[key][response.response_id()].tp_ =
                std::chrono::system_clock::now();
#ifndef BENCH_CACHE_ROUTING
            try_request(pending_put_response_map_[key][response.response_id()]
                            .request_);
#else
            if (pending_request_map_.find(key) == pending_request_map_.end()) {
                pending_request_map_[key].first = std::chrono::system_clock::now();
            }
            pending_request_map_[key].second.push_back(pending_put_response_map_[key][response.response_id()].request_);
#endif
          } else {
            // error no == 0
            result.push_back(response);
            key_latency = (double)std::chrono::duration_cast<std::chrono::microseconds> (
                    std::chrono::system_clock::now() -
                    pending_put_response_map_[key][response.response_id()].tp_).count();

            pending_put_response_map_[key].erase(response.response_id());
            if (pending_put_response_map_[key].size() == 0) {
              pending_put_response_map_.erase(key);
            }
          }
        }
      }
    }

    if (enableReconfig) {
        // Retry the pending request map
        for (const auto& pair : pending_request_map_) {
            if (std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now() - pair.second.first)
                    .count() > timeout_) {
                log_->error("Time out in receiving routing info");
                // query to the routing tier timed out
                pending_request_map_[pair.first].first = std::chrono::system_clock::now();
                query_routing_async(pair.first);
            }
        }

#ifdef SINGLE_OUTSTANDING
        // Retry the pending get response map
        set<Key> to_remove_get;
        for (const auto& pair : pending_get_response_map_) {
            if (std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now() - pair.second.tp_)
                    .count() > timeout_) {
                log_->error("Time out in receiving get request response");
                invalidate_cache_for_worker(pair.second.worker_addr_);
                to_remove_get.insert(pair.first);
#ifndef BENCH_CACHE_ROUTING
                try_request(pending_get_response_map_[pair.first].request_);
#else
                if (pending_request_map_.find(pair.first) == pending_request_map_.end()) {
                    pending_request_map_[pair.first].first = std::chrono::system_clock::now();
                }
                pending_request_map_[pair.first].second.push_back(pending_get_response_map_[pair.first].request_);
#endif
            }
        }

        for (const Key& key : to_remove_get) {
            pending_get_response_map_.erase(key);
        }
#else
        // Retry the pending get response map
        map<Key, set<string>> to_remove_get;
        for (const auto& key_map_pair : pending_get_response_map_) {
            for (const auto& id_map_pair :
                    pending_get_response_map_[key_map_pair.first]) {
                if (std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now() -
                            pending_get_response_map_[key_map_pair.first][id_map_pair.first]
                            .tp_)
                        .count() > timeout_) {
                    log_->error("Time out in receiving get request response");
                    invalidate_cache_for_worker(id_map_pair.second.worker_addr_);
                    to_remove_get[key_map_pair.first].insert(id_map_pair.first);
#ifndef BENCH_CACHE_ROUTING
                    try_request(pending_get_response_map_[key_map_pair.first][id_map_pair.first].request_);
#else
                    if (pending_request_map_.find(key_map_pair.first) == pending_request_map_.end()) {
                        pending_request_map_[key_map_pair.first].first = std::chrono::system_clock::now();
                    }
                    pending_request_map_[key_map_pair.first].second.push_back(pending_get_response_map_[key_map_pair.first][id_map_pair.first].request_);
#endif
                }
            }
        }

        for (const auto& key_set_pair : to_remove_get) {
            for (const auto& id : key_set_pair.second) {
                pending_get_response_map_[key_set_pair.first].erase(id);
            }
        }
#endif

        // Retry the pending put response map
        map<Key, set<string>> to_remove_put;
        for (const auto& key_map_pair : pending_put_response_map_) {
            for (const auto& id_map_pair :
                    pending_put_response_map_[key_map_pair.first]) {
                if (std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now() -
                            pending_put_response_map_[key_map_pair.first][id_map_pair.first]
                            .tp_)
                        .count() > timeout_) {
                    log_->error("Time out in receiving put request response");
                    invalidate_cache_for_worker(id_map_pair.second.worker_addr_);
                    to_remove_put[key_map_pair.first].insert(id_map_pair.first);
#ifndef BENCH_CACHE_ROUTING
                    try_request(pending_put_response_map_[key_map_pair.first][id_map_pair.first].request_);
#else
                    if (pending_request_map_.find(key_map_pair.first) == pending_request_map_.end()) {
                        pending_request_map_[key_map_pair.first].first = std::chrono::system_clock::now();
                    }
                    pending_request_map_[key_map_pair.first].second.push_back(pending_put_response_map_[key_map_pair.first][id_map_pair.first].request_);
#endif
                }
            }
        }

        for (const auto& key_set_pair : to_remove_put) {
            for (const auto& id : key_set_pair.second) {
                pending_put_response_map_[key_set_pair.first].erase(id);
            }
        }

        // Retry the pending update response map
        map<Key, set<string>> to_remove_update;
        for (const auto& key_map_pair : pending_update_response_map_) {
            for (const auto& id_map_pair :
                    pending_update_response_map_[key_map_pair.first]) {
                if (std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now() -
                            pending_update_response_map_[key_map_pair.first][id_map_pair.first]
                            .tp_)
                        .count() > timeout_) {
                    log_->error("Time out in receiving update request response");
                    invalidate_cache_for_worker(id_map_pair.second.worker_addr_);
                    to_remove_update[key_map_pair.first].insert(id_map_pair.first);
#ifndef BENCH_CACHE_ROUTING
                    try_request(pending_update_response_map_[key_map_pair.first][id_map_pair.first].request_);
#else
                    if (pending_request_map_.find(key_map_pair.first) == pending_request_map_.end()) {
                        pending_request_map_[key_map_pair.first].first = std::chrono::system_clock::now();
                    }
                    pending_request_map_[key_map_pair.first].second.push_back(pending_update_response_map_[key_map_pair.first][id_map_pair.first].request_);
#endif
                }
            }
        }

        for (const auto& key_set_pair : to_remove_update) {
            for (const auto& id : key_set_pair.second) {
                pending_update_response_map_[key_set_pair.first].erase(id);
            }
        }
    }

    return result;
  }

  vector<KeyResponse> receive_async3() {
    vector<KeyResponse> result;
    kZmqUtil->poll(0, &pollitems_);

    if (pollitems_[0].revents & ZMQ_POLLIN) {
#ifndef BENCH_CACHE_ROUTING
      string serialized = kZmqUtil->recv_string(&key_address_puller_);
      KeyAddressResponse response;
      response.ParseFromString(serialized);
      Key key = response.addresses(0).key();

      if (pending_request_map_.find(key) != pending_request_map_.end()) {
        if (response.error() == AnnaError::NO_SERVERS) {
          log_->error(
              "No servers have joined the cluster yet. Retrying request.");
          pending_request_map_[key].first = std::chrono::system_clock::now();

          query_routing_async(key);
        } else {
          // populate cache
          for (const Address& ip : response.addresses(0).ips()) {
            key_address_cache_[key].insert(ip);
          }

          // handle stuff in pending request map
          for (auto& req : pending_request_map_[key].second) {
            try_request(req);
          }

          // GC the pending request map
          pending_request_map_.erase(key);
        }
      }
#else
      string serialized = kZmqUtil->recv_string(&key_address_puller_);
      ClusterMembership membership;
      membership.ParseFromString(serialized);

      waiting_new_membership = false;

      if (!pending_request_map_.empty()) {
          global_hash_rings.clear();
          local_hash_rings.clear();
          key_address_cache_.clear();
#ifdef ENABLE_CLOVER_KVS
          threads.clear();
#endif

          for (const auto &tier : membership.tiers())
          {
              Tier id = tier.tier_id();

              for (const auto server : tier.servers())
              {
                  global_hash_rings[id].insert(server.public_ip(), server.private_ip(), 0, 0);
#ifdef ENABLE_CLOVER_KVS
                  for (const auto &pair : kTierMetadata)
                  {
                      TierMetadata tier = pair.second;
                      for (unsigned tid = 0; tid < tier.thread_number_; tid++)
                      {
                          threads.insert(ServerThread(server.public_ip(), server.private_ip(), tid).key_request_connect_address());
                      }
                  }
#endif
              }
          }

          int num_servers = 0;
          for (const auto &pair : global_hash_rings)
          {
              num_servers += pair.second.size();
          }

          if (num_servers == 0) {
              log_->error("No servers have joined the cluster yet. Retrying request.");
              query_routing_async("membership");
          } else {
#ifdef ENABLE_CLOVER_KVS
              log_->info("{} servers are active in the cluster ({}).", num_servers, threads.size());
#else
              log_->info("{} servers are active in the cluster.", num_servers);
#endif
              for (const auto &pair : kTierMetadata)
              {
                  TierMetadata tier = pair.second;
                  for (unsigned tid = 0; tid < tier.thread_number_; tid++)
                  {
                      local_hash_rings[tier.id_].insert(ut_.ip(), ut_.ip(), 0, tid);
                  }
              }

              for (const auto &pair : pending_request_map_) {
                  for (auto &req : pending_request_map_[pair.first].second) {
                      try_request(req);
                  }
              }

              pending_request_map_.clear();
          }
      }
#endif
    }

    if (pollitems_[1].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&response_puller_);
      KeyResponse response;
      response.ParseFromString(serialized);
      Key key = response.tuples(0).key();

      if (response.type() == RequestType::GET) {
#ifdef SINGLE_OUTSTANDING
        if (pending_get_response_map_.find(key) !=
            pending_get_response_map_.end()) {
          if (check_tuple(response.tuples(0))) {
            // error no == 2, so re-issue request
            pending_get_response_map_[key].tp_ =
                std::chrono::system_clock::now();
#ifndef BENCH_CACHE_ROUTING
            try_request(pending_get_response_map_[key].request_);
#else
            if (pending_request_map_.find(key) == pending_request_map_.end()) {
                pending_request_map_[key].first = std::chrono::system_clock::now();
            }
            pending_request_map_[key].second.push_back(pending_get_response_map_[key].request_);
#endif
          } else {
            // error no == 0 or 1
            result.push_back(response);
            pending_get_response_map_.erase(key);
          }
        }
#else
        if (pending_get_response_map_.find(key) !=
                pending_get_response_map_.end() &&
                pending_get_response_map_[key].find(response.response_id()) !=
                pending_get_response_map_[key].end()) {
            if (check_tuple(response.tuples(0))) {
                // error no == 2, so re-issue request
                pending_get_response_map_[key][response.response_id()].tp_ =
                    std::chrono::system_clock::now();
#ifndef BENCH_CACHE_ROUTING
                try_request(pending_get_response_map_[key][response.response_id()]
                        .request_);
#else
                if (pending_request_map_.find(key) == pending_request_map_.end()) {
                    pending_request_map_[key].first = std::chrono::system_clock::now();
                }
                pending_request_map_[key].second.push_back(pending_get_response_map_[key][response.response_id()].request_);
#endif
            } else {
                // error no == 0 or 1
                result.push_back(response);
                pending_get_response_map_[key].erase(response.response_id());

                if (pending_get_response_map_[key].size() == 0) {
                    pending_get_response_map_.erase(key);
                }
            }
        }
#endif
      } else if (response.type() == RequestType::UPDATE) {
        if (pending_update_response_map_.find(key) !=
                pending_update_response_map_.end() &&
            pending_update_response_map_[key].find(response.response_id()) !=
                pending_update_response_map_[key].end()) {
          if (check_tuple(response.tuples(0))) {
            // error no == 2, so re-issue request
            pending_update_response_map_[key][response.response_id()].tp_ =
                std::chrono::system_clock::now();
#ifndef BENCH_CACHE_ROUTING
            try_request(pending_update_response_map_[key][response.response_id()]
                            .request_);
#else
            if (pending_request_map_.find(key) == pending_request_map_.end()) {
                pending_request_map_[key].first = std::chrono::system_clock::now();
            }
            pending_request_map_[key].second.push_back(pending_update_response_map_[key][response.response_id()].request_);
#endif
          } else {
            // error no == 0
            result.push_back(response);
            pending_update_response_map_[key].erase(response.response_id());

            if (pending_update_response_map_[key].size() == 0) {
              pending_update_response_map_.erase(key);
            }
          }
        }
      } else {
        if (pending_put_response_map_.find(key) !=
                pending_put_response_map_.end() &&
            pending_put_response_map_[key].find(response.response_id()) !=
                pending_put_response_map_[key].end()) {
          if (check_tuple(response.tuples(0))) {
            // error no == 2, so re-issue request
            pending_put_response_map_[key][response.response_id()].tp_ =
                std::chrono::system_clock::now();
#ifndef BENCH_CACHE_ROUTING
            try_request(pending_put_response_map_[key][response.response_id()]
                            .request_);
#else
            if (pending_request_map_.find(key) == pending_request_map_.end()) {
                pending_request_map_[key].first = std::chrono::system_clock::now();
            }
            pending_request_map_[key].second.push_back(pending_put_response_map_[key][response.response_id()].request_);
#endif
          } else {
            // error no == 0
            result.push_back(response);
            pending_put_response_map_[key].erase(response.response_id());

            if (pending_put_response_map_[key].size() == 0) {
              pending_put_response_map_.erase(key);
            }
          }
        }
      }
    }

    return result;
  }

  /**
   * Set the logger used by the client.
   */
  void set_logger(logger log) { log_ = log; }

  /**
   * Clears the key address cache held by this client.
   */
  void clear_cache() { key_address_cache_.clear(); }

  /**
   * Return the ZMQ context used by this client.
   */
  zmq::context_t* get_context() { return &context_; }

  /**
   * Return the random seed used by this client.
   */
  unsigned get_seed() { return seed_; }

  bool check_all_pending_requests_done() {
      return (pending_request_map_.empty() &&
              pending_get_response_map_.empty() &&
              pending_put_response_map_.empty() &&
              pending_update_response_map_.empty());
  }

 private:
  /**
   * A recursive helper method for the get and put implementations that tries
   * to issue a request at most trial_limit times before giving up. It  checks
   * for the default failure modes (timeout, errno == 2, and cache
   * invalidation). If there are no issues, it returns the set of responses to
   * the respective implementations for them to deal with. This is the same as
   * the above implementation of try_multi_request, except it only operates on
   * a single request.
   */
  void try_request(KeyRequest& request) {
    // we only get NULL back for the worker thread if the query to the routing
    // tier timed out, which should never happen.
    Key key = request.tuples(0).key();
#ifndef BENCH_CACHE_ROUTING
    Address worker = get_worker_thread(key);
    if (worker.length() == 0) {
      // this means a key addr request is issued asynchronously
      if (pending_request_map_.find(key) == pending_request_map_.end()) {
        pending_request_map_[key].first = std::chrono::system_clock::now();
      }
      pending_request_map_[key].second.push_back(request);
      return;
    }

    request.mutable_tuples(0)->set_address_cache_size(key_address_cache_[key].size());
#else
    set<Address> workers = get_all_worker_threads(key);
    if (workers.size() == 0) {
      // this means a key addr request is issued asynchronously
      if (pending_request_map_.find(key) == pending_request_map_.end()) {
        pending_request_map_[key].first = std::chrono::system_clock::now();
      }
      pending_request_map_[key].second.push_back(request);
      return;
    }

    Address worker = *(next(begin(workers), rand_r(&seed_) % workers.size()));

    request.mutable_tuples(0)->set_address_cache_size(workers.size());
#endif

    send_request<KeyRequest>(request, socket_cache_[worker]);

    if (request.type() == RequestType::GET || request.type() == RequestType::READ) {
#ifdef SINGLE_OUTSTANDING
      if (pending_get_response_map_.find(key) ==
          pending_get_response_map_.end()) {
        pending_get_response_map_[key].tp_ = std::chrono::system_clock::now();
        pending_get_response_map_[key].request_ = request;
      }

      pending_get_response_map_[key].worker_addr_ = worker;
#else
      if (pending_get_response_map_[key].find(request.request_id()) ==
              pending_get_response_map_[key].end()) {
          pending_get_response_map_[key][request.request_id()].tp_ =
              std::chrono::system_clock::now();
          pending_get_response_map_[key][request.request_id()].request_ = request;
      }

      pending_get_response_map_[key][request.request_id()].worker_addr_ = worker;
#endif
    } else if (request.type() == RequestType::UPDATE) {
      if (pending_update_response_map_[key].find(request.request_id()) ==
          pending_update_response_map_[key].end()) {
        pending_update_response_map_[key][request.request_id()].tp_ =
            std::chrono::system_clock::now();
        pending_update_response_map_[key][request.request_id()].request_ = request;
      }

      pending_update_response_map_[key][request.request_id()].worker_addr_ = worker;
    } else {
      if (pending_put_response_map_[key].find(request.request_id()) ==
          pending_put_response_map_[key].end()) {
        pending_put_response_map_[key][request.request_id()].tp_ =
            std::chrono::system_clock::now();
        pending_put_response_map_[key][request.request_id()].request_ = request;
      }

      pending_put_response_map_[key][request.request_id()].worker_addr_ = worker;
    }
  }

  /**Reference --> Lancet: A self-correcting Latency Measuring Tool.
   * A helper method to check for the default failure modes for a request that
   * retrieves a response. It returns true if the caller method should reissue
   * the request (this happens if errno == 2). Otherwise, it returns false. It
   * invalidates the local cache if the information is out of date.
   */
  bool check_tuple(const KeyTuple& tuple) {
    Key key = tuple.key();

#ifdef SHARED_NOTHING
    // This is the special routine for shared-nothing simulation.
    // As we are not blocking the nodes involved in data reshuffling,
    // there can be a response in which key doesn't exist before
    // reshuffling has been done.
    if (tuple.error() == 1) {
      invalidate_cache_for_key(key, tuple);
      return true;
    }
#endif

    if (tuple.error() == 2) {
      invalidate_cache_for_key(key, tuple);
      return true;
    }

    if (tuple.invalidate()) {
#ifdef BENCH_CACHE_ROUTING
      key_address_cache_.erase(key);
#else
      invalidate_cache_for_key(key, tuple);
#endif
      update_cache_for_key(key, tuple);
    }

    return false;
  }

#ifndef BENCH_CACHE_ROUTING
  /**
   * When a server thread tells us to invalidate the cache for a key it's
   * because we likely have out of date information for that key; it sends us
   * the updated information for that key, and update our cache with that
   * information.
   */
  void invalidate_cache_for_key(const Key& key, const KeyTuple& tuple) {
    key_address_cache_.erase(key);
  }

  void update_cache_for_key(const Key& key, const KeyTuple& tuple) {
    for (const Address& ip : tuple.ips()) {
        key_address_cache_[key].insert(ip);
    }
  }

  /**
   * Invalidate the key caches for any key that previously had this worker in
   * its cache. The underlying assumption is that if the worker timed out, it
   * might have failed, and so we don't want to rely on it being alive for both
   * the key we were querying and any other key.
   */
  void invalidate_cache_for_worker(const Address& worker) {
    vector<string> tokens;
    split(worker, ':', tokens);
    string signature = tokens[1];
    set<Key> remove_set;

    for (const auto& key_pair : key_address_cache_) {
      for (const string& address : key_pair.second) {
        vector<string> v;
        split(address, ':', v);

        if (v[1] == signature) {
          remove_set.insert(key_pair.first);
        }
      }
    }

    for (const string& key : remove_set) {
      key_address_cache_.erase(key);
    }
  }
#else
  /**
   * When a server thread tells us to invalidate the cache for a key it's
   * because we likely have out of date information for that key; it sends us
   * the updated information for that key, and update our cache with that
   * information.
   */
  void invalidate_cache_for_key(const Key& key, const KeyTuple& tuple) {
    if (!waiting_new_membership) {
      query_routing_async("membership");
    }
  }

  void update_cache_for_key(const Key& key, const KeyTuple& tuple) {
    map<Key, set<Address>> key_address_map;
    unsigned global_replication_factor = 1;
    unsigned local_replication_factor = 1;
    for (const Address& ip : tuple.ips()) {
      vector<string> v;
      split(ip, ':', v);
      key_address_map[v[1]].insert(v[2]);
    }

    global_replication_factor = key_address_map.size();
    for (const auto &pair : key_address_map) {
      if (pair.second.size() > local_replication_factor)
        local_replication_factor = pair.second.size();
    }

    key_replication_map[key].global_replication_[Tier::MEMORY] = global_replication_factor;
    key_replication_map[key].local_replication_[Tier::MEMORY] = local_replication_factor;
  }

  /**
   * Invalidate the key caches for any key that previously had this worker in
   * its cache. The underlying assumption is that if the worker timed out, it
   * might have failed, and so we don't want to rely on it being alive for both
   * the key we were querying and any other key.
   */
  void invalidate_cache_for_worker(const Address& worker) {
    if (!waiting_new_membership) {
      query_routing_async("membership");
    }
  }
#endif

  /**
   * Prepare a data request object by populating the request ID, the key for
   * the request, and the response address. This method modifies the passed-in
   * KeyRequest and also returns a pointer to the KeyTuple contained by this
   * request.
   */
  KeyTuple* prepare_data_request(KeyRequest& request, const Key& key) {
    request.set_request_id(get_request_id());
    request.set_response_address(ut_.response_connect_address());

    KeyTuple* tp = request.add_tuples();
    tp->set_key(key);

    return tp;
  }

#ifdef BENCH_CACHE_ROUTING
  /**
   * returns all the worker threads for the key queried. If there are no cached
   * threads, a request is sent to the routing tier. If the query times out,
   * NULL is returned.
   */
  set<Address> get_all_worker_threads(const Key& key) {
    if (!waiting_new_membership) {
#ifndef ENABLE_CLOVER_KVS
        if (key_address_cache_.find(key) == key_address_cache_.end() ||
                key_address_cache_[key].size() == 0) {
            bool succeed;
            ServerThreadList threads = kHashRingUtil->get_responsible_threads(
                    "", key, false, global_hash_rings, local_hash_rings,
                    key_replication_map, socket_cache_, kMemTier, succeed, seed_);
            if (threads.size() != 0) {
                for (const auto &thread : threads)
                    key_address_cache_[key].insert(thread.key_request_connect_address());
                return key_address_cache_[key];
            } else {
                query_routing_async("membership");
            }
        } else {
            return key_address_cache_[key];
        }
#else
        if (threads.size() != 0) {
            return threads;
        } else {
            query_routing_async("membership");
        }
#endif
    }

    return set<Address>();
  }
#else
  /**
   * returns all the worker threads for the key queried. If there are no cached
   * threads, a request is sent to the routing tier. If the query times out,
   * NULL is returned.
   */
  set<Address> get_all_worker_threads(const Key& key) {
    if (key_address_cache_.find(key) == key_address_cache_.end() ||
        key_address_cache_[key].size() == 0) {
      if (pending_request_map_.find(key) == pending_request_map_.end()) {
        query_routing_async(key);
      }
      return set<Address>();
    } else {
      return key_address_cache_[key];
    }
  }
#endif
  /**
   * Similar to the previous method, but only returns one (randomly chosen)
   * worker address instead of all of them.
   */
  Address get_worker_thread(const Key& key) {
    set<Address> local_cache = get_all_worker_threads(key);

    // This will be empty if the worker threads are not cached locally
    if (local_cache.size() == 0) {
      return "";
    }

    return *(next(begin(local_cache), rand_r(&seed_) % local_cache.size()));
  }

  /**
   * Returns one random routing thread's key address connection address. If the
   * client is running outside of the cluster (ie, it is querying the ELB),
   * there's only one address to choose from but 4 threads.
   */
  Address get_routing_thread() {
    return routing_threads_[rand_r(&seed_) % routing_threads_.size()]
        .key_address_connect_address();
  }

  /**
   * Send a query to the routing tier asynchronously.
   */
  void query_routing_async(const Key& key) {
    // define protobuf request objects
    KeyAddressRequest request;

    // populate request with response address, request id, etc.
    request.set_request_id(get_request_id());
    request.set_response_address(ut_.key_address_connect_address());
    request.add_keys(key);

    Address rt_thread = get_routing_thread();
    send_request<KeyAddressRequest>(request, socket_cache_[rt_thread]);
#ifdef BENCH_CACHE_ROUTING
    waiting_new_membership = true;
#endif
  }

  /**
   * Generates a unique request ID.
   */
  string get_request_id() {
    //if (++rid_ % 10000 == 0) rid_ = 0;
    return ut_.ip() + ":" + std::to_string(ut_.tid()) + "_" +
           std::to_string(rid_++);
  }

  KeyResponse generate_bad_response(const KeyRequest& req) {
    KeyResponse resp;

    resp.set_type(req.type());
    resp.set_response_id(req.request_id());
    resp.set_error(AnnaError::TIMEOUT);

    KeyTuple* tp = resp.add_tuples();
    tp->set_key(req.tuples(0).key());

    if (req.type() == RequestType::PUT) {
      tp->set_lattice_type(req.tuples(0).lattice_type());
      tp->set_payload(req.tuples(0).payload());
    }

    return resp;
  }

 private:
  // the set of routing addresses outside the cluster
  vector<UserRoutingThread> routing_threads_;

  // the current request id
  uint64_t rid_;
  //unsigned rid_;

  // the random seed for this client
  unsigned seed_;

  // the IP and port functions for this thread
  UserThread ut_;

  // the ZMQ context we use to create sockets
  zmq::context_t context_;

  // cache for opened sockets
  SocketCache socket_cache_;

  // ZMQ receiving sockets
  zmq::socket_t key_address_puller_;
  zmq::socket_t response_puller_;

  vector<zmq::pollitem_t> pollitems_;

  // cache for retrieved worker addresses organized by key
  map<Key, set<Address>> key_address_cache_;

#ifdef BENCH_CACHE_ROUTING
  GlobalRingMap global_hash_rings;

  LocalRingMap local_hash_rings;

  map<Key, KeyReplication> key_replication_map;

  const vector<Tier> kMemTier = {Tier::MEMORY};

  bool waiting_new_membership;
#ifdef ENABLE_CLOVER_KVS
  set<Address> threads;
#endif
#endif

  // class logger
  logger log_;

  // GC timeout
  unsigned timeout_;

  // Whether assuming cluster reconfigurations
  bool enableReconfig;

  // keeps track of pending requests due to missing worker address
  map<Key, pair<TimePoint, vector<KeyRequest>>> pending_request_map_;

#ifdef SINGLE_OUTSTANDING
  // keeps track of pending get responses
  map<Key, PendingRequest> pending_get_response_map_;
#else
  map<Key, map<string, PendingRequest>> pending_get_response_map_;
#endif

  // keeps track of pending put responses
  map<Key, map<string, PendingRequest>> pending_put_response_map_;

  // keeps track of pending update responses
  map<Key, map<string, PendingRequest>> pending_update_response_map_;
};

#endif  // INCLUDE_ASYNC_CLIENT_HPP_