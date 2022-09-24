#include "kvs/kvs_handlers.hpp"
#include "yaml-cpp/yaml.h"

// define server report threshold (in second)
unsigned kServerReportThreshold = 15;

// define server's key monitoring threshold (in second)
unsigned kKeyMonitoringThreshold = 60;

unsigned kThreadNum;

Tier kSelfTier;
vector<Tier> kSelfTierIdVector;

unsigned kMemoryThreadCount;
unsigned kStorageThreadCount;

uint64_t kMemoryNodeCapacity;
uint64_t kStorageNodeCapacity;

unsigned kDefaultGlobalMemoryReplication;
unsigned kDefaultGlobalStorageReplication;
unsigned kDefaultLocalReplication;

bool kEnableElasticity;
bool kEnableSelectiveRep;
bool kEnableBatching;
unsigned kBatchingThreshold = 5000;
bool kEnableCaching;
bool kEnableAdaptiveCache;
double kWeightOrValueRatio;
bool kEnablePerfMonitor;
int kReqPullerHWM = 1000;
int kReqPullerBacklog = 100;

#ifdef SHARED_NOTHING
std::unordered_map<int, std::string> peer_ip_addresses;
#endif
uint64_t VALUE_SIZE = 256;
uint64_t SHORTCUT_SIZE = 16;
uint64_t LOG_BLOCK_HEADER_LEN = 40;
uint64_t MAX_LOG_BLOCK_LEN = (2 * 1024 * 1024) - LOG_BLOCK_HEADER_LEN;
uint64_t MAX_PREALLOC_NUM = 2;
unsigned kLogCacheLen = 1;

#ifndef ENABLE_DINOMO_KVS
hmap<Tier, TierMetadata, TierEnumHash> kTierMetadata;
#endif

ZmqUtil zmq_util;
ZmqUtilInterface *kZmqUtil = &zmq_util;

HashRingUtil hash_ring_util;
HashRingUtilInterface *kHashRingUtil = &hash_ring_util;

#ifdef ENABLE_DINOMO_KVS
#ifdef ENABLE_CLOVER_KVS
void clover_init(int num_initial_memory_nodes, int rank, char **clover_memc_ips);
#endif
#endif

template <typename DpmKVS>
void run(unsigned thread_id, Address public_ip, Address private_ip,
         Address seed_ip, vector<Address> routing_ips,
         vector<Address> monitoring_ips, Address management_ip)
{
    string log_file = "log_" + std::to_string(thread_id) + ".txt";
    string log_name = "server_log_" + std::to_string(thread_id);
    auto log = spdlog::basic_logger_mt(log_name, log_file, true);
    log->flush_on(spdlog::level::info);

    // each thread has a handle to itself
    ServerThread wt = ServerThread(public_ip, private_ip, thread_id);

    unsigned seed = time(NULL);
    seed += thread_id;

    // A monotonically increasing integer.
    unsigned rid = 0;

    // prepare the zmq context
    zmq::context_t context(1);

    auto res = context.setctxopt(ZMQ_MAX_SOCKETS, kMaxSocketNumber);
    if (res == 0)
    {
        log->info("Successfully set max socket number to {}", kMaxSocketNumber);
    }
    else
    {
        log->error("E: socket error number {} ({})", errno, zmq_strerror(errno));
    }

    SocketCache pushers(&context, ZMQ_PUSH);

#ifdef ENABLE_DINOMO_KVS
    // responsible for listening to the response of hash ring map change request
    zmq::socket_t response_puller(context, ZMQ_PULL);
    int timeout = 10000;

    response_puller.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
    response_puller.bind(wt.response_bind_address());
#endif

    // initialize hash ring maps
    GlobalRingMap global_hash_rings;
    LocalRingMap local_hash_rings;

    // for periodically redistributing data when node joins
    // Sekwon: for dinomo, this is unnecessary
    AddressKeysetMap join_gossip_map;

    // keep track of which key should be removed when node joins
    set<Key> join_remove_set;

    // for tracking IP addresses of extant caches
    set<Address> extant_caches;

    // For tracking the keys each extant cache is responsible for.
    // This is just our thread's cache of this.
    map<Address, set<Key> > cache_ip_to_keys;

    // For tracking the caches that hold a given key.
    // Inverse of cache_ip_to_keys.
    // We need the two structures because
    // key->caches is the one necessary for gossiping upon key updates,
    // but the mapping is provided to us in the form cache->keys,
    // so we need a local copy of this mapping in order to update key->caches
    // with dropped keys when we receive a fresh cache->keys record.
    map<Key, set<Address> > key_to_cache_ips;

    // pending events for asynchrony
    map<Key, vector<PendingRequest> > pending_requests;
    map<Key, vector<PendingGossip> > pending_gossip;

    // batching update events to reduce RTT
    map<Key, vector<PendingRequest> > batching_requests;

    // this map contains all keys that are actually stored in the KVS
#ifdef SHARED_NOTHING
    set<Key> stored_key_map;
#else
    map<Key, KeyProperty> stored_key_map;
#endif

    map<Key, KeyReplication> key_replication_map;

    // request server addresses from the seed node
    zmq::socket_t addr_requester(context, ZMQ_REQ);
    addr_requester.connect(RoutingThread(seed_ip, 0).seed_connect_address());
    kZmqUtil->send_string("join", &addr_requester);

    // receive and add all the addresses that seed node sent
    string serialized_addresses = kZmqUtil->recv_string(&addr_requester);
    ClusterMembership membership;
    membership.ParseFromString(serialized_addresses);

    // get join number from management node if we are running in Kubernetes
    string count_str;

    // if we are running the system outside of Kubernetes, we need to set the
    // management address to NULL in the conf file, otherwise we will hang
    // forever waiting to hear back about a restart count
    if (management_ip != "NULL")
    {
        zmq::socket_t join_count_requester(context, ZMQ_REQ);
        join_count_requester.connect(get_join_count_req_address(management_ip));
        kZmqUtil->send_string("restart:" + private_ip, &join_count_requester);
        count_str = kZmqUtil->recv_string(&join_count_requester);
    }
    else
    {
        count_str = "0";
    }

    int self_join_count = stoi(count_str);

    // populate addresses
    for (const auto &tier : membership.tiers())
    {
        Tier id = tier.tier_id();

        for (const auto server : tier.servers())
        {
            global_hash_rings[id].insert(server.public_ip(), server.private_ip(), 0,
                                         0);
        }
    }

    // add itself to global hash ring
    global_hash_rings[kSelfTier].insert(public_ip, private_ip, self_join_count,
                                        0);

    // form local hash rings
    for (const auto &pair : kTierMetadata)
    {
        TierMetadata tier = pair.second;
        for (unsigned tid = 0; tid < tier.thread_number_; tid++)
        {
            local_hash_rings[tier.id_].insert(public_ip, private_ip, 0, tid);
        }
    }

#if 0
#ifdef ENABLE_DINOMO_KVS
    if (thread_id == 0)
    {
        string msg = Tier_Name(kSelfTier) + ":" + public_ip + ":" + private_ip + ":" + count_str;
        unsigned rid = 0;

        for (const auto &pair : global_hash_rings)
        {
            GlobalHashRing hash_ring = pair.second;

            const ServerThreadList &st_sync = hash_ring.get_partition_range_changed_threads(public_ip, private_ip, 0);
            const ServerThreadSet &st_async = hash_ring.get_unique_servers();

            vector<NodeJoinRequestResponse> join_responses;
            for (const ServerThread &st : st_sync)
            {
                if (st.private_ip().compare(private_ip) != 0)
                {
                    bool succeed;
                    NodeJoinRequest req;
                    req.set_response_address(wt.response_connect_address());
                    string req_id = wt.response_connect_address() + ":" + std::to_string(rid);
                    req.set_request_id(req_id);
                    req.set_request_msg(msg);
                    rid++;

                    auto res = make_request<NodeJoinRequest, NodeJoinRequestResponse>(req,
                            pushers[st.node_join_connect_address()], response_puller, succeed);
                    if (!succeed)
                    {
                        log->error("Consistent node join timed out!");
                    }
                    else
                    {
                        join_responses.push_back(res);
                    }
                }
            }

            for (const NodeJoinRequestResponse &join_response : join_responses)
            {
                NodeJoinRequestUnblock ack;
                ack.set_response_id(join_response.request_id());

                string serialized_ack;
                ack.SerializeToString(&serialized_ack);
                kZmqUtil->send_string(serialized_ack, &pushers[join_response.response_address()]);
            }

            for (const ServerThread &st : st_async)
            {
                if (st.private_ip().compare(private_ip) != 0)
                {
                    if (std::find(st_sync.begin(), st_sync.end(), st) == st_sync.end()) 
                    {
                        NodeJoinRequest req;
                        req.set_response_address("");
                        req.set_request_id("");
                        req.set_request_msg(msg);

                        string serialized_msg;
                        req.SerializeToString(&serialized_msg);

                        kZmqUtil->send_string(serialized_msg, &pushers[st.node_join_connect_address()]);
                    }
                }
            }
        }

        msg = "join:" + msg;

        // notify proxies that this node has joined
        for (const string &address : routing_ips)
        {
            kZmqUtil->send_string(
                msg, &pushers[RoutingThread(address, 0).notify_connect_address()]);
        }

        // notify monitoring nodes that this node has joined
        for (const string &address : monitoring_ips)
        {
            kZmqUtil->send_string(
                msg, &pushers[MonitoringThread(address).notify_connect_address()]);
        }
    }
#else
    // thread 0 notifies other servers that it has joined
    if (thread_id == 0)
    {
        string msg = Tier_Name(kSelfTier) + ":" + public_ip + ":" + private_ip +
                     ":" + count_str;

        for (const auto &pair : global_hash_rings)
        {
            GlobalHashRing hash_ring = pair.second;

            for (const ServerThread &st : hash_ring.get_unique_servers())
            {
                if (st.private_ip().compare(private_ip) != 0)
                {
                    kZmqUtil->send_string(msg, &pushers[st.node_join_connect_address()]);
                }
            }
        }

        msg = "join:" + msg;

        // notify proxies that this node has joined
        for (const string &address : routing_ips)
        {
            kZmqUtil->send_string(
                msg, &pushers[RoutingThread(address, 0).notify_connect_address()]);
        }

        // notify monitoring nodes that this node has joined
        for (const string &address : monitoring_ips)
        {
            kZmqUtil->send_string(
                msg, &pushers[MonitoringThread(address).notify_connect_address()]);
        }
    }
#endif
#endif

    ////////////////// Sekwon: These serializer should be changed or removed for DINOMO
    SerializerMap serializers;

    Serializer *lww_serializer;
    Serializer *set_serializer;
    Serializer *ordered_set_serializer;
    Serializer *sk_causal_serializer;
    Serializer *mk_causal_serializer;
    Serializer *priority_serializer;

    DpmKVS *kvs_ = NULL;

    if (kSelfTier == Tier::MEMORY)
    {
#ifndef ENABLE_DINOMO_KVS
        DpmKVS *lww_kvs = new DpmKVS();
        kvs_ = lww_kvs;
        lww_serializer = new MemoryLWWSerializer<DpmKVS>(lww_kvs);
#else
#ifndef ENABLE_CLOVER_KVS
        DpmKVS *lww_kvs = new DpmKVS(kTierMetadata[Tier::MEMORY].node_capacity_ / (uint64_t)config_info.threads_per_memory, thread_id, kEnableBatching, kWeightOrValueRatio);
        kvs_ = lww_kvs;
        lww_serializer = new MemoryLWWSerializer<DpmKVS>(lww_kvs);
#else
        DpmKVS *lww_kvs = new DpmKVS(thread_id);
        kvs_ = lww_kvs;
        lww_serializer = new MemoryLWWSerializer<DpmKVS>(lww_kvs);
#endif
#endif

        MemorySetKVS *set_kvs = new MemorySetKVS();
        set_serializer = new MemorySetSerializer(set_kvs);

        MemoryOrderedSetKVS *ordered_set_kvs = new MemoryOrderedSetKVS();
        ordered_set_serializer = new MemoryOrderedSetSerializer(ordered_set_kvs);

        MemorySingleKeyCausalKVS *causal_kvs = new MemorySingleKeyCausalKVS();
        sk_causal_serializer = new MemorySingleKeyCausalSerializer(causal_kvs);

        MemoryMultiKeyCausalKVS *multi_key_causal_kvs =
            new MemoryMultiKeyCausalKVS();
        mk_causal_serializer =
            new MemoryMultiKeyCausalSerializer(multi_key_causal_kvs);

        MemoryPriorityKVS *priority_kvs = new MemoryPriorityKVS();
        priority_serializer = new MemoryPrioritySerializer(priority_kvs);
    }
    else if (kSelfTier == Tier::STORAGE)
    {
        lww_serializer = new DiskLWWSerializer(thread_id);
        set_serializer = new DiskSetSerializer(thread_id);
        ordered_set_serializer = new DiskOrderedSetSerializer(thread_id);
        sk_causal_serializer = new DiskSingleKeyCausalSerializer(thread_id);
        mk_causal_serializer = new DiskMultiKeyCausalSerializer(thread_id);
        priority_serializer = new DiskPrioritySerializer(thread_id);
    }
    else
    {
        log->info("Invalid node type");
        exit(1);
    }

    serializers[LatticeType::LWW] = lww_serializer;
    serializers[LatticeType::SET] = set_serializer;
    serializers[LatticeType::ORDERED_SET] = ordered_set_serializer;
    serializers[LatticeType::SINGLE_CAUSAL] = sk_causal_serializer;
    serializers[LatticeType::MULTI_CAUSAL] = mk_causal_serializer;
    serializers[LatticeType::PRIORITY] = priority_serializer;
    /////////////////////////////////////////////////////////////////////////////////

    // the set of changes made on this thread since the last round of gossip
    set<Key> local_changeset;

    // keep track of the key stat
    // the first entry is the size of the key,
    // the second entry is its lattice type.
    // keep track of key access timestamp
    map<Key, std::multiset<TimePoint>> key_access_tracker;
    // keep track of total access
    unsigned access_count;

    // listens for a new node joining
    zmq::socket_t join_puller(context, ZMQ_PULL);
    join_puller.bind(wt.node_join_bind_address());

    // listens for a node departing
    zmq::socket_t depart_puller(context, ZMQ_PULL);
    depart_puller.bind(wt.node_depart_bind_address());

    // responsible for listening for a command that this node should leave
    zmq::socket_t self_depart_puller(context, ZMQ_PULL);
    self_depart_puller.bind(wt.self_depart_bind_address());

    // responsible for handling requests
    zmq::socket_t request_puller(context, ZMQ_PULL);
    request_puller.setsockopt(ZMQ_RCVHWM, &kReqPullerHWM, sizeof(kReqPullerHWM));
    request_puller.setsockopt(ZMQ_BACKLOG, &kReqPullerBacklog, sizeof(kReqPullerBacklog));
    request_puller.bind(wt.key_request_bind_address());

    // responsible for processing gossip
    zmq::socket_t failover_responder(context, ZMQ_REP);
    failover_responder.bind(wt.failover_bind_address());

    // responsible for processing gossip
    zmq::socket_t gossip_puller(context, ZMQ_PULL);
    gossip_puller.bind(wt.gossip_bind_address());

    // responsible for listening for key replication factor response
    zmq::socket_t replication_response_puller(context, ZMQ_PULL);
    replication_response_puller.bind(wt.replication_response_bind_address());

    // responsible for listening for key replication factor change
    zmq::socket_t replication_change_puller(context, ZMQ_PULL);
    replication_change_puller.bind(wt.replication_change_bind_address());

    // responsible for listening for cached keys response messages.
    zmq::socket_t cache_ip_response_puller(context, ZMQ_PULL);
    cache_ip_response_puller.bind(wt.cache_ip_response_bind_address());

    // responsible for listening for function node IP lookup response messages.
    zmq::socket_t management_node_response_puller(context, ZMQ_PULL);
    management_node_response_puller.bind(
        wt.management_node_response_bind_address());

#ifdef ENABLE_DINOMO_KVS
#ifndef SHARED_NOTHING
    //  Initialize poll set
    vector<zmq::pollitem_t> pollitems = {
        {static_cast<void *>(join_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(depart_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(self_depart_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(request_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(gossip_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(replication_response_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(replication_change_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(failover_responder), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(management_node_response_puller), 0, ZMQ_POLLIN, 0}};

    vector<zmq::pollitem_t> failover_pollitems = {
        {static_cast<void *>(depart_puller), 0, ZMQ_POLLIN, 0}};

    vector<zmq::pollitem_t> drain_pollitems = {
        {static_cast<void *>(request_puller), 0, ZMQ_POLLIN, 0}};
#else
    //  Initialize poll set
    vector<zmq::pollitem_t> pollitems = {
        {static_cast<void *>(join_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(depart_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(self_depart_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(request_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(gossip_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(replication_response_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(replication_change_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(failover_responder), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(management_node_response_puller), 0, ZMQ_POLLIN, 0}};

    vector<zmq::pollitem_t> failover_pollitems = {
        {static_cast<void *>(depart_puller), 0, ZMQ_POLLIN, 0}};

    vector<zmq::pollitem_t> drain_pollitems = {
        {static_cast<void *>(request_puller), 0, ZMQ_POLLIN, 0}};
#endif
#else
    //  Initialize poll set
    vector<zmq::pollitem_t> pollitems = {
        {static_cast<void *>(join_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(depart_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(self_depart_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(request_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(gossip_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(replication_response_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(replication_change_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(cache_ip_response_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(management_node_response_puller), 0, ZMQ_POLLIN, 0}};
#endif

    auto gossip_start = std::chrono::system_clock::now();
    auto gossip_end = std::chrono::system_clock::now();
    auto report_start = std::chrono::system_clock::now();
    auto report_end = std::chrono::system_clock::now();
    auto batching_start = std::chrono::system_clock::now();
    auto batching_end = std::chrono::system_clock::now();

    unsigned long long working_time = 0, snapshot = 0;
    unsigned long long working_time_map[10] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    uint64_t num_working = 0;
    uint64_t num_idle = 0;
    unsigned epoch = 0;

//#ifdef SEKWON_TEST
#if 1
    if (thread_id == 0)
    {
        auto join_start = std::chrono::system_clock::now();

        string msg = Tier_Name(kSelfTier) + ":" + public_ip + ":" + private_ip + ":" + count_str;
        unsigned rid = 0;

        for (const auto &pair : global_hash_rings)
        {
            if (pair.first == kSelfTier) {
                GlobalHashRing hash_ring = pair.second;

                const ServerThreadList &st_sync = hash_ring.get_partition_range_changed_threads(public_ip, private_ip, 0);
                const ServerThreadSet &st_async = hash_ring.get_unique_servers();

                log->info("The number of existing nodes blocked = {}.", st_sync.size());
                set<string> req_ids;
                vector<NodeJoinRequestResponse> join_responses;
                for (const ServerThread &st : st_sync)
                {
                    if (st.private_ip().compare(private_ip) != 0)
                    {
                        NodeJoinRequest req;
                        req.set_response_address(wt.response_connect_address());
                        string req_id = wt.response_connect_address() + ":" + std::to_string(rid);
                        req.set_request_id(req_id);
                        req.set_request_msg(msg);
                        rid++;
#if 0
                        auto res = make_request<NodeJoinRequest, NodeJoinRequestResponse>(req,
                                pushers[st.node_join_connect_address()], response_puller, succeed);
                        if (!succeed)
                        {
                            while (!succeed) {
                                kZmqUtil->poll(0, &failover_pollitems);
                                if (pollitems[0].revents & ZMQ_POLLIN) {
                                    break;
                                } else {
                                    vector<NodeJoinRequestResponse> responses;
                                    set<string> req_ids{req.request_id()};
                                    succeed = receive<NodeJoinRequestResponse>(response_puller, req_ids, responses);
                                    if (succeed)
                                        join_responses.push_back(responses[0]);
                                }
                            }
                        }
                        else
                        {
                            join_responses.push_back(res);
                        }
#endif
                        send_request<NodeJoinRequest>(req, pushers[st.node_join_connect_address()]);
                        req_ids.insert(req.request_id());
                    }
                }

                if (req_ids.size() != 0)
                {
                    bool succeed = receive<NodeJoinRequestResponse>(response_puller, req_ids, join_responses);
                    if (!succeed)
                    {
                        log->error("Time out while waiting the join response from other existing nodes");
                    }
                }

                for (const NodeJoinRequestResponse &join_response : join_responses)
                {
                    NodeJoinRequestUnblock ack;
                    ack.set_response_id(join_response.request_id());
                    send_request<NodeJoinRequestUnblock>(ack, pushers[join_response.response_address()]);
                }

                for (const ServerThread &st : st_async)
                {
                    if (st.private_ip().compare(private_ip) != 0)
                    {
                        if (std::find(st_sync.begin(), st_sync.end(), st) == st_sync.end()) 
                        {
                            NodeJoinRequest req;
                            req.set_response_address("");
                            req.set_request_id("");
                            req.set_request_msg(msg);

                            send_request<NodeJoinRequest>(req, pushers[st.node_join_connect_address()]);
                        }
                    }
                }
            }
        }

        msg = "join:" + msg;

        // notify proxies that this node has joined
        for (const string &address : routing_ips)
        {
            kZmqUtil->send_string(
                msg, &pushers[RoutingThread(address, 0).notify_connect_address()]);
        }

        // notify monitoring nodes that this node has joined
        for (const string &address : monitoring_ips)
        {
            kZmqUtil->send_string(
                msg, &pushers[MonitoringThread(address).notify_connect_address()]);
        }

        auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - join_start).count();
        log->info("Elapsed time to complete join = {} microsecond.", time_elapsed);
    }
#endif

    // enter event loop
    while (true)
    {
        kZmqUtil->poll(0, &pollitems);

        snapshot = working_time;

        // receives a node join
        if (pollitems[0].revents & ZMQ_POLLIN)
        {
            auto work_start = std::chrono::system_clock::now();

            string serialized = kZmqUtil->recv_string(&join_puller);
            node_join_handler(thread_id, seed, public_ip, private_ip, log, serialized,
                              global_hash_rings, local_hash_rings, stored_key_map,
                              key_replication_map, batching_requests, join_remove_set,
                              pushers, wt, join_gossip_map, self_join_count, serializers, 
                              response_puller, failover_pollitems);

            auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::system_clock::now() - work_start)
                                    .count();
            working_time += time_elapsed;
            working_time_map[0] += time_elapsed;
        }

        if (pollitems[1].revents & ZMQ_POLLIN)
        {
            auto work_start = std::chrono::system_clock::now();

            string serialized = kZmqUtil->recv_string(&depart_puller);
            node_depart_handler(thread_id, public_ip, private_ip, global_hash_rings,
                                log, serialized, pushers);

            auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::system_clock::now() - work_start)
                                    .count();
            working_time += time_elapsed;
            working_time_map[1] += time_elapsed;
        }

        if (pollitems[2].revents & ZMQ_POLLIN)
        {
            string serialized = kZmqUtil->recv_string(&self_depart_puller);
            self_depart_handler(thread_id, seed, public_ip, private_ip, log,
                                serialized, global_hash_rings, local_hash_rings,
                                stored_key_map, key_replication_map, routing_ips,
                                monitoring_ips, wt, pushers, serializers,
                                pending_requests, batching_requests,
                                drain_pollitems, request_puller);
            return;
        }

#ifdef ENABLE_DRAIN_USER_REQUESTS
        while (pollitems[3].revents & ZMQ_POLLIN)
        {
            zmq::message_t message;
            int size = request_puller.recv(&message, ZMQ_DONTWAIT);
            if (size <= 0) {
                pollitems[3].revents = 0;
            } else {
                auto work_start = std::chrono::system_clock::now();

                string serialized = kZmqUtil->message_to_string(message);
                user_request_handler(access_count, seed, serialized, log,
                        global_hash_rings, local_hash_rings,
                        pending_requests, key_access_tracker, stored_key_map,
                        key_replication_map, local_changeset, wt,
                        serializers, pushers, batching_requests, kEnableBatching);

                auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::system_clock::now() - work_start)
                    .count();

                working_time += time_elapsed;
                working_time_map[3] += time_elapsed;
            }
        }
#else
        if (pollitems[3].revents & ZMQ_POLLIN)
        {
            auto work_start = std::chrono::system_clock::now();

            string serialized = kZmqUtil->recv_string(&request_puller);
            user_request_handler(access_count, seed, serialized, log,
                                 global_hash_rings, local_hash_rings,
                                 pending_requests, key_access_tracker, stored_key_map,
                                 key_replication_map, local_changeset, wt,
                                 serializers, pushers, batching_requests, kEnableBatching);

            auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::system_clock::now() - work_start)
                                    .count();

            working_time += time_elapsed;
            working_time_map[3] += time_elapsed;
        }
#endif

        if (pollitems[4].revents & ZMQ_POLLIN)
        {
            auto work_start = std::chrono::system_clock::now();

            string serialized = kZmqUtil->recv_string(&gossip_puller);
            gossip_handler(seed, serialized, global_hash_rings, local_hash_rings,
                           pending_gossip, stored_key_map, key_replication_map, wt,
                           serializers, pushers, log, batching_requests, kEnableBatching);

            auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::system_clock::now() - work_start)
                                    .count();
            working_time += time_elapsed;
            working_time_map[4] += time_elapsed;
        }

        // receives replication factor response
        if (pollitems[5].revents & ZMQ_POLLIN)
        {
            auto work_start = std::chrono::system_clock::now();

            string serialized = kZmqUtil->recv_string(&replication_response_puller);
            replication_response_handler(
                seed, access_count, log, serialized, global_hash_rings,
                local_hash_rings, pending_requests, pending_gossip,
                key_access_tracker, stored_key_map, key_replication_map,
                local_changeset, wt, serializers, pushers,
                batching_requests, kBatchingThreshold);

            auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::system_clock::now() - work_start)
                                    .count();
            working_time += time_elapsed;
            working_time_map[5] += time_elapsed;
        }

        // receive replication factor change
        if (pollitems[6].revents & ZMQ_POLLIN)
        {
            auto work_start = std::chrono::system_clock::now();

            string serialized = kZmqUtil->recv_string(&replication_change_puller);
            replication_change_handler(
                public_ip, private_ip, thread_id, seed, log, serialized,
                global_hash_rings, local_hash_rings, stored_key_map,
                key_replication_map, local_changeset, wt, serializers, pushers,
                response_puller);

            auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::system_clock::now() - work_start)
                                    .count();
            working_time += time_elapsed;
            working_time_map[6] += time_elapsed;
        }

#ifdef ENABLE_DINOMO_KVS
        if (pollitems[7].revents & ZMQ_POLLIN)
        {
            auto work_start = std::chrono::system_clock::now();

            string serialized = kZmqUtil->recv_string(&failover_responder);
            failover_handler(serialized, serializers, log);
            kZmqUtil->send_string("", &failover_responder);

            auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::system_clock::now() - work_start)
                                    .count();
            working_time += time_elapsed;
            working_time_map[7] += time_elapsed;
        }
#else
        // Receive cache IP lookup response.
        if (pollitems[7].revents & ZMQ_POLLIN)
        {
            auto work_start = std::chrono::system_clock::now();

            string serialized = kZmqUtil->recv_string(&cache_ip_response_puller);
            cache_ip_response_handler(serialized, cache_ip_to_keys, key_to_cache_ips);

            auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::system_clock::now() - work_start)
                                    .count();
            working_time += time_elapsed;
            working_time_map[7] += time_elapsed;
        }
#endif

        // Receive management node response.
        if (pollitems[8].revents & ZMQ_POLLIN)
        {
            auto work_start = std::chrono::system_clock::now();

            string serialized =
                kZmqUtil->recv_string(&management_node_response_puller);
            management_node_response_handler(
                serialized, extant_caches, cache_ip_to_keys, key_to_cache_ips,
                global_hash_rings, local_hash_rings, pushers, wt, rid);

            auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::system_clock::now() - work_start)
                                    .count();
            working_time += time_elapsed;
            working_time_map[8] += time_elapsed;
        }

#ifndef ENABLE_DINOMO_KVS
        // gossip updates to other threads
        gossip_end = std::chrono::system_clock::now();
        if (std::chrono::duration_cast<std::chrono::microseconds>(gossip_end -
                    gossip_start).count() >= PERIOD)
        {
            auto work_start = std::chrono::system_clock::now();
            // only gossip if we have changes
            // Sekwon: for DINOMO, this context should be skipped
            if (local_changeset.size() > 0)
            {
                AddressKeysetMap addr_keyset_map;

                bool succeed;
                for (const Key &key : local_changeset)
                {
                    // Get the threads that we need to gossip to.
                    ServerThreadList threads = kHashRingUtil->get_responsible_threads(
                        wt.replication_response_connect_address(), key, is_metadata(key),
                        global_hash_rings, local_hash_rings, key_replication_map, pushers,
                        kAllTiers, succeed, seed);

                    if (succeed)
                    {
                        for (const ServerThread &thread : threads)
                        {
                            if (!(thread == wt))
                            {
                                addr_keyset_map[thread.gossip_connect_address()].insert(key);
                            }
                        }
                    }
                    else
                    {
                        log->error("Missing key replication factor in gossip routine.");
                    }

                    // Get the caches that we need to gossip to.
                    if (key_to_cache_ips.find(key) != key_to_cache_ips.end())
                    {
                        set<Address> &cache_ips = key_to_cache_ips[key];
                        for (const Address &cache_ip : cache_ips)
                        {
                            CacheThread ct(cache_ip, 0);
                            addr_keyset_map[ct.cache_update_connect_address()].insert(key);
                        }
                    }
                }

                send_gossip(addr_keyset_map, pushers, serializers, stored_key_map);
                local_changeset.clear();
            }

            gossip_start = std::chrono::system_clock::now();
            auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::system_clock::now() - work_start)
                                    .count();

            working_time += time_elapsed;
            working_time_map[9] += time_elapsed;
        }
#else
        batching_end = std::chrono::system_clock::now();
        if (kEnableBatching && std::chrono::duration_cast<std::chrono::microseconds>(batching_end
                    - batching_start).count() >= kBatchingThreshold)
        {
            //unsigned num_batching_reqs = 0;
            auto work_start = std::chrono::system_clock::now();
            process_flush(serializers[LatticeType::LWW]);
            response_batching_requests(pushers, log, batching_requests, stored_key_map);

            batching_start = std::chrono::system_clock::now();
            auto time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::system_clock::now() - work_start)
                                    .count();

            working_time += time_elapsed;
            working_time_map[9] += time_elapsed;
        }
#endif

        if (working_time != snapshot)
            num_working++;
        else
            num_idle++;

        // Collect and store internal statistics,
        // fetch the most recent list of cache IPs,
        // and send out GET requests for the cached keys by cache IP.
        report_end = std::chrono::system_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::seconds>(report_end - report_start).count();
        if (duration >= kServerReportThreshold)
        {
            if (kEnablePerfMonitor) {
                epoch += 1;
                auto ts = generate_timestamp(wt.tid());

                Key key =
                    get_metadata_key(wt, kSelfTier, wt.tid(), MetadataType::server_stats);

                // compute total storage consumption
#ifdef ENABLE_DINOMO_KVS
                // Sekwon
                // TODO: Currently, we have no implementations to address the lack of storage consumption.
                //       We need a way to address this number to address the lack of disaggregated PM rather than memory consumption of compute nodes.
                kvs_->update_cache_miss_cost();
#endif
                unsigned long long consumption = 0;
#ifndef ENABLE_DINOMO_KVS
                for (const auto &key_pair : stored_key_map)
                {
                    consumption += key_pair.second.size_;
                }
#endif

                int index = 0;
                for (const unsigned long long &time : working_time_map)
                {
                    // cast to microsecond
                    double event_occupancy = (double)time / ((double)duration * 1000000);

                    if (event_occupancy > 0.02)
                    {
                        log->info("Event {} occupancy is {}.", std::to_string(index),
                                std::to_string(event_occupancy));
                    }
                    index++;
                }

                double occupancy = (double)working_time / ((double)duration * 1000000);
                if (occupancy > 0.02)
                {
                    log->info("Occupancy is {}.", std::to_string(occupancy));
                }

                ServerThreadStatistics stat;
#ifdef ENABLE_DINOMO_KVS
                stat.set_storage_consumption(kvs_->cache_miss_cost());
#else
                stat.set_storage_consumption(consumption / 1000); // cast to KB
#endif

#ifdef NODE_SATURATION_CHECK
                stat.set_occupancy((double)((double)num_working / (double)(num_idle + num_working)));
#else
                stat.set_occupancy(occupancy);
#endif
                stat.set_epoch(epoch);
                stat.set_access_count(access_count);
                if (kSelfTier == Tier::MEMORY) {
                    stat.set_value_cache_size(kvs_->value_cache_size());
                    stat.set_value_cache_hit_count(kvs_->value_cache_hit_counter());
                    stat.set_shortcut_cache_hit_count(kvs_->shortcut_cache_hit_counter());
                    stat.set_local_log_hit_count(kvs_->local_log_hit_counter());
                    stat.set_cache_miss_count(kvs_->cache_miss_counter());
                    stat.set_kvs_avg_latency((double)((double)working_time_map[3] / (double)access_count));
                    //stat.set_num_working(num_working_user_req);
                    //stat.set_num_idle(num_idle_user_req);
                }

                string serialized_stat;
                stat.SerializeToString(&serialized_stat);

                KeyRequest req;
                req.set_type(RequestType::PUT);
                prepare_put_tuple(req, key, LatticeType::LWW,
                        serialize(ts, serialized_stat));

#ifdef ENABLE_DINOMO_KVS
#if 0
                Address target_address = wt.key_request_connect_address();
                string serialized;
                req.SerializeToString(&serialized);
                kZmqUtil->send_string(serialized, &pushers[target_address]);
#else
                string serialized;
                req.SerializeToString(&serialized);
                for (const string &address : monitoring_ips)
                {
                    kZmqUtil->send_string(serialized, 
                            &pushers[MonitoringThread(address).internal_stat_report_connect_address()]);
                }
#endif
#else
                auto threads = kHashRingUtil->get_responsible_threads_metadata(
                        key, global_hash_rings[Tier::MEMORY], local_hash_rings[Tier::MEMORY]);
                if (threads.size() != 0)
                {
                    Address target_address = std::next(begin(threads), 
                            rand_r(&seed) % threads.size())->key_request_connect_address();
                    string serialized;
                    req.SerializeToString(&serialized);
                    kZmqUtil->send_string(serialized, &pushers[target_address]);
                }
#endif

                if (kEnableSelectiveRep) {
                    // compute key access stats
                    KeyAccessData access;
                    auto current_time = std::chrono::system_clock::now();

                    for (const auto &key_access_pair : key_access_tracker)
                    {
                        Key key = key_access_pair.first;
                        auto access_times = key_access_pair.second;

                        // garbage collect
                        for (const auto &time : access_times)
                        {
                            if (std::chrono::duration_cast<std::chrono::seconds>(current_time - 
                                        time).count() >= kKeyMonitoringThreshold)
                            {
                                access_times.erase(time);
#ifdef ENABLE_DINOMO_KVS
                                break;
#else
                                break;
#endif
                            }
                        }

                        // update key_access_frequency
                        KeyAccessData_KeyCount *tp = access.add_keys();
                        tp->set_key(key);
                        tp->set_access_count(access_times.size());
                    }

#ifdef ENABLE_DINOMO_KVS
                    key_access_tracker.clear(); //sekwon
#endif

                    // report key access stats
                    key = get_metadata_key(wt, kSelfTier, wt.tid(), MetadataType::key_access);
                    string serialized_access;
                    access.SerializeToString(&serialized_access);

                    req.Clear();
                    req.set_type(RequestType::PUT);
                    prepare_put_tuple(req, key, LatticeType::LWW,
                            serialize(ts, serialized_access));

#ifdef ENABLE_DINOMO_KVS
#if 0
                    target_address = wt.key_request_connect_address();
                    req.SerializeToString(&serialized);
                    kZmqUtil->send_string(serialized, &pushers[target_address]);
#else
                    req.SerializeToString(&serialized);
                    for (const string &address : monitoring_ips)
                    {
                        kZmqUtil->send_string(serialized, 
                                &pushers[MonitoringThread(address).internal_stat_report_connect_address()]);
                    }
#endif
#else
                    threads = kHashRingUtil->get_responsible_threads_metadata(
                            key, global_hash_rings[Tier::MEMORY], local_hash_rings[Tier::MEMORY]);
                    if (threads.size() != 0)
                    {
                        Address target_address = std::next(begin(threads), 
                                rand_r(&seed) % threads.size())->key_request_connect_address();
                        string serialized;
                        req.SerializeToString(&serialized);
                        kZmqUtil->send_string(serialized, &pushers[target_address]);
                    }
#endif
                } else {
                    key_access_tracker.clear();
                }

#ifndef ENABLE_DINOMO_KVS
                KeySizeData primary_key_size;
                for (const auto &key_pair : stored_key_map)
                {
                    if (is_primary_replica(key_pair.first, key_replication_map,
                                global_hash_rings, local_hash_rings, wt))
                    {
                        KeySizeData_KeySize *ks = primary_key_size.add_key_sizes();
                        ks->set_key(key_pair.first);
                        ks->set_size(key_pair.second.size_);
                    }
                }

                key = get_metadata_key(wt, kSelfTier, wt.tid(), MetadataType::key_size);

                string serialized_size;
                primary_key_size.SerializeToString(&serialized_size);

                req.Clear();
                req.set_type(RequestType::PUT);
                prepare_put_tuple(req, key, LatticeType::LWW,
                        serialize(ts, serialized_size));

                threads = kHashRingUtil->get_responsible_threads_metadata(
                        key, global_hash_rings[Tier::MEMORY], local_hash_rings[Tier::MEMORY]);
                if (threads.size() != 0)
                {
                    Address target_address =
                        std::next(begin(threads), 
                                rand_r(&seed) % threads.size())->key_request_connect_address();
                    string serialized;
                    req.SerializeToString(&serialized);
                    kZmqUtil->send_string(serialized, &pushers[target_address]);
                }
#endif

                report_start = std::chrono::system_clock::now();

#ifndef ENABLE_DINOMO_KVS
                // Get the most recent list of cache IPs.
                // (Actually gets the list of all current function executor nodes.)
                // (The message content doesn't matter here; it's an argless RPC call.)
                // Only do this if a management_ip is set -- i.e., we are not running in
                // local mode.
                if (management_ip != "NULL")
                {
                    kZmqUtil->send_string(wt.management_node_response_connect_address(),
                            &pushers[get_func_nodes_req_address(management_ip)]);
                }
#endif

                // reset stats tracked in memory
                working_time = 0;
                access_count = 0;
                memset(working_time_map, 0, sizeof(working_time_map));
                num_working = 0;
                num_idle = 0;
            } else {
                kvs_->update_cache_miss_cost();
                key_access_tracker.clear();

                report_start = std::chrono::system_clock::now();

                // reset stats tracked in memory
                working_time = 0;
                access_count = 0;
                memset(working_time_map, 0, sizeof(working_time_map));
                num_working = 0;
                num_idle = 0;
            }
        }

#ifdef SHARED_NOTHING
        // redistribute data after node joins
        // Sekwon: This context is only enabled when simulating shared-nothing
        // architecture while unnecessary for original DINOMO

        if (join_gossip_map.size() != 0)
        {
            send_gossip(join_gossip_map, pushers, serializers, stored_key_map);

            for (const string &key : join_remove_set)
            {
                serializers[LatticeType::LWW]->remove(key);
                stored_key_map.erase(key);
            }

            if (kEnableBatching) {
                process_flush(serializers[LatticeType::LWW]);
                response_batching_requests(pushers, log, batching_requests, stored_key_map);
            }

            join_remove_set.clear();
            join_gossip_map.clear();
        }

        //if (join_gossip_map.size() != 0)
        //{
        //    set<Address> remove_address_set;
        //    AddressKeysetMap addr_keyset_map;

        //    for (const auto &join_pair : join_gossip_map)
        //    {
        //        Address address = join_pair.first;
        //        set<Key> key_set = join_pair.second;
        //        // track all sent keys because we cannot modify the key_set while
        //        // iterating over it
        //        set<Key> sent_keys;

        //        for (const Key &key : key_set)
        //        {
        //            addr_keyset_map[address].insert(key);
        //            sent_keys.insert(key);
        //            // Sekwon: This threshold should be disabled to make the
        //            // simulated shared-nothing provide the consistency guarantee
        //            // close to strong consistency, although it is still eventually
        //            // consistent as long as using asynchronous messaging for gossiping.
        //            // However, we didn't change it further since it is just for
        //            // simulation purpose.
        //            //if (sent_keys.size() >= DATA_REDISTRIBUTE_THRESHOLD)
        //            //{
        //            //    break;
        //            //}
        //        }

        //        // remove the keys we just dealt with
        //        for (const Key &key : sent_keys)
        //        {
        //            key_set.erase(key);
        //        }

        //        if (key_set.size() == 0)
        //        {
        //            remove_address_set.insert(address);
        //        }
        //    }

        //    for (const Address &remove_address : remove_address_set)
        //    {
        //        join_gossip_map.erase(remove_address);
        //    }

        //    send_gossip(addr_keyset_map, pushers, serializers, stored_key_map);

        //    // remove keys
        //    if (join_gossip_map.size() == 0)
        //    {
        //        for (const string &key : join_remove_set)
        //        {
        //            serializers[stored_key_map[key].type_]->remove(key);
        //            stored_key_map.erase(key);
        //        }

        //        if (kEnableBatching) {
        //            process_flush(serializers[LatticeType::LWW]);
        //            response_batching_requests(pushers, log, batching_requests);
        //        }

        //        join_remove_set.clear();
        //    }
        //}
#endif
    }
}

int main(int argc, char *argv[])
{
    if (argc != 1)
    {
        std::cerr << "Usage: " << argv[0] << std::endl;
        return 1;
    }

    // populate metadata
    char *stype = getenv("SERVER_TYPE");
    if (stype != NULL)
    {
        if (strncmp(stype, "memory", 6) == 0)
        {
            kSelfTier = Tier::MEMORY;
        }
        else if (strncmp(stype, "storage", 3) == 0)
        {
            kSelfTier = Tier::STORAGE;
        }
        else
        {
            std::cout << "Unrecognized server type " << stype
                      << ". Valid types are memory or storage." << std::endl;
            return 1;
        }
    }
    else
    {
        std::cout << "No server type specified. The default behavior is to start the server in memory mode." << std::endl;
        kSelfTier = Tier::MEMORY;
    }

    kSelfTierIdVector = {kSelfTier};

    // read the YAML conf
    YAML::Node conf = YAML::LoadFile("conf/dinomo-config.yml");
    YAML::Node threads = conf["threads"];
    kMemoryThreadCount = threads["memory"].as<unsigned>();
    kStorageThreadCount = threads["storage"].as<unsigned>();

    YAML::Node capacities = conf["capacities"];
    kMemoryNodeCapacity = capacities["memory-cap"].as<unsigned>() * 1024 * 1024UL;
    kStorageNodeCapacity = capacities["storage-cap"].as<unsigned>() * 1024 * 1024UL;

    YAML::Node kvsConfig = conf["kvs_config"];
    VALUE_SIZE = kvsConfig["value-size"].as<uint64_t>();
    SHORTCUT_SIZE = kvsConfig["shortcut-size"].as<uint64_t>();
    uint64_t log_block_size = kvsConfig["log-block-size"].as<uint64_t>();
    MAX_LOG_BLOCK_LEN = (log_block_size * 1024 * 1024) - LOG_BLOCK_HEADER_LEN;
    kLogCacheLen = kvsConfig["log-cache-len"].as<unsigned>();
    MAX_PREALLOC_NUM = kvsConfig["log-block-prealloc-num"].as<uint64_t>();

    YAML::Node replication = conf["replication"];
    kDefaultGlobalMemoryReplication = replication["memory"].as<unsigned>();
    kDefaultGlobalStorageReplication = replication["storage"].as<unsigned>();
    kDefaultLocalReplication = replication["local"].as<unsigned>();

    YAML::Node policy = conf["policy"];
    kEnableElasticity = policy["elasticity"].as<bool>();
    kEnableSelectiveRep = policy["selective-rep"].as<bool>();
    kEnableBatching = policy["batching"].as<bool>();
    kBatchingThreshold = policy["batchingthreshold"].as<unsigned>();
    kEnableCaching = policy["caching"].as<bool>();
    kEnableAdaptiveCache = policy["adaptive-caching"].as<bool>();
    kWeightOrValueRatio = policy["weight-or-vratio"].as<double>();
    kEnablePerfMonitor = policy["perf-monitoring"].as<bool>();
    kServerReportThreshold = policy["server-report-period"].as<unsigned>();
    kKeyMonitoringThreshold = policy["server-key-monitor-period"].as<unsigned>();

#ifdef ENABLE_DINOMO_KVS
    YAML::Node net_config = conf["net_config"];
    kReqPullerHWM = net_config["request_puller_hwm"].as<int>();
    kReqPullerBacklog = net_config["request_puller_backlog"].as<int>();

    YAML::Node ib_config = conf["ib_config"];
    int total_available_memory_nodes = ib_config["total_available_memory_nodes"].as<int>();
    int num_initial_memory_nodes = ib_config["num_initial_memory_nodes"].as<int>();
    int num_initial_storage_nodes = ib_config["num_initial_storage_nodes"].as<int>();
    int threads_per_memory = ib_config["threads_per_memory"].as<int>();
    int threads_per_storage = ib_config["threads_per_storage"].as<int>();
    int num_storage_managers = ib_config["num_storage_managers"].as<int>();
    int msg_size = ib_config["msg_size"].as<int>();

    char *storage_node_ips[num_initial_storage_nodes];
    for (int i = 0; i < num_initial_storage_nodes; i++)
        storage_node_ips[i] = (char *) calloc(128, sizeof(char));
    YAML::Node storageIPs = ib_config["storage_node_ips"];

    int nodeCounter = 0;
    for (const YAML::Node &address : storageIPs)
    {
        strcpy(storage_node_ips[nodeCounter], (address.as<Address>()).c_str());
        nodeCounter++;
    }

    char *clover_memc_ips[1];
    clover_memc_ips[0] = (char *) calloc(128, sizeof(char));
    YAML::Node cloverMemcIPs = ib_config["clover_memc_ips"];

    nodeCounter = 0;
    for (const YAML::Node &address : cloverMemcIPs)
    {
        strcpy(clover_memc_ips[nodeCounter], (address.as<Address>()).c_str());
        nodeCounter++;
    }

    char *sock_port = (char *) calloc(128, sizeof(char));
    strcpy(sock_port, (ib_config["sock_port"].as<string>()).c_str());

    int rank = ib_config["rank"].as<int>();
    bool is_server = false;
#endif

    YAML::Node server = conf["server"];
    Address public_ip = server["public_ip"].as<string>();
    Address private_ip = server["private_ip"].as<string>();

    vector<Address> routing_ips;
    vector<Address> monitoring_ips;

    Address seed_ip = server["seed_ip"].as<string>();
    Address mgmt_ip = server["mgmt_ip"].as<string>();
    YAML::Node monitoring = server["monitoring"];
    YAML::Node routing = server["routing"];

    for (const YAML::Node &address : routing)
    {
        routing_ips.push_back(address.as<Address>());
    }

    for (const YAML::Node &address : monitoring)
    {
        monitoring_ips.push_back(address.as<Address>());
    }

    kTierMetadata[Tier::MEMORY] = TierMetadata(Tier::MEMORY, kMemoryThreadCount, kDefaultGlobalMemoryReplication, kMemoryNodeCapacity);
    kTierMetadata[Tier::STORAGE] = TierMetadata(Tier::STORAGE, kStorageThreadCount, kDefaultGlobalStorageReplication, kStorageNodeCapacity);

    kThreadNum = kTierMetadata[kSelfTier].thread_number_;

#ifdef ENABLE_DINOMO_KVS
#ifndef ENABLE_CLOVER_KVS
    ib_init(total_available_memory_nodes, num_initial_memory_nodes, 
            num_initial_storage_nodes, threads_per_memory, threads_per_storage,
            num_storage_managers, msg_size, storage_node_ips, sock_port, rank, is_server,
            (uint64_t)(kStorageNodeCapacity));
    //ib_sync_cross(0);
#else
    clover_init(num_initial_memory_nodes, rank, clover_memc_ips);
#endif

    for (int i = 0; i < num_initial_storage_nodes; i++)
        free(storage_node_ips[i]);
    free(clover_memc_ips[0]);
    free(sock_port);
#endif

    // start the initial threads based on kThreadNum
    vector<std::thread> worker_threads;
#ifndef ENABLE_DINOMO_KVS
    for (unsigned thread_id = 1; thread_id < kThreadNum; thread_id++) {
        worker_threads.push_back(std::thread(run<MemoryLWWKVS>, thread_id, public_ip, private_ip,
                    seed_ip, routing_ips, monitoring_ips, mgmt_ip));
    }
    run<MemoryLWWKVS>(0, public_ip, private_ip, seed_ip, routing_ips, monitoring_ips, mgmt_ip);
#else
#ifndef ENABLE_CLOVER_KVS
    if (kEnableCaching) {
        if (kEnableAdaptiveCache) {
            for (unsigned thread_id = 1; thread_id < kThreadNum; thread_id++) {
                worker_threads.push_back(std::thread(run<DinomoAHCKVS>, thread_id, public_ip, private_ip,
                            seed_ip, routing_ips, monitoring_ips, mgmt_ip));
            }
            run<DinomoAHCKVS>(0, public_ip, private_ip, seed_ip, routing_ips, monitoring_ips, mgmt_ip);
        } else {
            for (unsigned thread_id = 1; thread_id < kThreadNum; thread_id++) {
                worker_threads.push_back(std::thread(run<DinomoHCKVS>, thread_id, public_ip, private_ip,
                            seed_ip, routing_ips, monitoring_ips, mgmt_ip));
            }
            run<DinomoHCKVS>(0, public_ip, private_ip, seed_ip, routing_ips, monitoring_ips, mgmt_ip);
        }
    } else {
        for (unsigned thread_id = 1; thread_id < kThreadNum; thread_id++) {
            worker_threads.push_back(std::thread(run<DinomoECKVS>, thread_id, public_ip, private_ip,
                        seed_ip, routing_ips, monitoring_ips, mgmt_ip));
        }
        run<DinomoECKVS>(0, public_ip, private_ip, seed_ip, routing_ips, monitoring_ips, mgmt_ip);
    }
#else
    for (unsigned thread_id = 1; thread_id < kThreadNum; thread_id++) {
        worker_threads.push_back(std::thread(run<CloverKVS>, thread_id, public_ip, private_ip,
                    seed_ip, routing_ips, monitoring_ips, mgmt_ip));
    }
    run<CloverKVS>(0, public_ip, private_ip, seed_ip, routing_ips, monitoring_ips, mgmt_ip);
#endif
#endif

    // join on all threads to make sure they finish before exiting
    for (unsigned tid = 1; tid < kThreadNum; tid++)
    {
        worker_threads[tid].join();
    }

#ifdef ENABLE_DINOMO_KVS
#ifndef ENABLE_CLOVER_KVS
    ib_finalize();
#endif
#endif

    return 0;
}
