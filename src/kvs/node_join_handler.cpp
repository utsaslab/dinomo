#include "kvs/kvs_handlers.hpp"

void node_join_handler(unsigned thread_id, unsigned &seed, Address public_ip,
                       Address private_ip, logger log, string &serialized,
                       GlobalRingMap &global_hash_rings,
                       LocalRingMap &local_hash_rings,
#ifdef SHARED_NOTHING
                       set<Key> &stored_key_map,
#else
                       map<Key, KeyProperty> &stored_key_map,
#endif
                       map<Key, KeyReplication> &key_replication_map,
                       map<Key, vector<PendingRequest> > &batching_requests,
                       set<Key> &join_remove_set, SocketCache &pushers,
                       ServerThread &wt, AddressKeysetMap &join_gossip_map,
                       int self_join_count, SerializerMap &serializers,
                       zmq::socket_t &response_puller,
                       vector<zmq::pollitem_t> &failover_pollitems)
{
#if 1
    NodeJoinRequest join_req;
    join_req.ParseFromString(serialized);

    NodeJoinRequestResponse response;
    response.set_response_address(wt.response_connect_address());
    response.set_response_id(join_req.request_id());
    response.set_request_id(wt.response_connect_address() + ":" + std::to_string(0));

    vector<string> v;
    split(join_req.request_msg(), ':', v);

    Tier tier;
    Tier_Parse(v[0], &tier);
    Address new_server_public_ip = v[1];
    Address new_server_private_ip = v[2];
    int join_count = stoi(v[3]);

    set<string> req_ids;
    vector<NodeJoinRequestResponse> join_responses;

    // update global hash ring
    bool inserted = global_hash_rings[tier].insert(new_server_public_ip, new_server_private_ip, join_count, 0);
    if (inserted)
    {
        log->info("Received a node join for tier {}. New node is {}. It's join counter is {}.",
                Tier_Name(tier), new_server_public_ip, join_count);

        // only thread 0 communicates with other nodes and receives join messages
        // and it communicates that information to non-0 threads on its own machine
        if (thread_id == 0)
        {
            NodeJoinRequest self_notifier;
            self_notifier.set_response_address("");
            self_notifier.set_request_id("");
            self_notifier.set_request_msg(Tier_Name(kSelfTier) + ":" + public_ip + ":" + private_ip + ":" + std::to_string(self_join_count));

            string serialized_self_notifier;
            self_notifier.SerializeToString(&serialized_self_notifier);

            // send my IP to the new server node
            kZmqUtil->send_string(serialized_self_notifier, &pushers[ServerThread(new_server_public_ip, new_server_private_ip, 0).node_join_connect_address()]);

            // tell all worker threads about the new node join
            if (join_req.response_address() != "") {
                for (unsigned tid = 1; tid < kThreadNum; tid++)
                {
                    bool succeed;
                    NodeJoinRequest req;
                    req.set_response_address(wt.response_connect_address());
                    req.set_request_id(wt.response_connect_address() + ":" + std::to_string(tid));
                    req.set_request_msg(join_req.request_msg());
#if 0
                    auto res = make_request<NodeJoinRequest, NodeJoinRequestResponse>(req,
                            pushers[ServerThread(public_ip, private_ip, tid).node_join_connect_address()],
                            response_puller, succeed);
                    if (!succeed)
                    {
                        log->error("Time out in {} while waiting the join response from non-0 thread!", std::to_string(thread_id));
                    }
                    else
                    {
                        join_responses.push_back(res);
                    }
#endif
                    send_request<NodeJoinRequest>(req, pushers[ServerThread(public_ip, private_ip, tid).node_join_connect_address()]);
                    req_ids.insert(req.request_id());
                }
            }
            else
            {
                for (unsigned tid = 1; tid < kThreadNum; tid++)
                {
                    kZmqUtil->send_string(serialized, 
                            &pushers[ServerThread(public_ip, private_ip, tid).node_join_connect_address()]);
                }
            }
        }
    }

    if (tier == kSelfTier && join_req.response_address() != "")
    {
        process_merge(serializers[LatticeType::LWW], true);
        response_batching_requests(pushers, log, batching_requests, stored_key_map);
    }

    if (join_req.response_address() != "")
    {
        bool succeed;
        if (thread_id == 0)
        {
            if (req_ids.size() != 0)
            {
                succeed = receive<NodeJoinRequestResponse>(response_puller, req_ids, join_responses);
                if (!succeed)
                {
                    log->error("Time out while waiting the join response from local non-0 threads");
                }
            }

            auto res = make_request<NodeJoinRequestResponse, NodeJoinRequestUnblock>(response,
                    pushers[join_req.response_address()], response_puller, succeed);
            if (!succeed)
            {
#if 0
                while (!succeed) {
                    kZmqUtil->poll(0, &failover_pollitems);
                    if (failover_pollitems[0].revents & ZMQ_POLLIN) {
                        break;
                    } else {
                        vector<NodeJoinRequestUnblock> responses;
                        set<string> req_ids{response.request_id()};
                        succeed = receive<NodeJoinRequestUnblock>(response_puller, req_ids, responses);
                    }
                }
#endif
                log->error("Time out occurs while waiting unblock ack from newly joined node.");
            }

            for (const NodeJoinRequestResponse &join_response : join_responses)
            {
                NodeJoinRequestUnblock ack;
                ack.set_response_id(join_response.request_id());
                send_request<NodeJoinRequestUnblock>(ack, pushers[join_response.response_address()]);
            }
        }
        else
        {
            auto res = make_request<NodeJoinRequestResponse, NodeJoinRequestUnblock>(response,
                    pushers[join_req.response_address()], response_puller, succeed);
            if (!succeed)
            {
                log->error("Time out in {} while waiting the unblock ack from local 0 thread!", std::to_string(thread_id));
            }
        }
    }

#ifdef SHARED_NOTHING
    if (inserted && tier == kSelfTier) {
        bool succeed;
        for (const auto &stored_key : stored_key_map) {
            Key key = stored_key;
            ServerThreadList threads = kHashRingUtil->get_responsible_threads(
                    wt.replication_response_connect_address(), key, is_metadata(key),
                    global_hash_rings, local_hash_rings, key_replication_map, pushers,
                    kSelfTierIdVector, succeed, seed);

            if (succeed) {
                bool rejoin_responsible = false;
                if (join_count > 0) {
                    for (const ServerThread &thread : threads) {
                        if (thread.private_ip().compare(new_server_private_ip) == 0) {
                            join_gossip_map[thread.gossip_connect_address()].insert(key);
                        }
                    }
                } else if ((join_count == 0 &&
                            std::find(threads.begin(), threads.end(), wt) ==
                            threads.end())) {
                    join_remove_set.insert(key);

                    for (const ServerThread &thread : threads) {
                        join_gossip_map[thread.gossip_connect_address()].insert(key);
                    }
                }
            } else {
                log->error("Missing key replication factor in node join "
                        "routine. This should never happen.");
            }
        }
    }
#endif
#else
    vector<string> v;
    split(serialized, ':', v);

    Tier tier;
    Tier_Parse(v[0], &tier);
    Address new_server_public_ip = v[1];
    Address new_server_private_ip = v[2];
    int join_count = stoi(v[3]);

    // update global hash ring
    bool inserted = global_hash_rings[tier].insert(new_server_public_ip, new_server_private_ip, join_count, 0);

    if (inserted)
    {
        log->info("Received a node join for tier {}. New node is {}. It's join counter is {}.",
            Tier_Name(tier), new_server_public_ip, join_count);

        // only thread 0 communicates with other nodes and receives join messages
        // and it communicates that information to non-0 threads on its own machine
        if (thread_id == 0)
        {
#ifndef ENABLE_DINOMO_KVS
            // send my IP to the new server node
            kZmqUtil->send_string(Tier_Name(kSelfTier) + ":" + public_ip + ":" + private_ip + ":" + std::to_string(self_join_count), 
                    &pushers[ServerThread(new_server_public_ip, new_server_private_ip, 0).node_join_connect_address()]);

            // gossip the new node address between server nodes to ensure consistency
            // Sekwon: The reason of gossiping the new node address between server nodes, without just
            // presuming the new node already sent the new address to other server nodes, may be
            // to make sure the new node address is reflected first to the other nodes' hash ring
            // before receiving the next rerouted requests based on TCP ordered message transmission.
            int index = 0;
            for (const auto &pair : global_hash_rings)
            {
                const GlobalHashRing hash_ring = pair.second;
                Tier tier = pair.first;

                for (const ServerThread &st : hash_ring.get_unique_servers())
                {
                    // if the node is not myself and not the newly joined node, send the
                    // ip of the newly joined node in case of a race condition
                    string server_ip = st.private_ip();
                    if (server_ip.compare(private_ip) != 0 &&
                        server_ip.compare(new_server_private_ip) != 0)
                    {
                        kZmqUtil->send_string(serialized, &pushers[st.node_join_connect_address()]);
                    }
                }

                log->info("Hash ring for tier {} is size {}.", Tier_Name(tier), hash_ring.size());
            }
#endif

            // tell all worker threads about the new node join
            for (unsigned tid = 1; tid < kThreadNum; tid++)
            {
                kZmqUtil->send_string(serialized, 
                &pushers[ServerThread(public_ip, private_ip, tid).node_join_connect_address()]);
            }
        }

        if (tier == kSelfTier)
        {
#ifdef ENABLE_DINOMO_KVS    // Sekwon
            process_merge(serializers[LatticeType::LWW], true);
#else
            bool succeed;

            for (const auto &key_pair : stored_key_map)
            {
                Key key = key_pair.first;
                ServerThreadList threads = kHashRingUtil->get_responsible_threads(
                    wt.replication_response_connect_address(), key, is_metadata(key),
                    global_hash_rings, local_hash_rings, key_replication_map, pushers,
                    kSelfTierIdVector, succeed, seed);

                if (succeed)
                {
                    // there are two situations in which we gossip data to the joining node:
                    // 1) if the node is a new node and I am no longer responsible for the key
                    // 2) if the node is rejoining the cluster, and it is responsible for the key
                    // NOTE: This is currently inefficient because every server will
                    // gossip the key currently -- we might be able to hack around the
                    // hash ring to do it more efficiently, but I'm leaving this here for now
                    bool rejoin_responsible = false;

                    ////////////////// Sekwon: These contexts need to be changed //////////////////
                    // In the case of DINOMO, instead of redistributing (gossiping) keys across new
                    // partitions, all we need to do after node join is to enforce log segments which
                    // contain the keys of prior partitions to be merged to the main remote index.
                    // After the merge has been done, all existing compute node should be blocked until 
                    // receiving join-gossip messages from other nodes for consistency.
                    // * For an optimization, we may restrict the nodes involved in the enforced merge to
                    // the node adjacent to the new joined node in the consistent hash ring.
                    if (join_count > 0)
                    {
                        for (const ServerThread &thread : threads)
                        {
                            if (thread.private_ip().compare(new_server_private_ip) == 0)
                            {
                                join_gossip_map[thread.gossip_connect_address()].insert(key);
                            }
                        }
                    }
                    else if ((join_count == 0 && std::find(threads.begin(), threads.end(), wt) == threads.end()))
                    {
                        join_remove_set.insert(key);

                        for (const ServerThread &thread : threads)
                        {
                            join_gossip_map[thread.gossip_connect_address()].insert(key);
                        }
                    }
                    //////////////////////////////////////////////////////////////////////////////
                }
                else
                {
                    log->error("Missing key replication factor in node join routine. This should never happen.");
                }
            }
#endif
        }
    }
#endif
}