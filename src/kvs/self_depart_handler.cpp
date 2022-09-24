#include "kvs/kvs_handlers.hpp"

static void drain_user_requests(
    logger log, map<Key, vector<PendingRequest> > &pending_requests,
    map<Key, vector<PendingRequest> > &batching_requests,
    SocketCache &pushers, vector<zmq::pollitem_t> &drain_pollitems,
#ifdef SHARED_NOTHING
    zmq::socket_t &request_puller, set<Key> &stored_key_map)
#else
    zmq::socket_t &request_puller, map<Key, KeyProperty> &stored_key_map)
#endif
{
    // Drain user pending requests
    while (true) {
        kZmqUtil->poll(0, &drain_pollitems);
        if (drain_pollitems[0].revents & ZMQ_POLLIN) {
            string serialized = kZmqUtil->recv_string(&request_puller);

            KeyRequest request;
            request.ParseFromString(serialized);

            KeyResponse response;
            string response_id = request.request_id();
            response.set_response_id(request.request_id());

            response.set_type(request.type());

            RequestType request_type = request.type();
            string response_address = request.response_address();

            for (const auto &tuple : request.tuples())
            {
                Key key = tuple.key();

                KeyTuple *tp = response.add_tuples();
                tp->set_key(key);
                tp->set_lattice_type(tuple.lattice_type());
                tp->set_error(AnnaError::WRONG_THREAD);
            }

            if (response.tuples_size() > 0 && request.response_address() != "")
            {
                string serialized_response;
                response.SerializeToString(&serialized_response);
                kZmqUtil->send_string(serialized_response, &pushers[request.response_address()]);
            }
        } else {
            break;
        }
    }

    // Drain unresponsed batching requests
    response_batching_requests(pushers, log, batching_requests, stored_key_map);

    // Drain pending requests
    for (const auto &pending_pair : pending_requests)
    {
        for (const PendingRequest &request : pending_requests[pending_pair.first])
        {
            if (request.addr_ != "")
            {
                KeyResponse response;

                response.set_type(request.type_);

                if (request.response_id_ != "")
                {
                    response.set_response_id(request.response_id_);
                }

                KeyTuple *tp = response.add_tuples();
                tp->set_key(pending_pair.first);
                tp->set_error(AnnaError::WRONG_THREAD);

                string serialized_response;
                response.SerializeToString(&serialized_response);
                kZmqUtil->send_string(serialized_response, &pushers[request.addr_]);
            }
        }
    }
    pending_requests.clear();
}

void self_depart_handler(unsigned thread_id, unsigned &seed, Address public_ip,
                         Address private_ip, logger log, string &serialized,
                         GlobalRingMap &global_hash_rings,
                         LocalRingMap &local_hash_rings,
#ifdef SHARED_NOTHING
                         set<Key> &stored_key_map,
#else
                         map<Key, KeyProperty> &stored_key_map,
#endif
                         map<Key, KeyReplication> &key_replication_map,
                         vector<Address> &routing_ips,
                         vector<Address> &monitoring_ips, ServerThread &wt,
                         SocketCache &pushers, SerializerMap &serializers,
                         map<Key, vector<PendingRequest> > &pending_requests,
                         map<Key, vector<PendingRequest> > &batching_requests,
                         vector<zmq::pollitem_t> &drain_pollitems,
                         zmq::socket_t &request_puller)
{
    log->info("This node is departing.");
    global_hash_rings[kSelfTier].remove(public_ip, private_ip, 0);

#ifdef ENABLE_DINOMO_KVS
    process_merge(serializers[LatticeType::LWW], false);
#endif

    // thread 0 notifies other nodes in the cluster (of all types) that it is
    // leaving the cluster
    if (thread_id == 0)
    {
        string msg = Tier_Name(kSelfTier) + ":" + public_ip + ":" + private_ip;

        for (const auto &pair : global_hash_rings)
        {
            GlobalHashRing hash_ring = pair.second;

            for (const ServerThread &st : hash_ring.get_unique_servers())
            {
                kZmqUtil->send_string(msg, &pushers[st.node_depart_connect_address()]);
            }
        }

        msg = "depart:" + msg;

        // notify all routing nodes
        for (const string &address : routing_ips)
        {
            kZmqUtil->send_string(msg, &pushers[RoutingThread(address, 0).notify_connect_address()]);
        }

        // notify monitoring nodes
        for (const string &address : monitoring_ips)
        {
            kZmqUtil->send_string(msg, &pushers[MonitoringThread(address).notify_connect_address()]);
        }

        // tell all worker threads about the self departure
        for (unsigned tid = 1; tid < kThreadNum; tid++)
        {
            kZmqUtil->send_string(serialized, &pushers[ServerThread(public_ip, private_ip, tid).self_depart_connect_address()]);
        }
    }

#ifndef ENABLE_DINOMO_KVS
    AddressKeysetMap addr_keyset_map;
    bool succeed;

    /////////////////////// Sekwon: This context should be changed ////////////////////////////
    // For DINOMO, instead of gossiping (redistributing) stored key-value pairs to other nodes,
    // what we need to do before letting other nodes update local hash rings is to enforce the
    // pending log segments to be merged to the main index.
    for (const auto &key_pair : stored_key_map)
    {
        Key key = key_pair.first;
        ServerThreadList threads = kHashRingUtil->get_responsible_threads(
            wt.replication_response_connect_address(), key, is_metadata(key),
            global_hash_rings, local_hash_rings, key_replication_map, pushers,
            kAllTiers, succeed, seed);

        if (succeed)
        {
            // since we already removed this node from the hash ring, no need to
            // exclude it explicitly
            for (const ServerThread &thread : threads)
            {
                addr_keyset_map[thread.gossip_connect_address()].insert(key);
            }
        }
        else
        {
            log->error("Missing key replication factor in node depart routine");
        }
    }

    send_gossip(addr_keyset_map, pushers, serializers, stored_key_map);
#else
    drain_user_requests(log, pending_requests, batching_requests,
            pushers, drain_pollitems, request_puller, stored_key_map);

#ifdef SHARED_NOTHING
    AddressKeysetMap addr_keyset_map;
    bool succeed;

    /////////////////////// Sekwon: This context should be changed ////////////////////////////
    // For DINOMO, instead of gossiping (redistributing) stored key-value pairs to other nodes,
    // what we need to do before letting other nodes update local hash rings is to enforce the
    // pending log segments to be merged to the main index.
    for (const auto &stored_key : stored_key_map)
    {
        Key key = stored_key;
        ServerThreadList threads = kHashRingUtil->get_responsible_threads(
            wt.replication_response_connect_address(), key, is_metadata(key),
            global_hash_rings, local_hash_rings, key_replication_map, pushers,
            kAllTiers, succeed, seed);

        if (succeed)
        {
            // since we already removed this node from the hash ring, no need to
            // exclude it explicitly
            for (const ServerThread &thread : threads)
            {
                addr_keyset_map[thread.gossip_connect_address()].insert(key);
            }
        }
        else
        {
            log->error("Missing key replication factor in node depart routine");
        }
    }

    send_gossip(addr_keyset_map, pushers, serializers, stored_key_map);
#endif
#endif

    // Sekwon: This last send is the response to monitoring node to notify the departure has been done
    //kZmqUtil->send_string(public_ip + "_" + private_ip + "_" + Tier_Name(kSelfTier), &pushers[serialized]);
    kZmqUtil->send_string(public_ip + "_" + private_ip + "_" + std::to_string(kSelfTier), &pushers[serialized]);
}