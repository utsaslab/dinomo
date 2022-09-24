#include "kvs/kvs_handlers.hpp"

void user_request_handler(
    unsigned &access_count, unsigned &seed, string &serialized, logger log,
    GlobalRingMap &global_hash_rings, LocalRingMap &local_hash_rings,
    map<Key, vector<PendingRequest> > &pending_requests,
    map<Key, std::multiset<TimePoint> > &key_access_tracker,
#ifdef SHARED_NOTHING
    set<Key> &stored_key_map,
#else
    map<Key, KeyProperty> &stored_key_map,
#endif
    map<Key, KeyReplication> &key_replication_map, set<Key> &local_changeset,
    ServerThread &wt, SerializerMap &serializers, SocketCache &pushers,
    map<Key, vector<PendingRequest> > &batching_requests, bool batching)
{
    KeyRequest request;
    request.ParseFromString(serialized);

    KeyResponse response;
    string response_id = request.request_id();
    response.set_response_id(request.request_id());

    response.set_type(request.type());

    bool succeed;
    RequestType request_type = request.type();
    string response_address = request.response_address();

    for (const auto &tuple : request.tuples())
    {
        // first check if the thread is responsible for the key
        Key key = tuple.key();
        string payload = tuple.payload();

#ifdef ENABLE_DINOMO_KVS
        if (is_metadata(key))
        {
            KeyTuple *tp = response.add_tuples();
            tp->set_key(key);
            if (request_type == RequestType::GET || request_type == RequestType::READ)
            {
                std::pair<string, AnnaError> res;
                res = process_get_meta(key, serializers[LatticeType::LWW]);
                if (res.first == "")
                {
                    tp->set_error(AnnaError::KEY_DNE);
                }
                else
                {
                    tp->set_lattice_type(LatticeType::LWW);
                    tp->set_payload(res.first);
                    tp->set_error(res.second);
                }
            }
            else if (request_type == RequestType::PUT || request_type == RequestType::INSERT || request_type == RequestType::UPDATE)
            {
                process_put_meta(key, tuple.lattice_type(), payload, serializers[tuple.lattice_type()]);
                tp->set_lattice_type(tuple.lattice_type());
            }
            else
            {
                log->error("Unknown request type {} in user request handler.", request_type);
            }

            key_access_tracker[key].insert(std::chrono::system_clock::now());
            access_count += 1;
        }
        else
        {
#endif
#ifdef DISABLE_KVS
            KeyTuple *tp = response.add_tuples();
            tp->set_key(key);
            tp->set_lattice_type(tuple.lattice_type());
            tp->set_payload("");
            tp->set_error(AnnaError::NO_ERROR);
#else
#ifdef ENABLE_CLOVER_KVS
            // if we know the responsible threads, we process the request
            KeyTuple *tp = response.add_tuples();
            tp->set_key(key);
            if (request_type == RequestType::GET || request_type == RequestType::READ)
            {
                std::pair<string, AnnaError> res;
                res = process_get(key, serializers[LatticeType::LWW]);
                if (res.first == "")
                {
                    tp->set_error(AnnaError::KEY_DNE);
                }
                else
                {
                    tp->set_lattice_type(LatticeType::LWW);
                    tp->set_payload(res.first);
                    tp->set_error(res.second);
                }
            }
            else if (request_type == RequestType::PUT || request_type == RequestType::INSERT)
            {
                process_put(key, tuple.lattice_type(), payload, serializers[tuple.lattice_type()]);
                tp->set_lattice_type(tuple.lattice_type());
            }
            else if (request_type == RequestType::UPDATE)
            {
                process_update(key, tuple.lattice_type(), payload, serializers[tuple.lattice_type()]);
                tp->set_lattice_type(tuple.lattice_type());
            }
            else
            {
                log->error("Unknown request type {} in user request handler.", request_type);
            }

            key_access_tracker[key].insert(std::chrono::system_clock::now());
            access_count += 1;
#else
            ServerThreadList threads = kHashRingUtil->get_responsible_threads(
                    wt.replication_response_connect_address(), key, false,
                    global_hash_rings, local_hash_rings, key_replication_map, pushers,
                    kSelfTierIdVector, succeed, seed);

            if (succeed)
            {
#ifdef ENABLE_DINOMO_KVS // Sekwon
                if (std::find(threads.begin(), threads.end(), wt) == threads.end())
                {
#ifndef SHARED_NOTHING
                    // if we don't know what threads are responsible, we issue a rep
                    // factor request and make the request pending
                    //kHashRingUtil->issue_replication_factor_request(
                    //        wt.replication_response_connect_address(), key,
                    //        global_hash_rings[Tier::MEMORY], local_hash_rings[Tier::MEMORY],
                    //        pushers, seed);
                    kHashRingUtil->issue_replication_factor_local_request(
                            wt.replication_response_connect_address(), key,
                            pushers, wt.key_request_connect_address());

                    pending_requests[key].push_back(
                            PendingRequest(request_type, tuple.lattice_type(), payload,
                                response_address, response_id));
#else
                    // this means that this node is not responsible for this key
                    KeyTuple *tp = response.add_tuples();

                    tp->set_key(key);
                    tp->set_lattice_type(tuple.lattice_type());
                    tp->set_error(AnnaError::WRONG_THREAD);
#endif
                }
                else
                {
                    // if we know the responsible threads, we process the request
                    KeyTuple *tp = response.add_tuples();
                    tp->set_key(key);
                    if (request_type == RequestType::GET || request_type == RequestType::READ)
                    {
#ifndef SHARED_NOTHING
                        std::pair<string, AnnaError> res;
                        if (threads.size() > 1)
                        {
                            res = process_get_replicated(key, serializers[LatticeType::LWW]);
                        }
                        else
                        {
                            res = process_get(key, serializers[LatticeType::LWW]);
                        }

                        if (res.first == "")
                        {
                            tp->set_error(AnnaError::KEY_DNE);
                        }
                        else
                        {
                            tp->set_lattice_type(LatticeType::LWW);
                            tp->set_payload(res.first);
                            tp->set_error(res.second);
                        }
#else
                        if (stored_key_map.find(key) == stored_key_map.end()) 
                        {
                            tp->set_error(AnnaError::KEY_DNE);
                        }
                        else
                        {
                            auto res = process_get(key, serializers[LatticeType::LWW]);
                            tp->set_lattice_type(LatticeType::LWW);
                            tp->set_payload(res.first);
                            tp->set_error(res.second);
                        }
#endif
                    }
                    else if (request_type == RequestType::PUT || request_type == RequestType::INSERT)
                    {
#ifndef SHARED_NOTHING
                        if (threads.size() > 1)
                        {
                            process_put_replicated(key, tuple.lattice_type(), payload, serializers[tuple.lattice_type()]);
                        }
                        else
                        {
                            unsigned ret = process_put(key, tuple.lattice_type(), payload, serializers[tuple.lattice_type()]);
                            if (batching) {
                                key_access_tracker[key].insert(std::chrono::system_clock::now());
                                access_count += 1;
                                if (ret) response_batching_requests(pushers, log, batching_requests, stored_key_map);
                                return batching_requests[key].push_back(PendingRequest(request_type,
                                            tuple.lattice_type(), "", response_address, response_id));
                            }
                        }
#else
                        unsigned ret = process_put(key, tuple.lattice_type(), payload, serializers[tuple.lattice_type()]);
                        if (batching) {
                            key_access_tracker[key].insert(std::chrono::system_clock::now());
                            access_count += 1;
                            if (ret) response_batching_requests(pushers, log, batching_requests, stored_key_map);
                            return batching_requests[key].push_back(PendingRequest(request_type,
                                        tuple.lattice_type(), "", response_address, response_id));
                        }
                        stored_key_map.insert(key);
#endif
                        tp->set_lattice_type(tuple.lattice_type());
                    }
                    else if (request_type == RequestType::UPDATE)
                    {
#ifndef SHARED_NOTHING
                        if (threads.size() > 1)
                        {
                            process_update_replicated(key, tuple.lattice_type(), payload, serializers[tuple.lattice_type()]);
                        }
                        else
                        {
                            unsigned ret = process_update(key, tuple.lattice_type(), payload, serializers[tuple.lattice_type()]);
                            if (batching) {
                                key_access_tracker[key].insert(std::chrono::system_clock::now());
                                access_count += 1;
                                if (ret) response_batching_requests(pushers, log, batching_requests, stored_key_map);
                                return batching_requests[key].push_back(PendingRequest(request_type,
                                            tuple.lattice_type(), "", response_address, response_id));
                            }
                        }
                        tp->set_lattice_type(tuple.lattice_type());
#else
                        if (stored_key_map.find(key) == stored_key_map.end()) 
                        {
                            tp->set_error(AnnaError::KEY_DNE);
                        } else {
                            unsigned ret = process_update(key, tuple.lattice_type(), payload, serializers[tuple.lattice_type()]);
                            if (batching) {
                                key_access_tracker[key].insert(std::chrono::system_clock::now());
                                access_count += 1;
                                if (ret) response_batching_requests(pushers, log, batching_requests, stored_key_map);
                                return batching_requests[key].push_back(PendingRequest(request_type,
                                            tuple.lattice_type(), "", response_address, response_id));
                            }
                            tp->set_lattice_type(tuple.lattice_type());
                        }
#endif
                    }
                    else
                    {
                        log->error("Unknown request type {} in user request handler.", request_type);
                    }

                    if (tuple.address_cache_size() > 0 &&
                            tuple.address_cache_size() != threads.size())
                    {
                        tp->set_invalidate(true);
                        for (const ServerThread &thread : threads)
                        {
                            tp->add_ips(thread.key_request_connect_address());
                        }
                    }

                    key_access_tracker[key].insert(std::chrono::system_clock::now());
                    access_count += 1;
                }
#else
                if (std::find(threads.begin(), threads.end(), wt) == threads.end())
                {
                    if (is_metadata(key))
                    {
                        // this means that this node is not responsible for this metadata key
                        KeyTuple *tp = response.add_tuples();

                        tp->set_key(key);
                        tp->set_lattice_type(tuple.lattice_type());
                        tp->set_error(AnnaError::WRONG_THREAD);
                    }
                    else
                    {
                        // if we don't know what threads are responsible, we issue a rep
                        // factor request and make the request pending
                        kHashRingUtil->issue_replication_factor_request(
                                wt.replication_response_connect_address(), key,
                                global_hash_rings[Tier::MEMORY], local_hash_rings[Tier::MEMORY],
                                pushers, seed);

                        pending_requests[key].push_back(
                                PendingRequest(request_type, tuple.lattice_type(), payload,
                                    response_address, response_id));
                    }
                }
                else
                {
                    // if we know the responsible threads, we process the request
                    KeyTuple *tp = response.add_tuples();
                    tp->set_key(key);

                    if (request_type == RequestType::GET)
                    {
                        if (stored_key_map.find(key) == stored_key_map.end() ||
                                stored_key_map[key].type_ == LatticeType::NONE)
                        {

                            tp->set_error(AnnaError::KEY_DNE);
                        }
                        else
                        {
                            auto res = process_get(key, serializers[stored_key_map[key].type_]);
                            tp->set_lattice_type(stored_key_map[key].type_);
                            tp->set_payload(res.first);
                            tp->set_error(res.second);
                        }
                    }
                    else if (request_type == RequestType::PUT)
                    {
                        if (tuple.lattice_type() == LatticeType::NONE)
                        {
                            log->error("PUT request missing lattice type.");
                        }
                        else if (stored_key_map.find(key) != stored_key_map.end() &&
                                stored_key_map[key].type_ != LatticeType::NONE &&
                                stored_key_map[key].type_ != tuple.lattice_type())
                        {
                            log->error("Lattice type mismatch for key {}: query is {} but we expect {}.",
                                    key, LatticeType_Name(tuple.lattice_type()), LatticeType_Name(stored_key_map[key].type_));
                        }
                        else
                        {
                            process_put(key, tuple.lattice_type(), payload,
                                    serializers[tuple.lattice_type()], stored_key_map);
                            local_changeset.insert(key);
                            tp->set_lattice_type(tuple.lattice_type());
                        }
                    }
                    else
                    {
                        log->error("Unknown request type {} in user request handler.", request_type);
                    }

                    if (tuple.address_cache_size() > 0 &&
                            tuple.address_cache_size() != threads.size())
                    {
                        tp->set_invalidate(true);
                    }

                    key_access_tracker[key].insert(std::chrono::system_clock::now());
                    access_count += 1;
                }
#endif
            }
            else
            {
                pending_requests[key].push_back(
                        PendingRequest(request_type, tuple.lattice_type(), payload,
                            response_address, response_id));
            }
#endif
#endif
#ifdef ENABLE_DINOMO_KVS
        }
#endif
    }

    if (response.tuples_size() > 0 && request.response_address() != "")
    {
        string serialized_response;
        response.SerializeToString(&serialized_response);
        kZmqUtil->send_string(serialized_response, &pushers[request.response_address()]);
    }
}
