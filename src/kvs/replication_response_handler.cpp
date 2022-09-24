#include "kvs/kvs_handlers.hpp"

void replication_response_handler(
    unsigned &seed, unsigned &access_count, logger log, string &serialized,
    GlobalRingMap &global_hash_rings, LocalRingMap &local_hash_rings,
    map<Key, vector<PendingRequest> > &pending_requests,
    map<Key, vector<PendingGossip> > &pending_gossip,
    map<Key, std::multiset<TimePoint> > &key_access_tracker,
#ifdef SHARED_NOTHING
    set<Key> &stored_key_map,
#else
    map<Key, KeyProperty> &stored_key_map,
#endif
    map<Key, KeyReplication> &key_replication_map, set<Key> &local_changeset,
    ServerThread &wt, SerializerMap &serializers, SocketCache &pushers,
    map<Key, vector<PendingRequest>> &batching_requests, bool batching)
{
    KeyResponse response;
    response.ParseFromString(serialized);

    // we assume tuple 0 because there should only be one tuple responding to a
    // replication factor request
    KeyTuple tuple = response.tuples(0);
    Key key = get_key_from_metadata(tuple.key());

    AnnaError error = tuple.error();

    if (error == AnnaError::NO_ERROR)
    {
        LWWValue lww_value;
        lww_value.ParseFromString(tuple.payload());
        ReplicationFactor rep_data;
        rep_data.ParseFromString(lww_value.value());

        for (const auto &global : rep_data.global())
        {
            key_replication_map[key].global_replication_[global.tier()] = global.value();
        }

        for (const auto &local : rep_data.local())
        {
            key_replication_map[key].local_replication_[local.tier()] = local.value();
        }
    }
    else if (error == AnnaError::KEY_DNE)
    {
        // KEY_DNE means that the receiving thread was responsible for the metadata
        // but didn't have any values stored -- we use the default rep factor
        init_replication(key_replication_map, key);
    }
    else if (error == AnnaError::WRONG_THREAD)
    {
        // this means that the node that received the rep factor request was not
        // responsible for that metadata
        auto respond_address = wt.replication_response_connect_address();
        kHashRingUtil->issue_replication_factor_request(
            respond_address, key, global_hash_rings[Tier::MEMORY],
            local_hash_rings[Tier::MEMORY], pushers, seed);
        return;
    }
    else
    {
        log->error("Unexpected error type {} in replication factor response.",
                   error);
        return;
    }

    bool succeed;
#ifdef ENABLE_DINOMO_KVS
    if (pending_requests.find(key) != pending_requests.end())
    {
        ServerThreadList threads = kHashRingUtil->get_responsible_threads(
            wt.replication_response_connect_address(), key, is_metadata(key),
            global_hash_rings, local_hash_rings, key_replication_map, pushers,
            kSelfTierIdVector, succeed, seed);

        if (succeed)
        {
            bool responsible = std::find(threads.begin(), threads.end(), wt) != threads.end();

            for (const PendingRequest &request : pending_requests[key])
            {
                auto now = std::chrono::system_clock::now();

                if (!responsible && request.addr_ != "")
                {
                    KeyResponse response;

                    response.set_type(request.type_);

                    if (request.response_id_ != "")
                    {
                        response.set_response_id(request.response_id_);
                    }

                    KeyTuple *tp = response.add_tuples();
                    tp->set_key(key);
                    tp->set_error(AnnaError::WRONG_THREAD);

                    string serialized_response;
                    response.SerializeToString(&serialized_response);
                    kZmqUtil->send_string(serialized_response, &pushers[request.addr_]);
                }
                else if (responsible && request.addr_ == "")
                {
                    // only put requests should fall into this category
                    if (request.type_ == RequestType::PUT || request.type_ == RequestType::INSERT)
                    {
                        if (threads.size() > 1)
                        {
                            process_put_replicated(key, request.lattice_type_, request.payload_, serializers[request.lattice_type_]);
                        }
                        else
                        {
                            unsigned ret = process_put(key, request.lattice_type_, request.payload_, serializers[request.lattice_type_]);
                            if (batching) {
                                key_access_tracker[key].insert(now);
                                access_count += 1;
                                pending_requests.erase(key);
                                if (ret) response_batching_requests(pushers, log, batching_requests, stored_key_map);
                                return batching_requests[key].push_back(PendingRequest(request.type_, request.lattice_type_, 
                                            "", request.addr_, request.response_id_));
                            }
                        }
                        key_access_tracker[key].insert(now);
                        access_count += 1;
                    }
                    else if (request.type_ == RequestType::UPDATE)
                    {
                        if (threads.size() > 1)
                        {
                            process_update_replicated(key, request.lattice_type_, request.payload_, serializers[request.lattice_type_]);
                        }
                        else
                        {
                            unsigned ret = process_update(key, request.lattice_type_, request.payload_, serializers[request.lattice_type_]);
                            if (batching) {
                                key_access_tracker[key].insert(now);
                                access_count += 1;
                                pending_requests.erase(key);
                                if (ret) response_batching_requests(pushers, log, batching_requests, stored_key_map);
                                return batching_requests[key].push_back(PendingRequest(request.type_, request.lattice_type_, 
                                            "", request.addr_, request.response_id_));
                            }
                        }
                        key_access_tracker[key].insert(now);
                        access_count += 1;
                    }
                    else
                    {
                        log->error("Received a GET request with no response address.");
                    }
                }
                else if (responsible && request.addr_ != "")
                {
                    KeyResponse response;

                    response.set_type(request.type_);

                    if (request.response_id_ != "")
                    {
                        response.set_response_id(request.response_id_);
                    }

                    KeyTuple *tp = response.add_tuples();
                    tp->set_key(key);

                    if (request.type_ == RequestType::GET || request.type_ == RequestType::READ)
                    {
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
                    }
                    else if (request.type_ == RequestType::PUT || request.type_ == RequestType::INSERT)
                    {
                        if (threads.size() > 1)
                        {
                            process_put_replicated(key, request.lattice_type_, request.payload_, serializers[request.lattice_type_]);
                        }
                        else
                        {
                            unsigned ret = process_put(key, request.lattice_type_, request.payload_, serializers[request.lattice_type_]);
                            if (batching) {
                                key_access_tracker[key].insert(now);
                                access_count += 1;
                                pending_requests.erase(key);
                                if (ret) response_batching_requests(pushers, log, batching_requests, stored_key_map);
                                return batching_requests[key].push_back(PendingRequest(request.type_, request.lattice_type_, 
                                            "", request.addr_, request.response_id_));
                            }
                        }
                        tp->set_lattice_type(request.lattice_type_);
                    }
                    else if (request.type_ == RequestType::UPDATE)
                    {
                        if (threads.size() > 1)
                        {
                            process_update_replicated(key, request.lattice_type_, request.payload_, serializers[request.lattice_type_]);
                        }
                        else
                        {
                            unsigned ret = process_update(key, request.lattice_type_, request.payload_, serializers[request.lattice_type_]);
                            if (batching) {
                                key_access_tracker[key].insert(now);
                                access_count += 1;
                                pending_requests.erase(key);
                                if (ret) response_batching_requests(pushers, log, batching_requests, stored_key_map);
                                return batching_requests[key].push_back(PendingRequest(request.type_, request.lattice_type_, 
                                            "", request.addr_, request.response_id_));
                            }
                        }
                        tp->set_lattice_type(request.lattice_type_);
                    }
                    else
                    {
                        log->error("Unknown request type {} in user request handler.", request.type_);
                    }
                    key_access_tracker[key].insert(now);
                    access_count += 1;

                    string serialized_response;
                    response.SerializeToString(&serialized_response);
                    kZmqUtil->send_string(serialized_response, &pushers[request.addr_]);
                }
            }
        }
        else
        {
            log->error(
                "Missing key replication factor in process pending request routine.");
        }

        pending_requests.erase(key);
    }
#else
    if (pending_requests.find(key) != pending_requests.end())
    {
        ServerThreadList threads = kHashRingUtil->get_responsible_threads(
            wt.replication_response_connect_address(), key, is_metadata(key),
            global_hash_rings, local_hash_rings, key_replication_map, pushers,
            kSelfTierIdVector, succeed, seed);

        if (succeed)
        {
            bool responsible = std::find(threads.begin(), threads.end(), wt) != threads.end();

            for (const PendingRequest &request : pending_requests[key])
            {
                auto now = std::chrono::system_clock::now();

                if (!responsible && request.addr_ != "")
                {
                    KeyResponse response;

                    response.set_type(request.type_);

                    if (request.response_id_ != "")
                    {
                        response.set_response_id(request.response_id_);
                    }

                    KeyTuple *tp = response.add_tuples();
                    tp->set_key(key);
                    tp->set_error(AnnaError::WRONG_THREAD);

                    string serialized_response;
                    response.SerializeToString(&serialized_response);
                    kZmqUtil->send_string(serialized_response, &pushers[request.addr_]);
                }
                else if (responsible && request.addr_ == "")
                {
                    // only put requests should fall into this category
                    if (request.type_ == RequestType::PUT)
                    {
                        if (request.lattice_type_ == LatticeType::NONE)
                        {
                            log->error("PUT request missing lattice type.");
                        }
                        else if (stored_key_map.find(key) != stored_key_map.end() &&
                                 stored_key_map[key].type_ != LatticeType::NONE &&
                                 stored_key_map[key].type_ != request.lattice_type_)
                        {

                            log->error(
                                "Lattice type mismatch for key {}: query is {} but we expect "
                                "{}.",
                                key, LatticeType_Name(request.lattice_type_),
                                LatticeType_Name(stored_key_map[key].type_));
                        }
                        else
                        {
                            process_put(key, request.lattice_type_, request.payload_,
                                        serializers[request.lattice_type_], stored_key_map);
                            key_access_tracker[key].insert(now);

                            access_count += 1;
                            local_changeset.insert(key);
                        }
                    }
                    else
                    {
                        log->error("Received a GET request with no response address.");
                    }
                }
                else if (responsible && request.addr_ != "")
                {
                    KeyResponse response;

                    response.set_type(request.type_);

                    if (request.response_id_ != "")
                    {
                        response.set_response_id(request.response_id_);
                    }

                    KeyTuple *tp = response.add_tuples();
                    tp->set_key(key);

                    if (request.type_ == RequestType::GET)
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
                    else
                    {
                        if (request.lattice_type_ == LatticeType::NONE)
                        {
                            log->error("PUT request missing lattice type.");
                        }
                        else if (stored_key_map.find(key) != stored_key_map.end() &&
                                 stored_key_map[key].type_ != LatticeType::NONE &&
                                 stored_key_map[key].type_ != request.lattice_type_)
                        {
                            log->error(
                                "Lattice type mismatch for key {}: {} from query but {} "
                                "expected.",
                                key, LatticeType_Name(request.lattice_type_),
                                LatticeType_Name(stored_key_map[key].type_));
                        }
                        else
                        {
                            process_put(key, request.lattice_type_, request.payload_,
                                        serializers[request.lattice_type_], stored_key_map);
                            tp->set_lattice_type(request.lattice_type_);
                            local_changeset.insert(key);
                        }
                    }
                    key_access_tracker[key].insert(now);
                    access_count += 1;

                    string serialized_response;
                    response.SerializeToString(&serialized_response);
                    kZmqUtil->send_string(serialized_response, &pushers[request.addr_]);
                }
            }
        }
        else
        {
            log->error(
                "Missing key replication factor in process pending request routine.");
        }

        pending_requests.erase(key);
    }

    // Sekwon: This context to deal with pending gossip requests is not required for DINOMO
    // Once receiving new replication factor for the key, all we need to do is to make sure
    // flushing existing log segments before handling the next user requests.
    if (pending_gossip.find(key) != pending_gossip.end())
    {
        ServerThreadList threads = kHashRingUtil->get_responsible_threads(
            wt.replication_response_connect_address(), key, is_metadata(key),
            global_hash_rings, local_hash_rings, key_replication_map, pushers,
            kSelfTierIdVector, succeed, seed);

        if (succeed)
        {
            if (std::find(threads.begin(), threads.end(), wt) != threads.end())
            {
                for (const PendingGossip &gossip : pending_gossip[key])
                {
                    if (stored_key_map.find(key) != stored_key_map.end() &&
                        stored_key_map[key].type_ != LatticeType::NONE &&
                        stored_key_map[key].type_ != gossip.lattice_type_)
                    {
                        log->error("Lattice type mismatch for key {}: {} from query but {} expected.",
                                   key, LatticeType_Name(gossip.lattice_type_),
                                   LatticeType_Name(stored_key_map[key].type_));
                    }
                    else
                    {
                        process_put(key, gossip.lattice_type_, gossip.payload_,
                                    serializers[gossip.lattice_type_], stored_key_map);
                    }
                }
            }
            else
            {
                map<Address, KeyRequest> gossip_map;

                // forward the gossip
                for (const ServerThread &thread : threads)
                {
                    gossip_map[thread.gossip_connect_address()].set_type(RequestType::PUT);

                    for (const PendingGossip &gossip : pending_gossip[key])
                    {
                        prepare_put_tuple(gossip_map[thread.gossip_connect_address()], key,
                                          gossip.lattice_type_, gossip.payload_);
                    }
                }

                // redirect gossip
                for (const auto &gossip_pair : gossip_map)
                {
                    string serialized;
                    gossip_pair.second.SerializeToString(&serialized);
                    kZmqUtil->send_string(serialized, &pushers[gossip_pair.first]);
                }
            }
        }
        else
        {
            log->error(
                "Missing key replication factor in process pending gossip routine.");
        }

        pending_gossip.erase(key);
    }
#endif
}
