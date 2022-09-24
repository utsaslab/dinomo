#include "monitor/monitoring_utils.hpp"
#include "requests.hpp"

KeyReplication create_new_replication_vector(unsigned gm, unsigned ge,
                                             unsigned lm, unsigned le)
{
    KeyReplication rep;
    rep.global_replication_[Tier::MEMORY] = gm;
    rep.global_replication_[Tier::STORAGE] = ge;
    rep.local_replication_[Tier::MEMORY] = lm;
    rep.local_replication_[Tier::STORAGE] = le;

    return rep;
}

void prepare_replication_factor_update(
    const Key &key,
    map<Address, ReplicationFactorUpdate> &replication_factor_map,
    Address server_address, map<Key, KeyReplication> &key_replication_map)
{
    ReplicationFactor *rf = replication_factor_map[server_address].add_updates();
    rf->set_key(key);

    for (const auto &pair : key_replication_map[key].global_replication_)
    {
        ReplicationFactor_ReplicationValue *global = rf->add_global();
        global->set_tier(pair.first);
        global->set_value(pair.second);
    }

    for (const auto &pair : key_replication_map[key].local_replication_)
    {
        ReplicationFactor_ReplicationValue *local = rf->add_local();
        local->set_tier(pair.first);
        local->set_value(pair.second);
    }
}

#ifdef ENABLE_DINOMO_KVS
static bool is_primary_replica_global(const Key &key,
                        map<Key, KeyReplication> &key_replication_map,
                        GlobalRingMap &global_hash_rings, LocalRingMap &local_hash_rings,
                        const ServerThread &st, Tier kSelfTier)
{
    if (key_replication_map[key].global_replication_[kSelfTier] == 0)
    {
        return false;
    }
    else
    {
        if (kSelfTier > Tier::MEMORY)
        {
            bool has_upper_tier_replica = false;
            for (const Tier &tier : kAllTiers)
            {
                if (tier < kSelfTier &&
                    key_replication_map[key].global_replication_[tier] > 0)
                {
                    has_upper_tier_replica = true;
                }
            }
            if (has_upper_tier_replica)
            {
                return false;
            }
        }

        auto global_pos = global_hash_rings[kSelfTier].find(key);
        if (global_pos != global_hash_rings[kSelfTier].end() &&
            st.private_ip().compare(global_pos->second.private_ip()) == 0)
        {
            return true;
        }

        return false;
    }
}
#endif

// assume the caller has the replication factor for the keys and the requests
// are valid (rep factor <= total number of nodes in a tier)
void change_replication_factor(map<Key, KeyReplication> &requests,
                               GlobalRingMap &global_hash_rings,
                               LocalRingMap &local_hash_rings,
                               vector<Address> &routing_ips,
                               map<Key, KeyReplication> &key_replication_map,
                               SocketCache &pushers, MonitoringThread &mt,
                               zmq::socket_t &response_puller, logger log,
                               unsigned &rid)
{
    // used to keep track of the original replication factors for the requested
    // keys
    map<Key, KeyReplication> orig_key_replication_map_info;

    // store the new replication factor synchronously in storage servers
    map<Address, KeyRequest> addr_request_map;

#ifdef ENABLE_DINOMO_KVS
    // form the replication factor update synchronous request map
    map<Address, ReplicationFactorUpdate> replication_factor_map_sync;

    // form the replication factor update asynchronous request map
    map<Address, ReplicationFactorUpdate> replication_factor_map_async;
#else
    // form the replication factor update request map
    map<Address, ReplicationFactorUpdate> replication_factor_map
#endif

    // propagate requests for replication factor updates (put) to a specific node
    for (const auto &request_pair : requests)
    {
        Key key = request_pair.first;
        KeyReplication new_rep = request_pair.second;
        orig_key_replication_map_info[key] = key_replication_map[key];

        // don't send an update if we're not changing the metadata
        if (new_rep == key_replication_map[key])
        {
            continue;
        }

        // update the metadata map
        key_replication_map[key].global_replication_ = new_rep.global_replication_;
        key_replication_map[key].local_replication_ = new_rep.local_replication_;

        // prepare data to be stored in the storage tier
        ReplicationFactor rep_data;
        rep_data.set_key(key);

        for (const auto &pair : key_replication_map[key].global_replication_)
        {
            ReplicationFactor_ReplicationValue *global = rep_data.add_global();
            global->set_tier(pair.first);
            global->set_value(pair.second);
        }

        for (const auto &pair : key_replication_map[key].local_replication_)
        {
            ReplicationFactor_ReplicationValue *local = rep_data.add_local();
            local->set_tier(pair.first);
            local->set_value(pair.second);
        }

        Key rep_key = get_metadata_key(key, MetadataType::replication);

        string serialized_rep_data;
        rep_data.SerializeToString(&serialized_rep_data);
        prepare_metadata_put_request(
            rep_key, serialized_rep_data, global_hash_rings[Tier::MEMORY],
            local_hash_rings[Tier::MEMORY], addr_request_map,
            mt.response_connect_address(), rid);
    }

    // send updates to storage nodes
    set<Key> failed_keys;
    for (const auto &request_pair : addr_request_map)
    {
        bool succeed;
        auto res = make_request<KeyRequest, KeyResponse>(
            request_pair.second, pushers[request_pair.first], response_puller,
            succeed);

        if (!succeed)
        {
            log->error("Replication factor put timed out!");

            for (const auto &tuple : request_pair.second.tuples())
            {
                failed_keys.insert(get_key_from_metadata(tuple.key()));
            }
        }
        else
        {
            for (const auto &tuple : res.tuples())
            {
                if (tuple.error() == 2)
                {
                    log->error(
                        "Replication factor put for key {} rejected due to incorrect "
                        "address.",
                        tuple.key());

                    failed_keys.insert(get_key_from_metadata(tuple.key()));
                }
            }
        }
    }

#ifdef ENABLE_DINOMO_KVS
    // propagate requests for replication factor updates to all relevant nodes
    // Sekwon: when propagating requests for replication factor updates, depending on 
    // the types of changes, we need ways to make nodes converged consistently. 
    // 1) When a replication factor increases, if it starts from default replication 
    // factor, we first need to make sure primary replica installs an indirect pointer 
    // to the shared DPM synchronously, and then to allow other replicas (slaves) to 
    // be converged to accept requests for the replicated keys
    // 2) When a replication factor decreases and becomes back default replication factor,
    // we first need to make sure slave replicas change their replication factors to react
    // synchronously, and then to allow primary replica to safely remove the indirect pointer.
    for (const auto &request_pair : requests)
    {
        Key key = request_pair.first;

        if (failed_keys.find(key) == failed_keys.end())
        {
            for (const Tier &tier : kAllTiers)
            {
                unsigned rep = std::max(
                    key_replication_map[key].global_replication_[tier],
                    orig_key_replication_map_info[key].global_replication_[tier]);
                ServerThreadList threads =
                    responsible_global(key, rep, global_hash_rings[tier]);

                // 1. replication factor increase: whether the increase starts from default?
                // 2. replication factor decrease: whether the decrease results in a single replica?
                //      - Then, who is primary replica?
                unsigned kDefaultGlobalReplication = (tier == Tier::MEMORY ? kDefaultGlobalMemoryReplication : kDefaultGlobalStorageReplication);
                if (key_replication_map[key].global_replication_[tier] > orig_key_replication_map_info[key].global_replication_[tier])
                {
                    if (orig_key_replication_map_info[key].global_replication_[tier] == kDefaultGlobalReplication)
                    {
                        for (const ServerThread &thread : threads)
                        {
                            if (is_primary_replica_global(key, key_replication_map, global_hash_rings, local_hash_rings, thread, tier))
                            {
                                prepare_replication_factor_update(key, replication_factor_map_sync,
                                thread.replication_change_connect_address(), key_replication_map);
                            }
                            else
                            {
                                prepare_replication_factor_update(key, replication_factor_map_async,
                                thread.replication_change_connect_address(), key_replication_map);
                            }
                        }
                    }
                    else
                    {
                        for (const ServerThread &thread : threads)
                        {
                            prepare_replication_factor_update(key, replication_factor_map_async,
                            thread.replication_change_connect_address(), key_replication_map);
                        }
                    }
                }
                else if (key_replication_map[key].global_replication_[tier] < orig_key_replication_map_info[key].global_replication_[tier])
                {
                    if (key_replication_map[key].global_replication_[tier] == kDefaultGlobalReplication)
                    {
                        for (const ServerThread &thread : threads)
                        {
                            if (is_primary_replica_global(key, key_replication_map, global_hash_rings, local_hash_rings, thread, tier))
                            {
                                prepare_replication_factor_update(key, replication_factor_map_async,
                                thread.replication_change_connect_address(), key_replication_map);
                            }
                            else
                            {
                                prepare_replication_factor_update(key, replication_factor_map_sync,
                                thread.replication_change_connect_address(), key_replication_map);
                            }
                        }
                    }
                    else
                    {
                        for (const ServerThread &thread : threads)
                        {
                            prepare_replication_factor_update(key, replication_factor_map_async,
                            thread.replication_change_connect_address(), key_replication_map);
                        }
                    }
                }
                else
                {
                    for (const ServerThread &thread : threads)
                    {
                        prepare_replication_factor_update(key, replication_factor_map_async,
                                thread.replication_change_connect_address(), key_replication_map);
                    }
                }
            }

            // form replication factor update requests for routing nodes
            for (const string &address : routing_ips)
            {
                prepare_replication_factor_update(
                    key, replication_factor_map_async,
                    RoutingThread(address, 0).replication_change_connect_address(),
                    key_replication_map);
            }
        }
    }

    for (auto &rep_factor_pair : replication_factor_map_sync)
    {
        rep_factor_pair.second.set_response_address(mt.response_connect_address());
        string req_id = mt.response_connect_address() + ":" + std::to_string(rid);
        rep_factor_pair.second.set_request_id(req_id);
        rid += 1;
    }

    for (auto &rep_factor_pair : replication_factor_map_async)
    {
        // Sekwon
        rep_factor_pair.second.set_response_address("");
        rep_factor_pair.second.set_request_id("");
    }

    // Sekwon: send replication factor update synchronously to all relevant nodes
    set<string> req_ids;
    vector<ReplicationFactorUpdateResponse> replica_responses;
    for (const auto &rep_factor_pair : replication_factor_map_sync)
    {
#if 0
        bool succeed;
        auto res = make_request<ReplicationFactorUpdate, ReplicationFactorUpdateResponse>(
            rep_factor_pair.second, pushers[rep_factor_pair.first], response_puller, succeed);

        if (!succeed)
        {
            log->error("Replication factor update timed out!");
        }
#else
        send_request<ReplicationFactorUpdate>(rep_factor_pair.second, pushers[rep_factor_pair.first]);
        req_ids.insert(rep_factor_pair.second.request_id());
#endif
    }

    if (req_ids.size() != 0)
    {
        bool succeed = receive<ReplicationFactorUpdateResponse>(response_puller, req_ids, replica_responses);
        if (!succeed)
        {
            log->error("Time out occurs while waiting responses to change replication factors");
        }
    }

    // send replication factor update asynchronously to all relevant nodes
    for (const auto &rep_factor_pair : replication_factor_map_async)
    {
        string serialized_msg;
        rep_factor_pair.second.SerializeToString(&serialized_msg);
        kZmqUtil->send_string(serialized_msg, &pushers[rep_factor_pair.first]);
    }

    // restore rep factor for failed keys
    for (const string &key : failed_keys)
    {
        key_replication_map[key] = orig_key_replication_map_info[key];
    }
#else
    for (const auto &request_pair : requests)
    {
        Key key = request_pair.first;

        if (failed_keys.find(key) == failed_keys.end())
        {
            for (const Tier &tier : kAllTiers)
            {
                unsigned rep = std::max(
                    key_replication_map[key].global_replication_[tier],
                    orig_key_replication_map_info[key].global_replication_[tier]);
                ServerThreadList threads =
                    responsible_global(key, rep, global_hash_rings[tier]);

                for (const ServerThread &thread : threads)
                {
                    prepare_replication_factor_update(
                        key, replication_factor_map,
                        thread.replication_change_connect_address(), key_replication_map);
                }
            }

            // form replication factor update requests for routing nodes
            for (const string &address : routing_ips)
            {
                prepare_replication_factor_update(
                    key, replication_factor_map,
                    RoutingThread(address, 0).replication_change_connect_address(),
                    key_replication_map);
            }
        }
    }

    // send replication factor update to all relevant nodes
    for (const auto &rep_factor_pair : replication_factor_map)
    {
        string serialized_msg;
        rep_factor_pair.second.SerializeToString(&serialized_msg);
        kZmqUtil->send_string(serialized_msg, &pushers[rep_factor_pair.first]);
    }

    // restore rep factor for failed keys
    for (const string &key : failed_keys)
    {
        key_replication_map[key] = orig_key_replication_map_info[key];
    }
#endif
}