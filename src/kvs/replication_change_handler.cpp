#include "kvs/kvs_handlers.hpp"

void replication_change_handler(
    Address public_ip, Address private_ip, unsigned thread_id, unsigned &seed,
    logger log, string &serialized, GlobalRingMap &global_hash_rings,
#ifdef SHARED_NOTHING
    LocalRingMap &local_hash_rings, set<Key> &stored_key_map,
#else
    LocalRingMap &local_hash_rings, map<Key, KeyProperty> &stored_key_map,
#endif
    map<Key, KeyReplication> &key_replication_map, set<Key> &local_changeset,
    ServerThread &wt, SerializerMap &serializers, SocketCache &pushers,
    zmq::socket_t &response_puller)
{
    log->info("Received a replication factor change.");

    ReplicationFactorUpdate rep_change;
    rep_change.ParseFromString(serialized);

#ifdef ENABLE_DINOMO_KVS // Sekwon
    ReplicationFactorUpdateResponse response;
    response.set_response_id(rep_change.request_id());
    //response.set_error(DinomoError::NO_ERROR);
#endif

    AddressKeysetMap addr_keyset_map;
    set<Key> remove_set;

    bool succeed, synchronous = false;

    if (thread_id == 0)
    {
        // tell all worker threads about the replication factor change
        if (rep_change.response_address() != "") {
            for (unsigned tid = 1; tid < kThreadNum; tid++)
            {
                ReplicationFactorUpdate req;
                req.ParseFromString(serialized);
                req.set_response_address(wt.response_connect_address());
                req.set_request_id(wt.response_connect_address() + ":" + std::to_string(tid));
                auto res = make_request<ReplicationFactorUpdate, ReplicationFactorUpdateResponse>(req,
                        pushers[ServerThread(public_ip, private_ip, tid).replication_change_connect_address()],
                        response_puller, succeed);
                if (!succeed)
                {
                    log->error("Local replication factor update timed out!");
                }
            }
        } else {
            for (unsigned tid = 1; tid < kThreadNum; tid++)
            {
                kZmqUtil->send_string(serialized,
                        &pushers[ServerThread(public_ip, private_ip, tid).replication_change_connect_address()]);
            }
        }
    }

    // for every key, update the replication factor and check if the node is still
    // responsible for the key
#ifdef ENABLE_DINOMO_KVS // Sekwon
    for (const ReplicationFactor &key_rep : rep_change.updates())
    {
        Key key = key_rep.key();
        for (const auto &global : key_rep.global())
        {
            if (global.tier() == Tier::MEMORY) {
                unsigned origin_replication_factor = key_replication_map[key].global_replication_[global.tier()];
                unsigned kDefaultGlobalReplication = (global.tier() == Tier::MEMORY ? kDefaultGlobalMemoryReplication : kDefaultGlobalStorageReplication);
                if (is_primary_replica(key, key_replication_map, global_hash_rings, local_hash_rings, wt))
                {
                    if (origin_replication_factor == kDefaultGlobalReplication && global.value() > origin_replication_factor)
                    {   // non-replicated to increase
                        // These steps should be synchronously done with monitoring nodes.
                        process_switch_to_replication(key, serializers[LatticeType::LWW]);
                        synchronous = true;
                    }
                    else if (origin_replication_factor > kDefaultGlobalReplication && global.value() == kDefaultGlobalReplication)
                    {   // replicated to decrease becoming non-replicated
                        process_revert_to_non_replication(key, serializers[LatticeType::LWW]);
                    }

                    key_replication_map[key].global_replication_[global.tier()] = global.value();
                }
                else
                {
                    ServerThreadList threads = kHashRingUtil->get_responsible_threads(wt.replication_response_connect_address(),
                            key, is_metadata(key), global_hash_rings, local_hash_rings, key_replication_map, pushers,
                            kSelfTierIdVector, succeed, seed);
                    if (succeed)
                    {
                        bool responsible = std::find(threads.begin(), threads.end(), wt) != threads.end();
                        if (responsible && origin_replication_factor > global.value() && global.value() == kDefaultGlobalReplication)
                        {
                            // replication factor decrease
                            key_replication_map[key].global_replication_[global.tier()] = global.value();
                            threads = kHashRingUtil->get_responsible_threads(wt.replication_response_connect_address(), key,
                                    is_metadata(key), global_hash_rings, local_hash_rings, key_replication_map, pushers,
                                    kSelfTierIdVector, succeed, seed);

                            if (succeed)
                            {
                                // check whether this thread will lose the ownership of the key after changing replication factor
                                if (std::find(threads.begin(), threads.end(), wt) == threads.end())
                                {
                                    // Lose the ownership
                                    // This step should be synchronously done with monitoring nodes
                                    process_invalidate(key, serializers[LatticeType::LWW]);
                                    synchronous = true;
                                }
                            }
                            else
                            {
                                log->error("Missing key replication factor in rep factor change routine.");
                            }
                        }
                        else
                        {
                            key_replication_map[key].global_replication_[global.tier()] = global.value();
                        }
                    }
                    else
                    {
                        log->error("Missing key replication factor in rep factor change routine.");

                        // just update the replication factor
                        key_replication_map[key].global_replication_[global.tier()] = global.value();
                    }
                }
            } else {
                key_replication_map[key].global_replication_[global.tier()] = global.value();
            }
        }

        for (const auto &local : key_rep.local())
        {
            key_replication_map[key].local_replication_[local.tier()] = local.value();
        }
    }

    if (rep_change.response_address() != "")
    {
        string serialized_response;
        response.SerializeToString(&serialized_response);
        kZmqUtil->send_string(serialized_response, &pushers[rep_change.response_address()]);
    }
#else
    for (const ReplicationFactor &key_rep : rep_change.updates())
    {
        Key key = key_rep.key();
        // if this thread has the key stored before the change
        if (stored_key_map.find(key) != stored_key_map.end())
        {
            ServerThreadList orig_threads = kHashRingUtil->get_responsible_threads(
                wt.replication_response_connect_address(), key, is_metadata(key),
                global_hash_rings, local_hash_rings, key_replication_map, pushers,
                kAllTiers, succeed, seed);

            if (succeed)
            {
                // update the replication factor
                bool decrement = false;

                for (const auto &global : key_rep.global())
                {
                    if (global.value() < key_replication_map[key].global_replication_[global.tier()])
                    {
                        decrement = true;
                    }

                    key_replication_map[key].global_replication_[global.tier()] = global.value();
                }

                for (const auto &local : key_rep.local())
                {
                    if (local.value() < key_replication_map[key].local_replication_[local.tier()])
                    {
                        decrement = true;
                    }

                    key_replication_map[key].local_replication_[local.tier()] = local.value();
                }

                ServerThreadList threads = kHashRingUtil->get_responsible_threads(
                    wt.replication_response_connect_address(), key, is_metadata(key),
                    global_hash_rings, local_hash_rings, key_replication_map, pushers,
                    kAllTiers, succeed, seed);

                if (succeed)
                {
                    if (std::find(threads.begin(), threads.end(), wt) == threads.end())
                    {   // this thread is no longer
                        // responsible for this key
                        remove_set.insert(key);

                        // add all the new threads that this key should be sent to
                        for (const ServerThread &thread : threads)
                        {
                            addr_keyset_map[thread.gossip_connect_address()].insert(key);
                        }
                    }

                    // decrement represents whether the total global or local rep factor
                    // has been reduced; if that's not the case, and I am the "first"
                    // thread responsible for this key, then I gossip it to the new
                    // threads that are responsible for it
                    if (!decrement && orig_threads.begin()->id() == wt.id())
                    {
                        std::unordered_set<ServerThread, ThreadHash> new_threads;

                        for (const ServerThread &thread : threads)
                        {
                            if (std::find(orig_threads.begin(), orig_threads.end(), thread) == orig_threads.end())
                            {
                                new_threads.insert(thread);
                            }
                        }

                        for (const ServerThread &thread : new_threads)
                        {
                            addr_keyset_map[thread.gossip_connect_address()].insert(key);
                        }
                    }
                }
                else
                {
                    log->error("Missing key replication factor in rep factor change routine.");
                }
            }
            else
            {
                log->error("Missing key replication factor in rep factor change routine.");

                // just update the replication factor
                for (const auto &global : key_rep.global())
                {
                    key_replication_map[key].global_replication_[global.tier()] = global.value();
                }

                for (const auto &local : key_rep.local())
                {
                    key_replication_map[key].local_replication_[local.tier()] = local.value();
                }
            }
        }
        else
        {
            // just update the replication factor
            for (const auto &global : key_rep.global())
            {
                key_replication_map[key].global_replication_[global.tier()] = global.value();
            }

            for (const auto &local : key_rep.local())
            {
                key_replication_map[key].local_replication_[local.tier()] = local.value();
            }
        }
    }

    send_gossip(addr_keyset_map, pushers, serializers, stored_key_map);

    // remove keys
    for (const string &key : remove_set)
    {
        serializers[stored_key_map[key].type_]->remove(key);
        stored_key_map.erase(key);
        local_changeset.erase(key);
    }
#endif
}