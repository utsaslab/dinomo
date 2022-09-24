#include "kvs/kvs_handlers.hpp"

#ifdef SHARED_NOTHING
void send_gossip(AddressKeysetMap &addr_keyset_map, SocketCache &pushers,
                 SerializerMap &serializers,
                 set<Key> &stored_key_map)
#else
void send_gossip(AddressKeysetMap &addr_keyset_map, SocketCache &pushers,
                 SerializerMap &serializers,
                 map<Key, KeyProperty> &stored_key_map)
#endif
{
    map<Address, KeyRequest> gossip_map;

    for (const auto &key_pair : addr_keyset_map)
    {
        string address = key_pair.first;
        RequestType type;
        RequestType_Parse("PUT", &type);
        gossip_map[address].set_type(type);

        for (const auto &key : key_pair.second)
        {
            LatticeType type;
            if (stored_key_map.find(key) == stored_key_map.end())
            {
                // we don't have this key stored, so skip
                continue;
            }
            else
            {
#ifdef SHARED_NOTHING
                type = LatticeType::LWW;
#else
                type = stored_key_map[key].type_;
#endif
            }

            auto res = process_get(key, serializers[type]);

            // If there's no error for the get request
            if (res.second == 0)
            {
                prepare_put_tuple(gossip_map[address], key, type, res.first);
            }
        }
    }

    // send gossip
    for (const auto &gossip_pair : gossip_map)
    {
        string serialized;
        gossip_pair.second.SerializeToString(&serialized);
        kZmqUtil->send_string(serialized, &pushers[gossip_pair.first]);
    }
}

#ifdef SHARED_NOTHING
void send_failover_gossip(AddressKeysetMap &addr_keyset_map, SocketCache &pushers)
{
    map<Address, KeyRequest> gossip_map;

    for (const auto &key_pair : addr_keyset_map)
    {
        string address = key_pair.first;
        RequestType type;
        RequestType_Parse("PUT", &type);
        gossip_map[address].set_type(type);

        for (const auto &key : key_pair.second)
        {
            prepare_put_tuple(gossip_map[address], key, LatticeType::LWW, "");
        }
    }

    // send gossip
    for (const auto &gossip_pair : gossip_map)
    {
        string serialized;
        gossip_pair.second.SerializeToString(&serialized);
        kZmqUtil->send_string(serialized, &pushers[gossip_pair.first]);
    }
}
#endif

#ifndef ENABLE_DINOMO_KVS
std::pair<string, AnnaError> process_get(const Key &key,
                                         Serializer *serializer)
{
    AnnaError error = AnnaError::NO_ERROR;
    auto res = serializer->get(key, error);
    return std::pair<string, AnnaError>(std::move(res), error);
}

void process_put(const Key &key, LatticeType lattice_type,
                 const string &payload, Serializer *serializer,
                 map<Key, KeyProperty> &stored_key_map)
{
    stored_key_map[key].size_ = serializer->put(key, payload);
    stored_key_map[key].type_ = std::move(lattice_type);
}
#else

#ifdef SHARED_NOTHING
void response_batching_requests(SocketCache &pushers, logger log,
        map<Key, vector<PendingRequest>> &batching_requests,
        set<Key> &stored_key_map)
#else
void response_batching_requests(SocketCache &pushers, logger log,
        map<Key, vector<PendingRequest>> &batching_requests,
        map<Key, KeyProperty> &stored_key_map)
#endif
{
    //uint64_t num_batching = 0;
    for (const auto &batch_pair : batching_requests)
    {
        for (const PendingRequest &request : batching_requests[batch_pair.first])
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
                tp->set_key(batch_pair.first);

                string serialized_response;
                response.SerializeToString(&serialized_response);
                kZmqUtil->send_string(serialized_response, &pushers[request.addr_]);
            }

#ifdef SHARED_NOTHING
            if (request.type_ == RequestType::PUT || request.type_ == RequestType::INSERT) {
                stored_key_map.insert(batch_pair.first);
            }
#endif
            //num_batching++;
        }
    }
    batching_requests.clear();
    //log->info("The number of batched write requests = {}", num_batching);
}

std::pair<string, AnnaError> process_get(const Key &key, Serializer *serializer)
{
    AnnaError error = AnnaError::NO_ERROR;
    auto res = serializer->get(key, error);
    return std::pair<string, AnnaError>(std::move(res), error);
}

std::pair<string, AnnaError> process_get_replicated(const Key &key, Serializer *serializer)
{
    AnnaError error = AnnaError::NO_ERROR;
    auto res = serializer->getReplicated(key, error);
    return std::pair<string, AnnaError>(std::move(res), error);
}

std::pair<string, AnnaError> process_get_meta(const Key &key, Serializer *serializer)
{
    AnnaError error = AnnaError::NO_ERROR;
    auto res = serializer->getMeta(key, error);
    return std::pair<string, AnnaError>(std::move(res), error);
}

unsigned process_put(const Key &key, LatticeType lattice_type,
const string &payload, Serializer *serializer)
{
    return serializer->put(key, payload);
}

void process_put_replicated(const Key &key, LatticeType lattice_type,
const string &payload, Serializer *serializer)
{
    serializer->putReplicated(key, payload);
}

void process_put_meta(const Key &key, LatticeType lattice_type,
const string &payload, Serializer *serializer)
{
    serializer->putMeta(key, payload);
}

unsigned process_update(const Key &key, LatticeType lattice_type,
const string &payload, Serializer *serializer)
{
    return serializer->update(key, payload);
}

void process_update_replicated(const Key &key, LatticeType lattice_type,
const string &payload, Serializer *serializer)
{
    serializer->updateReplicated(key, payload);
}

void process_merge(Serializer *serializer, bool clear_cache)
{
    serializer->merge(clear_cache);
}

void process_flush(Serializer *serializer)
{
    serializer->flush();
}

void process_failover(Serializer *serializer, int failed_node_rank)
{
    serializer->failover(failed_node_rank);
}

void process_invalidate(const Key &key, Serializer *serializer)
{
    serializer->invalidate(key);
}

void process_switch_to_replication(const Key &key, Serializer *serializer)
{
    serializer->swap(key, true);
}

void process_revert_to_non_replication(const Key &key, Serializer *serializer)
{
    serializer->swap(key, false);
}
#endif
bool is_primary_replica(const Key &key,
                        map<Key, KeyReplication> &key_replication_map,
                        GlobalRingMap &global_hash_rings,
                        LocalRingMap &local_hash_rings, const ServerThread &st)
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
            auto local_pos = local_hash_rings[kSelfTier].find(key);

            if (local_pos != local_hash_rings[kSelfTier].end() &&
                st.tid() == local_pos->second.tid())
            {
                return true;
            }
        }

        return false;
    }
}