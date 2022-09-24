#include "monitor/monitoring_utils.hpp"
#include "monitor/policies.hpp"

void movement_policy(logger log, GlobalRingMap &global_hash_rings,
        LocalRingMap &local_hash_rings, TimePoint &grace_start,
        SummaryStats &ss, unsigned &memory_node_count,
        unsigned &storage_node_count, unsigned &new_memory_count,
        unsigned &new_storage_count, Address management_ip,
        map<Key, KeyReplication> &key_replication_map,
        map<Key, unsigned> &key_access_summary,
        map<Key, unsigned> &key_size, MonitoringThread &mt,
        SocketCache &pushers, zmq::socket_t &response_puller,
        vector<Address> &routing_ips, unsigned &rid) {
    // promote hot keys to memory tier
    map<Key, KeyReplication> requests;

    int time_elapsed = 0;
    unsigned long long required_storage = 0;
    unsigned long long free_storage = 0;
    bool overflow = false;

    if (kEnableTiering) {
        free_storage =
            (kMaxMemoryNodeConsumption *
             kTierMetadata[Tier::MEMORY].node_capacity_ * memory_node_count -
             ss.total_memory_consumption);
        for (const auto &key_access_pair : key_access_summary) {
            Key key = key_access_pair.first;
            unsigned access_count = key_access_pair.second;

            if (!is_metadata(key) && access_count > kKeyPromotionThreshold &&
                    key_replication_map[key].global_replication_[Tier::MEMORY] == 0 &&
                    key_size.find(key) != key_size.end()) {
                required_storage += key_size[key];
                if (required_storage > free_storage) {
                    overflow = true;
                } else {
                    requests[key] = create_new_replication_vector(
                            key_replication_map[key].global_replication_[Tier::MEMORY] + 1,
                            key_replication_map[key].global_replication_[Tier::STORAGE] - 1,
                            key_replication_map[key].local_replication_[Tier::MEMORY],
                            key_replication_map[key].local_replication_[Tier::STORAGE]);
                }
            }
        }

        change_replication_factor(requests, global_hash_rings, local_hash_rings,
                routing_ips, key_replication_map, pushers, mt,
                response_puller, log, rid);

        log->info("Promoting {} keys into memory tier.", requests.size());
        time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now() - grace_start)
            .count();

        if (kEnableElasticity && overflow && new_memory_count == 0 &&
                time_elapsed > kGracePeriod) {
            unsigned total_memory_node_needed =
                ceil((ss.total_memory_consumption + required_storage) /
                        (kMaxMemoryNodeConsumption *
                         kTierMetadata[Tier::MEMORY].node_capacity_));

            if (total_memory_node_needed > memory_node_count) {
                unsigned node_to_add = (total_memory_node_needed - memory_node_count);
                add_node(log, "memory", node_to_add, new_memory_count, pushers,
                        management_ip);
            }
        }
    }

    requests.clear();
    required_storage = 0;

    // demote cold keys to storage tier
    if (kEnableTiering) {
        free_storage =
            (kMaxStorageNodeConsumption * kTierMetadata[Tier::STORAGE].node_capacity_ *
             storage_node_count -
             ss.total_storage_consumption);
        overflow = false;

        for (const auto &key_access_pair : key_access_summary) {
            Key key = key_access_pair.first;
            unsigned access_count = key_access_pair.second;

            if (!is_metadata(key) && access_count < kKeyDemotionThreshold &&
                    key_replication_map[key].global_replication_[Tier::MEMORY] > 0 &&
                    key_size.find(key) != key_size.end()) {
                required_storage += key_size[key];
                if (required_storage > free_storage) {
                    overflow = true;
                } else {
                    requests[key] =
                        create_new_replication_vector(0, kMinimumReplicaNumber, 1, 1);
                }
            }
        }

        change_replication_factor(requests, global_hash_rings, local_hash_rings,
                routing_ips, key_replication_map, pushers, mt,
                response_puller, log, rid);

        log->info("Demoting {} keys into storage tier.", requests.size());
        if (kEnableElasticity && overflow && new_storage_count == 0 &&
                time_elapsed > kGracePeriod) {
            unsigned total_storage_node_needed = ceil(
                    (ss.total_storage_consumption + required_storage) /
                    (kMaxStorageNodeConsumption * kTierMetadata[Tier::STORAGE].node_capacity_));

            if (total_storage_node_needed > storage_node_count) {
                unsigned node_to_add = (total_storage_node_needed - storage_node_count);
                add_node(log, "storage", node_to_add, new_storage_count, pushers,
                        management_ip);
            }
        }
    }

    requests.clear();

    if (kEnableSelectiveRep) {
#ifdef REPLICA_GRACE_PERIOD
        auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now() - grace_start).count();
        if (time_elapsed > kGracePeriod) {
            bool ChangeReplicationFactor = false;
#endif
            // reduce the replication factor of some keys that are not so hot anymore
            KeyReplication minimum_rep =
                create_new_replication_vector(1, kMinimumReplicaNumber - 1, 1, 1);
            for (const auto &key_access_pair : key_access_summary) {
                Key key = key_access_pair.first;
                unsigned access_count = key_access_pair.second;

                if (!is_metadata(key) && access_count <= ss.key_access_mean - ss.key_access_std &&
                        !(key_replication_map[key] == minimum_rep)) {
                    log->info("Key {} accessed {} times (threshold is {}).", key,
                            access_count, ss.key_access_mean);
                    requests[key] =
                        create_new_replication_vector(1, kMinimumReplicaNumber - 1, 1, 1);
                    log->info("Dereplication for key {}. M: {}->{}. E: {}->{}", key,
                            key_replication_map[key].global_replication_[Tier::MEMORY],
                            requests[key].global_replication_[Tier::MEMORY],
                            key_replication_map[key].global_replication_[Tier::STORAGE],
                            requests[key].global_replication_[Tier::STORAGE]);
#ifdef REPLICA_GRACE_PERIOD
                    ChangeReplicationFactor = true;
#endif
                }
            }

            change_replication_factor(requests, global_hash_rings, local_hash_rings,
                    routing_ips, key_replication_map, pushers, mt,
                    response_puller, log, rid);

#ifdef REPLICA_GRACE_PERIOD
            if (ChangeReplicationFactor) grace_start = std::chrono::system_clock::now();
        }
#endif
    }

    requests.clear();
}
