#include "monitor/monitoring_utils.hpp"
#include "monitor/policies.hpp"

void slo_policy(logger log, GlobalRingMap &global_hash_rings,
        LocalRingMap &local_hash_rings, TimePoint &grace_start,
        SummaryStats &ss, unsigned &memory_node_count,
        unsigned &new_memory_count, bool &removing_memory_node,
        Address management_ip,
        map<Key, KeyReplication> &key_replication_map,
        map<Key, unsigned> &key_access_summary, MonitoringThread &mt,
        map<Address, unsigned> &departing_node_map,
        SocketCache &pushers, zmq::socket_t &response_puller,
        vector<Address> &routing_ips, unsigned &rid,
        map<Key, std::pair<double, unsigned> > &latency_miss_ratio_map)
{
    // check latency to trigger elasticity or selective replication
    map<Key, KeyReplication> requests;
    if ((kUsingAvgLatency ? ss.avg_latency : ss.tail_latency) > kSloWorst && new_memory_count == 0)
    {
        log->info("Observed latency ({}) violates SLO({}).", (kUsingAvgLatency ? ss.avg_latency : ss.tail_latency), kSloWorst);

        // figure out if we should do hot key replication or add nodes
        if (kEnableElasticity && ss.min_memory_occupancy > 0.20
                && memory_node_count < (unsigned)kMaxMemoryTierSize)
        {
            unsigned node_to_add = 1;
                //ceil(((kUsingAvgLatency ? ss.avg_latench : ss.tail_latency) / kSloWorst - 1) * memory_node_count);

            if (node_to_add < 1)
                node_to_add = 1;

            // trigger elasticity
            auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now() - grace_start)
                .count();
            if (time_elapsed > kGracePeriod)
            {
                add_node(log, "memory", node_to_add, new_memory_count, pushers,
                        management_ip);
            }
        }
        else if (kEnableSelectiveRep)
        {
#ifdef REPLICA_GRACE_PERIOD
            auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now() - grace_start).count();
            if (time_elapsed > kGracePeriod) {
                bool ChangeReplicationFactor = false;
#endif
                for (const auto &key_access_pair : key_access_summary)
                {
                    Key key = key_access_pair.first;
                    unsigned access_count = key_access_pair.second;

                    if (!is_metadata(key) &&
                            access_count > ss.key_access_mean + (3 * ss.key_access_std) &&
                            latency_miss_ratio_map.find(key) != latency_miss_ratio_map.end())
                    {
                        log->info("Key {} accessed {} times (threshold is {}).", key,
                                access_count, ss.key_access_mean + (3 *ss.key_access_std));

                        unsigned target_rep_factor =
                            key_replication_map[key].global_replication_[Tier::MEMORY] *
                            latency_miss_ratio_map[key].first;

                        if (target_rep_factor == key_replication_map[key].global_replication_[Tier::MEMORY]) {
                            target_rep_factor += 1;
                        }

                        unsigned current_mem_rep =
                            key_replication_map[key].global_replication_[Tier::MEMORY];
                        if (target_rep_factor > current_mem_rep &&
                                current_mem_rep < memory_node_count)
                        {
                            unsigned new_mem_rep =
                                std::min(memory_node_count, target_rep_factor);
                            unsigned new_storage_rep =
                                std::max(kMinimumReplicaNumber - new_mem_rep, (unsigned)0);
                            requests[key] = create_new_replication_vector(
                                    new_mem_rep, new_storage_rep,
                                    key_replication_map[key].local_replication_[Tier::MEMORY],
                                    key_replication_map[key].local_replication_[Tier::STORAGE]);
                            log->info(
                                    "Global hot key replication for key {}. M: {}->{}.", key,
                                    key_replication_map[key].global_replication_[Tier::MEMORY],
                                    requests[key].global_replication_[Tier::MEMORY]);
#ifdef REPLICA_GRACE_PERIOD
                            ChangeReplicationFactor = true;
#endif
                        }
                        else
                        {
                            if (key_replication_map[key].global_replication_[Tier::MEMORY] == memory_node_count &&
                                    kMemoryThreadCount > key_replication_map[key].local_replication_[Tier::MEMORY])
                            {
                                requests[key] = create_new_replication_vector(
                                        key_replication_map[key].global_replication_[Tier::MEMORY],
                                        key_replication_map[key].global_replication_[Tier::STORAGE],
                                        kMemoryThreadCount,
                                        key_replication_map[key].local_replication_[Tier::STORAGE]);
                                log->info(
                                        "Local hot key replication for key {}. T: {}->{}.", key,
                                        key_replication_map[key].local_replication_[Tier::MEMORY],
                                        requests[key].local_replication_[Tier::MEMORY]);
#ifdef REPLICA_GRACE_PERIOD
                                ChangeReplicationFactor = true;
#endif
                            }
                        }
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
    }
    else if (kEnableElasticity && !removing_memory_node &&
            ss.min_memory_occupancy < 0.15 &&
            memory_node_count > std::max(ss.required_memory_node,
                (unsigned)kMinMemoryTierSize))
    {
        auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now() - grace_start)
            .count();

        if (time_elapsed > kGracePeriod)
        {
            // before sending remove command, first adjust relevant key's replication
            // factor
            for (const auto &key_access_pair : key_access_summary)
            {
                Key key = key_access_pair.first;

                if (!is_metadata(key) &&
                        key_replication_map[key].global_replication_[Tier::MEMORY] ==
                        (global_hash_rings[Tier::MEMORY].size() / kVirtualThreadNum))
                {
                    unsigned new_mem_rep =
                        key_replication_map[key].global_replication_[Tier::MEMORY] - 1;
                    unsigned new_storage_rep =
                        std::max(kMinimumReplicaNumber - new_mem_rep, (unsigned)0);
                    requests[key] = create_new_replication_vector(
                            new_mem_rep, new_storage_rep,
                            key_replication_map[key].local_replication_[Tier::MEMORY],
                            key_replication_map[key].local_replication_[Tier::STORAGE]);
                    log->info("Dereplication for key {}. M: {}->{}. E: {}->{}", key,
                            key_replication_map[key].global_replication_[Tier::MEMORY],
                            requests[key].global_replication_[Tier::MEMORY],
                            key_replication_map[key].global_replication_[Tier::STORAGE],
                            requests[key].global_replication_[Tier::STORAGE]);
                }
            }

            change_replication_factor(requests, global_hash_rings, local_hash_rings,
                    routing_ips, key_replication_map, pushers, mt,
                    response_puller, log, rid);

            ServerThread node = ServerThread(ss.min_occupancy_memory_public_ip,
                    ss.min_occupancy_memory_private_ip, 0);
            log->info("Remove Node {}/{} due to underutilization.",
                    ss.min_occupancy_memory_public_ip,
                    ss.min_occupancy_memory_private_ip);
            remove_node(log, node, "memory", removing_memory_node, pushers,
                    departing_node_map, mt);
        }
    }
}
