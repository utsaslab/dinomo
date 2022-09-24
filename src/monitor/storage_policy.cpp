#include "monitor/monitoring_utils.hpp"
#include "monitor/policies.hpp"

void storage_policy(logger log, GlobalRingMap &global_hash_rings,
                    TimePoint &grace_start, SummaryStats &ss,
                    unsigned &memory_node_count, unsigned &storage_node_count,
                    unsigned &new_memory_count, unsigned &new_storage_count,
                    bool &removing_storage_node, Address management_ip,
                    MonitoringThread &mt,
                    map<Address, unsigned> &departing_node_map,
                    SocketCache &pushers)
{
    // check storage consumption and trigger elasticity if necessary
    if (kEnableElasticity)
    {
        if (new_memory_count == 0 && ss.required_memory_node > memory_node_count)
        {
            auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                                    std::chrono::system_clock::now() - grace_start)
                                    .count();
            if (time_elapsed > kGracePeriod)
            {
                add_node(log, "memory", kNodeAdditionBatchSize, new_memory_count,
                         pushers, management_ip);
            }
        }

        if (kEnableTiering && new_storage_count == 0 &&
            ss.required_storage_node > storage_node_count)
        {
            auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                                    std::chrono::system_clock::now() - grace_start)
                                    .count();
            if (time_elapsed > kGracePeriod)
            {
                add_node(log, "storage", kNodeAdditionBatchSize, new_storage_count, pushers,
                         management_ip);
            }
        }

        if (kEnableTiering &&
            ss.avg_storage_consumption_percentage < kMinStorageNodeConsumption &&
            !removing_storage_node &&
            storage_node_count >
                std::max(ss.required_storage_node, (unsigned)kMinStorageTierSize))
        {
            auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                                    std::chrono::system_clock::now() - grace_start)
                                    .count();

            if (time_elapsed > kGracePeriod)
            {
                // pick a random storage node and send remove node command
                auto node = next(global_hash_rings[Tier::STORAGE].begin(),
                                 rand() % global_hash_rings[Tier::STORAGE].size())
                                ->second;
                remove_node(log, node, "storage", removing_storage_node, pushers,
                            departing_node_map, mt);
            }
        }
    }
}
