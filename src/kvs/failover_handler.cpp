#include "kvs/kvs_handlers.hpp"

void failover_handler(string &serialized, SerializerMap &serializers, logger log)
{
#ifndef ENABLE_CLOVER_KVS
    vector<string> v;
    split(serialized, ':', v);

    Tier tier;
    Tier_Parse(v[0], &tier);
    int failed_node_rank = std::stoul(v[1], nullptr, 0);

    log->info("Receive failover (tier = {}, rank = {}).", tier, failed_node_rank);

    if (tier == Tier::MEMORY) {
        process_failover(serializers[LatticeType::LWW], failed_node_rank);
    } else {
        log->error("Unknown tier type {} in failover request.", tier);
    }
#endif
}