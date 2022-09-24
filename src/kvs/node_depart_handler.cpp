#include "kvs/kvs_handlers.hpp"

void node_depart_handler(unsigned thread_id, Address public_ip,
                         Address private_ip, GlobalRingMap &global_hash_rings,
                         logger log, string &serialized, SocketCache &pushers)
{
    vector<string> v;
    split(serialized, ':', v);

    Tier tier;
    Tier_Parse(v[0], &tier);
    Address departing_public_ip = v[1];
    Address departing_private_ip = v[2];
    log->info("Received departure for node {}/{} on tier {}.",
              departing_public_ip, departing_private_ip, tier);

    // update hash ring
    global_hash_rings[tier].remove(departing_public_ip, departing_private_ip, 0);

    if (thread_id == 0)
    {
        // tell all worker threads about the node departure
        for (unsigned tid = 1; tid < kThreadNum; tid++)
        {
            kZmqUtil->send_string(serialized, &pushers[ServerThread(public_ip, private_ip, tid).node_depart_connect_address()]);
        }

        for (const auto &pair : global_hash_rings)
        {
            log->info("Hash ring for tier {} size is {}.", Tier_Name(pair.first), pair.second.size());
        }
    }
}
