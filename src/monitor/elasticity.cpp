#include "monitor/monitoring_utils.hpp"

void add_node(logger log, string tier, unsigned number, unsigned &adding,
              SocketCache &pushers, const Address &management_ip)
{
    log->info("Adding {} node(s) in tier {}.", std::to_string(number), tier);

    //string mgmt_addr = "tcp://" + management_ip + ":7001";
    string mgmt_addr = "tcp://" + management_ip + ":8101";
    string message = "add:" + std::to_string(number) + ":" + tier;

    kZmqUtil->send_string(message, &pushers[mgmt_addr]);
    adding = number;
}

void remove_node(logger log, ServerThread &node, string tier, bool &removing,
                 SocketCache &pushers,
                 map<Address, unsigned> &departing_node_map,
                 MonitoringThread &mt)
{
    auto connection_addr = node.self_depart_connect_address();
    departing_node_map[node.private_ip()] =
        kTierMetadata[Tier::MEMORY].thread_number_;
    auto ack_addr = mt.depart_done_connect_address();

    kZmqUtil->send_string(ack_addr, &pushers[connection_addr]);
    removing = true;
}
