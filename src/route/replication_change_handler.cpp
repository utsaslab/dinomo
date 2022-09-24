#include "route/routing_handlers.hpp"

void replication_change_handler(logger log, string &serialized,
                                SocketCache &pushers,
                                map<Key, KeyReplication> &key_replication_map,
                                unsigned thread_id, Address ip)
{
  if (thread_id == 0)
  {
    // tell all worker threads about the replication factor change
    for (unsigned tid = 1; tid < kRoutingThreadCount; tid++)
    {
      kZmqUtil->send_string(
          serialized, &pushers[RoutingThread(ip, tid)
                                   .replication_change_connect_address()]);
    }
  }

  ReplicationFactorUpdate update;
  update.ParseFromString(serialized);

  for (const auto &key_rep : update.updates())
  {
    Key key = key_rep.key();
    log->info("Received a replication factor change for key {}.", key);
    for (const ReplicationFactor_ReplicationValue &global : key_rep.global())
    {
      key_replication_map[key].global_replication_[global.tier()] = global.value();
    }

    for (const ReplicationFactor_ReplicationValue &local : key_rep.local())
    {
      key_replication_map[key].local_replication_[local.tier()] = local.value();
    }
  }
}