#include "monitor/monitoring_handlers.hpp"

void membership_handler(
    logger log, string &serialized, GlobalRingMap &global_hash_rings,
    unsigned &new_memory_count, unsigned &new_storage_count, TimePoint &grace_start,
    vector<Address> &routing_ips, StorageStats &memory_consumption,
    StorageStats &storage_consumption, OccupancyStats &memory_occupancy,
    OccupancyStats &storage_occupancy,
    map<Key, map<Address, unsigned>> &key_access_frequency) {
  vector<string> v;

  split(serialized, ':', v);
  string type = v[0];

  Tier tier;
  Tier_Parse(v[1], &tier);
  Address new_server_public_ip = v[2];
  Address new_server_private_ip = v[3];

  if (type == "join") {
    log->info("Received join from server {}/{} in tier {}.",
              new_server_public_ip, new_server_private_ip,
              std::to_string(tier));
    if (tier == Tier::MEMORY) {
      global_hash_rings[tier].insert(new_server_public_ip,
                                     new_server_private_ip, 0, 0);

      if (new_memory_count > 0) {
        new_memory_count -= 1;
      }

      // reset grace period timer
      grace_start = std::chrono::system_clock::now();
    } else if (tier == Tier::STORAGE) {
      global_hash_rings[tier].insert(new_server_public_ip,
                                     new_server_private_ip, 0, 0);

      if (new_storage_count > 0) {
        new_storage_count -= 1;
      }

      // reset grace period timer
      grace_start = std::chrono::system_clock::now();
    } else if (tier == Tier::ROUTING) {
      routing_ips.push_back(new_server_public_ip);
    } else {
      log->error("Invalid tier: {}.", std::to_string(tier));
    }

    for (const auto &pair : global_hash_rings) {
      log->info("Hash ring for tier {} is size {}.", pair.first,
                pair.second.size());
    }
  } else if (type == "depart") {
    log->info("Received depart from server {}/{}.", new_server_public_ip,
              new_server_private_ip);

    // update hash ring
    global_hash_rings[tier].remove(new_server_public_ip, new_server_private_ip,
                                   0);
    if (tier == Tier::MEMORY) {
      memory_consumption.erase(new_server_private_ip);
      memory_occupancy.erase(new_server_private_ip);

      // NOTE: No const here because we are calling erase
      for (auto &key_access_pair : key_access_frequency) {
        for (unsigned i = 0; i < kMemoryThreadCount; i++) {
          key_access_pair.second.erase(new_server_private_ip + ":" +
                                       std::to_string(i));
        }
      }
    } else if (tier == Tier::STORAGE) {
      storage_consumption.erase(new_server_private_ip);
      storage_occupancy.erase(new_server_private_ip);

      // NOTE: No const here because we are calling erase
      for (auto &key_access_pair : key_access_frequency) {
        for (unsigned i = 0; i < kStorageThreadCount; i++) {
          key_access_pair.second.erase(new_server_private_ip + ":" +
                                       std::to_string(i));
        }
      }
    } else {
      log->error("Invalid tier: {}.", std::to_string(tier));
    }

    for (const auto &pair : global_hash_rings) {
      log->info("Hash ring for tier {} is size {}.", pair.first,
                pair.second.size());
    }
  }
}
