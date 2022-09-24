#ifndef KVS_INCLUDE_MONITOR_MONITORING_UTILS_HPP_
#define KVS_INCLUDE_MONITOR_MONITORING_UTILS_HPP_

#include "hash_ring.hpp"
#include "metadata.pb.h"
#include "requests.hpp"

// define monitoring threshold (in second)
extern unsigned kMonitoringThreshold;

// define the grace period for triggering elasticity action (in second)
extern unsigned kGracePeriod;

// the default number of nodes to add concurrently for storage
const unsigned kNodeAdditionBatchSize = 2;

// define capacity for both tiers
const double kMaxMemoryNodeConsumption = 0.6;
const double kMinMemoryNodeConsumption = 0.3;
const double kMaxStorageNodeConsumption = 0.75;
const double kMinStorageNodeConsumption = 0.5;

// define threshold for promotion/demotion
const unsigned kKeyPromotionThreshold = 0;
const unsigned kKeyDemotionThreshold = 1;

// define minimum number of nodes for each tier
const unsigned kMaxMemoryTierSize = 6;
const unsigned kMinMemoryTierSize = 4;
const unsigned kMinStorageTierSize = 0;

// value size in KB
const unsigned kValueSize = 256;

struct SummaryStats {
  void clear() {
    key_access_mean = 0;
    key_access_std = 0;
    total_memory_access = 0;
    total_storage_access = 0;
    total_memory_consumption = 0;
    total_storage_consumption = 0;
    max_memory_consumption_percentage = 0;
    max_storage_consumption_percentage = 0;
    avg_memory_consumption_percentage = 0;
    avg_storage_consumption_percentage = 0;
    required_memory_node = 0;
    required_storage_node = 0;
    max_memory_occupancy = 0;
    min_memory_occupancy = 1;
    avg_memory_occupancy = 0;
    max_storage_occupancy = 0;
    min_storage_occupancy = 1;
    avg_storage_occupancy = 0;
    min_occupancy_memory_public_ip = Address();
    min_occupancy_memory_private_ip = Address();
    avg_latency = 0;
    tail_latency = 0;
    median_latency = 0;
    min_latency = 0;
    max_latency = 0;
    total_throughput = 0;
    total_value_cache_size = 0;
    total_value_cache_hits = 0;
    total_shortcut_cache_hits = 0;
    total_local_log_hits = 0;
    total_cache_misses = 0;
    avg_kvs_latency = 0;
  }

  SummaryStats() { clear(); }
  double key_access_mean;
  double key_access_std;
  unsigned total_memory_access;
  unsigned total_storage_access;
  unsigned long long total_memory_consumption;
  unsigned long long total_storage_consumption;
  double max_memory_consumption_percentage;
  double max_storage_consumption_percentage;
  double avg_memory_consumption_percentage;
  double avg_storage_consumption_percentage;
  unsigned required_memory_node;
  unsigned required_storage_node;
  double max_memory_occupancy;
  double min_memory_occupancy;
  double avg_memory_occupancy;
  double max_storage_occupancy;
  double min_storage_occupancy;
  double avg_storage_occupancy;
  Address min_occupancy_memory_public_ip;
  Address min_occupancy_memory_private_ip;
  double avg_latency;
  double tail_latency;
  double median_latency;
  double min_latency;
  double max_latency;
  double total_throughput;
  uint64_t total_value_cache_size;
  uint64_t total_value_cache_hits;
  uint64_t total_shortcut_cache_hits;
  uint64_t total_local_log_hits;
  uint64_t total_cache_misses;
  double avg_kvs_latency;
};

#ifndef ENABLE_DINOMO_KVS
void collect_internal_stats(
    GlobalRingMap &global_hash_rings, LocalRingMap &local_hash_rings,
    SocketCache &pushers, MonitoringThread &mt, zmq::socket_t &response_puller,
    logger log, unsigned &rid,
    map<Key, map<Address, unsigned> > &key_access_frequency,
    map<Key, unsigned> &key_size, StorageStats &memory_consumption,
    StorageStats &storage_consumption, CacheStats &value_cache_sizes,
    CacheStats &value_cache_hits, CacheStats &shortcut_cache_hits,
    CacheStats &local_log_hits, CacheStats &cache_misses,
    OccupancyStats &memory_occupancy, OccupancyStats &storage_occupancy,
    AccessStats &memory_accesses, AccessStats &storage_accesses,
    KvsStats &kvs_avg_latencies);
#else
void collect_internal_stats(logger log, string &serialized, unsigned &rid,
    map<Key, map<Address, unsigned> > &key_access_frequency,
    map<Key, unsigned> &key_size, StorageStats &memory_consumption,
    StorageStats &storage_consumption, CacheStats &value_cache_sizes,
    CacheStats &value_cache_hits, CacheStats &shortcut_cache_hits,
    CacheStats &local_log_hits, CacheStats &cache_misses,
    OccupancyStats &memory_occupancy, OccupancyStats &storage_occupancy,
    AccessStats &memory_accesses, AccessStats &storage_accesses,
    KvsStats &kvs_avg_latencies);
#endif

void compute_summary_stats(
    map<Key, map<Address, unsigned>> &key_access_frequency,
    StorageStats &memory_consumption, StorageStats &storage_consumption,
    CacheStats &value_cache_sizes, CacheStats &value_cache_hits,
    CacheStats &shortcut_cache_hits, CacheStats &local_log_hits,
    CacheStats &cache_misses, OccupancyStats &memory_occupancy,
    OccupancyStats &storage_occupancy, AccessStats &memory_accesses,
    AccessStats &storage_accesses, map<Key, unsigned> &key_access_summary,
    SummaryStats &ss, logger log, unsigned &server_monitoring_epoch,
    KvsStats &kvs_avg_latencies, unsigned kServerReportPeriod);

void collect_external_stats(map<string, std::tuple<double, double, double, double, double, unsigned>> &user_latency,
                            map<string, double> &user_throughput,
                            SummaryStats &ss, logger log, unsigned elapsed_time);

KeyReplication create_new_replication_vector(unsigned gm, unsigned ge,
                                             unsigned lm, unsigned le);

void prepare_replication_factor_update(
    const Key &key,
    map<Address, ReplicationFactorUpdate> &replication_factor_map,
    Address server_address, map<Key, KeyReplication> &key_replication_map);

void change_replication_factor(map<Key, KeyReplication> &requests,
                               GlobalRingMap &global_hash_rings,
                               LocalRingMap &local_hash_rings,
                               vector<Address> &routing_ips,
                               map<Key, KeyReplication> &key_replication_map,
                               SocketCache &pushers, MonitoringThread &mt,
                               zmq::socket_t &response_puller, logger log,
                               unsigned &rid);

void add_node(logger log, string tier, unsigned number, unsigned &adding,
              SocketCache &pushers, const Address &management_ip);

void remove_node(logger log, ServerThread &node, string tier,
                 bool &removing_flag, SocketCache &pushers,
                 map<Address, unsigned> &departing_node_map,
                 MonitoringThread &mt);

#endif // KVS_INCLUDE_MONITOR_MONITORING_UTILS_HPP_