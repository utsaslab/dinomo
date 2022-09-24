#include "monitor/monitoring_handlers.hpp"
#include "monitor/monitoring_utils.hpp"
#include "monitor/policies.hpp"
#include "yaml-cpp/yaml.h"

unsigned kMemoryThreadCount;
unsigned kStorageThreadCount;
unsigned kBenchmarkNodeCount;
unsigned kBenchmarkThreadCount;

uint64_t kMemoryNodeCapacity;
uint64_t kStorageNodeCapacity;

unsigned kDefaultGlobalMemoryReplication;
unsigned kDefaultGlobalStorageReplication;
unsigned kDefaultLocalReplication;
unsigned kMinimumReplicaNumber;

bool kEnableElasticity;
bool kEnableTiering;
bool kEnableSelectiveRep;
bool kUsingAvgLatency;
bool kEnablePerfMonitor;

unsigned kSloWorst;
unsigned kGracePeriod;
unsigned kMonitoringThreshold;

#ifndef ENABLE_DINOMO_KVS
// read-only per-tier metadata
hmap<Tier, TierMetadata, TierEnumHash> kTierMetadata;
#endif

ZmqUtil zmq_util;
ZmqUtilInterface *kZmqUtil = &zmq_util;

HashRingUtil hash_ring_util;
HashRingUtilInterface *kHashRingUtil = &hash_ring_util;

int main(int argc, char *argv[]) {
    auto log = spdlog::basic_logger_mt("monitoring_log", "log.txt", true);
    log->flush_on(spdlog::level::info);

    if (argc != 1) {
        std::cerr << "Usage: " << argv[0] << std::endl;
        return 1;
    }

    // read the YAML conf
    YAML::Node conf = YAML::LoadFile("conf/dinomo-config.yml");
    YAML::Node monitoring = conf["monitoring"];
    Address ip = monitoring["ip"].as<Address>();
    Address management_ip = monitoring["mgmt_ip"].as<Address>();

    YAML::Node policy = conf["policy"];
    kEnableElasticity = policy["elasticity"].as<bool>();
    kEnableSelectiveRep = policy["selective-rep"].as<bool>();
    kEnableTiering = policy["tiering"].as<bool>();
    kUsingAvgLatency = policy["using_avg_latency"].as<bool>();
    kSloWorst = policy["latencythreshold"].as<unsigned>();
    kEnablePerfMonitor = policy["perf-monitoring"].as<bool>();
    kGracePeriod = policy["grace-period"].as<unsigned>();
    kMonitoringThreshold = policy["policy-decision-period"].as<unsigned>();
    unsigned kServerReportPeriod = policy["server-report-period"].as<unsigned>();

    log->info("Elasticity policy enabled: {}", kEnableElasticity);
    log->info("Tiering policy enabled: {}", kEnableTiering);
    log->info("Selective replication policy enabled: {}", kEnableSelectiveRep);
    log->info("Use avg latency for SLO: {}", kUsingAvgLatency);
    log->info("Performance monitoring enabled: {}", kEnablePerfMonitor);
    log->info("Configured grace period (sec): {}", kGracePeriod);
    log->info("Policy decision period (sec): {}", kMonitoringThreshold);

    YAML::Node threads = conf["threads"];
    kMemoryThreadCount = threads["memory"].as<unsigned>();
    kStorageThreadCount = threads["storage"].as<unsigned>();
    kBenchmarkThreadCount = threads["benchmark"].as<unsigned>();

    YAML::Node benchConfig = conf["bench_config"];
    kBenchmarkNodeCount = benchConfig["num-benchmark-nodes"].as<unsigned>();

    YAML::Node capacities = conf["capacities"];
    kMemoryNodeCapacity = capacities["memory-cap"].as<unsigned>() * 1024 * 1024UL;
    kStorageNodeCapacity = capacities["storage-cap"].as<unsigned>() * 1024 * 1024UL;

    YAML::Node replication = conf["replication"];
    kDefaultGlobalMemoryReplication = replication["memory"].as<unsigned>();
    kDefaultGlobalStorageReplication = replication["storage"].as<unsigned>();
    kDefaultLocalReplication = replication["local"].as<unsigned>();
    kMinimumReplicaNumber = replication["minimum"].as<unsigned>();

    kTierMetadata[Tier::MEMORY] = TierMetadata(Tier::MEMORY, kMemoryThreadCount,
    kDefaultGlobalMemoryReplication, kMemoryNodeCapacity);
    kTierMetadata[Tier::STORAGE] = TierMetadata(Tier::STORAGE, kStorageThreadCount,
    kDefaultGlobalStorageReplication, kStorageNodeCapacity);

    GlobalRingMap global_hash_rings;
    LocalRingMap local_hash_rings;

    // form local hash rings
    for (const auto &pair : kTierMetadata) {
        TierMetadata tier = pair.second;
        for (unsigned tid = 0; tid < tier.thread_number_; tid++) {
            local_hash_rings[tier.id_].insert(ip, ip, 0, tid);
        }
    }

    // keep track of the keys' replication info
    map<Key, KeyReplication> key_replication_map;

    unsigned memory_node_count;
    unsigned storage_node_count;

    map<Key, map<Address, unsigned>> key_access_frequency;

    map<Key, unsigned> key_access_summary;

    map<Key, unsigned> key_size;

    StorageStats memory_consumption;
    StorageStats storage_consumption;

    CacheStats value_cache_sizes;
    CacheStats value_cache_hits;
    CacheStats shortcut_cache_hits;
    CacheStats local_log_hits;
    CacheStats cache_misses;

    OccupancyStats memory_occupancy;
    OccupancyStats storage_occupancy;

    AccessStats memory_accesses;
    AccessStats storage_accesses;

    KvsStats kvs_avg_latencies;

    SummaryStats ss;

    //map<string, double> user_latency;

    map<string, std::tuple<double, double, double, double, double, unsigned>> user_latency;

    map<string, double> user_throughput;

    map<Key, std::pair<double, unsigned>> latency_miss_ratio_map;

    vector<Address> routing_ips;

    MonitoringThread mt = MonitoringThread(ip);

    zmq::context_t context(1);
    SocketCache pushers(&context, ZMQ_PUSH);

    // responsible for listening to the response of the replication factor change request
    zmq::socket_t response_puller(context, ZMQ_PULL);
    int timeout = 10000;
    //int timeout = -1;

    response_puller.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
    response_puller.bind(mt.response_bind_address());

    // keep track of departing node status
    map<Address, unsigned> departing_node_map;

    // responsible for both node join and departure
    zmq::socket_t notify_puller(context, ZMQ_PULL);
    notify_puller.bind(mt.notify_bind_address());

    // responsible for receiving depart done notice
    zmq::socket_t depart_done_puller(context, ZMQ_PULL);
    depart_done_puller.bind(mt.depart_done_bind_address());

    // responsible for receiving feedback from users
    zmq::socket_t feedback_puller(context, ZMQ_PULL);
    feedback_puller.bind(mt.feedback_report_bind_address());

    // responsible for receiving internal stats from memory nodes
    zmq::socket_t internal_stat_puller(context, ZMQ_PULL);
    internal_stat_puller.bind(mt.internal_stat_report_bind_address());

    vector<zmq::pollitem_t> pollitems = {
        {static_cast<void *>(notify_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(depart_done_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(feedback_puller), 0, ZMQ_POLLIN, 0},
        {static_cast<void *>(internal_stat_puller), 0, ZMQ_POLLIN, 0}};

    auto report_start = std::chrono::system_clock::now();
    auto report_end = std::chrono::system_clock::now();

    auto grace_start = std::chrono::system_clock::now();

    unsigned new_memory_count = 0;
    unsigned new_storage_count = 0;
    bool removing_memory_node = false;
    bool removing_storage_node = false;

    unsigned server_monitoring_epoch = 0;

    unsigned rid = 0;

    while (true) {
        kZmqUtil->poll(0, &pollitems);

        if (pollitems[0].revents & ZMQ_POLLIN) {
            string serialized = kZmqUtil->recv_string(&notify_puller);
            membership_handler(log, serialized, global_hash_rings, new_memory_count,
            new_storage_count, grace_start, routing_ips, memory_consumption, storage_consumption,
            memory_occupancy, storage_occupancy, key_access_frequency);
        }

        if (pollitems[1].revents & ZMQ_POLLIN) {
            string serialized = kZmqUtil->recv_string(&depart_done_puller);
            depart_done_handler(log, serialized, departing_node_map, management_ip,
            removing_memory_node, removing_storage_node, pushers, grace_start);
        }

        if (pollitems[2].revents & ZMQ_POLLIN) {
            string serialized = kZmqUtil->recv_string(&feedback_puller);
            feedback_handler(log, serialized, user_latency, user_throughput,
                             latency_miss_ratio_map, kBenchmarkNodeCount,
                             kBenchmarkThreadCount);
        }

        if (pollitems[3].revents & ZMQ_POLLIN) {
            string serialized = kZmqUtil->recv_string(&internal_stat_puller);
            collect_internal_stats(log, serialized, rid, key_access_frequency, key_size,
                    memory_consumption, storage_consumption, value_cache_sizes,
                    value_cache_hits, shortcut_cache_hits, local_log_hits, cache_misses, memory_occupancy,
                    storage_occupancy, memory_accesses, storage_accesses, kvs_avg_latencies);
        }

        report_end = std::chrono::system_clock::now();
        auto elapsed_time = std::chrono::duration_cast<std::chrono::seconds>(report_end - report_start).count();

        if (elapsed_time >= kMonitoringThreshold) {
            server_monitoring_epoch += 1;

            memory_node_count = global_hash_rings[Tier::MEMORY].size() / kVirtualThreadNum;
            storage_node_count = global_hash_rings[Tier::STORAGE].size() / kVirtualThreadNum;

            ss.clear();

            if (kEnablePerfMonitor) {
                //collect_internal_stats(global_hash_rings, local_hash_rings, pushers, mt, response_puller,
                //        log, rid, key_access_frequency, key_size, memory_consumption, storage_consumption, value_cache_sizes,
                //        value_cache_hits, shortcut_cache_hits, local_log_hits, cache_misses, memory_occupancy,
                //        storage_occupancy, memory_accesses, storage_accesses, kvs_avg_latencies);

                compute_summary_stats(key_access_frequency, memory_consumption, storage_consumption, value_cache_sizes,
                        value_cache_hits, shortcut_cache_hits, local_log_hits, cache_misses, memory_occupancy,
                        storage_occupancy, memory_accesses, storage_accesses, key_access_summary, ss, log, 
                        server_monitoring_epoch, kvs_avg_latencies, kServerReportPeriod);

                log->info("Aggregated value cache ratio = {}", (double)((double)ss.total_value_cache_size / (double)(kMemoryNodeCapacity * (uint64_t)memory_node_count)));
            }

            collect_external_stats(user_latency, user_throughput, ss, log, elapsed_time);

            // initialize replication factor for new keys
            for (const auto &key_access_pair : key_access_summary) {
                Key key = key_access_pair.first;
                if (!is_metadata(key) && key_replication_map.find(key) == key_replication_map.end()) {
                    init_replication(key_replication_map, key);
                }
            }

#ifndef ENABLE_CLOVER_KVS
#ifdef ENABLE_DINOMO_KVS
            //if (kEnableSelectiveRep) {
            //    // Zero out the counts of non-accessed keys within monitoring thresholds
            //    for (const auto &key_replication_pair : key_replication_map) {
            //        Key key = key_replication_pair.first;
            //        if (!is_metadata(key) && key_access_summary.find(key) == key_access_summary.end()) {
            //            key_access_summary[key] = 0;
            //        }
            //    }
            //}
#endif

#ifndef ENABLE_DINOMO_KVS
            storage_policy(log, global_hash_rings, grace_start, ss, memory_node_count,
                           storage_node_count, new_memory_count, new_storage_count,
                           removing_storage_node, management_ip, mt, departing_node_map,
                           pushers);
#endif

            movement_policy(log, global_hash_rings, local_hash_rings, grace_start, ss,
                            memory_node_count, storage_node_count, new_memory_count,
                            new_storage_count, management_ip, key_replication_map,
                            key_access_summary, key_size, mt, pushers,
                            response_puller, routing_ips, rid);

            slo_policy(log, global_hash_rings, local_hash_rings, grace_start, ss,
                       memory_node_count, new_memory_count, removing_memory_node,
                       management_ip, key_replication_map, key_access_summary, mt,
                       departing_node_map, pushers, response_puller, routing_ips, rid,
                       latency_miss_ratio_map);
#endif
            key_access_frequency.clear();
            key_access_summary.clear();

            memory_consumption.clear();
            storage_consumption.clear();

            value_cache_sizes.clear();
            value_cache_hits.clear();
            shortcut_cache_hits.clear();
            local_log_hits.clear();
            cache_misses.clear();

            memory_occupancy.clear();
            storage_occupancy.clear();

            memory_accesses.clear();
            storage_accesses.clear();

            kvs_avg_latencies.clear();

            user_latency.clear();
            user_throughput.clear();
            latency_miss_ratio_map.clear();

            report_start = std::chrono::system_clock::now();
        }
    }
}