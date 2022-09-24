#include "monitor/monitoring_utils.hpp"
#include "requests.hpp"
#include "monitor/policies.hpp"

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
    KvsStats &kvs_avg_latencies)
{
    map<Address, KeyRequest> addr_request_map;
    //uint64_t sum_num_working = 0, sum_num_idle = 0;
    //double avg_cpu_util_user_req = 0.0;

    for (const Tier &tier : kAllTiers)
    {
        GlobalHashRing hash_ring = global_hash_rings[tier];

        for (const ServerThread &st : hash_ring.get_unique_servers())
        {
            for (unsigned i = 0; i < kTierMetadata[tier].thread_number_; i++)
            {
                Key key = get_metadata_key(st, tier, i, MetadataType::server_stats);
                prepare_metadata_get_request(key, global_hash_rings[Tier::MEMORY],
                                             local_hash_rings[Tier::MEMORY],
                                             addr_request_map,
                                             mt.response_connect_address(), rid);

                if (kEnableSelectiveRep) {
                    key = get_metadata_key(st, tier, i, MetadataType::key_access);
                    prepare_metadata_get_request(key, global_hash_rings[Tier::MEMORY],
                            local_hash_rings[Tier::MEMORY],
                            addr_request_map,
                            mt.response_connect_address(), rid);
                }

#ifndef ENABLE_DINOMO_KVS
                key = get_metadata_key(st, tier, i, MetadataType::key_size);
                prepare_metadata_get_request(key, global_hash_rings[Tier::MEMORY],
                                             local_hash_rings[Tier::MEMORY],
                                             addr_request_map,
                                             mt.response_connect_address(), rid);
#endif
            }
        }
    }

    for (const auto &addr_request_pair : addr_request_map)
    {
        bool succeed;
        auto res = make_request<KeyRequest, KeyResponse>(
            addr_request_pair.second, pushers[addr_request_pair.first],
            response_puller, succeed);

        if (succeed)
        {
            for (const KeyTuple &tuple : res.tuples())
            {
                if (tuple.error() == 0)
                {
                    vector<string> tokens = split_metadata_key(tuple.key());

                    string metadata_type = tokens[1];
                    Address ip_pair = tokens[2] + "/" + tokens[3];
                    unsigned tid = stoi(tokens[4]);
                    Tier tier;
                    Tier_Parse(tokens[5], &tier);

                    LWWValue lww_value;
                    lww_value.ParseFromString(tuple.payload());

                    if (metadata_type == "stats")
                    {
                        // deserialize the value
                        ServerThreadStatistics stat;
                        stat.ParseFromString(lww_value.value());

                        if (tier == Tier::MEMORY)
                        {
                            memory_consumption[ip_pair][tid] = stat.storage_consumption();
                            memory_occupancy[ip_pair][tid] =
                                std::pair<double, unsigned>(stat.occupancy(), stat.epoch());
                            memory_accesses[ip_pair][tid] = stat.access_count();
                            value_cache_sizes[ip_pair][tid] = stat.value_cache_size();
                            value_cache_hits[ip_pair][tid] = stat.value_cache_hit_count();
                            shortcut_cache_hits[ip_pair][tid] = stat.shortcut_cache_hit_count();
                            local_log_hits[ip_pair][tid] = stat.local_log_hit_count();
                            cache_misses[ip_pair][tid] = stat.cache_miss_count();
                            kvs_avg_latencies[ip_pair][tid] = stat.kvs_avg_latency();
                            //sum_num_working += stat.num_working();
                            //sum_num_idle += stat.num_idle();
                            //log->info("Memory node {} thread {} utilization is {}", ip_pair, tid,
                            //        (double)stat.num_working() / (double)(stat.num_working() + stat.num_idle()));
                        }
                        else
                        {
                            storage_consumption[ip_pair][tid] = stat.storage_consumption();
                            storage_occupancy[ip_pair][tid] =
                                std::pair<double, unsigned>(stat.occupancy(), stat.epoch());
                            storage_accesses[ip_pair][tid] = stat.access_count();
                        }
                    }
                    else if (metadata_type == "access")
                    {
                        // deserialized the value
                        KeyAccessData access;
                        access.ParseFromString(lww_value.value());

                        for (const auto &key_count : access.keys())
                        {
                            Key key = key_count.key();
                            key_access_frequency[key][ip_pair + ":" + std::to_string(tid)] =
                                key_count.access_count();
                        }
                    }
                    else if (metadata_type == "size")
                    {
                        // deserialized the size
                        KeySizeData key_size_msg;
                        key_size_msg.ParseFromString(lww_value.value());

                        for (const auto &key_size_tuple : key_size_msg.key_sizes())
                        {
                            key_size[key_size_tuple.key()] = key_size_tuple.size();
                        }
                    }
                }
                else if (tuple.error() == 1)
                {
                    log->error("Key {} doesn't exist.", tuple.key());
                }
                else
                {
                    // The hash ring should never be inconsistent.
                    log->error("Hash ring is inconsistent for key {}.", tuple.key());
                }
            }
        }
        else
        {
            log->error("Request timed out.");
            continue;
        }
    }

    //avg_cpu_util_user_req = (double)((double)sum_num_working / (double)(sum_num_working + sum_num_idle));
    //log->info("Average CPU utilization to handle user requests = {}", avg_cpu_util_user_req);
}
#else
void collect_internal_stats(logger log, string &serialized, unsigned &rid,
    map<Key, map<Address, unsigned> > &key_access_frequency,
    map<Key, unsigned> &key_size, StorageStats &memory_consumption,
    StorageStats &storage_consumption, CacheStats &value_cache_sizes,
    CacheStats &value_cache_hits, CacheStats &shortcut_cache_hits,
    CacheStats &local_log_hits, CacheStats &cache_misses,
    OccupancyStats &memory_occupancy, OccupancyStats &storage_occupancy,
    AccessStats &memory_accesses, AccessStats &storage_accesses,
    KvsStats &kvs_avg_latencies)
{
    KeyRequest request;
    request.ParseFromString(serialized);
    //uint64_t sum_num_working = 0, sum_num_idle = 0;
    //double avg_cpu_util_user_req = 0.0;

    for (const KeyTuple &tuple : request.tuples())
    {
        if (tuple.error() == 0)
        {
            vector<string> tokens = split_metadata_key(tuple.key());

            string metadata_type = tokens[1];
            Address ip_pair = tokens[2] + "/" + tokens[3];
            unsigned tid = stoi(tokens[4]);
            Tier tier;
            Tier_Parse(tokens[5], &tier);

            LWWValue lww_value;
            lww_value.ParseFromString(tuple.payload());

            if (metadata_type == "stats")
            {
                // deserialize the value
                ServerThreadStatistics stat;
                stat.ParseFromString(lww_value.value());

                if (tier == Tier::MEMORY)
                {
                    memory_consumption[ip_pair][tid] = stat.storage_consumption();
                    memory_occupancy[ip_pair][tid] =
                        std::pair<double, unsigned>(stat.occupancy(), stat.epoch());
                    memory_accesses[ip_pair][tid] = stat.access_count();
                    value_cache_sizes[ip_pair][tid] = stat.value_cache_size();
                    value_cache_hits[ip_pair][tid] = stat.value_cache_hit_count();
                    shortcut_cache_hits[ip_pair][tid] = stat.shortcut_cache_hit_count();
                    local_log_hits[ip_pair][tid] = stat.local_log_hit_count();
                    cache_misses[ip_pair][tid] = stat.cache_miss_count();
                    kvs_avg_latencies[ip_pair][tid] = stat.kvs_avg_latency();
                    //sum_num_working += stat.num_working();
                    //sum_num_idle += stat.num_idle();
                    //log->info("Memory node {} thread {} utilization is {}", ip_pair, tid,
                    //        (double)stat.num_working() / (double)(stat.num_working() + stat.num_idle()));
                }
                else
                {
                    storage_consumption[ip_pair][tid] = stat.storage_consumption();
                    storage_occupancy[ip_pair][tid] =
                        std::pair<double, unsigned>(stat.occupancy(), stat.epoch());
                    storage_accesses[ip_pair][tid] = stat.access_count();
                }
            }
            else if (metadata_type == "access")
            {
                // deserialized the value
                KeyAccessData access;
                access.ParseFromString(lww_value.value());

                for (const auto &key_count : access.keys())
                {
                    Key key = key_count.key();
                    key_access_frequency[key][ip_pair + ":" + std::to_string(tid)] =
                        key_count.access_count();
                }
            }
            else if (metadata_type == "size")
            {
                // deserialized the size
                KeySizeData key_size_msg;
                key_size_msg.ParseFromString(lww_value.value());

                for (const auto &key_size_tuple : key_size_msg.key_sizes())
                {
                    key_size[key_size_tuple.key()] = key_size_tuple.size();
                }
            }
        }
        else if (tuple.error() == 1)
        {
            log->error("Key {} doesn't exist.", tuple.key());
        }
        else
        {
            // The hash ring should never be inconsistent.
            log->error("Hash ring is inconsistent for key {}.", tuple.key());
        }
    }

    //avg_cpu_util_user_req = (double)((double)sum_num_working / (double)(sum_num_working + sum_num_idle));
    //log->info("Average CPU utilization to handle user requests = {}", avg_cpu_util_user_req);
}
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
    KvsStats &kvs_avg_latencies, unsigned kServerReportPeriod)
{
    // compute key access summary
    unsigned cnt = 0;
    double mean = 0;
    double ms = 0;

    for (const auto &key_access_pair : key_access_frequency)
    {
        Key key = key_access_pair.first;
        unsigned access_count = 0;

        for (const auto &per_machine_pair : key_access_pair.second)
        {
            access_count += per_machine_pair.second;
        }

        key_access_summary[key] = access_count;

        if (access_count > 0)
        {
            cnt += 1;

            double delta = access_count - mean;
            mean += (double)delta / cnt;

            double delta2 = access_count - mean;
            ms += delta * delta2;
        }
    }

    ss.key_access_mean = mean;
    ss.key_access_std = sqrt((double)ms / cnt);

    log->info("Access: mean={}, std={}", ss.key_access_mean, ss.key_access_std);

    // compute tier access summary
    cnt = 0;
    mean = 0;
    ms = 0;
    unsigned m_max_access_count = 0;
    unsigned m_min_access_count = UINT_MAX;
    for (const auto &accesses : memory_accesses)
    {
        for (const auto &thread_access : accesses.second)
        {
            ss.total_memory_access += thread_access.second;

            if (m_max_access_count < thread_access.second)
                m_max_access_count = thread_access.second;
            if (m_min_access_count > thread_access.second)
                m_min_access_count = thread_access.second;

            cnt += 1;
        }
    }

    mean = (double)((double)ss.total_memory_access / (double)cnt);

    for (const auto &accesses : memory_accesses)
    {
        for (const auto &thread_access : accesses.second)
        {
            double delta = (double)((double)thread_access.second - mean);
            ms += (delta * delta);
        }
    }

    for (const auto &access : storage_accesses)
    {
        for (const auto &thread_access : access.second)
        {
            ss.total_storage_access += thread_access.second;
        }
    }

    log->info("Total accesses: memory = {} storage = {}", ss.total_memory_access,
              ss.total_storage_access);
    // CV (Coefficient of Variance or Normalized std)
    log->info("Memory access: mean = {}, std = {}, cv = {}", mean,
            sqrt((double)ms / cnt), sqrt((double)ms / cnt) / mean);
    log->info("Memory access ratio: max = {} min = {}",
            (double)((double)m_max_access_count / (double)ss.total_memory_access),
            (double)((double)m_min_access_count / (double)ss.total_memory_access));
    log->info("Average kvs throughput: memory = {} storage = {}", 
            ss.total_memory_access / kServerReportPeriod,
              ss.total_storage_access / kServerReportPeriod);

    // compute storage consumption related statistics
    cnt = 0;
    unsigned m_count = 0;
    unsigned e_count = 0;

    for (const auto &memory_consumption : memory_consumption)
    {
        unsigned total_thread_consumption = 0;

        for (const auto &thread_storage : memory_consumption.second)
        {
            ss.total_memory_consumption += thread_storage.second;
            total_thread_consumption += thread_storage.second;
            cnt += 1;
        }

        double percentage = (double)total_thread_consumption /
                            (double)kTierMetadata[Tier::MEMORY].node_capacity_;
        log->info("Memory node {} storage consumption is {}", memory_consumption.first,
                  percentage);

        if (percentage > ss.max_memory_consumption_percentage)
        {
            ss.max_memory_consumption_percentage = percentage;
        }

        m_count += 1;
    }

    for (const auto &storage_consumption : storage_consumption)
    {
        unsigned total_thread_consumption = 0;

        for (const auto &thread_storage : storage_consumption.second)
        {
            ss.total_storage_consumption += thread_storage.second;
            total_thread_consumption += thread_storage.second;
        }

        double percentage = (double)total_thread_consumption /
                            (double)kTierMetadata[Tier::STORAGE].node_capacity_;
        log->info("storage node {} storage consumption is {}", storage_consumption.first,
                  percentage);

        if (percentage > ss.max_storage_consumption_percentage)
        {
            ss.max_storage_consumption_percentage = percentage;
        }
        e_count += 1;
    }

    if (m_count != 0)
    {
        ss.avg_memory_consumption_percentage =
            (double)ss.total_memory_consumption /
            ((double)m_count * kTierMetadata[Tier::MEMORY].node_capacity_);
        log->info("Average memory node consumption is {}",
                  ss.avg_memory_consumption_percentage);
        log->info("Max memory node consumption is {}",
                  ss.max_memory_consumption_percentage);
        log->info("Average cache miss cost (RTs) is {}",
                (double)((double)ss.total_memory_consumption / (double)cnt));
    }

    if (e_count != 0)
    {
        ss.avg_storage_consumption_percentage =
            (double)ss.total_storage_consumption /
            ((double)e_count * kTierMetadata[Tier::STORAGE].node_capacity_);
        log->info("Average storage node consumption is {}",
                  ss.avg_storage_consumption_percentage);
        log->info("Max storage node consumption is {}",
                  ss.max_storage_consumption_percentage);
    }

    ss.required_memory_node = ceil(
        ss.total_memory_consumption /
        (kMaxMemoryNodeConsumption * kTierMetadata[Tier::MEMORY].node_capacity_));
    ss.required_storage_node =
        ceil(ss.total_storage_consumption /
             (kMaxStorageNodeConsumption * kTierMetadata[Tier::STORAGE].node_capacity_));

    log->info("The system requires {} new memory nodes.",
              ss.required_memory_node);
    log->info("The system requires {} new storage nodes.", ss.required_storage_node);

    // compute cache related statistics
    for (const auto &value_cache_size : value_cache_sizes)
    {
        for (const auto &thread_cache : value_cache_size.second)
        {
            ss.total_value_cache_size += thread_cache.second;
        }
    }

    for (const auto &value_cache_hit : value_cache_hits)
    {
        for (const auto &thread_cache_hits : value_cache_hit.second)
        {
            ss.total_value_cache_hits += thread_cache_hits.second;
        }
    }

    for (const auto &shortcut_cache_hit : shortcut_cache_hits)
    {
        for (const auto &thread_cache_hits : shortcut_cache_hit.second)
        {
            ss.total_shortcut_cache_hits += thread_cache_hits.second;
        }
    }

    for (const auto &local_log_hit : local_log_hits)
    {
        for (const auto &thread_log_hits : local_log_hit.second)
        {
            ss.total_local_log_hits += thread_log_hits.second;
        }
    }

    for (const auto &cache_miss : cache_misses)
    {
        for (const auto &thread_cache_misses : cache_miss.second)
        {
            ss.total_cache_misses += thread_cache_misses.second;
        }
    }

    log->info("Cache hit rate = {}", (double)((double)(ss.total_value_cache_hits + ss.total_shortcut_cache_hits) /
                (double)(ss.total_value_cache_hits + ss.total_shortcut_cache_hits + ss.total_cache_misses)));
    log->info("Value cache hit ratio = {}", (double)((double)(ss.total_value_cache_hits) /
                (double)(ss.total_value_cache_hits + ss.total_shortcut_cache_hits)));
    log->info("Num of local log hits = {}", ss.total_local_log_hits);

    // compute occupancy related statistics
    double sum_memory_occupancy = 0.0;

    unsigned count = 0;

    for (const auto &memory_occ : memory_occupancy)
    {
        double sum_thread_occupancy = 0.0;
        unsigned thread_count = 0;

        for (const auto &thread_occ : memory_occ.second)
        {
            //log->info(
            //    "Memory node {} thread {} occupancy is {} at epoch {} (monitoring "
            //    "epoch {}).",
            //    memory_occ.first, thread_occ.first, thread_occ.second.first,
            //    thread_occ.second.second, server_monitoring_epoch);

            sum_thread_occupancy += thread_occ.second.first;
            thread_count += 1;
        }

        double node_occupancy = sum_thread_occupancy / thread_count;
        sum_memory_occupancy += node_occupancy;

        if (node_occupancy > ss.max_memory_occupancy)
        {
            ss.max_memory_occupancy = node_occupancy;
        }

        if (node_occupancy < ss.min_memory_occupancy)
        {
            ss.min_memory_occupancy = node_occupancy;
            vector<string> ips;
            split(memory_occ.first, '/', ips);
            ss.min_occupancy_memory_public_ip = ips[0];
            ss.min_occupancy_memory_private_ip = ips[1];
        }

        count += 1;
    }

    ss.avg_memory_occupancy = sum_memory_occupancy / count;
    log->info("Max memory node occupancy is {}", ss.max_memory_occupancy);
    log->info("Min memory node occupancy is {}", ss.min_memory_occupancy);
    log->info("Average memory node occupancy is {}", ss.avg_memory_occupancy);

    double sum_storage_occupancy = 0.0;

    count = 0;

    for (const auto &storage_occ : storage_occupancy)
    {
        double sum_thread_occupancy = 0.0;
        unsigned thread_count = 0;

        for (const auto &thread_occ : storage_occ.second)
        {
            //log->info(
            //    "storage node {} thread {} occupancy is {} at epoch {} (monitoring epoch "
            //    "{}).",
            //    storage_occ.first, thread_occ.first, thread_occ.second.first,
            //    thread_occ.second.second, server_monitoring_epoch);

            sum_thread_occupancy += thread_occ.second.first;
            thread_count += 1;
        }

        double node_occupancy = sum_thread_occupancy / thread_count;
        sum_storage_occupancy += node_occupancy;

        if (node_occupancy > ss.max_storage_occupancy)
        {
            ss.max_storage_occupancy = node_occupancy;
        }

        if (node_occupancy < ss.min_storage_occupancy)
        {
            ss.min_storage_occupancy = node_occupancy;
        }

        count += 1;
    }

    ss.avg_storage_occupancy = sum_storage_occupancy / count;
    log->info("Max storage node occupancy is {}", ss.max_storage_occupancy);
    log->info("Min storage node occupancy is {}", ss.min_storage_occupancy);
    log->info("Average storage node occupancy is {}", ss.avg_storage_occupancy);

    count = 0;
    double sum_kvs_latencies = 0;
    for (const auto &kvs_avg_latency : kvs_avg_latencies)
    {
        for (const auto &thread_kvs_latencies : kvs_avg_latency.second)
        {
            sum_kvs_latencies += thread_kvs_latencies.second;
            count += 1;
        }
    }

    ss.avg_kvs_latency = sum_kvs_latencies / count;
    log->info("Average kvs latency is {}", ss.avg_kvs_latency);
}

void collect_external_stats(map<string, std::tuple<double, double, double, double, double, unsigned>> &user_latency,
                            map<string, double> &user_throughput,
                            SummaryStats &ss, logger log, unsigned elapsed_time)
{
    // gather latency info
    if (user_latency.size() > 0)
    {
        // compute latency from users
        double sum_avg_latency = 0;
        double sum_tail_latency = 0;
        double sum_median_latency = 0;
        double sum_min_latency = 0;
        double sum_max_latency = 0;
        unsigned count = 0;

        for (const auto &latency_pair : user_latency)
        {
            sum_avg_latency += (std::get<0>(latency_pair.second) / (double)std::get<5>(latency_pair.second));
            sum_tail_latency += (std::get<1>(latency_pair.second) / (double)std::get<5>(latency_pair.second));
            sum_median_latency += (std::get<2>(latency_pair.second) / (double)std::get<5>(latency_pair.second));
            sum_min_latency += (std::get<3>(latency_pair.second) / (double)std::get<5>(latency_pair.second));
            sum_max_latency += (std::get<4>(latency_pair.second) / (double)std::get<5>(latency_pair.second));
            count += 1;
        }

        ss.avg_latency = sum_avg_latency / count;
        ss.tail_latency = sum_tail_latency / count;
        ss.median_latency = sum_median_latency / count;
        ss.min_latency = sum_min_latency / count;
        ss.max_latency = sum_max_latency / count;
    }

    log->info("Average latency is {}", ss.avg_latency);
    log->info("Average tail latency is {}", ss.tail_latency); 
    log->info("Average median latency is {}", ss.median_latency); 
    log->info("Average min latency is {}", ss.min_latency); 
    log->info("Average max latency is {}", ss.max_latency); 

    // gather throughput info
    if (user_throughput.size() > 0)
    {
        // compute latency from users
        for (const auto &thruput_pair : user_throughput)
        {
            ss.total_throughput += thruput_pair.second;
        }
    }
    //ss.total_throughput = (ss.total_throughput / (double)elapsed_time);

    log->info("Total throughput is {}", ss.total_throughput);
}
