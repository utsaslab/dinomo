#include <stdlib.h>

#include "benchmark.pb.h"
#include "client/kvs_client.hpp"
#include "kvs_threads.hpp"
#include "yaml-cpp/yaml.h"

unsigned kBenchmarkThreadNum;
unsigned kBenchmarkNodeNum;
unsigned kRoutingThreadCount;
unsigned kDefaultLocalReplication;

bool kEnableReconfig;
unsigned kClientTimeout;
int kRespPullerHWM = 0;
int kRespPullerBacklog = 100;

#ifdef BENCH_CACHE_ROUTING
Tier kSelfTier;
vector<Tier> kSelfTierIdVector;

unsigned kMemoryThreadCount;
unsigned kStorageThreadCount;

uint64_t kMemoryNodeCapacity;
uint64_t kStorageNodeCapacity;

unsigned kDefaultGlobalMemoryReplication;
unsigned kDefaultGlobalStorageReplication;

HashRingUtil hash_ring_util;
HashRingUtilInterface *kHashRingUtil = &hash_ring_util;
#endif

#define BENCH_HOST_NAME_MAX             255
unsigned node_id;

ZmqUtil zmq_util;
ZmqUtilInterface *kZmqUtil = &zmq_util;

double get_base(unsigned N, double skew) {
    double base = 0;
    for (unsigned k = 1; k <= N; k++) {
        base += pow(k, -1 * skew);
    }
    return (1 / base);
}

double get_zipf_prob(unsigned rank, double skew, double base) {
    return pow(rank, -1 * skew) / base;
}

void receive(KvsClientInterface *client) {
    vector<KeyResponse> responses = client->receive_async();
    while (responses.size() == 0) {
        responses = client->receive_async();
    }
}

vector<KeyResponse> receive2(KvsClientInterface *client, double &key_latency) {
    vector<KeyResponse> responses = client->receive_async2(key_latency);
    while (responses.size() == 0) {
        responses = client->receive_async2(key_latency);
    }
    return responses;
}

int sample(int n, unsigned &seed, double base,
        vector<double> &sum_probs) {
    double z;           // Uniform random number (0 < z < 1)
    int zipf_value;     // Computed exponential value to be returned
    int i;              // Loop counter
    int low, high, mid; // Binary-search bounds

    // Pull a uniform random number (0 < z < 1)
    do {
        z = rand_r(&seed) / static_cast<double>(RAND_MAX);
    } while ((z == 0) || (z == 1));

    // Map z to the value
    low = 1, high = n;

    do {
        mid = floor((low + high) / 2);
        if (sum_probs[mid] >= z && sum_probs[mid - 1] < z) {
            zipf_value = mid;
            break;
        } else if (sum_probs[mid] >= z) {
            high = mid - 1;
        } else {
            low = mid + 1;
        }
    } while (low <= high);

    // Assert that zipf_value is between 1 and N
    assert((zipf_value >= 1) && (zipf_value <= n));

    return zipf_value;
}

string generate_key(unsigned n) {
    return std::to_string(n);
    //return string(8 - std::to_string(n).length(), '0') + std::to_string(n);
}

void run(const unsigned &thread_id,
        const vector<UserRoutingThread> &routing_threads,
        const vector<MonitoringThread> &monitoring_threads,
        const Address &ip) {
    KvsClient client(routing_threads, ip, thread_id, kClientTimeout,
            kEnableReconfig, kRespPullerHWM, kRespPullerBacklog);
    string log_file = "log_" + std::to_string(thread_id) + ".txt";
    string logger_name = "benchmark_log_" + std::to_string(thread_id);
    auto log = spdlog::basic_logger_mt(logger_name, log_file, true);
    log->flush_on(spdlog::level::info);

    client.set_logger(log);
    unsigned seed = client.get_seed();

    // observed per-key avg latency
    map<Key, std::pair<double, unsigned>> observed_latency;

    // key map for requests used for RUN phase
    vector<string> key_reqs_map;

    // responsible for pulling benchmark commands
    zmq::context_t &context = *(client.get_context());
    SocketCache pushers(&context, ZMQ_PUSH);
    zmq::socket_t command_puller(context, ZMQ_PULL);
    command_puller.bind("tcp://*:" +
            std::to_string(thread_id + kBenchmarkCommandPort));

    vector<zmq::pollitem_t> pollitems = {
        {static_cast<void *>(command_puller), 0, ZMQ_POLLIN, 0}};

    while (true) {
        kZmqUtil->poll(-1, &pollitems);

        if (pollitems[0].revents & ZMQ_POLLIN) {
            string msg = kZmqUtil->recv_string(&command_puller);
            log->info("Received benchmark command: {}", msg);

            vector<string> v;
            split(msg, ':', v);
            string mode = v[0];

            if (mode == "WARM") {
#ifdef SINGLE_OUTSTANDING
                unsigned num_keys = stoi(v[1]);
                unsigned length = stoi(v[2]);
                unsigned num_reqs = stoi(v[3]);
                double zipf = stod(v[4]);

                vector<double> sum_probs;
                sum_probs.reserve(num_keys);
                double base;

                if (zipf > 0) {
                    log->info("Zipf coefficient is {}.", zipf);
                    base = get_base(num_keys, zipf);
                    sum_probs[0] = 0;

                    for (unsigned i = 1; i <= num_keys; i++) {
                        sum_probs[i] = sum_probs[i - 1] + base / pow((double)i, zipf);
                    }
                } else {
                    log->info("Using a uniform random distribution.");
                }

                // warm up cache
                client.clear_cache();
                key_reqs_map.clear();
                auto warmup_start = std::chrono::system_clock::now();

                for (unsigned i = 0; i < num_reqs; i++) {
                    unsigned k;
                    if (zipf > 0) {
                        k = sample(num_keys, seed, base, sum_probs);
                    } else {
                        k = rand_r(&seed) % (num_keys) + 1;
                    }

                    Key key = generate_key(k);
                    key_reqs_map.push_back(key);

                    client.get_async(key);
                    receive(&client);

                    if (i % 50000 == 0 && i != 0) {
                        log->info("Warming up cache with {} keys.", i);
                    }
                }

                auto warmup_time = std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::system_clock::now() - warmup_start)
                    .count();
                log->info("Cache warm-up took {} seconds.", warmup_time);

                UserFeedback feedback;
                feedback.set_uid(ip + ":" + std::to_string(thread_id) + ":" + "WARM");
                feedback.set_finish(true);

                string serialized_feedback;
                feedback.SerializeToString(&serialized_feedback);

                for (const MonitoringThread &thread : monitoring_threads) {
                    kZmqUtil->send_string(
                            serialized_feedback,
                            &pushers[thread.feedback_report_connect_address()]);
                }
#else
                unsigned num_keys = stoi(v[1]);
                unsigned length = stoi(v[2]);
                unsigned num_reqs = stoi(v[3]);
                double zipf = stod(v[4]);

                vector<double> sum_probs;
                sum_probs.reserve(num_keys);
                double base;

                if (zipf > 0) {
                    log->info("Zipf coefficient is {}.", zipf);
                    base = get_base(num_keys, zipf);
                    sum_probs[0] = 0;

                    for (unsigned i = 1; i <= num_keys; i++) {
                        sum_probs[i] = sum_probs[i - 1] + base / pow((double)i, zipf);
                    }
                } else {
                    log->info("Using a uniform random distribution.");
                }

                // warm up cache
                client.clear_cache();
                key_reqs_map.clear();
                vector<KeyResponse> responses;
                uint64_t num_requests = 0, num_responses = 0;
                auto warmup_start = std::chrono::system_clock::now();

                for (unsigned i = 0; i < num_reqs; i++) {
                    unsigned k;
                    if (zipf > 0) {
                        k = sample(num_keys, seed, base, sum_probs);
                    } else {
                        k = rand_r(&seed) % (num_keys) + 1;
                    }

                    Key key = generate_key(k);
                    key_reqs_map.push_back(key);
                }

                for (const auto key : key_reqs_map) {
                    client.get_async(key);
                    num_requests++;

                    responses = client.receive_async3();
                    num_responses += responses.size();
                }

                while (num_requests != num_responses) {
                    responses = client.receive_async3();
                    num_responses += responses.size();
                }

                auto warmup_time = std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::system_clock::now() - warmup_start)
                    .count();
                log->info("Cache warm-up took {} seconds.", warmup_time);

                UserFeedback feedback;
                feedback.set_uid(ip + ":" + std::to_string(thread_id) + ":" + "WARM");
                feedback.set_finish(true);

                string serialized_feedback;
                feedback.SerializeToString(&serialized_feedback);

                for (const MonitoringThread &thread : monitoring_threads) {
                    kZmqUtil->send_string(
                            serialized_feedback,
                            &pushers[thread.feedback_report_connect_address()]);
                }
#endif
            } else if (mode == "WARM-RUN") {
#ifdef SINGLE_OUTSTANDING
                string type;
                double read_ratio = stod(v[1]);
                unsigned length = stoi(v[2]);
                unsigned report_period = stoi(v[3]);
                unsigned time = stoi(v[4]);

                auto kit = key_reqs_map.begin();

                vector<string> req_ratio_map;
                for (double i = 0; i < ((double)100 * read_ratio); i++)
                    req_ratio_map.push_back("G");
                for (double i = 0; i < ((double)100 * ((double)1 - read_ratio)); i++)
                    req_ratio_map.push_back("U");
                auto it = req_ratio_map.begin();

                size_t count = 0;
                auto benchmark_start = std::chrono::system_clock::now();
                auto benchmark_end = std::chrono::system_clock::now();
                auto epoch_start = std::chrono::system_clock::now();
                auto epoch_end = std::chrono::system_clock::now();
                auto total_time = std::chrono::duration_cast<std::chrono::seconds>(
                        benchmark_end - benchmark_start)
                    .count();
                unsigned epoch = 1;

                observed_latency.clear();
                std::vector<double> latency_map;
                double latency_sum = 0;

                unsigned ts = generate_timestamp(thread_id);
                LWWPairLattice<string> val(
                        TimestampValuePair<string>(ts, string(length, 'a')));
                string value = serialize(val);

                while (true) {
                    double key_latency = 0;

                    Key key = *kit;
                    if (++kit == key_reqs_map.end())
                        kit = key_reqs_map.begin();

                    type = *it;
                    if (++it == req_ratio_map.end())
                        it = req_ratio_map.begin();

                    if (type == "G") {
                        auto req_start = std::chrono::system_clock::now();

                        client.get_async(key);
                        receive(&client);
                        count += 1;

                        auto req_end = std::chrono::system_clock::now();
                        key_latency = (double)std::chrono::duration_cast<std::chrono::microseconds>(
                                req_end - req_start).count();
                    } else if (type == "P") {
                        auto req_start = std::chrono::system_clock::now();

                        client.put_async(key, value, LatticeType::LWW);
                        receive(&client);
                        count += 1;

                        auto req_end = std::chrono::system_clock::now();
                        key_latency = (double)std::chrono::duration_cast<std::chrono::microseconds>(
                                req_end - req_start).count();
                    } else if (type == "U") {
                        auto req_start = std::chrono::system_clock::now();

                        client.update_async(key, value, LatticeType::LWW);
                        receive(&client);
                        count += 1;

                        auto req_end = std::chrono::system_clock::now();
                        key_latency = (double)std::chrono::duration_cast<std::chrono::microseconds>(
                                req_end - req_start).count();
                    } else if (type == "M") {
                        auto req_start = std::chrono::system_clock::now();

                        client.put_async(key, value, LatticeType::LWW);
                        receive(&client);
                        client.get_async(key);
                        receive(&client);
                        count += 2;

                        auto req_end = std::chrono::system_clock::now();
                        key_latency =
                            (double)std::chrono::duration_cast<std::chrono::microseconds>(
                                    req_end - req_start).count() / 2;
                    } else {
                        log->info("{} is an invalid request type.", type);
                    }

                    latency_map.push_back(key_latency);
                    latency_sum += key_latency;

                    if (observed_latency.find(key) == observed_latency.end()) {
                        observed_latency[key].first = key_latency;
                        observed_latency[key].second = 1;
                    } else {
                        observed_latency[key].first =
                            (observed_latency[key].first * observed_latency[key].second +
                             key_latency) /
                            (observed_latency[key].second + 1);
                        observed_latency[key].second += 1;
                    }

                    epoch_end = std::chrono::system_clock::now();
                    auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                            epoch_end - epoch_start)
                        .count();

                    // report throughput every report_period seconds
                    if (time_elapsed >= report_period) {
                        double throughput = (double)count / (double)time_elapsed;
                        log->info("[Epoch {}] Report period is {} seconds.", epoch, time_elapsed);
                        log->info("[Epoch {}] Throughput is {} ops/seconds.", epoch, throughput);
                        log->info("[Epoch {}] {} keys are accessed out of {} ops.", epoch,
                                observed_latency.size(), count);
                        epoch += 1;

                        auto avg_latency = (count > 0 ? (double)latency_sum / (double)count : (double)time_elapsed * (double)1000000);
                        double min_latency, max_latency, median_latency, tail_latency;
                        if (latency_map.size() > 0) {
                            std::sort(latency_map.begin(), latency_map.end());
                            min_latency = latency_map[0];
                            max_latency = latency_map[latency_map.size() - 1];
                            int percentile = trunc((double)latency_map.size() * 0.5);
                            median_latency = latency_map[percentile];
                            percentile = trunc((double)latency_map.size() * 0.99);
                            tail_latency = latency_map[percentile];
                        } else {
                            min_latency = avg_latency;
                            max_latency = avg_latency;
                            median_latency = avg_latency;
                            tail_latency = avg_latency;
                        }

                        log->info("[Epoch {}] Average latency {} us.", epoch, avg_latency);
                        log->info("[Epoch {}] Min latency {} us.", epoch, min_latency);
                        log->info("[Epoch {}] Max latency {} us.", epoch, max_latency);
                        log->info("[Epoch {}] Median latency {} us.", epoch, median_latency);
                        log->info("[Epoch {}] 99 tail latency {} us.", epoch, tail_latency);

                        UserFeedback feedback;
                        feedback.set_uid(ip + ":" + std::to_string(thread_id));
                        feedback.set_avg_latency(avg_latency);
                        feedback.set_min_latency(min_latency);
                        feedback.set_max_latency(max_latency);
                        feedback.set_median_latency(median_latency);
                        feedback.set_tail_latency(tail_latency);
                        //feedback.set_throughput((double)count);
                        feedback.set_throughput(throughput);

                        for (const auto &key_latency_pair : observed_latency) {
                            if (key_latency_pair.second.first > 1) {
                                UserFeedback_KeyLatency *kl = feedback.add_key_latency();
                                kl->set_key(key_latency_pair.first);
                                kl->set_latency(key_latency_pair.second.first);
                            }
                        }

                        string serialized_latency;
                        feedback.SerializeToString(&serialized_latency);

                        for (const MonitoringThread &thread : monitoring_threads) {
                            kZmqUtil->send_string(
                                    serialized_latency,
                                    &pushers[thread.feedback_report_connect_address()]);
                        }

                        count = 0;
                        latency_map.clear();
                        latency_sum = 0;
                        observed_latency.clear();
                        epoch_start = std::chrono::system_clock::now();
                    }

                    benchmark_end = std::chrono::system_clock::now();
                    total_time = std::chrono::duration_cast<std::chrono::seconds>(
                            benchmark_end - benchmark_start)
                        .count();
                    if (total_time > time) {
                        break;
                    }
                }

                log->info("Finished");
                UserFeedback feedback;

                feedback.set_uid(ip + ":" + std::to_string(thread_id));
                feedback.set_finish(true);

                string serialized_latency;
                feedback.SerializeToString(&serialized_latency);

                for (const MonitoringThread &thread : monitoring_threads) {
                    kZmqUtil->send_string(
                            serialized_latency,
                            &pushers[thread.feedback_report_connect_address()]);
                }
#else
                string type;
                double read_ratio = stod(v[1]);
                unsigned length = stoi(v[2]);
                unsigned report_period = stoi(v[3]);
                unsigned time = stoi(v[4]);
                size_t max_outstanding = stoul(v[5], nullptr, 0);
                unsigned num_keys = 0;
                unsigned is_update_only = 1;
                if (v.size() == 8) {
                    num_keys = stoi(v[6]);
                    is_update_only = stoi(v[7]);
                }

                auto kit = key_reqs_map.begin();

                vector<string> req_ratio_map;
                for (double i = 0; i < ((double)100 * read_ratio); i++)
                    req_ratio_map.push_back("G");
                for (double i = 0; i < ((double)100 * ((double)1 - read_ratio)); i++)
                    is_update_only ? req_ratio_map.push_back("U") : req_ratio_map.push_back("P");
                auto it = req_ratio_map.begin();

                unsigned num_loaded_keys = num_keys;
                size_t count = 0, outstanding = 0;
                auto benchmark_start = std::chrono::system_clock::now();
                auto benchmark_end = std::chrono::system_clock::now();
                auto epoch_start = std::chrono::system_clock::now();
                auto epoch_end = std::chrono::system_clock::now();
                auto total_time = std::chrono::duration_cast<std::chrono::seconds>(
                        benchmark_end - benchmark_start).count();
                unsigned epoch = 1, put_count = 0;

                observed_latency.clear();
                std::vector<double> latency_map;
                double latency_sum = 0;
                vector<KeyResponse> responses;

                unsigned ts = generate_timestamp(thread_id);
                LWWPairLattice<string> val(
                        TimestampValuePair<string>(ts, string(length, 'a')));
                string value = serialize(val);

                while (true) {
                    unsigned k;
                    double key_latency = 0;

                    if (outstanding < max_outstanding) {
                        Key key;
                        type = *it;
                        if (++it == req_ratio_map.end())
                            it = req_ratio_map.begin();

                        if (type == "P") {
                            k = (num_loaded_keys + 1) + ((kBenchmarkThreadNum * kBenchmarkNodeNum) * 
                                    put_count) + ((node_id * kBenchmarkThreadNum) + thread_id);
                            key = generate_key(k);

                            client.put_async(key, value, LatticeType::LWW);
                            put_count++;
                            outstanding++;
                        } else {
                            key = *kit;
                            if (++kit == key_reqs_map.end())
                                kit = key_reqs_map.begin();

                            if (type == "G") {
                                client.get_async(key);
                                outstanding++;
                            } else if (type == "U") {
                                client.update_async(key, value, LatticeType::LWW);
                                outstanding++;
                            } else if (type == "M") {
                                client.put_async(key, value, LatticeType::LWW);
                                client.get_async(key);
                                outstanding += 2;
                            } else {
                                log->info("{} is an invalid request type.", type);
                            }
                        }
                    }

                    responses = client.receive_async2(key_latency);
                    if (responses.size() > 0) {
                        count++;
                        outstanding--;

                        Key key = responses[0].tuples(0).key();
                        latency_map.push_back(key_latency);
                        latency_sum += key_latency;

                        if (observed_latency.find(key) == observed_latency.end()) {
                            observed_latency[key].first = key_latency;
                            observed_latency[key].second = 1;
                        } else {
                            observed_latency[key].first =
                                (observed_latency[key].first * observed_latency[key].second +
                                 key_latency) /
                                (observed_latency[key].second + 1);
                            observed_latency[key].second += 1;
                        }
                    }

                    epoch_end = std::chrono::system_clock::now();
                    auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                            epoch_end - epoch_start).count();

                    // report throughput every report_period seconds
                    if (time_elapsed >= report_period) {
                        log->info("[Epoch {}] Outstanding requests {}.", epoch, outstanding);

                        double throughput = (double)count / (double)time_elapsed;
                        log->info("[Epoch {}] Report period is {} seconds.", epoch, time_elapsed);
                        log->info("[Epoch {}] Throughput is {} ops/seconds.", epoch, throughput);
                        log->info("[Epoch {}] {} keys are accessed out of {} ops.", epoch,
                                observed_latency.size(), count);
                        epoch += 1;

                        auto avg_latency = (count > 0 ? (double)latency_sum / (double)count : (double)time_elapsed * (double)1000000);
                        double min_latency, max_latency, median_latency, tail_latency;
                        if (latency_map.size() > 0) {
                            std::sort(latency_map.begin(), latency_map.end());
                            min_latency = latency_map[0];
                            max_latency = latency_map[latency_map.size() - 1];
                            int percentile = trunc((double)latency_map.size() * 0.5);
                            median_latency = latency_map[percentile];
                            percentile = trunc((double)latency_map.size() * 0.99);
                            tail_latency = latency_map[percentile];
                        } else {
                            min_latency = avg_latency;
                            max_latency = avg_latency;
                            median_latency = avg_latency;
                            tail_latency = avg_latency;
                        }

                        log->info("[Epoch {}] Average latency {} us.", epoch, avg_latency);
                        log->info("[Epoch {}] Min latency {} us.", epoch, min_latency);
                        log->info("[Epoch {}] Max latency {} us.", epoch, max_latency);
                        log->info("[Epoch {}] Median latency {} us.", epoch, median_latency);
                        log->info("[Epoch {}] 99 tail latency {} us.", epoch, tail_latency);

                        UserFeedback feedback;
                        feedback.set_uid(ip + ":" + std::to_string(thread_id));
                        feedback.set_avg_latency(avg_latency);
                        feedback.set_min_latency(min_latency);
                        feedback.set_max_latency(max_latency);
                        feedback.set_median_latency(median_latency);
                        feedback.set_tail_latency(tail_latency);
                        //feedback.set_throughput((double)count);
                        feedback.set_throughput(throughput);

                        for (const auto &key_latency_pair : observed_latency) {
                            if (key_latency_pair.second.first > 1) {
                                UserFeedback_KeyLatency *kl = feedback.add_key_latency();
                                kl->set_key(key_latency_pair.first);
                                kl->set_latency(key_latency_pair.second.first);
                            }
                        }

                        string serialized_latency;
                        feedback.SerializeToString(&serialized_latency);

                        for (const MonitoringThread &thread : monitoring_threads) {
                            kZmqUtil->send_string(
                                    serialized_latency,
                                    &pushers[thread.feedback_report_connect_address()]);
                        }

                        count = 0;
                        latency_map.clear();
                        latency_sum = 0;
                        observed_latency.clear();
                        epoch_start = std::chrono::system_clock::now();
                    }

                    responses.clear();
                    benchmark_end = std::chrono::system_clock::now();
                    total_time = std::chrono::duration_cast<std::chrono::seconds>(
                            benchmark_end - benchmark_start)
                        .count();
                    if (total_time > time) {
                        while (!client.check_all_pending_requests_done()) {
                            client.receive_async2(key_latency);
                        }
                        break;
                    }
                }

                log->info("Finished");
                UserFeedback feedback;

                feedback.set_uid(ip + ":" + std::to_string(thread_id));
                feedback.set_finish(true);

                string serialized_latency;
                feedback.SerializeToString(&serialized_latency);

                for (const MonitoringThread &thread : monitoring_threads) {
                    kZmqUtil->send_string(
                            serialized_latency,
                            &pushers[thread.feedback_report_connect_address()]);
                }
#endif
            } else if (mode == "RUN") {
#ifdef SINGLE_OUTSTANDING
                string type;
                double read_ratio = stod(v[1]);
                unsigned num_keys = stoi(v[2]);
                unsigned length = stoi(v[3]);
                unsigned report_period = stoi(v[4]);
                unsigned time = stoi(v[5]);
                double zipf = stod(v[6]);

                vector<string> req_ratio_map;
                for (double i = 0; i < ((double)100 * read_ratio); i++)
                    req_ratio_map.push_back("G");
                for (double i = 0; i < ((double)100 * ((double)1 - read_ratio)); i++)
                    req_ratio_map.push_back("U");
                auto it = req_ratio_map.begin();

                vector<double> sum_probs;
                sum_probs.reserve(num_keys);
                double base;

                if (zipf > 0) {
                    log->info("Zipf coefficient is {}.", zipf);
                    base = get_base(num_keys, zipf);
                    sum_probs[0] = 0;

                    for (unsigned i = 1; i <= num_keys; i++) {
                        sum_probs[i] = sum_probs[i - 1] + base / pow((double)i, zipf);
                    }
                } else {
                    log->info("Using a uniform random distribution.");
                }

                size_t count = 0;
                auto benchmark_start = std::chrono::system_clock::now();
                auto benchmark_end = std::chrono::system_clock::now();
                auto epoch_start = std::chrono::system_clock::now();
                auto epoch_end = std::chrono::system_clock::now();
                auto total_time = std::chrono::duration_cast<std::chrono::seconds>(
                        benchmark_end - benchmark_start)
                    .count();
                unsigned epoch = 1;

                observed_latency.clear();
                std::vector<double> latency_map;
                double latency_sum = 0;

                unsigned ts = generate_timestamp(thread_id);
                LWWPairLattice<string> val(
                        TimestampValuePair<string>(ts, string(length, 'a')));
                string value = serialize(val);

                while (true) {
                    unsigned k;
                    double key_latency = 0;
                    if (zipf > 0) {
                        k = sample(num_keys, seed, base, sum_probs);
                    } else {
                        k = rand_r(&seed) % (num_keys) + 1;
                    }

                    Key key = generate_key(k);

                    type = *it;
                    if (++it == req_ratio_map.end())
                        it = req_ratio_map.begin();

                    if (type == "G") {
                        auto req_start = std::chrono::system_clock::now();

                        client.get_async(key);
                        receive(&client);
                        count += 1;

                        auto req_end = std::chrono::system_clock::now();
                        key_latency = (double)std::chrono::duration_cast<std::chrono::microseconds>(
                                req_end - req_start).count();
                    } else if (type == "P") {
                        auto req_start = std::chrono::system_clock::now();

                        client.put_async(key, value, LatticeType::LWW);
                        receive(&client);
                        count += 1;

                        auto req_end = std::chrono::system_clock::now();
                        key_latency = (double)std::chrono::duration_cast<std::chrono::microseconds>(
                                req_end - req_start).count();
                    } else if (type == "U") {
                        auto req_start = std::chrono::system_clock::now();

                        client.update_async(key, value, LatticeType::LWW);
                        receive(&client);
                        count += 1;

                        auto req_end = std::chrono::system_clock::now();
                        key_latency = (double)std::chrono::duration_cast<std::chrono::microseconds>(
                                req_end - req_start).count();
                    } else if (type == "M") {
                        auto req_start = std::chrono::system_clock::now();

                        client.put_async(key, value, LatticeType::LWW);
                        receive(&client);
                        client.get_async(key);
                        receive(&client);
                        count += 2;

                        auto req_end = std::chrono::system_clock::now();
                        key_latency =
                            (double)std::chrono::duration_cast<std::chrono::microseconds>(
                                    req_end - req_start).count() / 2;
                    } else {
                        log->info("{} is an invalid request type.", type);
                    }

                    latency_map.push_back(key_latency);
                    latency_sum += key_latency;

                    if (observed_latency.find(key) == observed_latency.end()) {
                        observed_latency[key].first = key_latency;
                        observed_latency[key].second = 1;
                    } else {
                        observed_latency[key].first =
                            (observed_latency[key].first * observed_latency[key].second +
                             key_latency) /
                            (observed_latency[key].second + 1);
                        observed_latency[key].second += 1;
                    }

                    epoch_end = std::chrono::system_clock::now();
                    auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                            epoch_end - epoch_start)
                        .count();

                    // report throughput every report_period seconds
                    if (time_elapsed >= report_period) {
                        double throughput = (double)count / (double)time_elapsed;
                        log->info("[Epoch {}] Report period is {} seconds.", epoch, time_elapsed);
                        log->info("[Epoch {}] Throughput is {} ops/seconds.", epoch, throughput);
                        log->info("[Epoch {}] {} keys are accessed out of {} ops.", epoch,
                                observed_latency.size(), count);
                        epoch += 1;

                        auto avg_latency = (count > 0 ? (double)latency_sum / (double)count : (double)time_elapsed * (double)1000000);
                        double min_latency, max_latency, median_latency, tail_latency;
                        if (latency_map.size() > 0) {
                            std::sort(latency_map.begin(), latency_map.end());
                            min_latency = latency_map[0];
                            max_latency = latency_map[latency_map.size() - 1];
                            int percentile = trunc((double)latency_map.size() * 0.5);
                            median_latency = latency_map[percentile];
                            percentile = trunc((double)latency_map.size() * 0.99);
                            tail_latency = latency_map[percentile];
                        } else {
                            min_latency = avg_latency;
                            max_latency = avg_latency;
                            median_latency = avg_latency;
                            tail_latency = avg_latency;
                        }

                        log->info("[Epoch {}] Average latency {} us.", epoch, avg_latency);
                        log->info("[Epoch {}] Min latency {} us.", epoch, min_latency);
                        log->info("[Epoch {}] Max latency {} us.", epoch, max_latency);
                        log->info("[Epoch {}] Median latency {} us.", epoch, median_latency);
                        log->info("[Epoch {}] 99 tail latency {} us.", epoch, tail_latency);

                        UserFeedback feedback;
                        feedback.set_uid(ip + ":" + std::to_string(thread_id));
                        feedback.set_avg_latency(avg_latency);
                        feedback.set_min_latency(min_latency);
                        feedback.set_max_latency(max_latency);
                        feedback.set_median_latency(median_latency);
                        feedback.set_tail_latency(tail_latency);
                        //feedback.set_throughput((double)count);
                        feedback.set_throughput(throughput);

                        for (const auto &key_latency_pair : observed_latency) {
                            if (key_latency_pair.second.first > 1) {
                                UserFeedback_KeyLatency *kl = feedback.add_key_latency();
                                kl->set_key(key_latency_pair.first);
                                kl->set_latency(key_latency_pair.second.first);
                            }
                        }

                        string serialized_latency;
                        feedback.SerializeToString(&serialized_latency);

                        for (const MonitoringThread &thread : monitoring_threads) {
                            kZmqUtil->send_string(
                                    serialized_latency,
                                    &pushers[thread.feedback_report_connect_address()]);
                        }

                        count = 0;
                        latency_map.clear();
                        latency_sum = 0;
                        observed_latency.clear();
                        epoch_start = std::chrono::system_clock::now();
                    }

                    benchmark_end = std::chrono::system_clock::now();
                    total_time = std::chrono::duration_cast<std::chrono::seconds>(
                            benchmark_end - benchmark_start)
                        .count();
                    if (total_time > time) {
                        break;
                    }
                }

                log->info("Finished");
                UserFeedback feedback;

                feedback.set_uid(ip + ":" + std::to_string(thread_id));
                feedback.set_finish(true);

                string serialized_latency;
                feedback.SerializeToString(&serialized_latency);

                for (const MonitoringThread &thread : monitoring_threads) {
                    kZmqUtil->send_string(
                            serialized_latency,
                            &pushers[thread.feedback_report_connect_address()]);
                }
#else
                string type;
                double read_ratio = stod(v[1]);
                unsigned num_keys = stoi(v[2]);
                unsigned length = stoi(v[3]);
                unsigned report_period = stoi(v[4]);
                unsigned time = stoi(v[5]);
                double zipf = stod(v[6]);
                size_t max_outstanding = stoul(v[7], nullptr, 0);
                unsigned is_update_only = 1;
                if (v.size() == 9)
                    is_update_only = stoi(v[8]);

                vector<string> req_ratio_map;
                for (double i = 0; i < ((double)100 * read_ratio); i++)
                    req_ratio_map.push_back("G");
                for (double i = 0; i < ((double)100 * ((double)1 - read_ratio)); i++)
                    is_update_only ? req_ratio_map.push_back("U") : req_ratio_map.push_back("P");

                auto it = req_ratio_map.begin();

                vector<double> sum_probs;
                sum_probs.reserve(num_keys);
                double base;

                if (zipf > 0) {
                    log->info("Zipf coefficient is {}.", zipf);
                    base = get_base(num_keys, zipf);
                    sum_probs[0] = 0;

                    for (unsigned i = 1; i <= num_keys; i++) {
                        sum_probs[i] = sum_probs[i - 1] + base / pow((double)i, zipf);
                    }
                } else {
                    log->info("Using a uniform random distribution.");
                }

                unsigned num_loaded_keys = num_keys;
                size_t count = 0, outstanding = 0;
                auto benchmark_start = std::chrono::system_clock::now();
                auto benchmark_end = std::chrono::system_clock::now();
                auto epoch_start = std::chrono::system_clock::now();
                auto epoch_end = std::chrono::system_clock::now();
                auto total_time = std::chrono::duration_cast<std::chrono::seconds>(
                        benchmark_end - benchmark_start)
                    .count();
                unsigned epoch = 1, put_count = 0;

                observed_latency.clear();
                std::vector<double> latency_map;
                double latency_sum = 0;
                vector<KeyResponse> responses;

                unsigned ts = generate_timestamp(thread_id);
                LWWPairLattice<string> val(
                        TimestampValuePair<string>(ts, string(length, 'a')));
                string value = serialize(val);

                while (true) {
                    unsigned k;
                    double key_latency = 0;
                    if (outstanding < max_outstanding) {
                        Key key;
                        type = *it;
                        if (++it == req_ratio_map.end())
                            it = req_ratio_map.begin();

                        if (type == "P") {
                            k = (num_loaded_keys + 1) + ((kBenchmarkThreadNum * kBenchmarkNodeNum) * 
                                    put_count) + ((node_id * kBenchmarkThreadNum) + thread_id);
                            key = generate_key(k);

                            client.put_async(key, value, LatticeType::LWW);
                            put_count++;
                            outstanding++;
                        } else {
                            if (zipf > 0) {
                                k = sample(num_keys, seed, base, sum_probs);
                            } else {
                                k = rand_r(&seed) % (num_keys) + 1;
                            }

                            key = generate_key(k);

                            if (type == "G") {
                                client.get_async(key);
                                outstanding++;
                            } else if (type == "U") {
                                client.update_async(key, value, LatticeType::LWW);
                                outstanding++;
                            } else if (type == "M") {
                                client.put_async(key, value, LatticeType::LWW);
                                client.get_async(key);
                                outstanding += 2;
                            } else {
                                log->info("{} is an invalid request type.", type);
                            }
                        }
                    }

                    responses = client.receive_async2(key_latency);
                    if (responses.size() > 0) {
                        count++;
                        outstanding--;
                        
                        Key key = responses[0].tuples(0).key();
                        latency_map.push_back(key_latency);
                        latency_sum += key_latency;

                        if (observed_latency.find(key) == observed_latency.end()) {
                            observed_latency[key].first = key_latency;
                            observed_latency[key].second = 1;
                        } else {
                            observed_latency[key].first =
                                (observed_latency[key].first * observed_latency[key].second +
                                 key_latency) /
                                (observed_latency[key].second + 1);
                            observed_latency[key].second += 1;
                        }
                    }

                    epoch_end = std::chrono::system_clock::now();
                    auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                            epoch_end - epoch_start)
                        .count();

                    // report throughput every report_period seconds
                    if (time_elapsed >= report_period) {
                        log->info("[Epoch {}] Outstanding requests {}.", epoch, outstanding);

                        double throughput = (double)count / (double)time_elapsed;
                        log->info("[Epoch {}] Report period is {} seconds.", epoch, time_elapsed);
                        log->info("[Epoch {}] Throughput is {} ops/seconds.", epoch, throughput);
                        log->info("[Epoch {}] {} keys are accessed out of {} ops.", epoch,
                                observed_latency.size(), count);
                        epoch += 1;

                        auto avg_latency = (count > 0 ? (double)latency_sum / (double)count : (double)time_elapsed * (double)1000000);
                        double min_latency, max_latency, median_latency, tail_latency;
                        if (latency_map.size() > 0) {
                            std::sort(latency_map.begin(), latency_map.end());
                            min_latency = latency_map[0];
                            max_latency = latency_map[latency_map.size() - 1];
                            int percentile = trunc((double)latency_map.size() * 0.5);
                            median_latency = latency_map[percentile];
                            percentile = trunc((double)latency_map.size() * 0.99);
                            tail_latency = latency_map[percentile];
                        } else {
                            min_latency = avg_latency;
                            max_latency = avg_latency;
                            median_latency = avg_latency;
                            tail_latency = avg_latency;
                        }

                        log->info("[Epoch {}] Average latency {} us.", epoch, avg_latency);
                        log->info("[Epoch {}] Min latency {} us.", epoch, min_latency);
                        log->info("[Epoch {}] Max latency {} us.", epoch, max_latency);
                        log->info("[Epoch {}] Median latency {} us.", epoch, median_latency);
                        log->info("[Epoch {}] 99 tail latency {} us.", epoch, tail_latency);

                        UserFeedback feedback;
                        feedback.set_uid(ip + ":" + std::to_string(thread_id));
                        feedback.set_avg_latency(avg_latency);
                        feedback.set_min_latency(min_latency);
                        feedback.set_max_latency(max_latency);
                        feedback.set_median_latency(median_latency);
                        feedback.set_tail_latency(tail_latency);
                        //feedback.set_throughput((double)count);
                        feedback.set_throughput(throughput);

                        for (const auto &key_latency_pair : observed_latency) {
                            if (key_latency_pair.second.first > 1) {
                                UserFeedback_KeyLatency *kl = feedback.add_key_latency();
                                kl->set_key(key_latency_pair.first);
                                kl->set_latency(key_latency_pair.second.first);
                            }
                        }

                        string serialized_latency;
                        feedback.SerializeToString(&serialized_latency);

                        for (const MonitoringThread &thread : monitoring_threads) {
                            kZmqUtil->send_string(
                                    serialized_latency,
                                    &pushers[thread.feedback_report_connect_address()]);
                        }

                        count = 0;
                        latency_map.clear();
                        latency_sum = 0;
                        observed_latency.clear();
                        epoch_start = std::chrono::system_clock::now();
                    }

                    responses.clear();
                    benchmark_end = std::chrono::system_clock::now();
                    total_time = std::chrono::duration_cast<std::chrono::seconds>(
                            benchmark_end - benchmark_start)
                        .count();
                    if (total_time > time) {
                        while (!client.check_all_pending_requests_done()) {
                            client.receive_async2(key_latency);
                        }
                        break;
                    }
                }

                log->info("Finished");
                UserFeedback feedback;

                feedback.set_uid(ip + ":" + std::to_string(thread_id));
                feedback.set_finish(true);

                string serialized_latency;
                feedback.SerializeToString(&serialized_latency);

                for (const MonitoringThread &thread : monitoring_threads) {
                    kZmqUtil->send_string(
                            serialized_latency,
                            &pushers[thread.feedback_report_connect_address()]);
                }
#endif
            } else if (mode == "RMW") {
            } else if (mode == "RLW") {
            } else if (mode == "LOAD") {
#ifdef SINGLE_OUTSTANDING
                unsigned num_keys = stoi(v[1]);
                unsigned length = stoi(v[2]);
                unsigned total_threads = stoi(v[3]);
                unsigned num_bench_nodes = stoi(v[4]);

                unsigned global_range = (num_keys / num_bench_nodes);
                unsigned local_range = (global_range / total_threads);
                unsigned start = (global_range * node_id) + (thread_id * local_range + 1);
                unsigned end = (global_range * node_id) + (thread_id * local_range + local_range + 1);

                if ((num_keys % (total_threads * num_bench_nodes)) != 0) {
                    if (thread_id == total_threads - 1)
                        end = (global_range * (node_id + 1)) + 1;
                }

                Key key;
                unsigned ts = generate_timestamp(thread_id);
                LWWPairLattice<string> val(
                        TimestampValuePair<string>(ts, string(length, 'a')));
                string value = serialize(val);

                auto load_start = std::chrono::system_clock::now();

                for (unsigned i = start; i < end; i++) {
                    if (i != start && (i - start) % 50000 == 0) {
                        log->info("Creating key {}.", i);
                    }

                    client.put_async(generate_key(i), value, LatticeType::LWW);
                    receive(&client);
                }

                auto load_time = std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::system_clock::now() - load_start)
                    .count();
                log->info("Loading data took {} seconds.", load_time);

                UserFeedback feedback;
                feedback.set_uid(ip + ":" + std::to_string(thread_id) + ":" + "LOAD");
                feedback.set_finish(true);

                string serialized_feedback;
                feedback.SerializeToString(&serialized_feedback);

                for (const MonitoringThread &thread : monitoring_threads) {
                    kZmqUtil->send_string(
                            serialized_feedback,
                            &pushers[thread.feedback_report_connect_address()]);
                }
#else
                unsigned num_keys = stoi(v[1]);
                unsigned length = stoi(v[2]);
                unsigned total_threads = stoi(v[3]);
                unsigned num_bench_nodes = stoi(v[4]);

                unsigned global_range = (num_keys / num_bench_nodes);
                unsigned local_range = (global_range / total_threads);
                unsigned start = (global_range * node_id) + (thread_id * local_range + 1);
                unsigned end = (global_range * node_id) + (thread_id * local_range + local_range + 1);

                if ((num_keys % (total_threads * num_bench_nodes)) != 0) {
                    if (thread_id == total_threads - 1)
                        end = (global_range * (node_id + 1)) + 1;
                }

                Key key;
                vector<KeyResponse> responses;
                uint64_t num_requests = 0, num_responses = 0;
                unsigned ts = generate_timestamp(thread_id);
                LWWPairLattice<string> val(
                        TimestampValuePair<string>(ts, string(length, 'a')));
                string value = serialize(val);

                auto load_start = std::chrono::system_clock::now();

                for (unsigned i = start; i < end; i++) {
                    if (i != start && (i - start) % 50000 == 0) {
                        log->info("Creating key {}.", i);
                    }

                    client.put_async(generate_key(i), value, LatticeType::LWW);
                    num_requests++;

                    responses = client.receive_async3();
                    num_responses += responses.size();
                }

                while (num_requests != num_responses) {
                    responses = client.receive_async3();
                    num_responses += responses.size();
                }

                auto load_time = std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::system_clock::now() - load_start)
                    .count();
                log->info("Loading data took {} seconds.", load_time);

                UserFeedback feedback;
                feedback.set_uid(ip + ":" + std::to_string(thread_id) + ":" + "LOAD");
                feedback.set_finish(true);

                string serialized_feedback;
                feedback.SerializeToString(&serialized_feedback);

                for (const MonitoringThread &thread : monitoring_threads) {
                    kZmqUtil->send_string(
                            serialized_feedback,
                            &pushers[thread.feedback_report_connect_address()]);
                }
#endif
            } else {
                log->info("{} is an invalid mode.", mode);
            }
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc != 1) {
        std::cerr << "Usage: " << argv[0] << std::endl;
        return 1;
    }

    // read the YAML conf
    YAML::Node conf = YAML::LoadFile("conf/dinomo-config.yml");

    YAML::Node policy = conf["policy"];
    kEnableReconfig = policy["reconfiguration"].as<bool>();
    kClientTimeout = policy["timeout"].as<unsigned>();

    YAML::Node net_config = conf["net_config"];
    kRespPullerHWM = net_config["response_puller_hwm"].as<int>();
    kRespPullerBacklog = net_config["response_puller_backlog"].as<int>();

    YAML::Node user = conf["user"];
    Address ip = user["ip"].as<string>();

    vector<MonitoringThread> monitoring_threads;
    vector<Address> routing_ips;

    YAML::Node monitoring = user["monitoring"];
    for (const YAML::Node &node : monitoring) {
        monitoring_threads.push_back(MonitoringThread(node.as<Address>()));
    }

#ifndef BENCH_CACHE_ROUTING
    YAML::Node threads = conf["threads"];
    kRoutingThreadCount = threads["routing"].as<unsigned>();
    kBenchmarkThreadNum = threads["benchmark"].as<unsigned>();
    kDefaultLocalReplication = conf["replication"]["local"].as<unsigned>();
#else
    YAML::Node threads = conf["threads"];
    kRoutingThreadCount = threads["routing"].as<unsigned>();
    kBenchmarkThreadNum = threads["benchmark"].as<unsigned>();
    kMemoryThreadCount = threads["memory"].as<unsigned>();
    kStorageThreadCount = threads["storage"].as<unsigned>();

    kSelfTier = Tier::MEMORY;
    kSelfTierIdVector = {kSelfTier};

    YAML::Node capacities = conf["capacities"];
    kMemoryNodeCapacity = capacities["memory-cap"].as<unsigned>() * 1024 * 1024UL;
    kStorageNodeCapacity = capacities["storage-cap"].as<unsigned>() * 1024 * 1024UL;

    YAML::Node replication = conf["replication"];
    kDefaultGlobalMemoryReplication = replication["memory"].as<unsigned>();
    kDefaultGlobalStorageReplication = replication["storage"].as<unsigned>();
    kDefaultLocalReplication = replication["local"].as<unsigned>();

    kTierMetadata[Tier::MEMORY] = TierMetadata(Tier::MEMORY, kMemoryThreadCount,
            kDefaultGlobalMemoryReplication, kMemoryNodeCapacity);
    kTierMetadata[Tier::STORAGE] = TierMetadata(Tier::STORAGE, kStorageThreadCount,
            kDefaultGlobalStorageReplication, kStorageNodeCapacity);
#endif

    YAML::Node benchConfig = conf["bench_config"];
    kBenchmarkNodeNum = benchConfig["num-benchmark-nodes"].as<unsigned>();

    vector<std::thread> benchmark_threads;

    YAML::Node routing = user["routing"];

    for (const YAML::Node &node : routing) {
        routing_ips.push_back(node.as<Address>());
    }

    vector<UserRoutingThread> routing_threads;
    for (const Address &ip : routing_ips) {
        for (unsigned i = 0; i < kRoutingThreadCount; i++) {
            routing_threads.push_back(UserRoutingThread(ip, i));
        }
    }

#ifdef ENABLE_DINOMO_KVS
    int n = BENCH_HOST_NAME_MAX;
    char *hostname = NULL;
    if((hostname = (char *)malloc(n)) == NULL) {
        perror("malloc error");
        exit(1);
    }

    if (gethostname(hostname, n) < 0) {
        perror("gethostname error");
        exit(1);
    }

    std::string host(hostname, n);
    node_id = stoi(host.substr(5));
#endif
    
    // NOTE: We create a new client for every single thread.
    for (unsigned thread_id = 1; thread_id < kBenchmarkThreadNum; thread_id++) {
        benchmark_threads.push_back(
                std::thread(run, thread_id, routing_threads, monitoring_threads, ip));
    }

    run(0, routing_threads, monitoring_threads, ip);
}
