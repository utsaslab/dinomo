#include "benchmark.pb.h"
#include "monitor/monitoring_handlers.hpp"

void feedback_handler(
    logger log, string &serialized,
    map<string, std::tuple<double, double, double, double, double, unsigned>> &user_latency,
    map<string, double> &user_throughput,
    map<Key, std::pair<double, unsigned>> &latency_miss_ratio_map,
    unsigned BenchmarkNodeCount, unsigned BenchmarkThreadCount) {
  UserFeedback fb;
  fb.ParseFromString(serialized);

  static unsigned num_finished_clients = 0;

  if (fb.finish()) {
    user_latency.erase(fb.uid());
    num_finished_clients++;

    if (num_finished_clients == (BenchmarkNodeCount * BenchmarkThreadCount)) {
        vector<string> v;
        split(fb.uid(), ':', v);
        if (v.size() > 2)
            log->info("{} has been finished from {} clients.", v[2], num_finished_clients);
        else
            log->info("RUN has been finished from {} clients.", num_finished_clients);
        num_finished_clients = 0;
    }
  } else {
    // collect latency and throughput feedback
    //user_latency[fb.uid()] += fb.latency();
    if (user_latency.find(fb.uid()) == user_latency.end()) {
      user_latency[fb.uid()] = std::make_tuple(fb.avg_latency(), fb.tail_latency(), fb.median_latency(), fb.min_latency(), fb.max_latency(), 1);
    } else {
      std::get<0>(user_latency[fb.uid()]) += fb.avg_latency();
      std::get<1>(user_latency[fb.uid()]) += fb.tail_latency();
      std::get<2>(user_latency[fb.uid()]) += fb.median_latency();
      std::get<3>(user_latency[fb.uid()]) += fb.min_latency();
      std::get<4>(user_latency[fb.uid()]) += fb.max_latency();
      std::get<5>(user_latency[fb.uid()]) += 1;
    }
    user_throughput[fb.uid()] = fb.throughput();
    //user_throughput[fb.uid()] += fb.throughput();

    // collect replication factor adjustment factors
    for (const auto &key_latency_pair : fb.key_latency()) {
      Key key = key_latency_pair.key();
      double observed_key_latency = key_latency_pair.latency();

      if (latency_miss_ratio_map.find(key) == latency_miss_ratio_map.end()) {
        latency_miss_ratio_map[key].first = observed_key_latency / kSloWorst;
        latency_miss_ratio_map[key].second = 1;
      } else {
        latency_miss_ratio_map[key].first =
            (latency_miss_ratio_map[key].first *
                 latency_miss_ratio_map[key].second +
             observed_key_latency / kSloWorst) /
            (latency_miss_ratio_map[key].second + 1);
        latency_miss_ratio_map[key].second += 1;
      }
    }
  }
}
