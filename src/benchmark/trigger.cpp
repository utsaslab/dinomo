//  Copyright 2019 U.C. Berkeley RISE Lab
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

#include <stdlib.h>

#include "common.hpp"
#include "hash_ring.hpp"
#include "kvs_common.hpp"
#include "threads.hpp"
#include "yaml-cpp/yaml.h"

// TODO(vikram): We probably don't want to have to define all of these here?
ZmqUtil zmq_util;
ZmqUtilInterface *kZmqUtil = &zmq_util;

HashRingUtil hash_ring_util;
HashRingUtilInterface *kHashRingUtil = &hash_ring_util;

unsigned kBenchmarkThreadNum = 1;
unsigned kRoutingThreadCount = 1;
unsigned kDefaultLocalReplication = 1;

#include <cstdio>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <array>

std::string get_cmd_outputs(const char* cmd) {
    std::array<char, 128> buffer;
    std::string result;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd, "r"), pclose);
    if (!pipe) {
        throw std::runtime_error("popen() failed!");
    }
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
        result += buffer.data();
    }
    return result;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <benchmark_threads>" << std::endl;
        return 1;
    }

    unsigned thread_num = atoi(argv[1]);

    // read the YAML conf
    vector<Address> benchmark_address, management_address;
    YAML::Node conf = YAML::LoadFile("conf/dinomo-config.yml");
    YAML::Node trigger = conf["trigger"];

    YAML::Node benchmark = trigger["benchmark"];
    for (const YAML::Node &node : benchmark) {
        benchmark_address.push_back(node.as<Address>());
    }

    YAML::Node management = trigger["management"];
    for (const YAML::Node &node : management) {
        management_address.push_back(node.as<Address>());
    }

    string mgmt_addr = "tcp://" + management_address[0] + ":8101";

    zmq::context_t context(1);
    SocketCache pushers(&context, ZMQ_PUSH);

    string command;
    while (true) {
        std::cout << "mode: WARM, RUN, LOAD, ADD, REMOVE" << std::endl;
        std::cout << "WARM:num_keys:value_length(bytes):num_requests:zipf(if == 0, uniform)" << std::endl;
#ifdef SINGLE_OUTSTANDING
        std::cout << "WARM-RUN:read_ratio:value_length(bytes):report_period(seconds):elapsed_running_time" << std::endl;
        std::cout << "RUN:read_ratio:num_keys:value_length(bytes):report_period(seconds):elapsed_running_time:zipf(if == 0, uniform)" << std::endl;
#else
        std::cout << "WARM-RUN:read_ratio:value_length(bytes):report_period(seconds):elapsed_running_time:num_outstanding:num_loaded_keys:is_update_only" << std::endl;
        std::cout << "RUN:read_ratio:num_keys:value_length(bytes):report_period(seconds):elapsed_running_time:zipf(if == 0, uniform):num_outstanding:is_update_only" << std::endl;
#endif
        std::cout << "LOAD:num_keys:value_length(bytes):total_threads_per_node:num_nodes" << std::endl;
        std::cout << "ADD:num_bench_nodes" << std::endl;
        std::cout << "REMOVE:num_bench_nodes" << std::endl;
        std::cout << "FAIL:num_failed_nodes" << std::endl;
        std::cout << "command> ";
        getline(std::cin, command);

        vector<string> v;
        split(command, ':', v);
        string mode = v[0];

        if (mode == "ADD") {
            string message = "add:" + v[1] + ":" + "benchmark";
            kZmqUtil->send_string(message, &pushers[mgmt_addr]);

            while (1) {
                string outputs = get_cmd_outputs("kubectl get pods -o wide | grep benchmark | grep Running | awk '{print $6}'");
                vector<string> bench_ips;
                split(outputs, '\n', bench_ips);
                if (bench_ips.size() == benchmark_address.size() + std::stoul(v[1], nullptr, 0)) {
                    benchmark_address.clear();
                    for (const Address address : bench_ips) {
                        benchmark_address.push_back(address);
                    }
                    break;
                }
                bench_ips.clear();
            }
        } else if (mode == "REMOVE") {
            unsigned num_remove_nodes = std::stoul(v[1], nullptr, 0);

            string outputs = get_cmd_outputs("kubectl get pods -o wide | grep benchmark | grep Running | awk '{print $6}'");
            vector<string> bench_ips;
            split(outputs, '\n', bench_ips);
            for (unsigned i = 0; i < num_remove_nodes; i++) {
                string message = "remove:" + bench_ips[i] + ":" + "benchmark";
                kZmqUtil->send_string(message, &pushers[mgmt_addr]);
            }
            bench_ips.clear();

            while (1) {
                outputs = get_cmd_outputs("kubectl get pods -o wide | grep benchmark | grep Running | awk '{print $6}'");
                split(outputs, '\n', bench_ips);
                if (bench_ips.size() == benchmark_address.size() - num_remove_nodes) {
                    benchmark_address.clear();
                    for (const Address address : bench_ips) {
                        benchmark_address.push_back(address);
                    }
                    bench_ips.clear();
                    break;
                }
                bench_ips.clear();
            }
        } else if (mode == "FAIL") {
            unsigned num_failed_nodes = std::stoul(v[1], nullptr, 0);

            string outputs = get_cmd_outputs("kubectl get pods -o wide | grep memory | grep Running | awk '{print $6}'");
            vector<string> mem_ips;
            split(outputs, '\n', mem_ips);
            for (unsigned i = 0; i < num_failed_nodes; i++) {
                string message = "remove:" + mem_ips[i] + ":" + "memory";
                kZmqUtil->send_string(message, &pushers[mgmt_addr]);
            }
            unsigned num_alive_nodes = mem_ips.size();
            mem_ips.clear();

            while (1) {
                outputs = get_cmd_outputs("kubectl get pods -o wide | grep memory | grep Running | awk '{print $6}'");
                split(outputs, '\n', mem_ips);
                if (mem_ips.size() == num_alive_nodes - num_failed_nodes) {
                    mem_ips.clear();
                    break;
                }
                mem_ips.clear();
            }
        } else {
            for (const Address address : benchmark_address) {
                for (unsigned tid = 0; tid < thread_num; tid++) {
                    BenchmarkThread bt = BenchmarkThread(address, tid);

                    kZmqUtil->send_string(command,
                            &pushers[bt.benchmark_command_address()]);
                }
            }
        }

        v.clear();
    }
}
