#include <stdio.h>
#include <stdlib.h>

#include "kvs/debug.h"
#include "kvs/ib_config.h"
#include "kvs/ib.h"
#include "kvs/setup_ib.h"
#include "kvs/dinomo_storage.hpp"
#include "yaml-cpp/yaml.h"

#include <string>

int run_client(int num_concurr_msgs);

FILE *log_fp = NULL;

#ifdef SHARED_NOTHING
std::unordered_map<int, std::string> peer_ip_addresses;
#endif
uint64_t VALUE_SIZE = 256;
uint64_t SHORTCUT_SIZE = 16;
uint64_t LOG_BLOCK_HEADER_LEN = sizeof(log_block);
uint64_t MAX_LOG_BLOCK_LEN = (2 * 1024 * 1024) - LOG_BLOCK_HEADER_LEN;
uint64_t MAX_PREALLOC_NUM = 2;

int main(int argc, char *argv[])
{
    int ret = 0;
    YAML::Node conf = YAML::LoadFile("conf/dinomo-config.yml");
    YAML::Node capacities = conf["capacities"];
    uint64_t storage_capacities = capacities["storage-cap"].as<unsigned>() * (1024 * 1024UL);

    YAML::Node kvsConfig = conf["kvs_config"];
    VALUE_SIZE = kvsConfig["value-size"].as<uint64_t>();
    SHORTCUT_SIZE = kvsConfig["shortcut-size"].as<uint64_t>();
    uint64_t log_block_size = kvsConfig["log-block-size"].as<uint64_t>();
    MAX_LOG_BLOCK_LEN = (log_block_size * 1024 * 1024) - LOG_BLOCK_HEADER_LEN;
    MAX_PREALLOC_NUM = kvsConfig["log-block-prealloc-num"].as<uint64_t>();

    YAML::Node ib_config = conf["ib_config"];
    int total_available_memory_nodes = ib_config["total_available_memory_nodes"].as<int>();
    int num_initial_memory_nodes = ib_config["num_initial_memory_nodes"].as<int>();
    int num_initial_storage_nodes = ib_config["num_initial_storage_nodes"].as<int>();
    int threads_per_memory = ib_config["threads_per_memory"].as<int>();
    int threads_per_storage = ib_config["threads_per_storage"].as<int>();
    int num_storage_managers = ib_config["num_storage_managers"].as<int>();
    int msg_size = ib_config["msg_size"].as<int>();

    char *storage_node_ips[num_initial_storage_nodes];
    for (int i = 0; i < num_initial_storage_nodes; i++)
        storage_node_ips[i] = (char *) calloc(128, sizeof(char));
    YAML::Node storageIPs = ib_config["storage_node_ips"];

    int nodeCounter = 0;
    for (const YAML::Node &address : storageIPs)
    {
        strcpy(storage_node_ips[nodeCounter], (address.as<std::string>()).c_str());
        nodeCounter++;
    }

    char *sock_port = (char *) calloc(128, sizeof(char));
    strcpy(sock_port, (ib_config["sock_port"].as<std::string>()).c_str());

    int rank = 0;
    bool is_server = true;

    ib_init(total_available_memory_nodes, num_initial_memory_nodes, 
            num_initial_storage_nodes, threads_per_memory, threads_per_storage, 
            num_storage_managers, msg_size, storage_node_ips, sock_port, rank, 
            is_server, storage_capacities);

    for (int i = 0; i < num_initial_storage_nodes; i++)
        free(storage_node_ips[i]);
    free(sock_port);

    if (config_info.is_server)
        ret = run_server(config_info.threads_per_storage);
    else
        ret = run_client(config_info.threads_per_memory);
    check(ret == 0, "Failed to run workload");

error:
    close_ib_connection();
    destroy_env();
    return ret;
}

int run_client(int num_concurr_msgs)
{
    return 0;
}
