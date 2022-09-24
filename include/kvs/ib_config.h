#ifndef CONFIG_H_
#define CONFIG_H_

#include <stdbool.h>
#include <inttypes.h>
#include <string>
#include <vector>
#include <unordered_map>

#ifdef ENABLE_INT_KEYS
#include "log_blocks.h"
#include "clht_lb_res.h"
#else
#include "icache.h"
#endif

enum ConfigFileAttr {
    ATTR_SERVERS = 1,
    ATTR_CLIENTS,
    ATTR_MSG_SIZE,
    ATTR_NUM_CONCURR_MSGS,
};

struct ConfigInfo {
    bool is_server;
    int total_available_memory_nodes;
    int num_initial_memory_nodes;
    int num_initial_storage_nodes;
    int threads_per_memory;
    int threads_per_storage;
    int num_storage_managers;
    int msg_size;
    char **storage_node_ips;
    char *sock_port;
    int rank;
    uint64_t storage_capacities;
}__attribute__((aligned(64)));

extern struct ConfigInfo config_info;

extern PMEMobjpool *pop;
extern uint64_t pool_uuid;
extern size_t pool_size;
#ifdef ENABLE_INT_KEYS
#ifndef SHARED_NOTHING
extern clht_t *farIdx;
#else
extern clht_t **farIdx;
extern std::unordered_map<int, std::string> peer_ip_addresses;
#endif
#else
extern iCache *farIdx;
#endif

extern uint64_t VALUE_SIZE;
extern uint64_t SHORTCUT_SIZE;

int ib_config(int total_available_memory_nodes, int num_initial_memory_nodes, 
        int num_initial_storage_nodes, int threads_per_memory, int threads_per_storage,
        int num_storage_managers, int msg_size, char **storage_node_ips, 
        char *sock_port, int rank, bool is_server, uint64_t storage_capacities);

int init_env();

void destroy_env();

void print_config_info();

#endif /* CONFIG_H_*/
