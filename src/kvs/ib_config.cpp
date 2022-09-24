#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/utsname.h>

#include "kvs/debug.h"
#include "kvs/ib_config.h"

struct ConfigInfo config_info;

int ib_config(int total_available_memory_nodes, int num_initial_memory_nodes, 
        int num_initial_storage_nodes, int threads_per_memory, int threads_per_storage,
        int num_storage_managers, int msg_size, char **storage_node_ips, 
        char *sock_port, int rank, bool is_server, uint64_t storage_capacities)
{
    // Presume this function is called only by clients
    config_info.is_server = is_server;

    config_info.total_available_memory_nodes = total_available_memory_nodes;

    config_info.num_initial_memory_nodes = num_initial_memory_nodes;

    config_info.num_initial_storage_nodes = num_initial_storage_nodes;

    config_info.threads_per_memory = threads_per_memory;

    config_info.threads_per_storage = threads_per_storage;

    config_info.num_storage_managers = num_storage_managers;

    config_info.msg_size = msg_size >= (MAX_LOG_BLOCK_LEN + LOG_BLOCK_HEADER_LEN) ? msg_size : (MAX_LOG_BLOCK_LEN + LOG_BLOCK_HEADER_LEN);

    config_info.storage_node_ips = (char **) calloc(config_info.num_initial_storage_nodes, sizeof(char *));
    for (int i = 0; i < config_info.num_initial_storage_nodes; i++) {
        config_info.storage_node_ips[i] = (char *) calloc(128, sizeof(char));
        strcpy(config_info.storage_node_ips[i], storage_node_ips[i]);
    }

    config_info.sock_port = (char *) calloc(128, sizeof(char));
    strcpy(config_info.sock_port, sock_port);

    config_info.rank = rank;

    config_info.storage_capacities = storage_capacities;

    return 0;
}

int init_env()
{
    char fname[64] = {'\0'};

    if (config_info.is_server) {
        sprintf(fname, "server[%d].log", config_info.rank);
    } else {
        sprintf(fname, "client[%d].log", config_info.rank);
    }

    log_fp = fopen(fname, "w");
    check(log_fp != NULL, "Failed to open log file");

    LOG(LOG_HEADER, "IB Echo Server");
    print_config_info();

    return 0;
error:
    return -1;
}

void destroy_env()
{
    LOG(LOG_HEADER, "Run Finished");
    if (log_fp != NULL)
        fclose(log_fp);
}

void print_config_info ()
{
    LOG(LOG_SUB_HEADER, "Configuraion");

    if (config_info.is_server) {
        LOG("is_server                 = %s", "true");
    } else {
        LOG("is_server                 = %s", "false");
    }

    LOG("rank                      = %d", config_info.rank);
    LOG("msg_size                  = %d", config_info.msg_size);
    LOG("threads per memory        = %d", config_info.threads_per_memory);
    LOG("threads per storage       = %d", config_info.threads_per_storage);
    LOG("sock_port                 = %s", config_info.sock_port);
    LOG("storage capacities        = %luGB", config_info.storage_capacities / (1024 * 1024 * 1024UL));

    LOG(LOG_SUB_HEADER, "End of Configuraion");
}
