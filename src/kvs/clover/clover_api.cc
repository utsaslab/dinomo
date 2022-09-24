#include "client.h"
#include "memory.h"
#include "mitsume.h"
#include "mitsume_tool.h"
#include "server.h"
#include <stdio.h>

#include "clover_api.h"

int MITSUME_CLT_NUM;
int MITSUME_MEM_NUM;

struct mitsume_ctx_clt *client_ctx = NULL;

void clover_init(int num_initial_memory_nodes, int rank, char **clover_memc_ips)
{
    int num_threads = 1;
    struct configuration_params *param_arr;
    param_arr = (struct configuration_params *)malloc(num_threads * sizeof(struct configuration_params));
    param_arr[0].base_port_index = 1;
    param_arr[0].num_servers = 1;
    param_arr[0].num_clients = num_initial_memory_nodes;
    MITSUME_CLT_NUM = num_initial_memory_nodes;
    param_arr[0].num_memorys = 1;
    MITSUME_MEM_NUM = 1;
    param_arr[0].machine_id = rank + 1;
    param_arr[0].total_threads = num_threads;
    param_arr[0].device_id = 0;
    param_arr[0].num_loopback = 2;
    
    strcpy(MEMCACHED_IP, clover_memc_ips[0]);

    printf("base_port_index = %d\n", param_arr[0].base_port_index);
    printf("num_servers = %d\n", param_arr[0].num_servers);
    printf("num_clients = %d\n", param_arr[0].num_clients);
    printf("num_memorys = %d\n", param_arr[0].num_memorys);
    printf("machine_id = %d\n", param_arr[0].machine_id);
    printf("total threads = %d\n", param_arr[0].total_threads);
    printf("device id = %d\n", param_arr[0].device_id);
    printf("num_loopback = %d\n", param_arr[0].num_loopback);

    client_ctx = run_client_config(&param_arr[0]);
}

int clover_put(const uint64_t key, const size_t key_len, const void *val, 
        const size_t val_len, const uint16_t thread_id)
{
    void *write;
    int ret;
    struct mitsume_consumer_metadata *thread_metadata = &client_ctx->thread_metadata[thread_id];
    struct thread_local_inf *local_inf = thread_metadata->local_inf;

    write = local_inf->user_input_space[thread_metadata->thread_id];
    memcpy(write, val, val_len);

    ret = mitsume_tool_open(thread_metadata, key, write, val_len, MITSUME_BENCHMARK_REPLICATION);
    if (ret != MITSUME_SUCCESS)
        MITSUME_INFO("error %lld %d\n", (unsigned long long int)key, ret);

    return ret;
}

int clover_update(const uint64_t key, const size_t key_len, const void *val, 
        const size_t val_len, const uint16_t thread_id, bool &cacheHit)
{
    void *write;
    int ret;
    struct mitsume_consumer_metadata *thread_metadata = &client_ctx->thread_metadata[thread_id];
    struct thread_local_inf *local_inf = thread_metadata->local_inf;

    write = local_inf->user_input_space[thread_metadata->thread_id];
    memcpy(write, val, val_len);

    ret = mitsume_tool_write(thread_metadata, key, write, val_len, MITSUME_TOOL_KVSTORE_WRITE, cacheHit);
    if (ret != MITSUME_SUCCESS)
        MITSUME_INFO("error %lld %d\n", (unsigned long long int)key, ret);

    return ret;
}

void *clover_get(const uint64_t key, const size_t key_len, uint32_t *read_size,
        const uint16_t thread_id, bool &cacheHit)
{
    void *read;
    int ret;
    struct mitsume_consumer_metadata *thread_metadata = &client_ctx->thread_metadata[thread_id];
    struct thread_local_inf *local_inf = thread_metadata->local_inf;

    read = local_inf->user_output_space[thread_metadata->thread_id];

    ret = mitsume_tool_read(thread_metadata, key, read, read_size, MITSUME_TOOL_KVSTORE_READ, cacheHit);
    if (ret != MITSUME_SUCCESS)
        MITSUME_INFO("error %lld %d\n", (unsigned long long int)key, ret);

    return read;
}

void *clover_get_meta(const char *key, const size_t key_len, int *val_len)
{
    void *val = NULL;
    *val_len = memcached_get_published(key, (void **)&val);
    if (*val_len < 0)
        *val_len = 0;

    return val;
}

void clover_put_meta(const char *key, const size_t key_len, void *value, const size_t val_len)
{
    return memcached_publish(key, value, (int)val_len);
}
