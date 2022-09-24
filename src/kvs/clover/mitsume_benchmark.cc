#include "mitsume_benchmark.h"
#include <fstream>

#define MITSUME_TARGET_KEY 10

atomic<long> total_op;
mutex output_lock;

volatile int start_flag = 0;
volatile int end_flag = 0;
volatile int ready_flag = 0;

#define MITSUME_TEST_LOAD_READ_R1 10
#define MITSUME_TEST_LOAD_DUMMY 50
#define MITSUME_TEST_LOAD_READ_R2 11
#define MITSUME_TEST_LOAD_WRITE_START 14

#define YCSB_VALUE_SIZE     80
#define LOAD_SIZE           30000000
#define RUN_SIZE            30000000

int wl;
int dstr;
std::vector<uint64_t> init_keys;
std::vector<uint64_t> keys;
std::vector<int> ops;

uint64_t num_clients;
uint64_t machine_id;

std::atomic<uint64_t> num_load_keys;
std::atomic<uint64_t> num_run_keys;

enum {
    OP_READ = 0,
    OP_SCAN = 1,
    OP_INSERT = 2,
    OP_UPDATE = 3,
    OP_DELETE = 4,
};

enum {
    WORKLOAD_A,
    WORKLOAD_B,
    WORKLOAD_C,
    WORKLOAD_D,
    WORKLOAD_F,
};

enum {
    ZIPFIAN,
    UNIFORM,
};

#define WORKLOAD_TYPE       WORKLOAD_F
#define WORKLOAD_DIST       ZIPFIAN

static inline uint32_t murmur_32_scramble(uint32_t k) {
    k *= 0xcc9e2d51;
    k = (k << 15) | (k >> 17);
    k *= 0x1b873593;
    return k;
}

uint32_t murmur3_32(const uint8_t* key, size_t len, uint32_t seed)
{
    uint32_t h = seed;
    uint32_t k;

    /*  Read in groups of 4. */
    for (size_t i = len >> 2; i; i--) {
        // Here is a source of differing results across endiannesses.
        // A swap here has no effects on hash properties though.
        memcpy(&k, key, sizeof(uint32_t));
        key += sizeof(uint32_t);
        h ^= murmur_32_scramble(k);
        h = (h << 13) | (h >> 19);
        h = h * 5 + 0xe6546b64;
    }

    /*  Read the rest. */
    k = 0;
    for (size_t i = len & 3; i; i--) {
        k <<= 8;
        k |= key[i - 1];
    }

    // A swap is *not* necessary here because the preceding loop already
    // places the low bytes in the low places according to whatever endianness
    // we use. Swaps only apply when the memory is copied in a chunk.
    h ^= murmur_32_scramble(k);
    /*  Finalize. */
    h ^= len;
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}


void *mitsume_benchmark_latency(void *input_metadata) {
    struct mitsume_consumer_metadata *thread_metadata =
        (struct mitsume_consumer_metadata *)input_metadata;
    void *write;
    void *read;
    uint32_t i, j;
    int ret;
    uint32_t read_size;
    int client_id = get_client_id(thread_metadata->local_ctx_clt);
    struct thread_local_inf *local_inf = thread_metadata->local_inf;
    vector<int> test_size = {128, 256, 512, 1024, 2048, 4096};

    chrono::nanoseconds before, after;

    mitsume_key key = MITSUME_TARGET_KEY;

    write = local_inf->user_input_space[0];
    read = local_inf->user_output_space[0];
    if (client_id != 0)
        return NULL;
    if (thread_metadata->thread_id != 0)
        return NULL;

    if (stick_this_thread_to_core(2)) {
        printf("set affinity fail\n");
        return NULL;
    }

    MITSUME_PRINT("------\n");
    ret = mitsume_tool_open(thread_metadata, key, write, 36,
            MITSUME_BENCHMARK_REPLICATION);
    for (j = 0; j < test_size.size(); j++) {
        before = get_current_ns();
        for (i = 0; i < MITSUME_BENCHMARK_TIME; i++) {
            bool cacheHit;
            ret = mitsume_tool_write(thread_metadata, key, write,
                    test_size[j] - 8 * MITSUME_BENCHMARK_REPLICATION,
                    MITSUME_TOOL_KVSTORE_WRITE, cacheHit);
            if (ret != MITSUME_SUCCESS)
                MITSUME_INFO("error %lld %d\n", (unsigned long long int)key, ret);
        }
        after = get_current_ns();
        cout << test_size[j] << ": average write time(us):"
            << (after - before).count() / MITSUME_BENCHMARK_TIME << endl;

        before = get_current_ns();
        for (i = 0; i < MITSUME_BENCHMARK_TIME; i++) {
            bool cacheHit;
            ret = mitsume_tool_read(thread_metadata, key, read, &read_size,
                    MITSUME_TOOL_KVSTORE_READ, cacheHit);
            if (ret != MITSUME_SUCCESS)
                MITSUME_INFO("error %lld %d\n", (unsigned long long int)key, ret);
        }
        after = get_current_ns();
        cout << test_size[j] << ": average read time(us):"
            << (after - before).count() / MITSUME_BENCHMARK_TIME << endl;
    }

    return 0;
}

void *sync_across_cns(void *input_metadata) {
    mitsume_sync_barrier_global();
    return 0;
}

void *mitsume_benchmark_ycsb_load(void *input_metadata) {
    struct mitsume_consumer_metadata *thread_metadata =
        (struct mitsume_consumer_metadata *)input_metadata;
    void *write;
    uint64_t i;
    int ret;
    int thread_id = thread_metadata->thread_id;
    int client_id = get_client_id(thread_metadata->local_ctx_clt);
    struct thread_local_inf *local_inf = thread_metadata->local_inf;

    write = local_inf->user_input_space[thread_id];
    //if (client_id != 0)
    //    return NULL;

    if (stick_this_thread_to_core(2 * thread_id)) {
        printf("set affinity fail\n");
        return NULL;
    }

    //uint64_t start_key = (LOAD_SIZE / MITSUME_BENCHMARK_THREAD_NUM) * (uint64_t)thread_id;
    //uint64_t end_key = start_key + (LOAD_SIZE / MITSUME_BENCHMARK_THREAD_NUM);

#ifdef ENABLE_DISAGGRE_PARTITION
    uint64_t start_key = (num_load_keys.load() / MITSUME_BENCHMARK_THREAD_NUM) * (uint64_t)thread_id;
    uint64_t end_key = (MITSUME_BENCHMARK_THREAD_NUM - 1 != (uint64_t)thread_id) ? start_key + (num_load_keys.load() / MITSUME_BENCHMARK_THREAD_NUM) : num_load_keys.load();
#else
    uint64_t start_key, end_key;
    start_key = (LOAD_SIZE / num_clients) * (machine_id - 1);
    start_key = start_key + (((LOAD_SIZE / num_clients) / MITSUME_BENCHMARK_THREAD_NUM) * (uint64_t)thread_id);
    if (thread_id != MITSUME_BENCHMARK_THREAD_NUM - 1)
        end_key = start_key + ((LOAD_SIZE / num_clients) / MITSUME_BENCHMARK_THREAD_NUM);
    else
        end_key = ((LOAD_SIZE / num_clients) * (machine_id - 1)) + (LOAD_SIZE / num_clients);
#endif

    for (i = start_key; i < end_key; i++) {
        ret = mitsume_tool_open(thread_metadata, init_keys[i], write, YCSB_VALUE_SIZE,
                MITSUME_BENCHMARK_REPLICATION);
        if (ret != MITSUME_SUCCESS)
            MITSUME_INFO("error %lld %d\n", (unsigned long long int)init_keys[i], ret);
    }

    return 0;
}

void *mitsume_benchmark_ycsb_run(void *input_metadata) {
    struct mitsume_consumer_metadata *thread_metadata =
        (struct mitsume_consumer_metadata *)input_metadata;
    void *write;
    void *read;
    uint64_t i;
    int ret;
    uint32_t read_size;
    int thread_id = thread_metadata->thread_id;
    int client_id = get_client_id(thread_metadata->local_ctx_clt);
    struct thread_local_inf *local_inf = thread_metadata->local_inf;

    write = local_inf->user_input_space[thread_id];
    read = local_inf->user_output_space[thread_id];
    //if (client_id != 0)
    //    return NULL;

    if (stick_this_thread_to_core(2 * thread_id)) {
        printf("set affinity fail\n");
        return NULL;
    }
    
    //uint64_t start_key = (RUN_SIZE / MITSUME_BENCHMARK_THREAD_NUM) * (uint64_t)thread_id;
    //uint64_t end_key = start_key + (RUN_SIZE / MITSUME_BENCHMARK_THREAD_NUM);

#ifdef ENABLE_DISAGGRE_PARTITION
    uint64_t start_key = (num_run_keys.load() / MITSUME_BENCHMARK_THREAD_NUM) * (uint64_t)thread_id;
    uint64_t end_key = (MITSUME_BENCHMARK_THREAD_NUM - 1 != (uint64_t)thread_id) ? start_key + (num_run_keys.load() / MITSUME_BENCHMARK_THREAD_NUM) : num_run_keys.load();
#else
    uint64_t start_key, end_key;
    start_key = (RUN_SIZE / num_clients) * (machine_id - 1);
    start_key = start_key + (((RUN_SIZE / num_clients) / MITSUME_BENCHMARK_THREAD_NUM) * (uint64_t)thread_id);
    if (thread_id != MITSUME_BENCHMARK_THREAD_NUM - 1)
        end_key = start_key + ((RUN_SIZE / num_clients) / MITSUME_BENCHMARK_THREAD_NUM);
    else
        end_key = ((RUN_SIZE / num_clients) * (machine_id - 1)) + (RUN_SIZE / num_clients);
#endif

    for (i = start_key; i < end_key; i++) {
        if (ops[i] == OP_INSERT) {
            ret = mitsume_tool_open(thread_metadata, keys[i], write, YCSB_VALUE_SIZE,
                    MITSUME_BENCHMARK_REPLICATION);
            if (ret != MITSUME_SUCCESS)
                MITSUME_INFO("error %lld %d\n", (unsigned long long int)keys[i], ret);
        } else if (ops[i] == OP_UPDATE) {
            bool cacheHit;
            ret = mitsume_tool_write(thread_metadata, keys[i], write,
                    YCSB_VALUE_SIZE, MITSUME_TOOL_KVSTORE_WRITE, cacheHit);
            if (ret != MITSUME_SUCCESS)
                MITSUME_INFO("error %lld %d\n", (unsigned long long int)keys[i], ret);
        } else if (ops[i] == OP_READ) {
            bool cacheHit;
            ret = mitsume_tool_read(thread_metadata, keys[i], read, &read_size,
                    MITSUME_TOOL_KVSTORE_READ, cacheHit);
            if (ret != MITSUME_SUCCESS)
                MITSUME_INFO("error %lld %d\n", (unsigned long long int)keys[i], ret);
        }
    }

    return 0;
}

#if 1
int mitsume_benchmark_thread_load(int thread_num,
        struct mitsume_ctx_clt *local_ctx_clt,
        void *(*fun_ptr)(void *input_metadata)) {
    int i = 0;
    total_op = 0;
    mitsume_sync_barrier_init(thread_num, get_client_id(local_ctx_clt),
            MITSUME_CLT_NUM);
    pthread_t thread_job[MITSUME_CLT_CONSUMER_NUMBER];
    assert(MITSUME_BENCHMARK_SIZE);
    assert(MITSUME_BENCHMARK_TIME);

    if (thread_num > MITSUME_CLT_CONSUMER_NUMBER) {
        die_printf("thread_num is larger than max clt number\n");
        exit(1);
    }
    sleep(1);

    auto starttime = std::chrono::system_clock::now();
    for (i = 0; i < thread_num; i++) {
        pthread_create(&thread_job[i], NULL, fun_ptr,
                &local_ctx_clt->thread_metadata[i]);
    }

    for (i = 0; i < thread_num; i++) {
        pthread_join(thread_job[i], NULL);
    }
    auto duration = std::chrono::duration_cast<std::chrono::microseconds> (
            std::chrono::system_clock::now() - starttime);
#ifdef CLOVER_DEBUG
#ifdef ENABLE_DISAGGRE_PARTITION
    printf("Throughput: load, %f , ops/us\n", ((num_load_keys.load()) * 1.0) / duration.count());
    printf("Latency: load, %f ,sec\n", duration.count() / 1000000.0);
    printf("Bandwidth: load, %f ,GB/sec\n", ((((rdma_read_payload.load() + rdma_write_payload.load() + (rdma_send_alloc_payload.load() + rdma_send_insert_payload.load() + 
                                rdma_send_fetch_payload.load() + rdma_send_gc_payload.load()) + rdma_cas_payload.load()) * 1.0) / (duration.count())) * 1000000.0) / (1024 * 1024 * 1024 * 1.0));
    printf("Num Operations: %lu\n", num_load_keys.load());
#else
    printf("Throughput: load, %f , ops/us\n", ((LOAD_SIZE / num_clients) * 1.0) / duration.count());
    printf("Latency: load, %f ,sec\n", duration.count() / 1000000.0);
    printf("Bandwidth: load, %f ,GB/sec\n", ((((rdma_read_payload.load() + rdma_write_payload.load() + (rdma_send_alloc_payload.load() + rdma_send_insert_payload.load() + 
                                rdma_send_fetch_payload.load() + rdma_send_gc_payload.load()) + rdma_cas_payload.load()) * 1.0) / (duration.count())) * 1000000.0) / (1024 * 1024 * 1024 * 1.0));
    printf("Num Operations: %lu\n", (LOAD_SIZE / num_clients));
#endif

    printf("Network Latency: load, %f ,sec\n", ((rdma_read_latency.load() + rdma_write_latency.load() + rdma_cas_latency.load() + rdma_send_alloc_latency.load() + 
                rdma_send_insert_latency.load() + rdma_send_fetch_latency.load() + rdma_send_gc_latency.load()) * 1.0) / 1000000000.0);
    printf("RDMA READ RTTs: %lu\n", rdma_read_rtts.load());
    printf("RDMA READ payload: %lu\n", rdma_read_payload.load());
    printf("RDMA READ latency: %lu\n", rdma_read_latency.load());
    printf("RDMA WRITE RTTs: %lu\n", rdma_write_rtts.load());
    printf("RDMA WRITE payload: %lu\n", rdma_write_payload.load());
    printf("RDMA WRITE latency: %lu\n", rdma_write_latency.load());

    printf("RDMA send_alloc RTTs: %lu\n", rdma_send_alloc_rtts.load());
    printf("RDMA send_alloc payload: %lu\n", rdma_send_alloc_payload.load());
    printf("RDMA send_alloc latency: %lu\n", rdma_send_alloc_latency.load());
    printf("RDMA send_insert RTTs: %lu\n", rdma_send_insert_rtts.load());
    printf("RDMA send_insert payload: %lu\n", rdma_send_insert_payload.load());
    printf("RDMA send_insert latency: %lu\n", rdma_send_insert_latency.load());
    printf("RDMA send_fetch RTTs: %lu\n", rdma_send_fetch_rtts.load());
    printf("RDMA send_fetch payload: %lu\n", rdma_send_fetch_payload.load());
    printf("RDMA send_fetch latency: %lu\n", rdma_send_fetch_latency.load());
    printf("RDMA send_gc RTTs: %lu\n", rdma_send_gc_rtts.load());
    printf("RDMA send_gc payload: %lu\n", rdma_send_gc_payload.load());
    printf("RDMA send_gc latency: %lu\n", rdma_send_gc_latency.load());

    printf("RDMA SEND RTTs: %lu\n", rdma_send_alloc_rtts.load() + rdma_send_insert_rtts.load() + rdma_send_fetch_rtts.load() + rdma_send_gc_rtts.load());
    printf("RDMA SEND payload: %lu\n", rdma_send_alloc_payload.load() + rdma_send_insert_payload.load() + rdma_send_fetch_payload.load() + rdma_send_gc_payload.load());
    printf("RDMA SEND latency: %lu\n", rdma_send_alloc_latency.load() + rdma_send_insert_latency.load() + rdma_send_fetch_latency.load() + rdma_send_gc_latency.load());

    printf("RDMA CAS RTTs: %lu\n", rdma_cas_rtts.load());
    printf("RDMA CAS payload: %lu\n", rdma_cas_payload.load());
    printf("RDMA CAS latency: %lu\n", rdma_cas_latency.load());

    printf("Local cache hit counter = %lu\n", local_cache_hit_counter.load());
    printf("Local cache miss counter = %lu\n", local_cache_miss_counter.load());
    printf("Miss rate = %f percentage\n\n", (local_cache_miss_counter.load() * 1.0) / ((local_cache_hit_counter.load() + local_cache_miss_counter.load()) * 1.0) * 100.0);

    rdma_write_rtts.store(0);
    rdma_cas_rtts.store(0);
    rdma_read_rtts.store(0);
    rdma_send_alloc_rtts.store(0);
    rdma_send_insert_rtts.store(0);
    rdma_send_fetch_rtts.store(0);
    rdma_send_gc_rtts.store(0);

    rdma_write_latency.store(0);
    rdma_cas_latency.store(0);
    rdma_read_latency.store(0);
    rdma_send_alloc_latency.store(0);
    rdma_send_insert_latency.store(0);
    rdma_send_fetch_latency.store(0);
    rdma_send_gc_latency.store(0);

    rdma_write_payload.store(0);
    rdma_cas_payload.store(0);
    rdma_read_payload.store(0);
    rdma_send_alloc_payload.store(0);
    rdma_send_insert_payload.store(0);
    rdma_send_fetch_payload.store(0);
    rdma_send_gc_payload.store(0);

    local_cache_hit_counter.store(0);
    local_cache_miss_counter.store(0);
#endif

    return MITSUME_SUCCESS;
}

int mitsume_benchmark_thread_run(int thread_num,
        struct mitsume_ctx_clt *local_ctx_clt,
        void *(*fun_ptr)(void *input_metadata)) {
    int i = 0;
    total_op = 0;
    mitsume_sync_barrier_init(thread_num, get_client_id(local_ctx_clt),
            MITSUME_CLT_NUM);
    pthread_t thread_job[MITSUME_CLT_CONSUMER_NUMBER];
    assert(MITSUME_BENCHMARK_SIZE);
    assert(MITSUME_BENCHMARK_TIME);

    if (thread_num > MITSUME_CLT_CONSUMER_NUMBER) {
        die_printf("thread_num is larger than max clt number\n");
        exit(1);
    }
    sleep(1);

    for (i = 0; i < thread_num; i++) {
        pthread_create(&thread_job[i], NULL, sync_across_cns,
                &local_ctx_clt->thread_metadata[i]);
    }
    for (i = 0; i < thread_num; i++) {
        pthread_join(thread_job[i], NULL);
    }

    auto starttime = std::chrono::system_clock::now();
    for (i = 0; i < thread_num; i++) {
        pthread_create(&thread_job[i], NULL, fun_ptr,
                &local_ctx_clt->thread_metadata[i]);
    }

    for (i = 0; i < thread_num; i++) {
        pthread_join(thread_job[i], NULL);
    }
    auto duration = std::chrono::duration_cast<std::chrono::microseconds> (
            std::chrono::system_clock::now() - starttime);
#ifdef CLOVER_DEBUG
#ifdef ENABLE_DISAGGRE_PARTITION
    printf("Throughput: run, %f , ops/us\n", ((num_run_keys.load()) * 1.0) / duration.count());
    printf("Latency: run, %f ,sec\n", duration.count() / 1000000.0);
    printf("Bandwidth: run, %f ,GB/sec\n", ((((rdma_read_payload.load() + rdma_write_payload.load() + (rdma_send_alloc_payload.load() + rdma_send_insert_payload.load() + 
                                rdma_send_fetch_payload.load() + rdma_send_gc_payload.load()) + rdma_cas_payload.load()) * 1.0) / (duration.count())) * 1000000.0) / (1024 * 1024 * 1024 * 1.0));
    printf("Num Operations: %lu\n", num_run_keys.load());
#else
    printf("Throughput: run, %f , ops/us\n", ((RUN_SIZE / num_clients) * 1.0) / duration.count());
    printf("Latency: run, %f ,sec\n", duration.count() / 1000000.0);
    printf("Bandwidth: run, %f ,GB/sec\n", ((((rdma_read_payload.load() + rdma_write_payload.load() + (rdma_send_alloc_payload.load() + rdma_send_insert_payload.load() + 
                                rdma_send_fetch_payload.load() + rdma_send_gc_payload.load()) + rdma_cas_payload.load()) * 1.0) / (duration.count())) * 1000000.0) / (1024 * 1024 * 1024 * 1.0));
    printf("Num Operations: %lu\n", (RUN_SIZE / num_clients));
#endif

    printf("Network Latency: run, %f ,sec\n", ((rdma_read_latency.load() + rdma_write_latency.load() + rdma_cas_latency.load() + rdma_send_alloc_latency.load() + 
                rdma_send_insert_latency.load() + rdma_send_fetch_latency.load() + rdma_send_gc_latency.load()) * 1.0) / 1000000000.0);
    printf("RDMA READ RTTs: %lu\n", rdma_read_rtts.load());
    printf("RDMA READ payload: %lu\n", rdma_read_payload.load());
    printf("RDMA READ latency: %lu\n", rdma_read_latency.load());
    printf("RDMA WRITE RTTs: %lu\n", rdma_write_rtts.load());
    printf("RDMA WRITE payload: %lu\n", rdma_write_payload.load());
    printf("RDMA WRITE latency: %lu\n", rdma_write_latency.load());

    printf("RDMA send_alloc RTTs: %lu\n", rdma_send_alloc_rtts.load());
    printf("RDMA send_alloc payload: %lu\n", rdma_send_alloc_payload.load());
    printf("RDMA send_alloc latency: %lu\n", rdma_send_alloc_latency.load());
    printf("RDMA send_insert RTTs: %lu\n", rdma_send_insert_rtts.load());
    printf("RDMA send_insert payload: %lu\n", rdma_send_insert_payload.load());
    printf("RDMA send_insert latency: %lu\n", rdma_send_insert_latency.load());
    printf("RDMA send_fetch RTTs: %lu\n", rdma_send_fetch_rtts.load());
    printf("RDMA send_fetch payload: %lu\n", rdma_send_fetch_payload.load());
    printf("RDMA send_fetch latency: %lu\n", rdma_send_fetch_latency.load());
    printf("RDMA send_gc RTTs: %lu\n", rdma_send_gc_rtts.load());
    printf("RDMA send_gc payload: %lu\n", rdma_send_gc_payload.load());
    printf("RDMA send_gc latency: %lu\n", rdma_send_gc_latency.load());

    printf("RDMA SEND RTTs: %lu\n", rdma_send_alloc_rtts.load() + rdma_send_insert_rtts.load() + rdma_send_fetch_rtts.load() + rdma_send_gc_rtts.load());
    printf("RDMA SEND payload: %lu\n", rdma_send_alloc_payload.load() + rdma_send_insert_payload.load() + rdma_send_fetch_payload.load() + rdma_send_gc_payload.load());
    printf("RDMA SEND latency: %lu\n", rdma_send_alloc_latency.load() + rdma_send_insert_latency.load() + rdma_send_fetch_latency.load() + rdma_send_gc_latency.load());

    printf("RDMA CAS RTTs: %lu\n", rdma_cas_rtts.load());
    printf("RDMA CAS payload: %lu\n", rdma_cas_payload.load());
    printf("RDMA CAS latency: %lu\n", rdma_cas_latency.load());

    printf("Local cache hit counter = %lu\n", local_cache_hit_counter.load());
    printf("Local cache miss counter = %lu\n", local_cache_miss_counter.load());
    printf("Miss rate = %f percentage\n\n", (local_cache_miss_counter.load() * 1.0) / ((local_cache_hit_counter.load() + local_cache_miss_counter.load()) * 1.0) * 100.0);

    rdma_write_rtts.store(0);
    rdma_cas_rtts.store(0);
    rdma_read_rtts.store(0);
    rdma_send_alloc_rtts.store(0);
    rdma_send_insert_rtts.store(0);
    rdma_send_fetch_rtts.store(0);
    rdma_send_gc_rtts.store(0);

    rdma_write_latency.store(0);
    rdma_cas_latency.store(0);
    rdma_read_latency.store(0);
    rdma_send_alloc_latency.store(0);
    rdma_send_insert_latency.store(0);
    rdma_send_fetch_latency.store(0);
    rdma_send_gc_latency.store(0);

    rdma_write_payload.store(0);
    rdma_cas_payload.store(0);
    rdma_read_payload.store(0);
    rdma_send_alloc_payload.store(0);
    rdma_send_insert_payload.store(0);
    rdma_send_fetch_payload.store(0);
    rdma_send_gc_payload.store(0);

    local_cache_hit_counter.store(0);
    local_cache_miss_counter.store(0);
#endif

    for (i = 0; i < thread_num; i++) {
        pthread_create(&thread_job[i], NULL, sync_across_cns,
                &local_ctx_clt->thread_metadata[i]);
    }
    for (i = 0; i < thread_num; i++) {
        pthread_join(thread_job[i], NULL);
    }

    return MITSUME_SUCCESS;
}
#else
int mitsume_benchmark_thread(int thread_num,
        struct mitsume_ctx_clt *local_ctx_clt,
        void *(*fun_ptr)(void *input_metadata)) {
    int i = 0;
    total_op = 0;
    mitsume_sync_barrier_init(thread_num, get_client_id(local_ctx_clt),
            MITSUME_CLT_NUM);
    pthread_t thread_job[MITSUME_CLT_CONSUMER_NUMBER];
    chrono::milliseconds before, after;
    assert(MITSUME_BENCHMARK_SIZE);
    assert(MITSUME_BENCHMARK_TIME);

    if (thread_num > MITSUME_CLT_CONSUMER_NUMBER) {
        die_printf("thread_num is larger than max clt number\n");
        exit(1);
    }
    sleep(1);
    for (i = 0; i < thread_num; i++) {
        pthread_create(&thread_job[i], NULL, fun_ptr,
                &local_ctx_clt->thread_metadata[i]);
    }
    while (ready_flag == 0)
        ;
    start_flag = 1;
    cout << "start waiting" << endl;
    before = get_current_ms();
    sleep(MITSUME_BENCHMARK_RUN_TIME);
    cout << "after waiting" << endl;
    end_flag = 1;
    after = get_current_ms();
    for (i = 0; i < thread_num; i++) {
        pthread_join(thread_job[i], NULL);
    }
    // MITSUME_PRINT("all %d threads are finished\n", thread_num);
    cout << total_op.load() << endl;
    cout << fixed << "throughput "
        << ((float)total_op.load() / (after - before).count()) * 1000
        << " /seconds" << endl;
    // mitsume_stat_show();
    return MITSUME_SUCCESS;
}
#endif

int mitsume_benchmark(struct mitsume_ctx_clt *local_ctx_clt, int _num_clients, int _machine_id) {
#if 1
    wl = WORKLOAD_TYPE;
    dstr = WORKLOAD_DIST;
    init_keys.reserve(LOAD_SIZE);
    keys.reserve(RUN_SIZE);
    ops.reserve(RUN_SIZE);

    num_clients = _num_clients;
    machine_id = _machine_id;

    memset(&init_keys[0], 0x00, LOAD_SIZE * sizeof(uint64_t));
    memset(&keys[0], 0x00, RUN_SIZE * sizeof(uint64_t));
    memset(&ops[0], 0x00, RUN_SIZE * sizeof(int));

    num_load_keys.store(0);
    num_run_keys.store(0);

    std::string init_file, txn_file;

    if (wl == WORKLOAD_A) {
        if (dstr == ZIPFIAN) {
            printf("WorkloadA, Zipfian\n");
            init_file = "/home/cc/projects/Delta-Hybrid/index-microbench/workloads/zipfian/0.99/load_unif_int.dat";
            txn_file = "/home/cc/projects/Delta-Hybrid/index-microbench/workloads/zipfian/0.99/txnsa_unif_int.dat";
        } else {
            printf("WorkloadA, Uniform\n");
            init_file = "/home/cc/projects/Delta-Hybrid/index-microbench/workloads/uniform/load_unif_int.dat";
            txn_file = "/home/cc/projects/Delta-Hybrid/index-microbench/workloads/uniform/txnsa_unif_int.dat";
        }
    } else if (wl == WORKLOAD_B) {
        if (dstr == ZIPFIAN) {
            printf("WorkloadB, Zipfian\n");
            init_file = "/home/cc/projects/Delta-Hybrid/index-microbench/workloads/zipfian/0.99/load_unif_int.dat";
            txn_file = "/home/cc/projects/Delta-Hybrid/index-microbench/workloads/zipfian/0.99/txnsb_unif_int.dat";
        } else {
            printf("WorkloadB, Uniform\n");
            init_file = "/home/cc/projects/Delta-Hybrid/index-microbench/workloads/uniform/load_unif_int.dat";
            txn_file = "/home/cc/projects/Delta-Hybrid/index-microbench/workloads/uniform/txnsb_unif_int.dat";
        }
    } else if (wl == WORKLOAD_C) {
        if (dstr == ZIPFIAN) {
            printf("WorkloadC, Zipfian\n");
            init_file = "/home/cc/projects/Delta-Hybrid/index-microbench/workloads/zipfian/0.99/load_unif_int.dat";
            txn_file = "/home/cc/projects/Delta-Hybrid/index-microbench/workloads/zipfian/0.99/txnsc_unif_int.dat";
        } else {
            printf("WorkloadC, Uniform\n");
            init_file = "/home/cc/projects/Delta-Hybrid/index-microbench/workloads/uniform/load_unif_int.dat";
            txn_file = "/home/cc/projects/Delta-Hybrid/index-microbench/workloads/uniform/txnsc_unif_int.dat";
        }
    } else if (wl == WORKLOAD_D) {
        if (dstr == ZIPFIAN) {
            printf("WorkloadD, Zipfian\n");
            init_file = "/home/cc/projects/Delta-Hybrid/index-microbench/workloads/zipfian/0.99/load_unif_int.dat";
            txn_file = "/home/cc/projects/Delta-Hybrid/index-microbench/workloads/zipfian/0.99/txnsd_unif_int.dat";
        } else {
            printf("WorkloadD, Uniform\n");
            init_file = "/home/cc/projects/Delta-Hybrid/index-microbench/workloads/uniform/load_unif_int.dat";
            txn_file = "/home/cc/projects/Delta-Hybrid/index-microbench/workloads/uniform/txnsd_unif_int.dat";
        }
    } else if (wl == WORKLOAD_F) {
        if (dstr == ZIPFIAN) {
            printf("WorkloadF, Zipfian\n");
            init_file = "/home/cc/projects/Delta-Hybrid/index-microbench/workloads/zipfian/0.99/load_unif_int.dat";
            txn_file = "/home/cc/projects/Delta-Hybrid/index-microbench/workloads/zipfian/0.99/txnsf_unif_int.dat";
        } else {
            printf("WorkloadF, Uniform\n");
            init_file = "/home/cc/projects/Delta-Hybrid/index-microbench/workloads/uniform/load_unif_int.dat";
            txn_file = "/home/cc/projects/Delta-Hybrid/index-microbench/workloads/uniform/txnsf_unif_int.dat";
        }
    }

    std::string op;
    uint64_t key;

    std::string insert("INSERT");
    std::string update("UPDATE");
    std::string read("READ");

    int count = 0;
    std::ifstream infile_load(init_file);
    while ((count < LOAD_SIZE) && infile_load.good()) {
        infile_load >> op >> key;
        if (op.compare(insert) != 0) {
            std::cout << "READING LOAD FILE FAIL!\n";
            return -1;
        }

#ifdef ENABLE_DISAGGRE_PARTITION
        if ((int)((uint64_t)murmur3_32((uint8_t *)&key, sizeof(uint64_t), 0x9747b28c) % (uint64_t)(num_clients)) == (int)(machine_id - 1)) {
            init_keys.push_back(key);
            num_load_keys++;
        }
#else
        init_keys.push_back(key);
#endif
        count++;
    }

    fprintf(stderr, "Loaded %d keys\n", count);

    count = 0;
    std::ifstream infile_txn(txn_file);
    while ((count < RUN_SIZE) && infile_txn.good()) {
        infile_txn >> op >> key;

#ifdef ENABLE_DISAGGRE_PARTITION
        if ((int)((uint64_t)murmur3_32((uint8_t *)&key, sizeof(uint64_t), 0x9747b28c) % (uint64_t)(num_clients)) == (int)(machine_id - 1)) {
            if (op.compare(insert) == 0) {
                ops.push_back(OP_INSERT);
                keys.push_back(key);
            } else if (op.compare(update) == 0) {
                ops.push_back(OP_UPDATE);
                keys.push_back(key);
            } else if (op.compare(read) == 0) {
                ops.push_back(OP_READ);
                keys.push_back(key);
            } else {
                std::cout << "UNRECOGNIZED CMD!\n";
                return -1;
            }

            num_run_keys++;
        }
#else
        if (op.compare(insert) == 0) {
            ops.push_back(OP_INSERT);
            keys.push_back(key);
        } else if (op.compare(update) == 0) {
            ops.push_back(OP_UPDATE);
            keys.push_back(key);
        } else if (op.compare(read) == 0) {
            ops.push_back(OP_READ);
            keys.push_back(key);
        } else {
            std::cout << "UNRECOGNIZED CMD!\n";
            return -1;
        }
#endif

        count++;
    }

    system("free -m");
    mitsume_benchmark_thread_load(MITSUME_BENCHMARK_THREAD_NUM, local_ctx_clt, &mitsume_benchmark_ycsb_load);
    system("free -m");
    mitsume_benchmark_thread_run(MITSUME_BENCHMARK_THREAD_NUM, local_ctx_clt, &mitsume_benchmark_ycsb_run);
    system("free -m");
#else
    mitsume_benchmark_thread(MITSUME_BENCHMARK_THREAD_NUM, local_ctx_clt, &mitsume_benchmark_latency);
#endif
    return 0;
}
