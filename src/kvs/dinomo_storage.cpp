#include <stdlib.h>
#include <stdbool.h>
#include <sys/time.h>

#include "kvs/debug.h"
#include "kvs/ib.h"
#include "kvs/setup_ib.h"
#include "kvs/ib_config.h"
#include "kvs/sock.h"
#include "kvs/dinomo_storage.hpp"

#include <stack>

#include <libcuckoo/cuckoohash_map.hh>

#include <random>

#define GET_RANK(x)                 (x >> 16)
#define GET_CLIENT_THREAD_ID(x)     (((1 << 16) - 1) & x)
#define IS_MERGE_ALL(x)             ((1 << 31) & x)
#define IS_FAILOVER(x)              ((1 << 30) & x)
#define IS_INSTALL_INDIRECT(x)      ((1 << 29) & x)
#define IS_REMOVE_INDIRECT(x)       ((1 << 28) & x)
#define IS_PUT_META(x)              ((1 << 27) & x)
#define IS_GET_META(x)              ((1 << 26) & x)

#define INVALIDATE_TYPE_BITS(x)     ((x << 6) >> 6)

#define ACTIVATE(x)                 ((1 << 15) & x)

#ifdef SHARED_NOTHING
GlobalRingMap global_hash_rings;
LocalRingMap local_hash_rings;
std::vector<std::unordered_map<uint64_t, uint64_t>> stored_key_maps;
const vector<Tier> kMemTier = {Tier::MEMORY};

Tier kSelfTier;
vector<Tier> kSelfTierIdVector;

unsigned kMemoryThreadCount;
unsigned kStorageThreadCount;

uint64_t kMemoryNodeCapacity;
uint64_t kStorageNodeCapacity;

unsigned kDefaultGlobalMemoryReplication = 1;
unsigned kDefaultGlobalStorageReplication = 0;
unsigned kDefaultLocalReplication = 1;

ZmqUtil zmq_util;
ZmqUtilInterface *kZmqUtil = &zmq_util;

HashRingUtil hash_ring_util;
HashRingUtilInterface *kHashRingUtil = &hash_ring_util;
#endif

// define dpm report threshold (in seconds)
const unsigned kDPMReportThreshold = 4;

tbb::concurrent_queue<uint64_t> *reserved_alloc_queue;
libcuckoo::cuckoohash_map<std::string, void *> *metadata_store;

std::atomic<uint64_t> merge_counter;
std::mutex *progress_table;

#include "ssmem.h"

typedef struct request_data {
    size_t key_len;
    char *key;
    uint64_t val_len;
    void *value;
} RequestData;

typedef struct metadata_value {
    uint64_t val_len;
    char value[];
} MetadataValue;

typedef struct thread_data {
    uint32_t id;
#ifndef SHARED_NOTHING
    clht_t *ht;
#else
    clht_t **ht;
#endif
} thread_data_t;

typedef struct barrier {
    pthread_cond_t complete;
    pthread_mutex_t mutex;
    int count;
    int crossing;
} barrier_t;

void barrier_init(barrier_t *b, int n) {
    pthread_cond_init(&b->complete, NULL);
    pthread_mutex_init(&b->mutex, NULL);
    b->count = n;
    b->crossing = 0;
}

void barrier_cross(barrier_t *b) {
    pthread_mutex_lock(&b->mutex);
    b->crossing++;
    if (b->crossing < b->count) {
        pthread_cond_wait(&b->complete, &b->mutex);
    } else {
        pthread_cond_broadcast(&b->complete);
        b->crossing = 0;
    }
    pthread_mutex_unlock(&b->mutex);
}

barrier_t barrier;

thread_data_t *tds;

static inline void mfence() {
    asm volatile("mfence":::"memory");
}

static inline void clflush(char *data, int len, bool fence)
{
    volatile char *ptr = (char *)((unsigned long)data &~(63));
    if (fence)
        mfence();
    for (; ptr<data+len; ptr+=64) {
#ifdef CLFLUSH
        asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
#elif CLFLUSH_OPT
        asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(ptr)));
#elif CLWB
        asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(ptr)));
#endif
    }
    if (fence)
        mfence();
}

RequestData *deserialize(char *buf_ptr, bool is_put)
{
    uint64_t offset = 0, aligned_len = 0;
    RequestData *req_data = (RequestData *) malloc(sizeof(RequestData));

    if (is_put)
    {
        memcpy(&req_data->key_len, buf_ptr, sizeof(size_t));
        offset += sizeof(size_t);
        req_data->key = (char *)((uint64_t)buf_ptr + offset);
        offset += req_data->key_len;
        memcpy(&req_data->val_len, buf_ptr + offset, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        req_data->value = (void *)((uint64_t)buf_ptr + offset);
    }
    else
    {
        memcpy(&req_data->key_len, buf_ptr, sizeof(size_t));
        offset += sizeof(size_t);
        req_data->key = (char *)((uint64_t)buf_ptr + offset);
        req_data->val_len = 0;
        req_data->value = NULL;
    }

    return req_data;
}

void *ib_connection_manager_thread(void *arg)
{
    int i = 0, n = 0, ret = 0, sockfd = 0, peer_sockfd = 0, peer_idx = -1;
    struct sockaddr_in peer_addr;
    socklen_t peer_addr_len = sizeof(struct sockaddr_in);
    char sock_buf[64] = {'\0'};
    struct QPInfo *local_qp_info = NULL;
    struct QPInfo *remote_qp_info = NULL;

    sockfd = sock_create_bind(config_info.sock_port);
    check(sockfd > 0, "Failed to create server socket");

    while (1) {
        listen(sockfd, 5);

        peer_sockfd = accept(sockfd, (struct sockaddr *)&peer_addr, &peer_addr_len);
        check(peer_sockfd > 0, "Failed to create peer_sockfd");
#ifdef SHARED_NOTHING
        std::string peer_ip(inet_ntoa(peer_addr.sin_addr));
#endif

        // get qp_info from client
        remote_qp_info = (struct QPInfo *) calloc(config_info.threads_per_memory, sizeof(struct QPInfo));
        check(remote_qp_info != NULL, "Failed to allocate remote_qp_info");

        ret = sock_get_qp_info(peer_sockfd, remote_qp_info, config_info.threads_per_memory);
        check(ret == 0, "Failed to get qp_info from client");

        peer_idx = remote_qp_info[0].rank;

#ifdef SHARED_NOTHING
        peer_ip_addresses[peer_idx] = peer_ip;
        global_hash_rings[Tier::MEMORY].insert(peer_ip, peer_ip, 0, 0);
#endif

        local_qp_info = (struct QPInfo *) calloc(config_info.threads_per_memory, sizeof(struct QPInfo));
        check(local_qp_info != NULL, "Failed to allocate local_qp_info");
        for (i = 0; i < config_info.threads_per_memory; i++) {
            local_qp_info[i].lid = ib_res.port_attr.lid;
            local_qp_info[i].qp_num = ib_res.qp[(peer_idx * config_info.threads_per_memory) + i]->qp_num;
            local_qp_info[i].rank = config_info.rank;
            local_qp_info[i].rkey_pool = ib_res.mr_pool->rkey;
#ifndef SHARED_NOTHING
            local_qp_info[i].raddr_pool = (uintptr_t) ib_res.ib_pool;
#else
            local_qp_info[i].raddr_pool = (uintptr_t) ib_res.ib_partitioned_pool[(uint64_t)((peer_idx * config_info.threads_per_memory) + i)];
#endif
            local_qp_info[i].rkey_buf = ib_res.mr_buf->rkey;
            local_qp_info[i].raddr_buf = (uintptr_t) ib_res.ib_buf + 
                ((uint64_t)((peer_idx * config_info.threads_per_memory) + i) * (uint64_t)config_info.msg_size);
        }

        ret = sock_set_qp_info(peer_sockfd, local_qp_info, config_info.threads_per_memory);
        check(ret == 0, "Failed to send qp_info to client[%d]", peer_idx);

        // change send QP state to RTS
        for (i = 0; i < config_info.threads_per_memory; i++) {
            ret = modify_qp_to_rts(ib_res.qp[(peer_idx * config_info.threads_per_memory) + i],
                    remote_qp_info[i].qp_num, remote_qp_info[i].lid);
            check(ret == 0, "Failed to modify qp[%d] to rts", (peer_idx * config_info.threads_per_memory) + i);
            LOG("\tqp[%" PRIu32 "] <-> qp[%" PRIu32 "]", ib_res.qp[(peer_idx * config_info.threads_per_memory) + i]->qp_num,
                    remote_qp_info[i].qp_num);
        }

        // sync with clients
        n = sock_read(peer_sockfd, sock_buf, sizeof(SOCK_SYNC_MSG));
        check(n == sizeof(SOCK_SYNC_MSG), "Failed to receive sync from client");

        n = sock_write(peer_sockfd, sock_buf, sizeof(SOCK_SYNC_MSG));
        check(n == sizeof(SOCK_SYNC_MSG), "Failed to write sync to client");

        close(peer_sockfd);

        free(local_qp_info);
        free(remote_qp_info);
    }

error:
    pthread_exit((void *)0);
}

MetadataValue *get_metadata(RequestData *req_data)
{
    MetadataValue *mv = NULL;

    auto find_functor = [&](void * &v) mutable {
        MetadataValue *old_mv = (MetadataValue *)v;
        mv = (MetadataValue *)malloc(sizeof(MetadataValue) + old_mv->val_len);
        memcpy(mv, old_mv, sizeof(MetadataValue) + old_mv->val_len);
    };

    metadata_store->find_fn(std::string(req_data->key, req_data->key_len), find_functor);

    return mv;
}

void put_metadata(RequestData *req_data)
{
    MetadataValue *mv = (MetadataValue *)malloc(sizeof(MetadataValue) + req_data->val_len);
    mv->val_len = req_data->val_len;
    memcpy(mv->value, req_data->value, req_data->val_len);
    if (!metadata_store->contains(std::string(req_data->key, req_data->key_len)))
    {
        metadata_store->insert(std::string(req_data->key, req_data->key_len), mv);
    }
    else
    {
        auto update_functor = [&](void * &v) {
            free(v);
            v = std::forward<void *>(mv);
        };

        metadata_store->update_fn(std::string(req_data->key, req_data->key_len), update_functor);
    }
}

#ifndef SHARED_NOTHING
void merge(long thread_id, uint64_t *log_mapping_table, int client_rank, int client_thread_id)
#else
void merge(long thread_id, uint64_t **log_mapping_table, int client_rank, int client_thread_id)
#endif
{
    // checkpoint the whole oplogs
    uint64_t offset, snapshot_n;
    int k = ((int)client_rank * config_info.threads_per_memory) + (int)client_thread_id;
    log_block *first = NULL, *prev = NULL, *next = NULL;
    std::stack<log_block *> work_stack;

    progress_table[k].lock();

#ifndef SHARED_NOTHING
    first = (log_block *)pmemobj_direct({pool_uuid, log_mapping_table[k]});
#else
    first = (log_block *)pmemobj_direct({pool_uuid, *log_mapping_table[k]});
#endif

    if (first != NULL) {
        prev = first;
        next = (log_block *)pmemobj_direct({pool_uuid, prev->next.load()});
        if (next != NULL) {
            while (1) {
                work_stack.push(prev);
                snapshot_n = next->next.load();
                if (snapshot_n != 0) {
                    prev = next;
                    next = (log_block *)pmemobj_direct({pool_uuid, snapshot_n});
                } else {
                    break;
                }
            }
        } else {
            next = prev;
            prev = NULL;
        }

        do {
            offset = next->merged_offset.load();
            oplogs *opL = NULL;
            while (offset < next->offset.load()) {
                opL = (oplogs *)(next->buffer + offset);
                if (opL->op_type == OP_INSERT) {
#ifndef SHARED_NOTHING
                    if (!clht_put(tds[thread_id].ht, opL->key, pmemobj_oid(opL).off))
                        fprintf(stderr, "[%s] Fail to insert new key (%lu)\n", __func__, opL->key);
#else
                    if (!clht_put(tds[thread_id].ht[k], opL->key, pmemobj_oid(opL).off))
                        fprintf(stderr, "[%s] Fail to insert new key (%lu)\n", __func__, opL->key);
                    stored_key_maps[k][opL->key] = pmemobj_oid(opL).off;
#endif
                } else if (opL->op_type == OP_UPDATE) {
#ifndef SHARED_NOTHING
                    uint64_t update_ret = clht_update(tds[thread_id].ht, opL->key, pmemobj_oid(opL).off);
#else
                    uint64_t update_ret = clht_update(tds[thread_id].ht[k], opL->key, pmemobj_oid(opL).off);
                    stored_key_maps[k][opL->key] = pmemobj_oid(opL).off;
#endif
                    if (update_ret != 0) {
                        oplogs *oLog = (oplogs *)pmemobj_direct({pool_uuid, update_ret});
                        log_block *log_block_merged = (log_block *)((uint64_t)oLog - (uint64_t)oLog->offset - LOG_BLOCK_HEADER_LEN);
                        log_block_merged->local_invalid_counter.fetch_add(1);
                        if (log_block_merged != next && log_block_merged->valid_counter.load() == 
                                (log_block_merged->local_invalid_counter.load() + 
                                 log_block_merged->global_invalid_counter.load())) {
                            reserved_alloc_queue->push((uint64_t)log_block_merged);
                        }
                    }
                } else if (opL->op_type == OP_DELETE) {
#ifndef SHARED_NOTHING
                    uint64_t remove_ret = clht_remove(tds[thread_id].ht, opL->key);
#else
                    uint64_t remove_ret = clht_remove(tds[thread_id].ht[k], opL->key);
                    stored_key_maps[k].erase(opL->key);
#endif
                    if (remove_ret != 0) {
                        oplogs *oLog = (oplogs *)pmemobj_direct({pool_uuid, remove_ret});
                        log_block *log_block_merged = (log_block *)((uint64_t)oLog - (uint64_t)oLog->offset - LOG_BLOCK_HEADER_LEN);
                        log_block_merged->local_invalid_counter.fetch_add(1);
                        if (log_block_merged != next && log_block_merged->valid_counter.load() == 
                                (log_block_merged->local_invalid_counter.load() + 
                                 log_block_merged->global_invalid_counter.load())) {
                            reserved_alloc_queue->push((uint64_t)log_block_merged);
                        }
                    }
                } else {
                    // There are no more log entries
                    break;
                }

                offset = offset + sizeof(oplogs) + opL->val_len;
            }

            next->merged_offset.store(next->offset.load(), std::memory_order_relaxed);

            if (next != first) {
                prev->next.store(0, std::memory_order_relaxed);
                clflush((char *)&prev->next, sizeof(uint64_t), true);

                if (next->valid_counter.load() == (next->local_invalid_counter.load() + next->global_invalid_counter.load())) {
                    reserved_alloc_queue->push((uint64_t)next);
                    //PMEMoid invalid_log_block = pmemobj_oid(next);
                    //pmemobj_free(&invalid_log_block);
                }
            }

            next = prev;
            if (work_stack.size() > 0) {
                work_stack.pop();
                if (next != first)
                    prev = work_stack.top();
                else
                    prev = NULL;
            }
        } while (next != NULL);
    }

    progress_table[k].unlock();
}

void *server_manager_thread(void *arg)
{
    int ret = 0, i = 0, j = 0, k = 0, n = 0;
    long thread_id = (long)arg;
    int num_concurr_msgs = config_info.threads_per_memory;
    int msg_size = config_info.msg_size;

    pthread_t self;
    cpu_set_t cpuset;

    int num_wc = ib_res.num_qps / config_info.num_storage_managers;
    struct ibv_qp **qp = ib_res.qp;
    struct ibv_cq *cq = ib_res.cq[thread_id % config_info.num_storage_managers];
    struct ibv_cq **send_cq = ib_res.send_cq;
    struct ibv_srq *srq = ib_res.srq;
    struct ibv_wc *wc = NULL;
    uint32_t lkey = ib_res.mr_buf->lkey;

    char *buf_ptr = ib_res.ib_buf + (msg_size * thread_id);
    //char *buf_base = ib_res.ib_buf + (msg_size * thread_id);
    //int buf_offset = 0;
    size_t buf_size = msg_size;

    uint32_t imm_data = 0;
    int num_acked_peers = 0;
    uint64_t insertion_cnt = 0;

#ifndef SHARED_NOTHING
    tds[thread_id].id = thread_id;
    tds[thread_id].ht = farIdx;

    clht_gc_thread_init(tds[thread_id].ht, tds[thread_id].id);
    barrier_cross(&barrier);

    uint64_t *log_mapping_table = (uint64_t *) pmemobj_direct({pool_uuid, tds[thread_id].ht->log_table_addr});
#else
    zmq::context_t context(1);

    auto res = context.setctxopt(ZMQ_MAX_SOCKETS, kMaxSocketNumber);
    if (res == 0)
    {
        fprintf(stderr, "Thread %ld successfully set max socket number to %u\n", thread_id, kMaxSocketNumber);
    }
    else
    {
        fprintf(stderr, "Thread %ld: socket error number %d (%d)\n", errno, zmq_strerror(errno));
    }

    SocketCache pushers(&context, ZMQ_PUSH);

    map<Key, KeyReplication> key_replication_map;

    AddressKeysetMap failover_gossip_map;

    tds[thread_id].id = thread_id;
    tds[thread_id].ht = farIdx;

    uint64_t **log_mapping_table = (uint64_t **) malloc(config_info.total_available_memory_nodes * config_info.threads_per_memory);

    for (int i = 0; i < config_info.total_available_memory_nodes * config_info.threads_per_memory; i++) {
        clht_gc_thread_init(tds[thread_id].ht[i], tds[thread_id].id);
        log_mapping_table[i] = (uint64_t *) pmemobj_direct({pool_uuid, tds[thread_id].ht[i]->log_table_addr});
    }

    barrier_cross(&barrier);
#endif

    auto report_start = std::chrono::system_clock::now();
    auto report_end = std::chrono::system_clock::now();

    unsigned epoch = 0;
    double dpm_utilization = 0;
    uint64_t num_idle = 0, num_working = 0, num_alloc = 0, num_reuse = 0;

    // set thread affinity
    CPU_ZERO(&cpuset);
    CPU_SET((int)thread_id * 2, &cpuset);
    self = pthread_self();
    ret = pthread_setaffinity_np(self, sizeof(cpu_set_t), &cpuset);
    check(ret == 0, "thread[%ld]: failed to set thread affinity", thread_id);

    wc = (struct ibv_wc *) calloc(num_wc, sizeof(struct ibv_wc));
    check(wc != NULL, "thread[%ld]: failed to allocate wc", thread_id);

    // pre-post recvs
    if (thread_id == 0) {
        for (i = 0; i < ib_res.num_qps; i++) {
            buf_ptr = ib_res.ib_buf + (msg_size * i);
            ret = post_srq_recv((uint32_t)msg_size, lkey, (uint64_t)buf_ptr, srq, buf_ptr);
        }
    }

    while (1) {
        // poll cq
        n = ibv_poll_cq(cq, num_wc, wc);
        if (n == 0)
            num_idle++;
        else if (n > 0)
            num_working++;
        else
            check(0, "thread[%ld]: failed to poll cq", thread_id);


        for (j = 0; j < n; j++) {
            if (wc[j].status != IBV_WC_SUCCESS) {
                if (wc[j].opcode == IBV_WC_SEND) {
                    check(0, "thread[%ld]: send failed status: %s, byte len = %u",
                            thread_id, ibv_wc_status_str(wc[j].status), ntohl(wc[j].byte_len));
                } else {
                    check(0, "thread[%ld]: recv failed status: %s, byte len = %u",
                            thread_id, ibv_wc_status_str(wc[j].status), ntohl(wc[j].byte_len));
                }
            }

            if (wc[j].opcode == IBV_WC_RECV) {
                int client_rank, client_thread_id;
                char *msg_ptr = (char *)wc[j].wr_id;
                imm_data = ntohl(wc[j].imm_data);
                if (!IS_MERGE_ALL(imm_data)) {
                    // immediate data contains the information of the queue pair associated with sender
                    uint32_t raw_imm_data = INVALIDATE_TYPE_BITS(imm_data);
                    client_rank = GET_RANK(raw_imm_data);
                    client_thread_id = GET_CLIENT_THREAD_ID(raw_imm_data);

                    uint64_t raq;
                    uint64_t *raddrs = (uint64_t *) msg_ptr;
                    for (k = 0; k < MAX_PREALLOC_NUM; k++) {
                        if (reserved_alloc_queue->try_pop(raq)) {
                            //((log_block *)(raq))->merged_offset.store(0, std::memory_order_release);
                            //((log_block *)(raq))->offset.store(0, std::memory_order_release);
                            memset((void *)raq, 0, sizeof(log_block) + MAX_LOG_BLOCK_LEN);
                            //clflush((char *)raq, sizeof(log_block) + MAX_LOG_BLOCK_LEN, true);
                            raddrs[k] = raq;
                            num_reuse++;
                        } else {
                            PMEMoid ret;
                            if (pmemobj_zalloc(pop, &ret, sizeof(log_block) + MAX_LOG_BLOCK_LEN, 0)) {
                                fprintf(stderr, "pmemobj_alloc failed\n");
                                exit(0);
                            }
                            raddrs[k] = (uint64_t) pmemobj_direct(ret);
                            num_alloc++;
                        }
                    }

                    // It needs to be clarified how the associated qp can be identified
#ifdef SINGLE_DPM_MANAGER_THREAD
                    post_send_poll(MAX_PREALLOC_NUM * sizeof(uint64_t), lkey, 0, qp[(client_rank * num_concurr_msgs) + client_thread_id], msg_ptr,
                            send_cq[0]);
#else
                    post_send_poll(MAX_PREALLOC_NUM * sizeof(uint64_t), lkey, 0, qp[(client_rank * num_concurr_msgs) + client_thread_id], msg_ptr,
                            send_cq[(client_rank * num_concurr_msgs) + client_thread_id]);
#endif
                } else {
                    // Invalidate the leftmost significant bit
                    uint32_t raw_imm_data = INVALIDATE_TYPE_BITS(imm_data);
                    client_rank = GET_RANK(raw_imm_data);
                    client_thread_id = GET_CLIENT_THREAD_ID(raw_imm_data);

                    if (IS_GET_META(imm_data))
                    {
                        RequestData *req_data = deserialize(msg_ptr, false);
                        MetadataValue *mv = get_metadata(req_data);
                        if (mv != NULL) {
                            memcpy(msg_ptr, mv, sizeof(MetadataValue) + mv->val_len);
#ifdef SINGLE_DPM_MANAGER_THREAD
                            post_send_poll(sizeof(MetadataValue) + mv->val_len, lkey, 0, qp[(client_rank * num_concurr_msgs) + client_thread_id], msg_ptr,
                                    send_cq[0]);
#else
                            post_send_poll(sizeof(MetadataValue) + mv->val_len, lkey, 0, qp[(client_rank * num_concurr_msgs) + client_thread_id], msg_ptr,
                                    send_cq[(client_rank * num_concurr_msgs) + client_thread_id]);
#endif
                        } else {
                            mv = (MetadataValue *) malloc(sizeof(MetadataValue));
                            mv->val_len = 0;
                            memcpy(msg_ptr, mv, sizeof(MetadataValue));
#ifdef SINGLE_DPM_MANAGER_THREAD
                            post_send_poll(sizeof(MetadataValue), lkey, 0, qp[(client_rank * num_concurr_msgs) + client_thread_id], msg_ptr,
                                    send_cq[0]);
#else
                            post_send_poll(sizeof(MetadataValue), lkey, 0, qp[(client_rank * num_concurr_msgs) + client_thread_id], msg_ptr,
                                    send_cq[(client_rank * num_concurr_msgs) + client_thread_id]);
#endif
                        }

                        free(req_data);
                        free(mv);
                    }
                    else if (IS_PUT_META(imm_data))
                    {
                        RequestData *req_data = deserialize(msg_ptr, true);
                        put_metadata(req_data);

#ifdef SINGLE_DPM_MANAGER_THREAD
                        post_send_imm_poll(0, lkey, 0, MSG_CTL_COMMIT, qp[(client_rank * num_concurr_msgs) + client_thread_id], msg_ptr,
                                send_cq[0]);
#else
                        post_send_imm_poll(0, lkey, 0, MSG_CTL_COMMIT, qp[(client_rank * num_concurr_msgs) + client_thread_id], msg_ptr,
                                send_cq[(client_rank * num_concurr_msgs) + client_thread_id]);
#endif
                        free(req_data);
                    }
                    else if (IS_REMOVE_INDIRECT(imm_data))
                    {
                        uint64_t key = 0;
                        memcpy(&key, msg_ptr, sizeof(uint64_t));
#ifndef SHARED_NOTHING
                        clht_remove_indirect(tds[thread_id].ht, key);
#else
                        clht_remove_indirect(tds[thread_id].ht[(client_rank * num_concurr_msgs) + client_thread_id], key);
#endif

#ifdef SINGLE_DPM_MANAGER_THREAD
                        post_send_imm_poll(0, lkey, 0, MSG_CTL_STOP, qp[(client_rank * num_concurr_msgs) + client_thread_id], msg_ptr,
                                send_cq[0]);
#else
                        post_send_imm_poll(0, lkey, 0, MSG_CTL_STOP, qp[(client_rank * num_concurr_msgs) + client_thread_id], msg_ptr,
                                send_cq[(client_rank * num_concurr_msgs) + client_thread_id]);
#endif
                    }
                    else if (IS_INSTALL_INDIRECT(imm_data))
                    {
                        uint64_t key = 0;
                        PMEMoid indirect_ptr;

                        memcpy(&key, msg_ptr, sizeof(uint64_t));
                        if (pmemobj_zalloc(pop, &indirect_ptr, sizeof(uint64_t), 0)) {
                            fprintf(stderr, "pmemobj_alloc for an indirect pointer failed\n");
                            exit(0);
                        }
#ifndef SHARED_NOTHING
                        clht_install_indirect(tds[thread_id].ht, key, indirect_ptr.off);
#else
                        clht_install_indirect(tds[thread_id].ht[(client_rank * num_concurr_msgs) + client_thread_id], key, indirect_ptr.off);
#endif

#ifdef SINGLE_DPM_MANAGER_THREAD
                        post_send_imm_poll(0, lkey, 0, MSG_CTL_STOP, qp[(client_rank * num_concurr_msgs) + client_thread_id], msg_ptr,
                                send_cq[0]);
#else
                        post_send_imm_poll(0, lkey, 0, MSG_CTL_STOP, qp[(client_rank * num_concurr_msgs) + client_thread_id], msg_ptr,
                                send_cq[(client_rank * num_concurr_msgs) + client_thread_id]);
#endif
                    }
                    else
                    {
                        if (IS_FAILOVER(imm_data)) {
                            int failed_node_rank = client_thread_id;
                            client_thread_id = 0;

                            // Merge for failover
                            for (int failed_thread_id = 0; failed_thread_id < config_info.threads_per_memory; failed_thread_id++) {
                                merge(thread_id, log_mapping_table, failed_node_rank, failed_thread_id);
                            }
#ifdef SHARED_NOTHING
                            bool succeed = false;
                            unsigned seed = 0;
                            global_hash_rings[Tier::MEMORY].remove(peer_ip_addresses[failed_node_rank], peer_ip_addresses[failed_node_rank], 0);
                            for (i = 0; i < config_info.threads_per_memory; i++) {
                                for (const auto &kvpair : stored_key_maps[(failed_node_rank * config_info.threads_per_memory) + i]) {
                                    Key key = std::to_string(kvpair.first);
                                    ServerThreadList threads = kHashRingUtil->get_responsible_threads(
                                            "", key, false, global_hash_rings, local_hash_rings,
                                            key_replication_map, pushers, kMemTier, succeed, seed);
                                    if (succeed) {
                                        for (const ServerThread &thread : threads) {
                                            for (unsigned rid = 0; rid < config_info.total_available_memory_nodes; rid++) {
                                                if (peer_ip_addresses[rid] == thread.private_ip()) {
                                                    if (!clht_put(tds[thread_id].ht[(rid * config_info.threads_per_memory) +
                                                                thread.tid()], kvpair.first, kvpair.second)) {
                                                        fprintf(stderr, "[%s] Fail to insert new key (%lu)\n", __func__, kvpair.first);
                                                    } else {
                                                        stored_key_maps[(rid * config_info.threads_per_memory) + thread.tid()][kvpair.first] = kvpair.second;
                                                        clht_remove(tds[thread_id].ht[(failed_node_rank * config_info.threads_per_memory) + i], kvpair.first);
                                                    }

                                                    failover_gossip_map[thread.gossip_connect_address()].insert(key);
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                                stored_key_maps[(failed_node_rank * config_info.threads_per_memory) + i].clear();
                            }

                            send_failover_gossip(failover_gossip_map, pushers);
                            failover_gossip_map.clear();
#endif
                        } else {
                            // Merge for reconfiguration
                            merge(thread_id, log_mapping_table, client_rank, client_thread_id);
                        }
#if 0
                        merge_counter++;
                        if (merge_counter.load() == config_info.num_clients) {
                            for (uint64_t z = 0; z < merge_counter.load(); z++) {
                                post_send_imm(0, lkey, 0, MSG_CTL_STOP, qp[(z * num_concurr_msgs) + 0], msg_ptr); 
                            }
                            merge_counter.store(0);
                        }
#else
                        // ack ready for accepting reconfiguration
#ifdef SINGLE_DPM_MANAGER_THREAD
                        post_send_imm_poll(0, lkey, 0, MSG_CTL_STOP, qp[(client_rank * num_concurr_msgs) + client_thread_id], msg_ptr,
                                send_cq[0]);
#else
                        post_send_imm_poll(0, lkey, 0, MSG_CTL_STOP, qp[(client_rank * num_concurr_msgs) + client_thread_id], msg_ptr,
                                send_cq[(client_rank * num_concurr_msgs) + client_thread_id]);
#endif
#endif
                    }
                }

                // post a new receive
                post_srq_recv((uint32_t)msg_size, lkey, wc[j].wr_id, srq, msg_ptr);
            }
        }

        report_end = std::chrono::system_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::seconds>(report_end - report_start).count();
        if (duration >= kDPMReportThreshold)
        {
            epoch += 1;
            dpm_utilization = (double)num_working / (double)(num_idle + num_working);
            fprintf(stderr, "Thread id[%ld], Epoch[%u], Utilization = %g, Usage = %lu, Alloc = %lu, Reuse = %lu, Rehash = %u\n",
                    thread_id, epoch, dpm_utilization, num_alloc * (sizeof(log_block) + MAX_LOG_BLOCK_LEN),
                    num_alloc, num_reuse, num_rehashing);
            num_idle = 0, num_working = 0;
            report_start = std::chrono::system_clock::now();
        }
    }

    free(wc);
    pthread_exit((void *)0);

error:
    if (wc != NULL)
        free(wc);
    pthread_exit((void *)-1);
}

void *server_worker_thread(void *arg)
{
    int ret = 0, i = 0, j = 0, k = 0, n = 0;
    long thread_id = (long)arg;
    int msg_size = config_info.msg_size;

    pthread_t self;
    cpu_set_t cpuset;

    int num_wc = 1;
    struct ibv_qp **qp = ib_res.qp;
    struct ibv_cq *cq = ib_res.cq[thread_id % config_info.num_storage_managers];
    struct ibv_srq *srq = ib_res.srq;
    struct ibv_wc *wc = NULL;
    uint32_t lkey = ib_res.mr_buf->lkey;

    char *buf_ptr = ib_res.ib_buf + (msg_size * thread_id);
    //char *buf_base = ib_res.ib_buf + (msg_size * thread_id);
    //int buf_offset = 0;
    size_t buf_size = msg_size;

    uint32_t imm_data = 0;
    int num_acked_peers = 0;
    bool stop = false;
    uint64_t insertion_cnt = 0;

    uint64_t offset, snapshot_n;
    log_block *first, *prev, *next;
    std::stack<log_block *> work_stack;

#ifndef SHARED_NOTHING
    tds[thread_id].id = thread_id;
    tds[thread_id].ht = farIdx;

    clht_gc_thread_init(tds[thread_id].ht, tds[thread_id].id);
    barrier_cross(&barrier);

    uint64_t *log_mapping_table = (uint64_t *) pmemobj_direct({pool_uuid, tds[thread_id].ht->log_table_addr});
#else
    tds[thread_id].id = thread_id;
    tds[thread_id].ht = farIdx;

    uint64_t **log_mapping_table = (uint64_t **) malloc(config_info.total_available_memory_nodes * config_info.threads_per_memory);

    for (i = 0; i < config_info.total_available_memory_nodes * config_info.threads_per_memory; i++) {
        clht_gc_thread_init(tds[thread_id].ht[i], tds[thread_id].id);
        log_mapping_table[i] = (uint64_t *) pmemobj_direct({pool_uuid, tds[thread_id].ht[i]->log_table_addr});
    }

    barrier_cross(&barrier);
#endif

    auto report_start = std::chrono::system_clock::now();
    auto report_end = std::chrono::system_clock::now();

    unsigned epoch = 0;
    double dpm_utilization = 0;
    uint64_t num_idle = 0, num_working = 0;
    bool succeed_poll = false;

    // set thread affinity
    CPU_ZERO(&cpuset);
    CPU_SET((int)thread_id * 2, &cpuset);
    self = pthread_self();
    ret = pthread_setaffinity_np(self, sizeof(cpu_set_t), &cpuset);
    check(ret == 0, "thread[%ld]: failed to set thread affinity", thread_id);

    // pre-post recvs
    wc = (struct ibv_wc *) calloc(num_wc, sizeof(struct ibv_wc));
    check(wc != NULL, "thread[%ld]: failed to allocate wc", thread_id);

    while (stop != true) {
#ifndef SHARED_NOTHING
        for (i = 0; i < tds[thread_id].ht->log_table_size; i++) {
#else
        for (i = 0; i < config_info.total_available_memory_nodes * config_info.threads_per_memory; i++) {
#endif
            first = NULL, prev = NULL, next = NULL;

            if (progress_table[i].try_lock()) {

#ifndef SHARED_NOTHING
                first = (log_block *)pmemobj_direct({pool_uuid, log_mapping_table[i]});
#else
                first = (log_block *)pmemobj_direct({pool_uuid, *log_mapping_table[i]});
#endif
                if (first != NULL) {
                    prev = first;
                    next = (log_block *)pmemobj_direct({pool_uuid, prev->next.load()});
                    if (next != NULL) {
                        while (1) {
                            work_stack.push(prev);
                            snapshot_n = next->next.load();
                            if (snapshot_n != 0) {
                                prev = next;
                                next = (log_block *)pmemobj_direct({pool_uuid, snapshot_n});
                            } else {
                                break;
                            }
                        }

                        do {
                            offset = next->merged_offset.load();
                            oplogs *opL = NULL;
                            while (offset < next->offset.load()) {
                                opL = (oplogs *)(next->buffer + offset);
                                if (opL->op_type == OP_INSERT) {
#ifndef SHARED_NOTHING
                                    if (!clht_put(tds[thread_id].ht, opL->key, pmemobj_oid(opL).off))
                                        fprintf(stderr, "[%s] Fail to insert new key (%lu)\n", __func__, opL->key);
#else
                                    if (!clht_put(tds[thread_id].ht[i], opL->key, pmemobj_oid(opL).off))
                                        fprintf(stderr, "[%s] Fail to insert new key (%lu)\n", __func__, opL->key);
                                    stored_key_maps[i][opL->key] = pmemobj_oid(opL).off;
#endif
                                } else if (opL->op_type == OP_UPDATE) {
#ifndef SHARED_NOTHING
                                    uint64_t update_ret = clht_update(tds[thread_id].ht, opL->key, pmemobj_oid(opL).off);
#else
                                    uint64_t update_ret = clht_update(tds[thread_id].ht[i], opL->key, pmemobj_oid(opL).off);
                                    stored_key_maps[i][opL->key] = pmemobj_oid(opL).off;
#endif
                                    if (update_ret != 0) {
                                        oplogs *oLog = (oplogs *)pmemobj_direct({pool_uuid, update_ret});
                                        log_block *log_block_merged = (log_block *)((uint64_t)oLog - (uint64_t)oLog->offset - LOG_BLOCK_HEADER_LEN);
                                        log_block_merged->local_invalid_counter.fetch_add(1);
                                        if (log_block_merged != next && log_block_merged->valid_counter.load() == 
                                                (log_block_merged->local_invalid_counter.load() + 
                                                 log_block_merged->global_invalid_counter.load())) {
                                            reserved_alloc_queue->push((uint64_t)log_block_merged);
                                        }
                                    }
                                } else if (opL->op_type == OP_DELETE) {
#ifndef SHARED_NOTHING
                                    uint64_t remove_ret = clht_remove(tds[thread_id].ht, opL->key);
#else
                                    uint64_t remove_ret = clht_remove(tds[thread_id].ht[i], opL->key);
                                    stored_key_maps[i].erase(opL->key);
#endif
                                    if (remove_ret != 0) {
                                        oplogs *oLog = (oplogs *)pmemobj_direct({pool_uuid, remove_ret});
                                        log_block *log_block_merged = (log_block *)((uint64_t)oLog - (uint64_t)oLog->offset - LOG_BLOCK_HEADER_LEN);
                                        log_block_merged->local_invalid_counter.fetch_add(1);
                                        if (log_block_merged != next && log_block_merged->valid_counter.load() == 
                                                (log_block_merged->local_invalid_counter.load() + 
                                                 log_block_merged->global_invalid_counter.load())) {
                                            reserved_alloc_queue->push((uint64_t)log_block_merged);
                                        }
                                    }
                                } else {
                                    // There are no more log entries
                                    break;
                                }

                                offset = offset + sizeof(oplogs) + opL->val_len;
                            }

                            next->merged_offset.store(next->offset.load(), std::memory_order_relaxed);

                            prev->next.store(0, std::memory_order_relaxed);
                            clflush((char *)&prev->next, sizeof(uint64_t), true);

                            if (next->valid_counter.load() == (next->local_invalid_counter.load() + next->global_invalid_counter.load())) {
                                reserved_alloc_queue->push((uint64_t)next);
                                //PMEMoid invalid_log_block = pmemobj_oid(next);
                                //pmemobj_free(&invalid_log_block);
                            }

                            next = prev;
                            work_stack.pop();
                            if (next != first) prev = work_stack.top();
                        } while (next != first);

                        progress_table[i].unlock();
                        succeed_poll = true;
                    } else {
                        progress_table[i].unlock();
                    }
                } else {
                    progress_table[i].unlock();
                }
            }
        }

        if (succeed_poll) {
            num_working++;
            succeed_poll = false;
        } else {
            num_idle++;
        }

        report_end = std::chrono::system_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::seconds>(report_end - report_start).count();
        if (duration >= kDPMReportThreshold)
        {
            epoch += 1;
            dpm_utilization = (double)num_working / (double)(num_idle + num_working);
            fprintf(stderr, "Thread id[%ld], Epoch[%u], Utilization = %g\n", thread_id, epoch, dpm_utilization);
            num_idle = 0, num_working = 0, succeed_poll = false;
            report_start = std::chrono::system_clock::now();
        }
    }

    free(wc);
    pthread_exit((void *)0);

error:
    if (wc != NULL)
        free(wc);
    pthread_exit((void *)-1);
}

void sync_with_clients()
{
    int ret = 0, n = 0;
    uint64_t i = 0, j = 0, k = 0;
    int num_concurr_msgs = config_info.threads_per_memory;
    int msg_size = config_info.msg_size;
    int num_peers = config_info.num_initial_memory_nodes;

    struct ibv_qp **qp = ib_res.qp;
    struct ibv_cq *cq = ib_res.cq[0];
    struct ibv_srq *srq = ib_res.srq;
    uint32_t lkey = ib_res.mr_buf->lkey;

    char *buf_ptr = ib_res.ib_buf;
    size_t buf_size = msg_size;

    uint32_t imm_data = 0;

    // pre-post recvs
    for (i = 0; i < config_info.num_storage_managers; i++) {
        buf_ptr = ib_res.ib_buf + (msg_size * i);
        ret = post_srq_recv((uint32_t)msg_size, lkey, (uint64_t)buf_ptr, srq, buf_ptr);
    }

    // signal the client to start
    for (i = 0; i < num_peers; i++) {
        ret = post_send_imm(0, lkey, 0, MSG_CTL_START, qp[i * num_concurr_msgs], buf_ptr);
        check(ret == 0, "Failed to signal the client to start");
    }

    LOG("Server: ready to work");

error:
    return ;
}

#ifdef DINOMO_LOCAL_TEST
void *merge_all(void *arg) {
    int ret = 0, i = 0;
    long thread_id = (long)arg;

    pthread_t self;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET((int)thread_id * 2, &cpuset);
    self = pthread_self();
    ret = pthread_setaffinity_np(self, sizeof(cpu_set_t), &cpuset);

    log_block *first = NULL, *next = NULL, *prev = NULL;
    uint64_t offset, snapshot_n;
    std::stack<log_block *> work_stack;

#ifndef SHARED_NOTHING
    tds[thread_id].id = thread_id;
    tds[thread_id].ht = farIdx;

    clht_gc_thread_init(tds[thread_id].ht, tds[thread_id].id);
    barrier_cross(&barrier);

    uint64_t *log_mapping_table = (uint64_t *) pmemobj_direct({pool_uuid, tds[thread_id].ht->log_table_addr});
#else
    tds[thread_id].id = thread_id;
    tds[thread_id].ht = farIdx;

    uint64_t **log_mapping_table = (uint64_t **) malloc(config_info.total_available_memory_nodes * config_info.threads_per_memory);

    for (i = 0; i < config_info.total_available_memory_nodes * config_info.threads_per_memory; i++) {
        clht_gc_thread_init(tds[thread_id].ht[i], tds[thread_id].id);
        log_mapping_table[i] = (uint64_t *) pmemobj_direct({pool_uuid, tds[thread_id].ht[i]->log_table_addr});
    }

    barrier_cross(&barrier);
#endif

#ifndef SHARED_NOTHING
    for (i = 0; i < tds[thread_id].ht->log_table_size; i++) {
#else
    for (i = 0; i < config_info.total_available_memory_nodes * config_info.threads_per_memory; i++) {
#endif
        first = NULL, next = NULL, prev = NULL;
        if (progress_table[i].try_lock()) {
#ifndef SHARED_NOTHING
            first = (log_block *)pmemobj_direct({pool_uuid, log_mapping_table[i]});
#else
            first = (log_block *)pmemobj_direct({pool_uuid, *log_mapping_table[i]});
#endif
            if (first != NULL) {
                prev = first;
                next = (log_block *)pmemobj_direct({pool_uuid, prev->next.load()});
                if (next != NULL) {
                    while (1) {
                        work_stack.push(prev);
                        snapshot_n = next->next.load();
                        if (snapshot_n != 0) {
                            prev = next;
                            next = (log_block *)pmemobj_direct({pool_uuid, snapshot_n});
                        } else {
                            break;
                        }
                    }
                } else {
                    next = prev;
                    prev = NULL;
                }

                do {
                    offset = next->merged_offset.load();
                    oplogs *opL = NULL;
                    while (offset < next->offset.load()) {
                        opL = (oplogs *)(next->buffer + offset);
                        if (opL->op_type == OP_INSERT) {
#ifndef SHARED_NOTHING
                            if (!clht_put(tds[thread_id].ht, opL->key, pmemobj_oid(opL).off))
                                fprintf(stderr, "[%s] Fail to insert new key (%lu)\n", __func__, opL->key);
#else
                            if (!clht_put(tds[thread_id].ht[i], opL->key, pmemobj_oid(opL).off))
                                fprintf(stderr, "[%s] Fail to insert new key (%lu)\n", __func__, opL->key);
                            stored_key_maps[i][opL->key] = pmemobj_oid(opL).off;
#endif
                        } else if (opL->op_type == OP_UPDATE) {
#ifndef SHARED_NOTHING
                            uint64_t update_ret = clht_update(tds[thread_id].ht, opL->key, pmemobj_oid(opL).off);
#else
                            uint64_t update_ret = clht_update(tds[thread_id].ht[i], opL->key, pmemobj_oid(opL).off);
                            stored_key_maps[i][opL->key] = pmemobj_oid(opL).off;
#endif
                            if (update_ret != 0) {
                                oplogs *oLog = (oplogs *)pmemobj_direct({pool_uuid, update_ret});
                                log_block *log_block_merged = (log_block *)((uint64_t)oLog - (uint64_t)oLog->offset - LOG_BLOCK_HEADER_LEN);
                                log_block_merged->local_invalid_counter.fetch_add(1);
                                if (log_block_merged != next && log_block_merged->valid_counter.load() == 
                                        (log_block_merged->local_invalid_counter.load() + 
                                         log_block_merged->global_invalid_counter.load())) {
                                    reserved_alloc_queue->push((uint64_t)log_block_merged);
                                }
                            }
                        } else if (opL->op_type == OP_DELETE) {
#ifndef SHARED_NOTHING
                            uint64_t remove_ret = clht_remove(tds[thread_id].ht, opL->key);
#else
                            uint64_t remove_ret = clht_remove(tds[thread_id].ht[i], opL->key);
                            stored_key_maps[i].erase(opL->key);
#endif
                            if (remove_ret != 0) {
                                oplogs *oLog = (oplogs *)pmemobj_direct({pool_uuid, remove_ret});
                                log_block *log_block_merged = (log_block *)((uint64_t)oLog - (uint64_t)oLog->offset - LOG_BLOCK_HEADER_LEN);
                                log_block_merged->local_invalid_counter.fetch_add(1);
                                if (log_block_merged != next && log_block_merged->valid_counter.load() == 
                                        (log_block_merged->local_invalid_counter.load() + 
                                         log_block_merged->global_invalid_counter.load())) {
                                    reserved_alloc_queue->push((uint64_t)log_block_merged);
                                }
                            }
                        } else {
                            // There are no more log entries
                            break;
                        }

                        offset = offset + sizeof(oplogs) + opL->val_len;
                    }

                    next->merged_offset.store(next->offset.load(), std::memory_order_relaxed);

                    if (next != first) {
                        prev->next.store(0, std::memory_order_relaxed);
                        clflush((char *)&prev->next, sizeof(uint64_t), true);

                        if (next->valid_counter.load() == (next->local_invalid_counter.load() + next->global_invalid_counter.load())) {
                            reserved_alloc_queue->push((uint64_t)next);
                            //PMEMoid invalid_log_block = pmemobj_oid(next);
                            //pmemobj_free(&invalid_log_block);
                        }
                    }

                    next = prev;
                    if (work_stack.size() > 0) {
                        work_stack.pop();
                        if (next != first)
                            prev = work_stack.top();
                        else
                            prev = NULL;
                    }
                } while (next != NULL);
            }

            progress_table[i].unlock();
        }
    }

    pthread_exit((void *)0);

error:
    pthread_exit((void *)-1);
}

uint64_t num_alloc = 0;
std::default_random_engine generator;
std::uniform_int_distribution<uint64_t> distribution(0, UINT64_MAX - 1);

void insert_log_blocks(uint64_t num_ops, uint32_t value_size)
{
    uint64_t i = 0, j = 0;
    oplogs *opL = NULL;
    log_block *nextLog = NULL;
    uint64_t *keys = new uint64_t[num_ops];

#ifndef SHARED_NOTHING
    uint64_t *log_mapping_table = (uint64_t *) pmemobj_direct({pool_uuid, tds[0].ht->log_table_addr});
#else
    uint64_t **log_mapping_table = (uint64_t **) malloc(config_info.total_available_memory_nodes * config_info.threads_per_memory);

    for (i = 0; i < config_info.total_available_memory_nodes * config_info.threads_per_memory; i++) {
        log_mapping_table[i] = (uint64_t *) pmemobj_direct({pool_uuid, tds[0].ht[i]->log_table_addr});
    }
#endif

    for (i = 0; i < num_ops; i++) {
        keys[i] = distribution(generator);
    }

    while (j < num_ops) {
#ifndef SHARED_NOTHING
        for (i = 0; i < tds[0].ht->log_table_size; i++) {
            nextLog = (log_block *)pmemobj_direct({pool_uuid, log_mapping_table[i]});
#else
        for (i = 0; i < config_info.total_available_memory_nodes * config_info.threads_per_memory; i++) {
            nextLog = (log_block *)pmemobj_direct({pool_uuid, *log_mapping_table[i]});
#endif
            if (nextLog == NULL) {
                PMEMoid ret;
                if (pmemobj_zalloc(pop, &ret, sizeof(log_block) + MAX_LOG_BLOCK_LEN, 0)) {
                    fprintf(stderr, "pmemobj_alloc failed\n");
                    exit(0);
                } else {
                    num_alloc++;
                }

                nextLog = (log_block *)pmemobj_direct(ret);
#ifndef SHARED_NOTHING
                nextLog->next.store(log_mapping_table[i]);
                log_mapping_table[i] = ret.off;
#else
                nextLog->next.store(*log_mapping_table[i]);
                *log_mapping_table[i] = ret.off;
#endif
            }
retry:
            if (nextLog->offset.load() + sizeof(oplogs) + value_size < MAX_LOG_BLOCK_LEN) {
                opL = (oplogs *)((uint64_t)nextLog->buffer + (uint64_t)nextLog->offset.load());
                opL->op_type = OP_INSERT;
                opL->key_len = sizeof(uint64_t);
                opL->val_len = value_size;
                opL->offset = nextLog->offset.load();
                opL->commit = 1;
                opL->key = keys[j];
                nextLog->offset.fetch_add(sizeof(oplogs) + value_size);
                nextLog->valid_counter++;
                j++;
            } else {
                PMEMoid ret;
                if (pmemobj_zalloc(pop, &ret, sizeof(log_block) + MAX_LOG_BLOCK_LEN, 0)) {
                    fprintf(stderr, "pmemobj_alloc failed\n");
                    exit(0);
                } else {
                    num_alloc++;
                }

                nextLog = (log_block *)pmemobj_direct(ret);
#ifndef SHARED_NOTHING
                nextLog->next.store(log_mapping_table[i]);
                log_mapping_table[i] = ret.off;
#else
                nextLog->next.store(*log_mapping_table[i]);
                *log_mapping_table[i] = ret.off;
#endif
                goto retry;
            }
            
            if (j % 1000000 == 0) {
                fprintf(stderr, "Num alloc = %lu, consumption = %lu\n", num_alloc, num_alloc * (sizeof(log_block) + MAX_LOG_BLOCK_LEN));
            }
        }
    }

#ifndef SHARED_NOTHING
    for (i = 0; i < tds[0].ht->log_table_size; i++) {
        nextLog = (log_block *)pmemobj_direct({pool_uuid, log_mapping_table[i]});
#else
    for (i = 0; i < config_info.total_available_memory_nodes * config_info.threads_per_memory; i++) {
        nextLog = (log_block *)pmemobj_direct({pool_uuid, *log_mapping_table[i]});
#endif
        while (nextLog != NULL) {
            clflush((char *)nextLog, sizeof(log_block) + MAX_LOG_BLOCK_LEN, true);
            nextLog = (log_block *)pmemobj_direct({pool_uuid, nextLog->next});
        }
    }

    fprintf(stderr, "Num alloc = %lu, consumption = %lu\n", num_alloc, num_alloc * (sizeof(log_block) + MAX_LOG_BLOCK_LEN));
#ifdef SHARED_NOTHING
    free(log_mapping_table);
#endif
}
#endif

int run_server(int threads_per_storage)
{
    int ret = 0;
    long num_threads = threads_per_storage;
    long i = 0;

    pthread_t *threads = NULL, *ib_connection_manager = NULL;
    pthread_attr_t attr;
    void *status;

    merge_counter.store(0);

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    threads = (pthread_t *) calloc(num_threads, sizeof(pthread_t));
#ifndef DINOMO_LOCAL_TEST
    check(threads != NULL, "Failed to allocate threads.");
#endif

    ib_connection_manager = (pthread_t *) calloc(1, sizeof(pthread_t));
#ifndef DINOMO_LOCAL_TEST
    check(ib_connection_manager != NULL, "Failed to allocate a connection manager thread.");
#endif

    reserved_alloc_queue = new tbb::concurrent_queue<uint64_t>;
    metadata_store = new libcuckoo::cuckoohash_map<std::string, void *>;

    barrier_init(&barrier, num_threads);
    tds = (thread_data_t *) malloc(num_threads * sizeof(thread_data_t));
    tds[0].ht = farIdx;
#ifndef SHARED_NOTHING
    progress_table = new std::mutex[tds[0].ht->log_table_size];
#else
    progress_table = new std::mutex[config_info.total_available_memory_nodes * config_info.threads_per_memory];

    kSelfTier = Tier::MEMORY;
    kSelfTierIdVector = {kSelfTier};

    kMemoryThreadCount = config_info.threads_per_memory;
    kStorageThreadCount = config_info.threads_per_storage;

    kMemoryNodeCapacity = 256 * 1024 * 1024UL;
    kStorageNodeCapacity = config_info.storage_capacities;

    kDefaultGlobalMemoryReplication = 1;
    kDefaultGlobalStorageReplication = 0;
    kDefaultLocalReplication = 1;

    kTierMetadata[Tier::MEMORY] = TierMetadata(Tier::MEMORY, kMemoryThreadCount, kDefaultGlobalMemoryReplication, kMemoryNodeCapacity);
    kTierMetadata[Tier::STORAGE] = TierMetadata(Tier::STORAGE, kStorageThreadCount, kDefaultGlobalStorageReplication, kStorageNodeCapacity);

    for (i = 0; i < config_info.num_initial_memory_nodes; i++)
        global_hash_rings[Tier::MEMORY].insert(peer_ip_addresses[i],
                peer_ip_addresses[i], 0, 0);
    for (i = 0; i < config_info.threads_per_memory; i++)
        local_hash_rings[Tier::MEMORY].insert(peer_ip_addresses[0],
                peer_ip_addresses[0], 0, i);

    for (i = 0; i < config_info.total_available_memory_nodes * config_info.threads_per_memory; i++)
        stored_key_maps.push_back(std::unordered_map<uint64_t, uint64_t>());
#endif

#ifndef DINOMO_LOCAL_TEST
    //sync_with_clients();

    ret = pthread_create(ib_connection_manager, &attr, ib_connection_manager_thread, NULL);
    check(ret == 0, "Failed to create an ib connection manager thread.");

    for (i = 0; i < num_threads; i++) {
        if (i < config_info.num_storage_managers) {
            ret = pthread_create(&threads[i], &attr, server_manager_thread, (void *)i);
            check(ret == 0, "Failed to create server_manager_thread[%ld]", i);
        } else {
            ret = pthread_create(&threads[i], &attr, server_worker_thread, (void *)i);
            check(ret == 0, "Failed to create server_worker_thread[%ld]", i);
        }
    }

    ret = pthread_join(ib_connection_manager[0], &status);
    check(ret == 0, "Failed to join connection manager thread.");

    for (i = 0; i < num_threads; i++) {
        ret = pthread_join(threads[i], &status);
        check(ret == 0, "Failed to join thread[%ld].", i);
    }
#else
    uint64_t num_ops = 30000000;
    insert_log_blocks(num_ops, VALUE_SIZE);

    // Measuring throughput to merge
    fprintf(stderr, "Start merge for load\n");
    auto begin = std::chrono::system_clock::now();
    for (i = 0; i < num_threads; i++) {
        ret = pthread_create(&threads[i], &attr, merge_all, (void *)i);
    }

    for (i = 0; i < num_threads; i++) {
        ret = pthread_join(threads[i], &status);
    }
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - begin);
    fprintf(stderr, "End merge for load: merge, %f ops/us\n", (double)num_ops/duration.count());
    fprintf(stderr, "Number of rehashing = %u\n", num_rehashing);

    num_ops = 100000000;
    insert_log_blocks(num_ops, VALUE_SIZE);

    // Measuring throughput to merge
    fprintf(stderr, "Start merge for run\n");
    sleep(5);
    begin = std::chrono::system_clock::now();
    for (i = 0; i < num_threads; i++) {
        ret = pthread_create(&threads[i], &attr, merge_all, (void *)i);
    }

    for (i = 0; i < num_threads; i++) {
        ret = pthread_join(threads[i], &status);
    }
    duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - begin);
    fprintf(stderr, "End merge for run: merge, %f ops/us\n", (double)num_ops/duration.count());
    fprintf(stderr, "Number of rehashing = %u\n", num_rehashing);

#endif

    pthread_attr_destroy(&attr);
    free(threads);

    delete[] progress_table;

#ifndef SHARED_NOTHING
    clht_gc_destroy(farIdx);
#else
    for (i = 0; i < config_info.total_available_memory_nodes * config_info.threads_per_memory; i++)
        clht_gc_destroy(farIdx[i]);
#endif

    return 0;

error:
    if (threads != NULL)
        free(threads);
    pthread_attr_destroy(&attr);

    return -1;
}
