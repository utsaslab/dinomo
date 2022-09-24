#ifndef DINOMO_COMPUTE_H_
#define DINOMO_COMPUTE_H_

#include <stdlib.h>
#include <stdbool.h>
#include <sys/time.h>
#include <functional>

#include "debug.h"
#include "ib_config.h"
#include "setup_ib.h"
#include "ib.h"

#include "adaptive-cache.h"
#include "hybrid-cache.h"
#include "empty-cache.h"

#include "tbb/concurrent_queue.h"

#define SET_IMM(x, y)               (uint32_t)(x << 16) | (y)
#define SET_MERGE_ALL(x)            (uint32_t)(x | (0b1 << 31))
#define SET_FAILOVER(x)             (uint32_t)(x | (0b11 << 30))
#define SET_INSTALL_INDIRECT(x)     (uint32_t)(x | (0b111 << 29))
#define SET_REMOVE_INDIRECT(x)      (uint32_t)(x | (0b1111 << 28))
#define SET_PUT_META(x)             (uint32_t)(x | (0b11111 << 27))
#define SET_GET_META(x)             (uint32_t)(x | (0b111111 << 26))

#define NO_LIMIT_LOG_LENGTH         10000000

extern unsigned kLogCacheLen;

template <typename T>
class Dinomo
{
public:
    Dinomo(uint64_t cache_size = 1024*1024*1024UL, uint16_t tid = 0, bool enable_batch = false, double weight_or_value_ratio = 5)
    {
        thread_id = tid;
        non_replicated_alloc = 0;
        replicated_alloc = 0;
        replicated_alloc_offset = 0;
        batching = enable_batch;
        batched_offset = 0;
        flushed_offset = 0;
        miss_cost_tracker.first = 0;
        miss_cost_tracker.second = 0;
        value_hit_counter = 0;
        shortcut_hit_counter = 0;
        log_hit_counter = 0;
        miss_counter = 0;
        rdma_read_counter = 0;
        rdma_read_payload = 0;
        rdma_write_counter = 0;
        rdma_write_payload = 0;
        rdma_send_counter = 0;
        rdma_send_payload = 0;
        rdma_recv_counter = 0;
        rdma_recv_payload = 0;
        rdma_cas_counter = 0;
        rdma_cas_payload = 0;
        rdma_faa_counter = 0;
        rdma_faa_payload = 0;

        h = (clht_t *) malloc(sizeof(clht_t));
        post_read_signaled_blocking_profile(sizeof(clht_t), ib_res.mr_buf->lkey, 0, ib_res.qp[thread_id],
        ib_res.ib_buf + (config_info.msg_size * thread_id), ib_res.raddr_pool[thread_id], 
        ib_res.rkey_pool[thread_id], ib_res.cq[thread_id], &rdma_read_counter, &rdma_read_payload);
        memcpy(h, ib_res.ib_buf + (config_info.msg_size * thread_id), sizeof(clht_t));

        memset((char *)(ib_res.ib_buf + (config_info.msg_size * thread_id)), 0, config_info.msg_size);
        post_write_signaled_blocking_profile(sizeof(uint64_t), ib_res.mr_buf->lkey, 0, ib_res.qp[thread_id],
                ib_res.ib_buf + (config_info.msg_size * thread_id), mapping_table_raddr(config_info.rank),
                ib_res.rkey_pool[thread_id], ib_res.cq[thread_id], &rdma_write_counter, &rdma_write_payload);

        cache = new T(cache_size, VALUE_SIZE, SHORTCUT_SIZE, weight_or_value_ratio);

        log_blocks_raddrs = new uint64_t[MAX_PREALLOC_NUM];
        memset(log_blocks_raddrs, 0x00, sizeof(uint64_t) * MAX_PREALLOC_NUM);

        BFilter = NULL;
        filterList = new std::list<bloomFilter *>;

        post_recv_profile((uint32_t)config_info.msg_size, ib_res.mr_buf->lkey, (uint64_t)(ib_res.ib_buf + (config_info.msg_size * thread_id)),
                ib_res.qp[thread_id], (char *)(ib_res.ib_buf + (config_info.msg_size * thread_id)), &rdma_recv_counter, &rdma_recv_payload);
    }

    unsigned put(const uint64_t key, const size_t key_len, const void *value, const uint64_t val_len, bool do_update);

    void putReplicated(const uint64_t key, const size_t key_len, const void *value, const uint64_t val_len, bool do_update);
    
    void putMeta(const char *key, const size_t key_len, const void *value, const uint64_t val_len);

    unsigned remove(const uint64_t key, const size_t key_len);

    std::pair<char *, size_t> get(const uint64_t key, const size_t key_len);

    std::pair<char *, size_t> getReplicated(const uint64_t key, const size_t key_len);
    
    std::pair<char *, size_t> getMeta(const char *key, const size_t key_len);

    uint64_t value_cache_size();
    
    void update_cache_miss_cost();
    
    uint64_t cache_miss_cost();

    void merge(bool clear_cache);

    void flush();

    void failover(int failed_node_rank);

    void swap(const uint64_t key, bool replicated);

    void invalidate_cache(const uint64_t key);

    void install_indirect_pointer(const uint64_t key);

    void remove_indirect_pointer(const uint64_t key);

    uint64_t value_cache_hit_counter()
    {
        uint64_t counter = value_hit_counter;
        value_hit_counter = 0;
        return counter;
    }

    uint64_t shortcut_cache_hit_counter()
    {
        uint64_t counter = shortcut_hit_counter;
        shortcut_hit_counter = 0;
        return counter;
    }

    uint64_t local_log_hit_counter()
    {
        uint64_t counter = log_hit_counter;
        log_hit_counter = 0;
        return counter;
    }

    uint64_t cache_miss_counter()
    {
        uint64_t counter = miss_counter;
        miss_counter = 0;
        return counter;
    }

    uint64_t remote_read_counter()
    {
        uint64_t counter = rdma_read_counter;
        rdma_read_counter = 0;
        return counter;
    }

    uint64_t remote_read_payload()
    {
        uint64_t payload = rdma_read_payload;
        rdma_read_payload = 0;
        return payload;
    }

    uint64_t remote_write_counter()
    {
        uint64_t counter = rdma_write_counter;
        rdma_write_counter = 0;
        return counter;
    }

    uint64_t remote_write_payload()
    {
        uint64_t payload = rdma_write_payload;
        rdma_write_payload = 0;
        return payload;
    }

    uint64_t remote_send_counter()
    {
        uint64_t counter = rdma_send_counter;
        rdma_send_counter = 0;
        return counter;
    }

    uint64_t remote_send_payload()
    {
        uint64_t payload = rdma_send_payload;
        rdma_send_payload = 0;
        return payload;
    }

    uint64_t remote_recv_counter()
    {
        uint64_t counter = rdma_recv_counter;
        rdma_recv_counter = 0;
        return counter;
    }

    uint64_t remote_recv_payload()
    {
        uint64_t payload = rdma_recv_payload;
        rdma_recv_payload = 0;
        return payload;
    }

    uint64_t remote_cas_counter()
    {
        uint64_t counter = rdma_cas_counter;
        rdma_cas_counter = 0;
        return counter;
    }

    uint64_t remote_cas_payload()
    {
        uint64_t payload = rdma_cas_payload;
        rdma_cas_payload = 0;
        return payload;
    }

    uint64_t remote_faa_counter()
    {
        uint64_t counter = rdma_faa_counter;
        rdma_faa_counter = 0;
        return counter;
    }

    uint64_t remote_faa_payload()
    {
        uint64_t payload = rdma_faa_payload;
        rdma_faa_payload = 0;
        return payload;
    }

private:

    uint64_t pop_log_blocks_raddr();

    void remove_preallocated_log_block(uint64_t raddr);

    void push_log_clean_list(uint32_t lkey, struct ibv_qp *qp, char *buf_ptr, uint32_t rkey, struct ibv_cq *cq);

    uint64_t mapping_table_raddr(int rank);

    bool find_from_cache(const uint64_t key, const size_t key_len, char *&valRaddr,
    size_t &valLen, uint64_t &verSion, std::function<char *(char *, size_t, uint64_t)> CopyCachedValue);

    std::pair<bool, void *> check_from_staging_pool(const uint64_t key, const size_t key_len, log_block *pool);

    void gc_log_list(uint32_t lkey, struct ibv_qp *qp, char *buf_ptr, uint32_t rkey, struct ibv_cq *cq);

    void clear_log_list();

    std::pair<bool, void *> get_from_oplogs(const uint64_t key, const size_t key_len);

    void push_log_list();

    void preallocate_log_blocks(uint32_t lkey, struct ibv_qp *qp, char *buf_ptr, struct ibv_cq *cq, struct ibv_wc *wc, int num_wc);
    
    size_t serialize(const char *key, const size_t key_len, const void *value, const uint64_t val_len, char *buf_ptr);

    void *deserialize(char *buf_ptr, size_t &val_len);

    clht_t *h;    

    T *cache;

    // Tuple member specification
    // 1. Remote offset to indirect pointer
    // 2. Remote offset to an operational log pointed by the indirect pointer
    // 3. Remote value size
    // 4. Cached remote value
    std::unordered_map<uint64_t, std::tuple<uint64_t, uint64_t, uint64_t, char *>> indirect_pointer_cache;

    uint64_t *log_blocks_raddrs;

    std::list<bloomFilter *> *filterList;

    bloomFilter *BFilter;

    uint16_t thread_id;

    uint64_t non_replicated_alloc;

    uint64_t replicated_alloc;

    uint32_t replicated_alloc_offset;

    uint64_t replicated_alloc_num_entries;

    bool batching;

    uint32_t batched_offset;

    uint32_t flushed_offset;

    // Aggregate RTs, Num of accesses to DPM index
    std::pair<uint64_t, uint64_t> miss_cost_tracker;

    uint64_t value_hit_counter;

    uint64_t shortcut_hit_counter;

    uint64_t log_hit_counter;

    uint64_t miss_counter;

    uint64_t rdma_read_counter;

    uint64_t rdma_read_payload;

    uint64_t rdma_write_counter;

    uint64_t rdma_write_payload;

    uint64_t rdma_send_counter;

    uint64_t rdma_send_payload;

    uint64_t rdma_recv_counter;

    uint64_t rdma_recv_payload;

    uint64_t rdma_cas_counter;

    uint64_t rdma_cas_payload;

    uint64_t rdma_faa_counter;

    uint64_t rdma_faa_payload;
};

template <typename T>
uint64_t Dinomo<T>::pop_log_blocks_raddr()
{
    uint64_t i, log_raddr;
    for (i = 0; i < MAX_PREALLOC_NUM; i++) {
        log_raddr = log_blocks_raddrs[i];
        if (log_raddr != 0) {
            log_blocks_raddrs[i] = 0;
            return log_raddr;
        }
    }

    return 0;
}

template <typename T>
void Dinomo<T>::remove_preallocated_log_block(uint64_t raddr)
{
    for (uint64_t i = 0; i < MAX_PREALLOC_NUM; i++) {
        if (log_blocks_raddrs[i] == raddr) {
            log_blocks_raddrs[i] = 0;
            return ;
        }
    }
}

template <typename T>
void Dinomo<T>::push_log_clean_list(uint32_t lkey, struct ibv_qp *qp, char *buf_ptr, uint32_t rkey, struct ibv_cq *cq)
{
    bloomFilter *bIter = NULL;
    uint64_t next = 0;

    if (kLogCacheLen != NO_LIMIT_LOG_LENGTH) {
        do {
            for (auto it = filterList->begin(); it != filterList->end(); ++it) {
                bIter = (bloomFilter *)(*it);
                if (((log_block *)(bIter->local_log_block))->next == 0) {
                    while (bIter != filterList->back()) {
                        bloomFilter *freeFilter = filterList->back();
                        free(freeFilter->bf);
                        free(freeFilter->local_log_block);
                        free(freeFilter);
                        filterList->pop_back();
                    }
                    break;
                } else {
                    post_read_signaled_blocking_profile(sizeof(uint64_t), lkey, 0, qp, (char *)buf_ptr,
                            bIter->raddr + LOG_BLOCK_HEADER_LEN - sizeof(uint64_t), rkey, cq,
                            &rdma_read_counter, &rdma_read_payload);
                    memcpy(&next, buf_ptr, sizeof(uint64_t));
                    // If local next and remote next are different, presume this log block
                    // including precessors has been already merged
                    if (next == 0 || next != ((log_block *)(bIter->local_log_block))->next) {
                        while (bIter != filterList->back()) {
                            bloomFilter *freeFilter = filterList->back();
                            free(freeFilter->bf);
                            free(freeFilter->local_log_block);
                            free(freeFilter);
                            filterList->pop_back();
                        }
                        break;
                    }
                }
            }
        } while (filterList->size() > kLogCacheLen);
    }

    filterList->push_front(BFilter);
}

template <typename T>
uint64_t Dinomo<T>::mapping_table_raddr(int rank)
{
    // h->log_table_addr = pool offset to the log table
#ifndef SHARED_NOTHING
    return (h->remote_start_addr + h->log_table_addr + (uint64_t)(((rank * config_info.threads_per_memory) + thread_id) * sizeof(uint64_t)));
#else
    return (h->remote_start_addr + h->log_table_addr);
#endif
}

template <typename T>
bool Dinomo<T>::find_from_cache(const uint64_t key, const size_t key_len, char *&valRaddr,
                     size_t &valLen, uint64_t &verSion, 
                     std::function<char *(char *, size_t, uint64_t)> CopyCachedValue)
{
    if (cache->find(key, valRaddr, valLen, verSion, CopyCachedValue))
    {
        if (valRaddr == NULL)
            shortcut_hit_counter++;
        else
            value_hit_counter++;
        return true;
    }
    else
    {
        miss_counter++;
        return false;
    }
}

template <typename T>
std::pair<bool, void *> Dinomo<T>::check_from_staging_pool(const uint64_t key,
        const size_t key_len, log_block *pool)
{
    uint64_t offset = 0;
    oplogs *oLog_p = NULL;

    offset = pool->offset.load();
    if (offset != 0) {
        if (bloom_check(BFilter, &key, key_len) != 0) {
            for (uint64_t j = 0; j < offset;) {
                oLog_p = (oplogs *)((uint64_t)pool->buffer + j);
                if (oLog_p->key == key)
                    return std::make_pair(true, oLog_p);

                j += sizeof(oplogs) + oLog_p->val_len;
            }
        }
    }

    return std::make_pair(false, nullptr);
}

template <typename T>
void Dinomo<T>::gc_log_list(uint32_t lkey, struct ibv_qp *qp, char *buf_ptr, uint32_t rkey, struct ibv_cq *cq)
{
    bloomFilter *bIter = NULL;
    uint64_t next = 0;

    for (auto it = filterList->begin(); it != filterList->end(); ++it) {
        bIter = (bloomFilter *)(*it);
        if (((log_block *)(bIter->local_log_block))->next == 0) {
            while (bIter != filterList->back()) {
                bloomFilter *freeFilter = filterList->back();
                free(freeFilter->bf);
                free(freeFilter->local_log_block);
                free(freeFilter);
                filterList->pop_back();
            }
            break;
        } else {
            post_read_signaled_blocking_profile(sizeof(uint64_t), lkey, 0, qp, (char *)buf_ptr,
                    bIter->raddr + LOG_BLOCK_HEADER_LEN - sizeof(uint64_t), rkey, cq,
                    &rdma_read_counter, &rdma_read_payload);
            memcpy(&next, buf_ptr, sizeof(uint64_t));
            if (next == 0 || next != ((log_block *)(bIter->local_log_block))->next) {
                while (bIter != filterList->back()) {
                    bloomFilter *freeFilter = filterList->back();
                    free(freeFilter->bf);
                    free(freeFilter->local_log_block);
                    free(freeFilter);
                    filterList->pop_back();
                }
                break;
            }
        }
    }
}

template <typename T>
void Dinomo<T>::clear_log_list()
{
    bloomFilter *bIter = NULL;

    for (auto it = filterList->begin(); it != filterList->end(); ++it) {
        bIter = (bloomFilter *)(*it);
        free(bIter->bf);
        free(bIter->local_log_block);
        free(bIter);
    }

    filterList->clear();
}

template <typename T>
std::pair<bool, void *> Dinomo<T>::get_from_oplogs(const uint64_t key, const size_t key_len)
{
    uint64_t offset = 0;
    uint64_t *log_table = NULL;
    log_block *snapshot_l = NULL;
    oplogs *opL = NULL;

    for (auto it = filterList->begin(); it != filterList->end(); ++it) {
        bloomFilter *bIter = (bloomFilter *)(*it);
        snapshot_l = (log_block *) bIter->local_log_block;
        if (bloom_check(bIter, &key, key_len) != 0) {
            while (snapshot_l->offset > offset) {
                opL = (oplogs *)((uint64_t)snapshot_l->buffer + offset);
                if (opL->key == key)
                    return std::make_pair(true, opL);

                offset = offset + sizeof(oplogs) + opL->val_len;
            }
        }

        offset = 0;
    }

    return std::make_pair(false, nullptr);
}

template <typename T>
void Dinomo<T>::push_log_list()
{
    filterList->push_front(BFilter);
}

template <typename T>
void Dinomo<T>::preallocate_log_blocks(uint32_t lkey_buf, struct ibv_qp *qp, char *buf_ptr, struct ibv_cq *cq, struct ibv_wc *wc, int num_wc)
{
    int ret = 0, n = 0;
    bool stop = false;

    ret = post_send_imm_profile(0, lkey_buf, (uint64_t)buf_ptr, SET_IMM(config_info.rank, thread_id), 
            qp, buf_ptr, &rdma_send_counter, &rdma_send_payload);
    check(ret == 0, "Thread[%ld]: failed to post send", thread_id);

    while (stop != true)
    {
        n = ibv_poll_cq(cq, num_wc, wc);
        if (n < 0)
            check(0, "Thread[%ld]: failed to poll cq", thread_id);

        for (uint64_t j = 0; j < n; j++)
        {
            if (wc[j].status != IBV_WC_SUCCESS)
            {
                if (wc[j].opcode == IBV_WC_SEND)
                {
                    check(0, "Thread[%ld]: send failed status: %s; wr_id = %" PRIx64 "",
                          thread_id, ibv_wc_status_str(wc[j].status), wc[j].wr_id);
                }
                else
                {
                    check(0, "Thread[%ld]: recv failed status: %s; wr_id = %" PRIx64 "",
                          thread_id, ibv_wc_status_str(wc[j].status), wc[j].wr_id);
                }
            }

            if (wc[j].opcode == IBV_WC_RECV)
            {
                ret = post_recv_profile((uint32_t)config_info.msg_size, lkey_buf, wc[j].wr_id, qp, (char *)wc[j].wr_id,
                        &rdma_recv_counter, &rdma_recv_payload);
                check(ret == 0, "Thread[%ld]: failed to post recv", thread_id);

                memcpy(log_blocks_raddrs, buf_ptr, sizeof(uint64_t) * MAX_PREALLOC_NUM);
                stop = true;
                break;
            }
        }
    }

error:
    return ;
}

template <typename T>
size_t Dinomo<T>::serialize(const char *key, const size_t key_len, const void *value, const uint64_t val_len, char *buf_ptr)
{
    size_t total_msg_size = 0;
    uint64_t offset = 0;
    uint64_t aligned_len = 0;
    if (key != NULL && key_len != 0)
    {
        memcpy(buf_ptr + offset, &key_len, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(buf_ptr + offset, key, key_len);
        offset += key_len;
        total_msg_size += (sizeof(size_t) + key_len);
    }

    if (value != NULL && val_len != 0)
    {
        memcpy(buf_ptr + offset, &val_len, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        memcpy(buf_ptr + offset, value, val_len);
        offset += val_len;
        total_msg_size += (sizeof(uint64_t) + val_len);
    }

    return total_msg_size;
}

template <typename T>
void *Dinomo<T>::deserialize(char *buf_ptr, size_t &val_len)
{
    void *val = NULL;
    memcpy(&val_len, buf_ptr, sizeof(uint64_t));
    if (val_len != 0) {
        val = (void *) malloc(val_len);
        memcpy(val, buf_ptr + sizeof(uint64_t), val_len);
    }
    return val;
}

template <typename T>
unsigned Dinomo<T>::put(const uint64_t key, const size_t key_len, const void *value, const uint64_t val_len, bool do_update)
{
#ifdef SEKWON_DEBUG
    printf("%s\n", __func__);
#endif
    unsigned ret = 0;
    int msg_size = config_info.msg_size;

    int num_wc = 1;

    struct ibv_qp *qp = ib_res.qp[thread_id];
    struct ibv_cq *cq = ib_res.cq[thread_id];
    struct ibv_wc *wc = NULL;
    struct ibv_srq *srq = ib_res.srq;

    uint32_t lkey_buf = ib_res.mr_buf->lkey;
    uint32_t lkey_pool = ib_res.mr_pool->lkey;

    char *buf_ptr = ib_res.ib_buf + (msg_size * thread_id);
    size_t buf_size = ib_res.ib_buf_size / ib_res.num_qps;

    uint32_t imm_data = 0;
    int num_acked_peers = 0;
    bool start_sending = false;
    bool stop = false;

    uint64_t current_batch = 0, next_batch = 0, next_batch_off = 0;
    uint32_t expected_off = 0;
    log_block *pool = NULL, *new_local_log_block = NULL;
    oplogs *oLog = NULL;

    uint64_t key_ = 0;
    void *value_ = NULL;

    // pre-post recvs
    wc = (struct ibv_wc *) calloc(num_wc, sizeof(struct ibv_wc));
    check(wc != NULL, "Thread[%ld]: failed to allocate wc", thread_id);

    // Staged log segment per thread
    pool = (log_block *)(ib_res.ib_pool + ((sizeof(log_block) + MAX_LOG_BLOCK_LEN) * (uint64_t)thread_id));

retry:
    oLog = NULL;
    current_batch = non_replicated_alloc;
    if (current_batch != 0) {
        if (batching) {
            if (batched_offset + sizeof(oplogs) + val_len <= MAX_LOG_BLOCK_LEN) {
                pool->valid_counter++;

                oLog = (oplogs *)((uint64_t)pool->buffer + (uint64_t)batched_offset);
                if (do_update != true) oLog->op_type = OP_INSERT;
                else oLog->op_type = OP_UPDATE;
                oLog->key_len = key_len;
                oLog->val_len = val_len;
                oLog->offset = batched_offset;
                oLog->commit = 1;
                oLog->key = key;
                memcpy(oLog->value, value, oLog->val_len);

                batched_offset += (sizeof(oplogs) + val_len);
            } else {
                post_write_signaled_blocking_profile(batched_offset - flushed_offset, lkey_pool, 0, qp,
                        (char *)((uint64_t)pool->buffer + (uint64_t)flushed_offset), 
                        current_batch + LOG_BLOCK_HEADER_LEN + (uint64_t)flushed_offset,
                        ib_res.rkey_pool[thread_id], cq, &rdma_write_counter, &rdma_write_payload);

                pool->offset = batched_offset;

                // Write local log header info except for merged_offset and next pointer
                post_write_signaled_blocking_profile(LOG_BLOCK_HEADER_LEN - sizeof(uint64_t) - sizeof(uint32_t),
                        lkey_pool, 0, qp, (char *)pool + sizeof(uint32_t), current_batch + sizeof(uint32_t), 
                        ib_res.rkey_pool[thread_id], cq, &rdma_write_counter, &rdma_write_payload);

                while(flushed_offset < batched_offset) {
                    oLog = (oplogs *)((uint64_t)pool->buffer + (uint64_t)flushed_offset);

                    bloom_add(BFilter, &oLog->key, oLog->key_len);
                    if (oLog->op_type != OP_DELETE) {
                        key_ = oLog->key;
                        value_ = (void *) malloc(oLog->val_len);
                        memcpy(value_, oLog->value, oLog->val_len);

                        if (oLog->op_type == OP_UPDATE) {
                            cache->update(key_, (char *)value_, oLog->val_len, 
                                    (uint64_t)(current_batch - h->remote_start_addr + 
                                        ((uint64_t)&pool->buffer - (uint64_t)pool) +
                                        (uint64_t)flushed_offset + sizeof(oplogs)));
                        } else if (oLog->op_type == OP_INSERT) {
                            cache->insert(key_, (char *)value_, oLog->val_len,
                                    (uint64_t)(current_batch - h->remote_start_addr +
                                        ((uint64_t)&pool->buffer - (uint64_t)pool) +
                                        (uint64_t)flushed_offset + sizeof(oplogs)));
                        }
                    } else {
                        cache->remove(oLog->key);
                    }

                    flushed_offset += (sizeof(oplogs) + oLog->val_len);
                }

                memcpy(BFilter->local_log_block, pool, sizeof(log_block) + MAX_LOG_BLOCK_LEN);
                push_log_clean_list(lkey_buf, qp, buf_ptr, ib_res.rkey_pool[thread_id], cq);

                pool->valid_counter = 0;
                pool->offset = 0;

                batched_offset = 0;
                flushed_offset = 0;

                next_batch = pop_log_blocks_raddr();
                non_replicated_alloc = next_batch;
                if (next_batch != 0) {
                    // Allocate new local log block
                    new_local_log_block = (log_block *) calloc(1, sizeof(log_block) + MAX_LOG_BLOCK_LEN);

                    // Initialize new bloom filter
                    BFilter = (bloomFilter *) calloc(1, sizeof(bloomFilter));
                    bloom_init(BFilter, (MAX_LOG_BLOCK_LEN / (sizeof(oplogs) + VALUE_SIZE)) + 1, 0.1);
                    BFilter->local_log_block = new_local_log_block;
                    BFilter->raddr = next_batch;
                    BFilter->ready = 1;

                    // Set the next pointer of new log block to point to the existing log block
                    post_read_signaled_blocking_profile(sizeof(uint64_t), lkey_buf, 0, qp, buf_ptr,
                            mapping_table_raddr(config_info.rank), ib_res.rkey_pool[thread_id], cq,
                            &rdma_read_counter, &rdma_read_payload);
                    memcpy(&pool->next, buf_ptr, sizeof(uint64_t));
                    post_write_signaled_blocking_profile(sizeof(uint64_t), lkey_pool, 0, qp, (char *)&pool->next,
                            next_batch + LOG_BLOCK_HEADER_LEN - sizeof(uint64_t), ib_res.rkey_pool[thread_id], cq,
                            &rdma_write_counter, &rdma_write_payload);

                    // Update the pointer in log mapping table to new log block
                    next_batch_off = next_batch - h->remote_start_addr;
                    memcpy(buf_ptr, &next_batch_off, sizeof(uint64_t));
                    post_write_signaled_blocking_profile(sizeof(uint64_t), lkey_buf, 0, qp, buf_ptr,
                            mapping_table_raddr(config_info.rank), ib_res.rkey_pool[thread_id], cq,
                            &rdma_write_counter, &rdma_write_payload);
                }

                ret = 1;
                goto retry;
            }
        } else {
            if (pool->offset + sizeof(oplogs) + val_len <= MAX_LOG_BLOCK_LEN) {
                pool->valid_counter++;

                oLog = (oplogs *)((uint64_t)pool->buffer + (uint64_t)pool->offset);
                if (do_update != true) oLog->op_type = OP_INSERT;
                else oLog->op_type = OP_UPDATE;
                oLog->key_len = key_len;
                oLog->val_len = val_len;
                oLog->offset = pool->offset;
                oLog->commit = 1;
                oLog->key = key;
                memcpy(oLog->value, value, oLog->val_len);
                post_write_signaled_blocking_profile(sizeof(oplogs) + oLog->val_len, lkey_pool, 0, qp,
                        (char *)oLog, current_batch + LOG_BLOCK_HEADER_LEN + pool->offset, 
                        ib_res.rkey_pool[thread_id], cq, &rdma_write_counter, &rdma_write_payload);

                key_ = key;
                value_ = (void *) malloc(val_len);
                memcpy(value_, value, val_len);

                bloom_add(BFilter, &oLog->key, oLog->key_len);
                if (oLog->op_type == OP_UPDATE) {
                    cache->update(key_, (char *)value_, val_len, (uint64_t)(current_batch -
                                h->remote_start_addr + ((uint64_t)&pool->buffer - (uint64_t)pool) +
                                (uint64_t)pool->offset + sizeof(oplogs)));
                } else if (oLog->op_type == OP_INSERT) {
                    cache->insert(key_, (char *)value_, val_len,
                            (uint64_t)(current_batch - h->remote_start_addr +
                                ((uint64_t)&pool->buffer - (uint64_t)pool) +
                                (uint64_t)pool->offset + sizeof(oplogs)));
                }

                pool->offset += (sizeof(oplogs) + val_len);
            } else {
                // Write local log header info except for merged_offset and next pointer
                post_write_signaled_blocking_profile(LOG_BLOCK_HEADER_LEN - sizeof(uint64_t) - sizeof(uint32_t),
                        lkey_pool, 0, qp, (char *)pool + sizeof(uint32_t), current_batch + sizeof(uint32_t), 
                        ib_res.rkey_pool[thread_id], cq, &rdma_write_counter, &rdma_write_payload);

                memcpy(BFilter->local_log_block, pool, sizeof(log_block) + MAX_LOG_BLOCK_LEN);
                push_log_clean_list(lkey_buf, qp, buf_ptr, ib_res.rkey_pool[thread_id], cq);

                pool->valid_counter = 0;
                pool->offset = 0;
                
                next_batch = pop_log_blocks_raddr();
                non_replicated_alloc = next_batch;
                if (next_batch != 0) {
                    // Allocate new local log block
                    new_local_log_block = (log_block *) calloc(1, sizeof(log_block) + MAX_LOG_BLOCK_LEN);

                    // Initialize new bloom filter
                    BFilter = (bloomFilter *) calloc(1, sizeof(bloomFilter));
                    bloom_init(BFilter, (MAX_LOG_BLOCK_LEN / (sizeof(oplogs) + VALUE_SIZE)) + 1, 0.1);
                    BFilter->local_log_block = new_local_log_block;
                    BFilter->raddr = next_batch;
                    BFilter->ready = 1;

                    // Set the next pointer of new log block to point to the existing log block
                    post_read_signaled_blocking_profile(sizeof(uint64_t), lkey_buf, 0, qp, buf_ptr,
                            mapping_table_raddr(config_info.rank), ib_res.rkey_pool[thread_id], cq,
                            &rdma_read_counter, &rdma_read_payload);
                    memcpy(&pool->next, buf_ptr, sizeof(uint64_t));
                    post_write_signaled_blocking_profile(sizeof(uint64_t), lkey_pool, 0, qp, (char *)&pool->next,
                            next_batch + LOG_BLOCK_HEADER_LEN - sizeof(uint64_t), ib_res.rkey_pool[thread_id], cq,
                            &rdma_write_counter, &rdma_write_payload);

                    // Update the pointer in log mapping table to new log block
                    next_batch_off = next_batch - h->remote_start_addr;
                    memcpy(buf_ptr, &next_batch_off, sizeof(uint64_t));
                    post_write_signaled_blocking_profile(sizeof(uint64_t), lkey_buf, 0, qp, buf_ptr,
                            mapping_table_raddr(config_info.rank), ib_res.rkey_pool[thread_id], cq,
                            &rdma_write_counter, &rdma_write_payload);
                }

                ret = 1;
                goto retry;
            }
        }
    } else {
        preallocate_log_blocks(lkey_buf, qp, buf_ptr, cq, wc, num_wc);

        next_batch = pop_log_blocks_raddr();
        non_replicated_alloc = next_batch;

        // Allocate new local log block
        new_local_log_block = (log_block *) calloc(1, sizeof(log_block) + MAX_LOG_BLOCK_LEN);

        // Initialize new bloom filter
        BFilter = (bloomFilter *) calloc(1, sizeof(bloomFilter));
        bloom_init(BFilter, (MAX_LOG_BLOCK_LEN / (sizeof(oplogs) + VALUE_SIZE)) + 1, 0.1);
        BFilter->local_log_block = new_local_log_block;
        BFilter->raddr = next_batch;
        BFilter->ready = 1;

        // Set the next pointer of new log block to point to the existing log block
        post_read_signaled_blocking_profile(sizeof(uint64_t), lkey_buf, 0, qp, buf_ptr,
        mapping_table_raddr(config_info.rank), ib_res.rkey_pool[thread_id], cq,
        &rdma_read_counter, &rdma_read_payload);
        memcpy(&pool->next, buf_ptr, sizeof(uint64_t));
        post_write_signaled_blocking_profile(sizeof(uint64_t), lkey_pool, 0, qp, (char *)&pool->next,
        next_batch + LOG_BLOCK_HEADER_LEN - sizeof(uint64_t), ib_res.rkey_pool[thread_id], cq,
        &rdma_write_counter, &rdma_write_payload);

        // Update the pointer in log mapping table to new log block
        next_batch_off = next_batch - h->remote_start_addr;
        memcpy(buf_ptr, &next_batch_off, sizeof(uint64_t));
        post_write_signaled_blocking_profile(sizeof(uint64_t), lkey_buf, 0, qp, buf_ptr,
        mapping_table_raddr(config_info.rank), ib_res.rkey_pool[thread_id], cq,
        &rdma_write_counter, &rdma_write_payload);

        stop = false;
        goto retry;
    }

    free(wc);
    return ret;
error:
    LOG("%s: failure happens", __func__);
    free(wc);
    return ret;
}

template <typename T>
void Dinomo<T>::putReplicated(const uint64_t key, const size_t key_len, const void *value, const uint64_t val_len, bool do_update)
{
#ifdef SEKWON_DEBUG
    printf("%s\n", __func__);
#endif
    int ret = 0, n = 0;
    int msg_size = config_info.msg_size;

    int num_wc = 1;

    struct ibv_qp *qp = ib_res.qp[thread_id];
    struct ibv_cq *cq = ib_res.cq[thread_id];
    struct ibv_wc *wc = NULL;
    struct ibv_srq *srq = ib_res.srq;

    uint32_t lkey_buf = ib_res.mr_buf->lkey;
    uint32_t lkey_pool = ib_res.mr_pool->lkey;

    char *buf_ptr = ib_res.ib_buf + (msg_size * thread_id);
    size_t buf_size = ib_res.ib_buf_size / ib_res.num_qps;

    uint32_t imm_data = 0;
    int num_acked_peers = 0;
    bool start_sending = false;
    bool stop = false;

    uint64_t current_batch = 0, next_batch = 0, next_batch_off = 0;
    uint32_t expected_off = 0;
    oplogs *oLog = NULL;

    // pre-post recvs
    wc = (struct ibv_wc *) calloc(num_wc, sizeof(struct ibv_wc));
    check(wc != NULL, "Thread[%ld]: failed to allocate wc", thread_id);

    while (1)
    {
        oLog = NULL;
        if (replicated_alloc)
        {
            if (replicated_alloc_offset + sizeof(oplogs) + val_len <= MAX_LOG_BLOCK_LEN)
            {
                oLog = (oplogs *)((uint64_t)buf_ptr);
                if (do_update != true)
                    oLog->op_type = OP_INSERT;
                else
                    oLog->op_type = OP_UPDATE;
                oLog->key_len = key_len;
                oLog->val_len = val_len;
                oLog->offset = replicated_alloc_offset;
                oLog->commit = 1;
                oLog->key = key;
                memcpy(oLog->value, value, oLog->val_len);
                post_write_signaled_blocking_profile(sizeof(oplogs) + oLog->val_len, lkey_buf, 0, qp,
                                             (char *)oLog, replicated_alloc + LOG_BLOCK_HEADER_LEN + (uint64_t)replicated_alloc_offset,
                                             ib_res.rkey_pool[thread_id], cq, &rdma_write_counter, &rdma_write_payload);

                // Update the indirect poiner to the new oplog
                //      - Search the indirect pointer from the local cache
                //          - Cache hit: CAS the address of the indirect pointer to the new oplog
                //          - Cache miss: Traverse remote index to get the address to the indirect pointer
                //                        CAS the address of the indirect pointer to the new oplog
                //                        Insert/update the address of the indirect pointer to the cache
                uint64_t offset_to_indirect_pointer;
                uint64_t offset_to_oplog = (replicated_alloc - h->remote_start_addr) + LOG_BLOCK_HEADER_LEN + (uint64_t)replicated_alloc_offset;
                auto search = indirect_pointer_cache.find(key);
                if (search != indirect_pointer_cache.end())
                {
                    offset_to_indirect_pointer = std::get<0>(search->second);
                }
                else
                {
                    // Presume clht_get returns indirect pointer, not value block itself.
                    offset_to_indirect_pointer = clht_get(NULL, key, key_len, 0, lkey_buf, qp, buf_ptr, ib_res.rkey_pool[thread_id],
                                                          cq, (uint64_t)ib_res.raddr_pool[thread_id], config_info.rank,
                                                          &rdma_read_counter, &rdma_read_payload);
                    check(offset_to_indirect_pointer != 0, "Thread[%ld]: Failed to get indirect pointer", thread_id);
                }

                post_read_signaled_blocking_profile(sizeof(uint64_t), lkey_buf, 0, qp, buf_ptr, 
                        h->remote_start_addr + offset_to_indirect_pointer, 
                        ib_res.rkey_pool[thread_id], cq, &rdma_read_counter, &rdma_read_payload);

#ifdef SEKWON_DEBUG
                printf("Indirect pointer address = %lu\n", h->remote_start_addr + offset_to_indirect_pointer);
                printf("Offset to oplog = %lu, Address to oplog = %lu\n", offset_to_oplog, h->remote_start_addr + offset_to_oplog);
#endif

                uint64_t expected = 0;
                do
                {
                    memcpy(&expected, buf_ptr, sizeof(uint64_t));
                    post_cas_signaled_blocking_profile(sizeof(uint64_t), lkey_buf, 0, qp, buf_ptr,
                                               h->remote_start_addr + offset_to_indirect_pointer,
                                               ib_res.rkey_pool[thread_id], cq, expected, offset_to_oplog,
                                               &rdma_cas_counter, &rdma_cas_payload);
                } while (memcmp(&expected, buf_ptr, sizeof(uint64_t)) != 0);

                post_read_signaled_blocking_profile(sizeof(oplogs), lkey_buf, 0, qp, buf_ptr, 
                        h->remote_start_addr + expected, ib_res.rkey_pool[thread_id], cq, 
                        &rdma_read_counter, &rdma_read_payload);
                post_fetch_add_signaled_blocking_profile(sizeof(uint64_t), lkey_buf, 0, qp, buf_ptr, h->remote_start_addr + expected - 
                ((oplogs *)buf_ptr)->offset - sizeof(uint64_t) - sizeof(uint64_t), ib_res.rkey_pool[thread_id], cq, 1ULL,
                &rdma_faa_counter, &rdma_faa_payload);

                // Update cache to new value
                char *value_ = (char *) malloc(val_len);
                memcpy(value_, value, val_len);
                if (search != indirect_pointer_cache.end()) {
                    if (std::get<3>(search->second) != nullptr)
                        free(std::get<3>(search->second));
                }
                indirect_pointer_cache[key] = std::make_tuple(offset_to_indirect_pointer, offset_to_oplog, val_len, value_);

                // Need to add an implementation comparing the valid counter and the invalid counter
                // If valid counter and invalid counters are equal, push back the log block to local preallocation pool
                // Due to the concern on concurrency race, it may be safe to intercept the role to release the block to remote compute

                replicated_alloc_num_entries += 1;
                replicated_alloc_offset += ((uint32_t)sizeof(oplogs) + (uint32_t)val_len);
                break;
            }
            else
            {
                // Synchronize the metadata (num_entries and offset) of replicated log block to remote PM
                memcpy(buf_ptr, &replicated_alloc_offset, sizeof(uint32_t));
                post_write_signaled_blocking_profile(sizeof(uint32_t), lkey_buf, 0, qp, buf_ptr, replicated_alloc + sizeof(uint32_t), 
                        ib_res.rkey_pool[thread_id], cq, &rdma_write_counter, &rdma_write_payload);
                memcpy(buf_ptr, &replicated_alloc_num_entries, sizeof(uint64_t));
                post_write_signaled_blocking_profile(sizeof(uint64_t), lkey_buf, 0, qp, buf_ptr, replicated_alloc + sizeof(uint32_t) + 
                        sizeof(uint32_t), ib_res.rkey_pool[thread_id], cq, &rdma_write_counter, &rdma_write_payload);
            }
        }

        replicated_alloc = pop_log_blocks_raddr();
        if (replicated_alloc != 0)
        {
            replicated_alloc_offset = 0;
            replicated_alloc_num_entries = 0;
        }
        else
        {
            preallocate_log_blocks(lkey_buf, qp, buf_ptr, cq, wc, num_wc);
            replicated_alloc = pop_log_blocks_raddr();
            replicated_alloc_offset = 0;
            replicated_alloc_num_entries = 0;
        }
    }

    free(wc);
    return ;
error:
    LOG("%s: failure happens", __func__);
    free(wc);
    return ;
}

template <typename T>
void Dinomo<T>::putMeta(const char *key, const size_t key_len, const void *value, const uint64_t val_len)
{
#ifdef SEKWON_DEBUG
    printf("%s\n", __func__);
#endif
    int ret = 0, n = 0;
    int msg_size = config_info.msg_size;

    int num_wc = 1;

    struct ibv_qp *qp = ib_res.qp[thread_id];
    struct ibv_cq *cq = ib_res.cq[thread_id];
    struct ibv_wc *wc = NULL;
    struct ibv_srq *srq = ib_res.srq;

    uint32_t lkey_buf = ib_res.mr_buf->lkey;
    uint32_t lkey_pool = ib_res.mr_pool->lkey;

    char *buf_ptr = ib_res.ib_buf + (msg_size * thread_id);
    size_t buf_size = ib_res.ib_buf_size / ib_res.num_qps;

    uint32_t imm_data = 0;
    bool stop = false;
    size_t req_size = 0;

    // pre-post recvs
    wc = (struct ibv_wc *) calloc(num_wc, sizeof(struct ibv_wc));
    check(wc != NULL, "Thread[%ld]: failed to allocate wc", thread_id);

    req_size = serialize(key, key_len, value, val_len, buf_ptr);
    ret = post_send_imm_profile((uint32_t)req_size, lkey_buf, (uint64_t)buf_ptr, 
            SET_PUT_META(SET_IMM(config_info.rank, thread_id)), 
            qp, buf_ptr, &rdma_send_counter, &rdma_send_payload);
    check(ret == 0, "Thread[%ld]: failed to post send", thread_id);

    while (stop != true)
    {
        n = ibv_poll_cq(cq, num_wc, wc);
        if (n < 0)
            check(0, "Thread[%ld]: failed to poll cq", thread_id);

        for (uint64_t j = 0; j < n; j++)
        {
            if (wc[j].status != IBV_WC_SUCCESS)
            {
                if (wc[j].opcode == IBV_WC_SEND)
                {
                    check(0, "Thread[%ld]: send failed status: %s; wr_id = %" PRIx64 "",
                            thread_id, ibv_wc_status_str(wc[j].status), wc[j].wr_id);
                }
                else
                {
                    check(0, "Thread[%ld]: recv failed status: %s; wr_id = %" PRIx64 "",
                            thread_id, ibv_wc_status_str(wc[j].status), wc[j].wr_id);
                }
            }

            if (wc[j].opcode == IBV_WC_RECV)
            {
                ret = post_recv_profile((uint32_t)config_info.msg_size, lkey_buf, wc[j].wr_id, qp, (char *)wc[j].wr_id,
                        &rdma_recv_counter, &rdma_recv_payload);
                check(ret == 0, "Thread[%ld]: failed to post recv", thread_id);

                if (ntohl(wc[j].imm_data) == MSG_CTL_COMMIT)
                {
                    stop = true;
                    break;
                }
            }
        }
    }

    free(wc);
    return ;
error:
    LOG("%s: failure happens", __func__);
    free(wc);
    return ;
}

template <typename T>
unsigned Dinomo<T>::remove(const uint64_t key, const size_t key_len)
{
#ifdef SEKWON_DEBUG
    printf("%s\n", __func__);
#endif
    unsigned ret = 0;
    int msg_size = config_info.msg_size;

    int num_wc = 1;

    struct ibv_qp *qp = ib_res.qp[thread_id];
    struct ibv_cq *cq = ib_res.cq[thread_id];
    struct ibv_wc *wc = NULL;
    struct ibv_srq *srq = ib_res.srq;

    uint32_t lkey_buf = ib_res.mr_buf->lkey;
    uint32_t lkey_pool = ib_res.mr_pool->lkey;

    char *buf_ptr = ib_res.ib_buf + (msg_size * thread_id);
    size_t buf_size = ib_res.ib_buf_size / ib_res.num_qps;

    uint32_t imm_data = 0;
    int num_acked_peers = 0;
    bool start_sending = false;
    bool stop = false;

    uint64_t current_batch = 0, next_batch = 0, next_batch_off = 0;
    uint32_t expected_off = 0;
    log_block *pool = NULL, *new_local_log_block = NULL;
    oplogs *oLog = NULL;

    uint64_t key_ = 0;
    void *value_ = NULL;

    // pre-post recvs
    wc = (struct ibv_wc *) calloc(num_wc, sizeof(struct ibv_wc));
    check(wc != NULL, "Thread[%ld]: failed to allocate wc", thread_id);

    // Staged log segment per thread
    pool = (log_block *)(ib_res.ib_pool + ((sizeof(log_block) + MAX_LOG_BLOCK_LEN) * (uint64_t)thread_id));

retry:
    oLog = NULL;
    current_batch = non_replicated_alloc;
    if (current_batch != 0) {
        if (batching) {
            if (batched_offset + sizeof(oplogs) <= MAX_LOG_BLOCK_LEN) {
                oLog = (oplogs *)((uint64_t)pool->buffer + (uint64_t)batched_offset);
                oLog->op_type = OP_DELETE;
                oLog->key_len = key_len;
                oLog->val_len = 0;
                oLog->offset = batched_offset;
                oLog->commit = 1;
                oLog->key = key;

                batched_offset += sizeof(oplogs);
            } else {
                post_write_signaled_blocking_profile(batched_offset - flushed_offset, lkey_pool, 0, qp,
                        (char *)((uint64_t)pool->buffer + (uint64_t)flushed_offset), 
                        current_batch + LOG_BLOCK_HEADER_LEN + (uint64_t)flushed_offset,
                        ib_res.rkey_pool[thread_id], cq, &rdma_write_counter, &rdma_write_payload);

                pool->offset = batched_offset;

                // Write local log header info except for merged_offset and next pointer
                post_write_signaled_blocking_profile(LOG_BLOCK_HEADER_LEN - sizeof(uint64_t) - sizeof(uint32_t),
                        lkey_pool, 0, qp, (char *)pool + sizeof(uint32_t), current_batch + sizeof(uint32_t), 
                        ib_res.rkey_pool[thread_id], cq, &rdma_write_counter, &rdma_write_payload);

                while(flushed_offset < batched_offset) {
                    oLog = (oplogs *)((uint64_t)pool->buffer + (uint64_t)flushed_offset);

                    bloom_add(BFilter, &oLog->key, oLog->key_len);
                    if (oLog->op_type != OP_DELETE) {
                        key_ = oLog->key;
                        value_ = (void *) malloc(oLog->val_len);
                        memcpy(value_, oLog->value, oLog->val_len);

                        if (oLog->op_type == OP_UPDATE) {
                            cache->update(key_, (char *)value_, oLog->val_len, 
                                    (uint64_t)(current_batch - h->remote_start_addr + 
                                        ((uint64_t)&pool->buffer - (uint64_t)pool) +
                                        (uint64_t)flushed_offset + sizeof(oplogs)));
                        } else if (oLog->op_type == OP_INSERT) {
                            cache->insert(key_, (char *)value_, oLog->val_len,
                                    (uint64_t)(current_batch - h->remote_start_addr +
                                        ((uint64_t)&pool->buffer - (uint64_t)pool) +
                                        (uint64_t)flushed_offset + sizeof(oplogs)));
                        }
                    } else {
                        cache->remove(oLog->key);
                    }

                    flushed_offset += (sizeof(oplogs) + oLog->val_len);
                }

                memcpy(BFilter->local_log_block, pool, sizeof(log_block) + MAX_LOG_BLOCK_LEN);
                push_log_clean_list(lkey_buf, qp, buf_ptr, ib_res.rkey_pool[thread_id], cq);

                pool->valid_counter = 0;
                pool->offset = 0;

                batched_offset = 0;
                flushed_offset = 0;

                next_batch = pop_log_blocks_raddr();
                non_replicated_alloc = next_batch;
                if (next_batch != 0) {
                    // Allocate new local log block
                    new_local_log_block = (log_block *) calloc(1, sizeof(log_block) + MAX_LOG_BLOCK_LEN);

                    // Initialize new bloom filter
                    BFilter = (bloomFilter *) calloc(1, sizeof(bloomFilter));
                    bloom_init(BFilter, (MAX_LOG_BLOCK_LEN / (sizeof(oplogs) + VALUE_SIZE)) + 1, 0.1);
                    BFilter->local_log_block = new_local_log_block;
                    BFilter->raddr = next_batch;
                    BFilter->ready = 1;

                    // Set the next pointer of new log block to point to the existing log block
                    post_read_signaled_blocking_profile(sizeof(uint64_t), lkey_buf, 0, qp, buf_ptr,
                            mapping_table_raddr(config_info.rank), ib_res.rkey_pool[thread_id], cq,
                            &rdma_read_counter, &rdma_read_payload);
                    memcpy(&pool->next, buf_ptr, sizeof(uint64_t));
                    post_write_signaled_blocking_profile(sizeof(uint64_t), lkey_pool, 0, qp, (char *)&pool->next,
                            next_batch + LOG_BLOCK_HEADER_LEN - sizeof(uint64_t), ib_res.rkey_pool[thread_id], cq,
                            &rdma_write_counter, &rdma_write_payload);

                    // Update the pointer in log mapping table to new log block
                    next_batch_off = next_batch - h->remote_start_addr;
                    memcpy(buf_ptr, &next_batch_off, sizeof(uint64_t));
                    post_write_signaled_blocking_profile(sizeof(uint64_t), lkey_buf, 0, qp, buf_ptr,
                            mapping_table_raddr(config_info.rank), ib_res.rkey_pool[thread_id], cq,
                            &rdma_write_counter, &rdma_write_payload);
                }

                ret = 1;
                goto retry;
            }
        } else {
            if (pool->offset + sizeof(oplogs) <= MAX_LOG_BLOCK_LEN) {
                oLog = (oplogs *)((uint64_t)pool->buffer + (uint64_t)pool->offset);
                oLog->op_type = OP_DELETE;
                oLog->key_len = key_len;
                oLog->val_len = 0;
                oLog->offset = pool->offset;
                oLog->commit = 1;
                oLog->key = key;
                post_write_signaled_blocking_profile(sizeof(oplogs), lkey_pool, 0, qp,
                        (char *)oLog, current_batch + LOG_BLOCK_HEADER_LEN + pool->offset, 
                        ib_res.rkey_pool[thread_id], cq, &rdma_write_counter, &rdma_write_payload);

                key_ = key;

                bloom_add(BFilter, &oLog->key, oLog->key_len);
                cache->remove(key_);

                pool->offset += (sizeof(oplogs) + oLog->val_len);
            } else {
                // Write local log header info except for merged_offset and next pointer
                post_write_signaled_blocking_profile(LOG_BLOCK_HEADER_LEN - sizeof(uint64_t) - sizeof(uint32_t),
                        lkey_pool, 0, qp, (char *)pool + sizeof(uint32_t), current_batch + sizeof(uint32_t), 
                        ib_res.rkey_pool[thread_id], cq, &rdma_write_counter, &rdma_write_payload);

                memcpy(BFilter->local_log_block, pool, sizeof(log_block) + MAX_LOG_BLOCK_LEN);
                push_log_clean_list(lkey_buf, qp, buf_ptr, ib_res.rkey_pool[thread_id], cq);

                pool->valid_counter = 0;
                pool->offset = 0;

                next_batch = pop_log_blocks_raddr();
                non_replicated_alloc = next_batch;
                if (next_batch != 0) {
                    // Allocate new local log block
                    new_local_log_block = (log_block *) calloc(1, sizeof(log_block) + MAX_LOG_BLOCK_LEN);

                    // Initialize new bloom filter
                    BFilter = (bloomFilter *) calloc(1, sizeof(bloomFilter));
                    bloom_init(BFilter, (MAX_LOG_BLOCK_LEN / (sizeof(oplogs) + VALUE_SIZE)) + 1, 0.1);
                    BFilter->local_log_block = new_local_log_block;
                    BFilter->raddr = next_batch;
                    BFilter->ready = 1;

                    // Set the next pointer of new log block to point to the existing log block
                    post_read_signaled_blocking_profile(sizeof(uint64_t), lkey_buf, 0, qp, buf_ptr,
                            mapping_table_raddr(config_info.rank), ib_res.rkey_pool[thread_id], cq,
                            &rdma_read_counter, &rdma_read_payload);
                    memcpy(&pool->next, buf_ptr, sizeof(uint64_t));
                    post_write_signaled_blocking_profile(sizeof(uint64_t), lkey_pool, 0, qp, (char *)&pool->next,
                            next_batch + LOG_BLOCK_HEADER_LEN - sizeof(uint64_t), ib_res.rkey_pool[thread_id], cq,
                            &rdma_write_counter, &rdma_write_payload);

                    // Update the pointer in log mapping table to new log block
                    next_batch_off = next_batch - h->remote_start_addr;
                    memcpy(buf_ptr, &next_batch_off, sizeof(uint64_t));
                    post_write_signaled_blocking_profile(sizeof(uint64_t), lkey_buf, 0, qp, buf_ptr,
                            mapping_table_raddr(config_info.rank), ib_res.rkey_pool[thread_id], cq,
                            &rdma_write_counter, &rdma_write_payload);
                }

                ret = 1;
                goto retry;
            }
        }
    } else {
        preallocate_log_blocks(lkey_buf, qp, buf_ptr, cq, wc, num_wc);

        next_batch = pop_log_blocks_raddr();
        non_replicated_alloc = next_batch;

        // Allocate new local log block
        new_local_log_block = (log_block *) calloc(1, sizeof(log_block) + MAX_LOG_BLOCK_LEN);

        // Initialize new bloom filter
        BFilter = (bloomFilter *) calloc(1, sizeof(bloomFilter));
        bloom_init(BFilter, (MAX_LOG_BLOCK_LEN / (sizeof(oplogs) + VALUE_SIZE)) + 1, 0.1);
        BFilter->local_log_block = new_local_log_block;
        BFilter->raddr = next_batch;
        BFilter->ready = 1;

        // Set the next pointer of new log block to point to the existing log block
        post_read_signaled_blocking_profile(sizeof(uint64_t), lkey_buf, 0, qp, buf_ptr,
        mapping_table_raddr(config_info.rank), ib_res.rkey_pool[thread_id], cq,
        &rdma_read_counter, &rdma_read_payload);
        memcpy(&pool->next, buf_ptr, sizeof(uint64_t));
        post_write_signaled_blocking_profile(sizeof(uint64_t), lkey_pool, 0, qp, (char *)&pool->next,
        next_batch + LOG_BLOCK_HEADER_LEN - sizeof(uint64_t), ib_res.rkey_pool[thread_id], cq,
        &rdma_write_counter, &rdma_write_payload);

        // Update the pointer in log mapping table to new log block
        next_batch_off = next_batch - h->remote_start_addr;
        memcpy(buf_ptr, &next_batch_off, sizeof(uint64_t));
        post_write_signaled_blocking_profile(sizeof(uint64_t), lkey_buf, 0, qp, buf_ptr,
        mapping_table_raddr(config_info.rank), ib_res.rkey_pool[thread_id], cq,
        &rdma_write_counter, &rdma_write_payload);

        stop = false;
        goto retry;
    }

    free(wc);
    return ret;
error:
    LOG("%s: failure happens", __func__);
    free(wc);
    return ret;
}

template <typename T>
std::pair<char *, size_t> Dinomo<T>::get(const uint64_t key, const size_t key_len)
{
#ifdef SEKWON_DEBUG
    printf("%s\n", __func__);
#endif
    int msg_size = config_info.msg_size;

    int num_wc = 1;
    struct ibv_qp *qp = ib_res.qp[thread_id];
    struct ibv_cq *cq = ib_res.cq[thread_id];
    struct ibv_wc *wc = NULL;
    struct ibv_srq *srq = ib_res.srq;

    uint32_t lkey_buf = ib_res.mr_buf->lkey;
    uint32_t lkey_pool = ib_res.mr_pool->lkey;

    char *buf_ptr = ib_res.ib_buf + (msg_size * thread_id);
    size_t buf_size = ib_res.ib_buf_size / ib_res.num_qps;

    uint32_t imm_data = 0;
    int num_acked_peers = 0;
    bool start_sending = false;
    bool stop = false;

    char *valRaddr = NULL;
    size_t valLen = 0;
    uint64_t verSion = 0;
    oplogs *log_ret = NULL;

    int ret = 0;
    log_block *pool = (log_block *)(ib_res.ib_pool + ((sizeof(log_block) + MAX_LOG_BLOCK_LEN) * (uint64_t)(thread_id)));

    auto CopyCachedValue = [&](char *rvalRaddr, size_t rvalLen, uint64_t rverSion) -> char * {
        if (rvalRaddr == NULL) {
            post_read_signaled_blocking_profile(rvalLen + sizeof(oplogs), lkey_buf, 0, qp, buf_ptr,
            h->remote_start_addr + rverSion - sizeof(oplogs), ib_res.rkey_pool[thread_id], cq,
            &rdma_read_counter, &rdma_read_payload);
        } else {
            memcpy(buf_ptr, rvalRaddr, rvalLen);
        }

        return (buf_ptr + sizeof(oplogs));
    };

    if (find_from_cache(key, key_len, valRaddr, valLen, verSion, CopyCachedValue))
    {
        if (valRaddr == NULL)
        {
            if (key_len == ((oplogs *)buf_ptr)->key_len && valLen == ((oplogs *)buf_ptr)->val_len
            && key == ((oplogs *)buf_ptr)->key)
            {
                return std::make_pair((char *)((oplogs *)buf_ptr)->value, (size_t)((oplogs *)buf_ptr)->val_len);
            }
            else
            {
                cache->remove(key);
                return get(key, key_len);
            }
        }
        else
        {
            return std::make_pair((char *)buf_ptr, valLen);
        }
    }
    else
    {
        auto retVal = check_from_staging_pool(key, key_len, pool);
        if (retVal.first == true)
        {
            log_hit_counter++;
            return std::make_pair((char *)((oplogs *)retVal.second)->value, (size_t)((oplogs *)retVal.second)->val_len);
        }

        if (filterList->size() > 1) gc_log_list(lkey_buf, qp, buf_ptr, ib_res.rkey_pool[thread_id], cq);
        retVal = get_from_oplogs(key, key_len);
        if (retVal.first == true)
        {
            log_hit_counter++;
            return std::make_pair((char *)((oplogs *)retVal.second)->value, (size_t)((oplogs *)retVal.second)->val_len);
        }
        else
        {
            while (1)
            {
                uint64_t snapshot_rdma_read_counter = rdma_read_counter;
                miss_cost_tracker.second++;
                uint64_t getVal = clht_get(NULL, key, key_len, 0, lkey_buf, qp, buf_ptr,
                ib_res.rkey_pool[thread_id], cq, (uint64_t)ib_res.raddr_pool[thread_id],
                config_info.rank, &rdma_read_counter, &rdma_read_payload);
                if (getVal == 0)
                    return std::make_pair(nullptr, 0);
                
                post_read_signaled_blocking_profile(sizeof(oplogs), lkey_buf, 0, qp, buf_ptr, 
                        h->remote_start_addr + getVal, ib_res.rkey_pool[thread_id], cq, 
                        &rdma_read_counter, &rdma_read_payload);
                post_read_signaled_blocking_profile(sizeof(oplogs) + ((oplogs *)buf_ptr)->val_len, 
                        lkey_buf, 0, qp, buf_ptr, h->remote_start_addr + getVal, 
                        ib_res.rkey_pool[thread_id], cq, &rdma_read_counter, &rdma_read_payload);
                miss_cost_tracker.first = miss_cost_tracker.first + (rdma_read_counter - snapshot_rdma_read_counter);
                if (((oplogs *)buf_ptr)->key == key)
                {
                    char *val = (char *) malloc(((oplogs *)buf_ptr)->val_len);
                    memcpy(val, ((oplogs *)buf_ptr)->value, ((oplogs *)buf_ptr)->val_len);
                    cache->insert(key, val, ((oplogs *)buf_ptr)->val_len, getVal + sizeof(oplogs));

                    return std::make_pair((char *)((oplogs *)buf_ptr)->value, (size_t)((oplogs *)buf_ptr)->val_len);
                }
            }
        }
    }
}

template <typename T>
std::pair<char *, size_t> Dinomo<T>::getReplicated(const uint64_t key, const size_t key_len)
{
#ifdef SEKWON_DEBUG
    printf("%s\n", __func__);
#endif
    int msg_size = config_info.msg_size;

    int num_wc = 1;
    struct ibv_qp *qp = ib_res.qp[thread_id];
    struct ibv_cq *cq = ib_res.cq[thread_id];
    struct ibv_wc *wc = NULL;
    struct ibv_srq *srq = ib_res.srq;

    uint32_t lkey_buf = ib_res.mr_buf->lkey;
    uint32_t lkey_pool = ib_res.mr_pool->lkey;

    char *buf_ptr = ib_res.ib_buf + (msg_size * thread_id);
    size_t buf_size = ib_res.ib_buf_size / ib_res.num_qps;

    char *valRaddr = NULL;
    size_t valLen = 0;
    uint64_t verSion = 0;
    oplogs *log_ret = NULL;

    int ret = 0;
    log_block *pool = (log_block *)(ib_res.ib_pool + ((sizeof(log_block) + MAX_LOG_BLOCK_LEN) * (uint64_t)(thread_id)));

    // Search the indirect pointer from the local cache
    //  - Cache hit: Read the address stored in the indirect pointer and read the oplogs pointed by the address
    //  - Cache miss: Traverse remote index to get the address to the indirect pointer and read the oplogs pointed by the address
    uint64_t offset_to_indirect_pointer;
    auto search = indirect_pointer_cache.find(key);
    if (search != indirect_pointer_cache.end())
    {
        offset_to_indirect_pointer = std::get<0>(search->second);
    }
    else
    {
        // Presume clht_get returns indirect pointer, not value block itself.
        offset_to_indirect_pointer = clht_get(NULL, key, key_len, 0, lkey_buf, qp, buf_ptr, ib_res.rkey_pool[thread_id],
                                              cq, (uint64_t)ib_res.raddr_pool[thread_id], config_info.rank,
                                              &rdma_read_counter, &rdma_read_payload);
        if (offset_to_indirect_pointer == 0)
            return std::make_pair(nullptr, 0);
    }

    post_read_signaled_blocking_profile(sizeof(uint64_t), lkey_buf, 0, qp, buf_ptr, 
            h->remote_start_addr + offset_to_indirect_pointer, 
            ib_res.rkey_pool[thread_id], cq, &rdma_read_counter, &rdma_read_payload);

    uint64_t offset_to_oplog;
    memcpy(&offset_to_oplog, buf_ptr, sizeof(uint64_t));
    if (offset_to_oplog == 0)
    {
        // Invalidate cached indirect pointer
        if (search != indirect_pointer_cache.end()) {
            if (std::get<3>(search->second) != nullptr)
                free(std::get<3>(search->second));
            indirect_pointer_cache.erase(key);
        }

        return std::make_pair(nullptr, 0);
    }

    // Offset to value stored in the indirect pointer matches to the cached offset
    // Return cached value without reading value remotely
    if (search != indirect_pointer_cache.end()) {
        if (std::get<1>(search->second) == offset_to_oplog) {
            value_hit_counter++;
            return std::make_pair(std::get<3>(search->second), std::get<2>(search->second));
        } else {
            miss_counter++;
        }
    }

    post_read_signaled_blocking_profile(sizeof(oplogs), lkey_buf, 0, qp, buf_ptr, 
            h->remote_start_addr + offset_to_oplog, 
            ib_res.rkey_pool[thread_id], cq, &rdma_read_counter, &rdma_read_payload);
    post_read_signaled_blocking_profile(sizeof(oplogs) + ((oplogs *)buf_ptr)->val_len, lkey_buf, 0, 
            qp, buf_ptr, h->remote_start_addr + offset_to_oplog, ib_res.rkey_pool[thread_id], 
            cq, &rdma_read_counter, &rdma_read_payload);
    if (((oplogs *)buf_ptr)->key != key || ((oplogs *)buf_ptr)->key_len != key_len)
    {
        // Read undesirable value, retry after invalidating indirect pointer cache
        if (search != indirect_pointer_cache.end()) {
            if (std::get<3>(search->second) != nullptr)
                free(std::get<3>(search->second));
            indirect_pointer_cache.erase(key);
        }
        return getReplicated(key, key_len);
    }

    // Update cached indirect pointer and value with new value
    char *value_ = (char *) malloc((uint64_t)((oplogs *)buf_ptr)->val_len);
    memcpy(value_, (char *)((oplogs *)buf_ptr)->value, (uint64_t)((oplogs *)buf_ptr)->val_len);
    if (search != indirect_pointer_cache.end()) {
        std::get<0>(search->second) = offset_to_indirect_pointer;
        std::get<1>(search->second) = offset_to_oplog;
        std::get<2>(search->second) = (uint64_t)((oplogs *)buf_ptr)->val_len;
        if (std::get<3>(search->second) != nullptr)
            free(std::get<3>(search->second));
        std::get<3>(search->second) = value_;
    } else {
        indirect_pointer_cache[key] = std::make_tuple(offset_to_indirect_pointer,
                offset_to_oplog, (uint64_t)((oplogs *)buf_ptr)->val_len, value_);
    }

    return std::make_pair((char *)((oplogs *)buf_ptr)->value, (size_t)((oplogs *)buf_ptr)->val_len);
}

template <typename T>
std::pair<char *, size_t> Dinomo<T>::getMeta(const char *key, const size_t key_len)
{
#ifdef SEKWON_DEBUG
    printf("%s\n", __func__);
#endif
    int ret = 0, n = 0;
    int msg_size = config_info.msg_size;

    int num_wc = 1;

    struct ibv_qp *qp = ib_res.qp[thread_id];
    struct ibv_cq *cq = ib_res.cq[thread_id];
    struct ibv_wc *wc = NULL;
    struct ibv_srq *srq = ib_res.srq;

    uint32_t lkey_buf = ib_res.mr_buf->lkey;
    uint32_t lkey_pool = ib_res.mr_pool->lkey;

    char *buf_ptr = ib_res.ib_buf + (msg_size * thread_id);
    size_t buf_size = ib_res.ib_buf_size / ib_res.num_qps;

    uint32_t imm_data = 0;
    bool stop = false;

    void *val;
    size_t val_len;
    size_t req_size;

    // pre-post recvs
    wc = (struct ibv_wc *) calloc(num_wc, sizeof(struct ibv_wc));
    check(wc != NULL, "Thread[%ld]: failed to allocate wc", thread_id);

    req_size = serialize(key, key_len, NULL, 0, buf_ptr);
    ret = post_send_imm_profile((uint32_t)req_size, lkey_buf, (uint64_t)buf_ptr, 
            SET_GET_META(SET_IMM(config_info.rank, thread_id)),
            qp, buf_ptr, &rdma_send_counter, &rdma_send_payload);
    check(ret == 0, "Thread[%ld]: failed to post send", thread_id);

    while (stop != true)
    {
        n = ibv_poll_cq(cq, num_wc, wc);
        if (n < 0)
            check(0, "Thread[%ld]: failed to poll cq", thread_id);

        for (uint64_t j = 0; j < n; j++)
        {
            if (wc[j].status != IBV_WC_SUCCESS)
            {
                if (wc[j].opcode == IBV_WC_SEND)
                {
                    check(0, "Thread[%ld]: send failed status: %s; wr_id = %" PRIx64 "",
                            thread_id, ibv_wc_status_str(wc[j].status), wc[j].wr_id);
                }
                else
                {
                    check(0, "Thread[%ld]: recv failed status: %s; wr_id = %" PRIx64 "",
                            thread_id, ibv_wc_status_str(wc[j].status), wc[j].wr_id);
                }
            }

            if (wc[j].opcode == IBV_WC_RECV)
            {
                ret = post_recv_profile((uint32_t)config_info.msg_size, lkey_buf, wc[j].wr_id, qp, (char *)wc[j].wr_id,
                        &rdma_recv_counter, &rdma_recv_payload);
                check(ret == 0, "Thread[%ld]: failed to post recv", thread_id);

                val = deserialize(buf_ptr, val_len);

                stop = true;
                break;
            }
        }
    }

    free(wc);
    return std::make_pair((char *)val, val_len);
error:
    LOG("%s: failure happens", __func__);
    free(wc);
    return std::make_pair(nullptr, 0);
}

template <typename T>
uint64_t Dinomo<T>::value_cache_size()
{
    return cache->valueCacheSize();
}

template <typename T>
void Dinomo<T>::update_cache_miss_cost()
{
    if (miss_cost_tracker.second > 0) {
        uint64_t AvgMissCost = miss_cost_tracker.first / miss_cost_tracker.second;
        cache->updateMissCost(AvgMissCost);
    }
    miss_cost_tracker.first = 0;
    miss_cost_tracker.second = 0;
}

template <typename T>
uint64_t Dinomo<T>::cache_miss_cost()
{
    return cache->MissCost();
}

template <typename T>
void Dinomo<T>::merge(bool clear_cache)
{
#ifdef SEKWON_DEBUG
    printf("%s\n", __func__);
#endif
    int ret = 0, n = 0;
    int msg_size = config_info.msg_size;

    int num_wc = 1;

    struct ibv_qp *qp = ib_res.qp[thread_id];
    struct ibv_cq *cq = ib_res.cq[thread_id];
    struct ibv_wc *wc = NULL;
    struct ibv_srq *srq = ib_res.srq;

    uint32_t lkey_buf = ib_res.mr_buf->lkey;
    uint32_t lkey_pool = ib_res.mr_pool->lkey;

    char *buf_ptr = ib_res.ib_buf + (msg_size * thread_id);
    size_t buf_size = ib_res.ib_buf_size / ib_res.num_qps;

    uint32_t imm_data = 0;
    int num_acked_peers = 0;
    bool start_sending = false;
    bool stop = false;

    uint64_t current_batch = 0, next_batch = 0, next_batch_off = 0;
    log_block *pool = NULL, *new_local_log_block = NULL;

    // pre-post recvs
    wc = (struct ibv_wc *) calloc(num_wc, sizeof(struct ibv_wc));
    check(wc != NULL, "Thread[%ld]: failed to allocate wc", thread_id);

    pool = (log_block *)(ib_res.ib_pool + ((sizeof(log_block) + MAX_LOG_BLOCK_LEN) * (uint64_t)thread_id));

    current_batch = non_replicated_alloc;
    if (current_batch != 0)
    {
        if (batching) {
            flush();
        } else {
            // Write local log header info except for merged_offset and next pointer
            post_write_signaled_blocking_profile(LOG_BLOCK_HEADER_LEN - sizeof(uint64_t) - sizeof(uint32_t), lkey_pool, 0,
                    qp, (char *)pool + sizeof(uint32_t), current_batch + sizeof(uint32_t), ib_res.rkey_pool[thread_id], cq,
                    &rdma_write_counter, &rdma_write_payload);
        }

        ret = post_send_imm_profile(0, lkey_buf, (uint64_t)buf_ptr, 
                SET_MERGE_ALL(SET_IMM(config_info.rank, thread_id)),
                qp, buf_ptr, &rdma_send_counter, &rdma_send_payload);
        check(ret == 0, "Thread[%ld]: failed to post send", thread_id);

        while (stop != true)
        {
            n = ibv_poll_cq(cq, num_wc, wc);
            if (n < 0)
                check(0, "Thread[%ld]: failed to poll cq", thread_id);
            
            for (uint64_t j = 0; j < n; j++)
            {
                if (wc[j].status != IBV_WC_SUCCESS)
                {
                    if (wc[j].opcode == IBV_WC_SEND)
                    {
                        check(0, "Thread[%ld]: send failed status: %s; wr_id = %" PRIx64 "",
                        thread_id, ibv_wc_status_str(wc[j].status), wc[j].wr_id);
                    }
                    else
                    {
                        check(0, "Thread[%ld]: recv failed status: %s; wr_id = %" PRIx64 "",
                        thread_id, ibv_wc_status_str(wc[j].status), wc[j].wr_id);
                    }
                }

                if (wc[j].opcode == IBV_WC_RECV)
                {
                    ret = post_recv_profile((uint32_t)config_info.msg_size, lkey_buf, wc[j].wr_id, qp, (char *)wc[j].wr_id,
                            &rdma_recv_counter, &rdma_recv_payload);
                    check(ret == 0, "Thread[%ld]: failed to post recv", thread_id);

                    if (ntohl(wc[j].imm_data) == MSG_CTL_STOP)
                    {
                        stop = true;
                        break;
                    }
                }
            }
        }

        clear_log_list();
    }

    if (clear_cache)
    {
        cache->clear();
        for (auto kvpair : indirect_pointer_cache) {
            if (std::get<3>(kvpair.second) != nullptr)
                free(std::get<3>(kvpair.second));
        }
        indirect_pointer_cache.clear();
    }

    free(wc);
    return ;
error:
    LOG("%s: failure happens", __func__);
    free(wc);
    return ;
}

template <typename T>
void Dinomo<T>::flush()
{
#ifdef SEKWON_DEBUG
    printf("%s\n", __func__);
#endif
    int msg_size = config_info.msg_size;
    int num_wc = 1;

    struct ibv_qp *qp = ib_res.qp[thread_id];
    struct ibv_cq *cq = ib_res.cq[thread_id];

    uint32_t lkey_buf = ib_res.mr_buf->lkey;
    uint32_t lkey_pool = ib_res.mr_pool->lkey;

    char *buf_ptr = ib_res.ib_buf + (msg_size * thread_id);
    size_t buf_size = ib_res.ib_buf_size / ib_res.num_qps;

    uint64_t current_batch = 0;
    log_block *pool = NULL;
    oplogs *oLog = NULL;

    uint64_t key_ = 0;
    void *value_ = NULL;

    // Staged log segment per thread
    pool = (log_block *)(ib_res.ib_pool + ((sizeof(log_block) + MAX_LOG_BLOCK_LEN) * (uint64_t)thread_id));

    current_batch = non_replicated_alloc;

    if (flushed_offset < batched_offset) {
        post_write_signaled_blocking_profile(batched_offset - flushed_offset, lkey_pool, 0, qp,
                (char *)((uint64_t)pool->buffer + (uint64_t)flushed_offset), 
                current_batch + LOG_BLOCK_HEADER_LEN + (uint64_t)flushed_offset,
                ib_res.rkey_pool[thread_id], cq, &rdma_write_counter, &rdma_write_payload);

        pool->offset = batched_offset;

        // Write local log header info except for merged_offset and next pointer
        post_write_signaled_blocking_profile(LOG_BLOCK_HEADER_LEN - sizeof(uint64_t) - sizeof(uint32_t), lkey_pool, 0,
                qp, (char *)pool + sizeof(uint32_t), current_batch + sizeof(uint32_t), ib_res.rkey_pool[thread_id], cq,
                &rdma_write_counter, &rdma_write_payload);

        while(flushed_offset < batched_offset) {
            oLog = (oplogs *)((uint64_t)pool->buffer + (uint64_t)flushed_offset);

            bloom_add(BFilter, &oLog->key, oLog->key_len);
            if (oLog->op_type != OP_DELETE) {
                key_ = oLog->key;
                value_ = (void *)malloc(oLog->val_len);
                memcpy(value_, oLog->value, oLog->val_len);

                if (oLog->op_type == OP_UPDATE) {
                    cache->update(key_, (char *)value_, oLog->val_len, 
                            (uint64_t)(current_batch - h->remote_start_addr + 
                                ((uint64_t)&pool->buffer - (uint64_t)pool) +
                                (uint64_t)flushed_offset + sizeof(oplogs)));
                } else if (oLog->op_type == OP_INSERT) {
                    cache->insert(key_, (char *)value_, oLog->val_len,
                            (uint64_t)(current_batch - h->remote_start_addr +
                                ((uint64_t)&pool->buffer - (uint64_t)pool) +
                                (uint64_t)flushed_offset + sizeof(oplogs)));
                }
            } else {
                cache->remove(oLog->key);
            }

            flushed_offset += (sizeof(oplogs) + oLog->val_len);
        }
    }
}

template <typename T>
void Dinomo<T>::failover(int failed_node_rank)
{
#ifdef SEKWON_DEBUG
    printf("%s\n", __func__);
#endif
    int ret = 0, n = 0;
    int msg_size = config_info.msg_size;

    int num_wc = 1;

    struct ibv_qp *qp = ib_res.qp[thread_id];
    struct ibv_cq *cq = ib_res.cq[thread_id];
    struct ibv_wc *wc = NULL;
    struct ibv_srq *srq = ib_res.srq;

    uint32_t lkey_buf = ib_res.mr_buf->lkey;
    uint32_t lkey_pool = ib_res.mr_pool->lkey;

    char *buf_ptr = ib_res.ib_buf + (msg_size * thread_id);
    size_t buf_size = ib_res.ib_buf_size / ib_res.num_qps;

    uint32_t imm_data = 0;
    int num_acked_peers = 0;
    bool start_sending = false;
    bool stop = false;

    uint64_t current_batch = 0, next_batch = 0, next_batch_off = 0;
    log_block *pool = NULL, *new_local_log_block = NULL;

    // pre-post recvs
    wc = (struct ibv_wc *) calloc(num_wc, sizeof(struct ibv_wc));
    check(wc != NULL, "Thread[%ld]: failed to allocate wc", thread_id);

    ret = post_send_imm_profile(0, lkey_buf, (uint64_t)buf_ptr, 
            SET_FAILOVER(SET_IMM(config_info.rank, failed_node_rank)),
            qp, buf_ptr, &rdma_send_counter, &rdma_send_payload);
    check(ret == 0, "Thread[%ld]: failed to post send", thread_id);

    while (stop != true)
    {
        n = ibv_poll_cq(cq, num_wc, wc);
        if (n < 0)
            check(0, "Thread[%ld]: failed to poll cq", thread_id);

        for (uint64_t j = 0; j < n; j++)
        {
            if (wc[j].status != IBV_WC_SUCCESS)
            {
                if (wc[j].opcode == IBV_WC_SEND)
                {
                    check(0, "Thread[%ld]: send failed status: %s; wr_id = %" PRIx64 "",
                            thread_id, ibv_wc_status_str(wc[j].status), wc[j].wr_id);
                }
                else
                {
                    check(0, "Thread[%ld]: recv failed status: %s; wr_id = %" PRIx64 "",
                            thread_id, ibv_wc_status_str(wc[j].status), wc[j].wr_id);
                }
            }

            if (wc[j].opcode == IBV_WC_RECV)
            {
                ret = post_recv_profile((uint32_t)config_info.msg_size, lkey_buf, wc[j].wr_id, qp, (char *)wc[j].wr_id,
                        &rdma_recv_counter, &rdma_recv_payload);
                check(ret == 0, "Thread[%ld]: failed to post recv", thread_id);

                if (ntohl(wc[j].imm_data) == MSG_CTL_STOP)
                {
                    stop = true;
                    break;
                }
            }
        }
    }

    free(wc);
    return ;
error:
    LOG("%s: failure happens", __func__);
    free(wc);
    return ;

}

template <typename T>
void Dinomo<T>::install_indirect_pointer(const uint64_t key)
{
#ifdef SEKWON_DEBUG
    printf("%s\n", __func__);
#endif
    int ret = 0, n = 0;
    int msg_size = config_info.msg_size;
    int num_wc = 1;

    struct ibv_qp *qp = ib_res.qp[thread_id];
    struct ibv_cq *cq = ib_res.cq[thread_id];
    struct ibv_wc *wc = NULL;

    uint32_t lkey_buf = ib_res.mr_buf->lkey;
    uint32_t lkey_pool = ib_res.mr_pool->lkey;

    char *buf_ptr = ib_res.ib_buf + (msg_size * thread_id);
    size_t buf_size = ib_res.ib_buf_size / ib_res.num_qps;

    uint32_t imm_data = 0;
    bool stop = false;

    // pre-post recvs
    wc = (struct ibv_wc *) calloc(num_wc, sizeof(struct ibv_wc));
    check(wc != NULL, "Thread[%ld]: failed to allocate wc", thread_id);

    memcpy(buf_ptr, &key, sizeof(uint64_t));

    ret = post_send_imm_profile(sizeof(uint64_t), lkey_buf, (uint64_t)buf_ptr, 
            SET_INSTALL_INDIRECT(SET_IMM(config_info.rank, thread_id)),
            qp, buf_ptr, &rdma_send_counter, &rdma_send_payload);
    check(ret == 0, "Thread[%ld]: failed to post send", thread_id);

    while (stop != true)
    {
        n = ibv_poll_cq(cq, num_wc, wc);
        if (n < 0)
            check(0, "Thread[%ld]: failed to poll cq", thread_id);

        for (uint64_t j = 0; j < n; j++)
        {
            if (wc[j].status != IBV_WC_SUCCESS)
            {
                if (wc[j].opcode == IBV_WC_SEND)
                {
                    check(0, "Thread[%ld]: send failed status: %s; wr_id = %" PRIx64 "",
                          thread_id, ibv_wc_status_str(wc[j].status), wc[j].wr_id);
                }
                else
                {
                    check(0, "Thread[%ld]: recv failed status: %s; wr_id = %" PRIx64 "",
                          thread_id, ibv_wc_status_str(wc[j].status), wc[j].wr_id);
                }
            }

            if (wc[j].opcode == IBV_WC_RECV)
            {
                ret = post_recv_profile((uint32_t)config_info.msg_size, lkey_buf, wc[j].wr_id, qp, (char *)wc[j].wr_id,
                        &rdma_recv_counter, &rdma_recv_payload);
                check(ret == 0, "Thread[%ld]: failed to post recv", thread_id);

                if (ntohl(wc[j].imm_data) == MSG_CTL_STOP)
                {
                    stop = true;
                    break;
                }
            }
        }
    }

    free(wc);
    return ;
error:
    LOG("%s: failure happens", __func__);
    free(wc);
    return ;
}

template <typename T>
void Dinomo<T>::remove_indirect_pointer(const uint64_t key)
{
#ifdef SEKWON_DEBUG
    printf("%s\n", __func__);
#endif
    int ret = 0, n = 0;
    int msg_size = config_info.msg_size;
    int num_wc = 1;

    struct ibv_qp *qp = ib_res.qp[thread_id];
    struct ibv_cq *cq = ib_res.cq[thread_id];
    struct ibv_wc *wc = NULL;

    uint32_t lkey_buf = ib_res.mr_buf->lkey;
    uint32_t lkey_pool = ib_res.mr_pool->lkey;

    char *buf_ptr = ib_res.ib_buf + (msg_size * thread_id);
    size_t buf_size = ib_res.ib_buf_size / ib_res.num_qps;

    uint32_t imm_data = 0;
    bool stop = false;

    // pre-post recvs
    wc = (struct ibv_wc *) calloc(num_wc, sizeof(struct ibv_wc));
    check(wc != NULL, "Thread[%ld]: failed to allocate wc", thread_id);

    memcpy(buf_ptr, &key, sizeof(uint64_t));

    ret = post_send_imm_profile(sizeof(uint64_t), lkey_buf, (uint64_t)buf_ptr, 
            SET_REMOVE_INDIRECT(SET_IMM(config_info.rank, thread_id)),
            qp, buf_ptr, &rdma_send_counter, &rdma_send_payload);
    check(ret == 0, "Thread[%ld]: failed to post send", thread_id);

    while (stop != true)
    {
        n = ibv_poll_cq(cq, num_wc, wc);
        if (n < 0)
            check(0, "Thread[%ld]: failed to poll cq", thread_id);

        for (uint64_t j = 0; j < n; j++)
        {
            if (wc[j].status != IBV_WC_SUCCESS)
            {
                if (wc[j].opcode == IBV_WC_SEND)
                {
                    check(0, "Thread[%ld]: send failed status: %s; wr_id = %" PRIx64 "",
                          thread_id, ibv_wc_status_str(wc[j].status), wc[j].wr_id);
                }
                else
                {
                    check(0, "Thread[%ld]: recv failed status: %s; wr_id = %" PRIx64 "",
                          thread_id, ibv_wc_status_str(wc[j].status), wc[j].wr_id);
                }
            }

            if (wc[j].opcode == IBV_WC_RECV)
            {
                ret = post_recv_profile((uint32_t)config_info.msg_size, lkey_buf, wc[j].wr_id, qp, (char *)wc[j].wr_id,
                        &rdma_recv_counter, &rdma_recv_payload);
                check(ret == 0, "Thread[%ld]: failed to post recv", thread_id);

                if (ntohl(wc[j].imm_data) == MSG_CTL_STOP)
                {
                    stop = true;
                    break;
                }
            }
        }
    }

    free(wc);
    return ;
error:
    LOG("%s: failure happens", __func__);
    free(wc);
    return ;
}

// This function must be called only by a primary replica
template <typename T>
void Dinomo<T>::swap(const uint64_t key, bool replicated)
{
#ifdef SEKWON_DEBUG
    printf("%s\n", __func__);
#endif
    if (replicated)
    {
        // non-replicated to replicated
        // Invalidate the cached entry and enforce log segments to be merged to remote main index
        // Send a request to disaggregated storage to install an indirect pointer for cache coherence
        merge(false);
        invalidate_cache(key);
        install_indirect_pointer(key);
    }
    else
    {
        // replicated to non-replicated
        // Invalidate the cached entry
        // Send a request to disaggregated storage to remove an indirect pointer
        invalidate_cache(key);
        remove_indirect_pointer(key);
    }
}

template <typename T>
void Dinomo<T>::invalidate_cache(const uint64_t key)
{
#ifdef SEKWON_DEBUG
    printf("%s\n", __func__);
#endif
    cache->remove(key);
    auto search = indirect_pointer_cache.find(key);
    if (search != indirect_pointer_cache.end()) {
        if (std::get<3>(search->second) != nullptr)
            free(std::get<3>(search->second));
        indirect_pointer_cache.erase(key);
    }
}

void ib_finalize();

void ib_sync_cross(uint16_t thread_unique_id);

#endif