#ifndef CLOVER_H_
#define CLOVER_H_

#include <utility>
#include <cstdint>
#include <stdlib.h>

#include "clover_api.h"

class Clover
{
public:
    Clover (uint16_t tid = 0)
    {
        thread_id = tid;
        shortcut_hit_counter = 0;
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
    }

    unsigned put(const uint64_t key, const size_t key_len, const void *value, const uint64_t val_len, bool do_update);

    void putReplicated(const uint64_t key, const size_t key_len, const void *value, const uint64_t val_len, bool do_update);
    
    void putMeta(const char *key, const size_t key_len, const void *value, const uint64_t val_len);

    std::pair<char *, size_t> get(const uint64_t key, const size_t key_len);

    std::pair<char *, size_t> getReplicated(const uint64_t key, const size_t key_len);
    
    std::pair<char *, size_t> getMeta(const char *key, const size_t key_len);

    uint64_t shortcut_cache_hit_counter()
    {
        uint64_t counter = shortcut_hit_counter;
        shortcut_hit_counter = 0;
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
    uint16_t thread_id;

    uint64_t shortcut_hit_counter;

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
#endif