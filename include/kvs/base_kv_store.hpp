#ifndef INCLUDE_KVS_BASE_KV_STORE_HPP_
#define INCLUDE_KVS_BASE_KV_STORE_HPP_

#include "anna.pb.h"
#include "lattices/core_lattices.hpp"

#ifdef ENABLE_DINOMO_KVS // Sekwon
#ifndef ENABLE_CLOVER_KVS
#include "kvs/dinomo_compute.hpp"
#else
#include "kvs/clover.hpp"
#endif
#endif

template <typename K, typename V> class KVStore {
    protected:
        MapLattice<K, V> db;

    public:
        KVStore<K, V>() {}

        KVStore<K, V>(MapLattice<K, V> &other) { db = other; }

        V get(const K &k, AnnaError &error) {
            if (!db.contains(k).reveal()) {
                error = AnnaError::KEY_DNE;
            }

            return db.at(k);
        }

        void put(const K &k, const V &v) { return db.at(k).merge(v); }

        unsigned size(const K &k) { return db.at(k).size().reveal(); }

        void remove(const K &k) { db.remove(k); }
};

#ifdef ENABLE_DINOMO_KVS // Sekwon

#ifdef __GNUC__
#  define UNUSED(x) x __attribute__((__unused__))
#else
#  define UNUSED(x) x
#endif

#ifndef ENABLE_CLOVER_KVS
template <typename T>
class DinomoKVStore {
    protected:
        Dinomo<T> db;

    public:
        DinomoKVStore() {}

        DinomoKVStore(uint64_t cache_size, uint16_t tid, bool enable_batch, double weight_or_value_ratio) : db(cache_size, tid, enable_batch, weight_or_value_ratio) {}

        DinomoKVStore(Dinomo<T> &other) { db = other; }

        std::pair<char *, size_t> get(const uint64_t k, const size_t key_len, AnnaError &error)
        {
            auto val = db.get(k, key_len);
            if (val.second == 0) {
                error = AnnaError::KEY_DNE;
            }

            return val;
        }

        unsigned put(const uint64_t k, const size_t key_len, const void *v, const size_t val_len)
        {
            return db.put(k, key_len, v, val_len, false);
        }

        unsigned update(const uint64_t k, const size_t key_len, const void *v, const size_t val_len)
        {
            return db.put(k, key_len, v, val_len, true);
        }

        void remove(const uint64_t k, const size_t key_len)
        {
            db.remove(k, key_len);
        }

        std::pair<char *, size_t> getMeta(const char *k, const size_t key_len, AnnaError &error)
        {
            auto val = db.getMeta(k, key_len);
            if (val.second == 0) {
                error = AnnaError::KEY_DNE;
            }

            return val;
        }

        void putMeta(const char *k, const size_t key_len, const void *v, const size_t val_len)
        {
            return db.putMeta(k, key_len, v, val_len);
        }

        std::pair<char *, size_t> getReplicated(const uint64_t k, const size_t key_len, AnnaError &error)
        {
            auto val = db.getReplicated(k, key_len);
            if (val.second == 0) {
                error = AnnaError::KEY_DNE;
            }

            return val;
        }

        void putReplicated(const uint64_t k, const size_t key_len, const void *v, const size_t val_len)
        {
            return db.putReplicated(k, key_len, v, val_len, false);
        }

        void updateReplicated(const uint64_t k, const size_t key_len, const void *v, const size_t val_len)
        {
            return db.putReplicated(k, key_len, v, val_len, true);
        }

        void merge(bool clear_cache) { db.merge(clear_cache); }

        void flush() { db.flush(); }

        void failover(int failed_node_rank) { db.failover(failed_node_rank); }

        void invalidate(const uint64_t k) { db.invalidate_cache(k); }

        void swap(const uint64_t k, const bool replicated) { db.swap(k, replicated); }

        unsigned size(const uint64_t k)
        {
            return 0;
        }

        uint64_t value_cache_size()
        {
            return db.value_cache_size();
        }
        
        void update_cache_miss_cost() { return db.update_cache_miss_cost(); }

        uint64_t cache_miss_cost() { return db.cache_miss_cost(); }

        uint64_t value_cache_hit_counter() { return db.value_cache_hit_counter(); }

        uint64_t shortcut_cache_hit_counter() { return db.shortcut_cache_hit_counter(); }

        uint64_t local_log_hit_counter() { return db.local_log_hit_counter(); }

        uint64_t cache_miss_counter() { return db.cache_miss_counter(); }

        uint64_t rdma_read_counter() { return db.remote_read_counter(); }

        uint64_t rdma_read_payload() { return db.remote_read_payload(); }

        uint64_t rdma_write_counter() { return db.remote_write_counter(); }

        uint64_t rdma_write_payload() { return db.remote_write_payload(); }

        uint64_t rdma_send_counter() { return db.remote_send_counter(); }

        uint64_t rdma_send_payload() { return db.remote_send_payload(); }

        uint64_t rdma_recv_counter() { return db.remote_recv_counter(); }

        uint64_t rdma_recv_payload() { return db.remote_recv_payload(); }

        uint64_t rdma_cas_counter() { return db.remote_cas_counter(); }

        uint64_t rdma_cas_payload() { return db.remote_cas_payload(); }

        uint64_t rdma_faa_counter() { return db.remote_faa_counter(); }

        uint64_t rdma_faa_payload() { return db.remote_faa_payload(); }
};
#else
class CloverKVStore {
    protected:
        Clover db;

    public:
        CloverKVStore() {}

        CloverKVStore(uint16_t tid) : db(tid) {}

        CloverKVStore(Clover &other) { db = other; }

        std::pair<char *, size_t> get(const uint64_t k, const size_t key_len, AnnaError &error)
        {
            auto val = db.get(k, key_len);
            if (val.second == 0) {
                error = AnnaError::KEY_DNE;
            }

            return val;
        }

        unsigned put(const uint64_t k, const size_t key_len, const void *v, const size_t val_len)
        {
            return db.put(k, key_len, v, val_len, false);
        }

        unsigned update(const uint64_t k, const size_t key_len, const void *v, const size_t val_len)
        {
            return db.put(k, key_len, v, val_len, true);
        }

        void remove(const uint64_t k, const size_t key_len) {}

        std::pair<char *, size_t> getMeta(const char *k, const size_t key_len, AnnaError &error)
        {
            auto val = db.getMeta(k, key_len);
            if (val.second == 0) {
                error = AnnaError::KEY_DNE;
            }

            return val;
        }

        void putMeta(const char *k, const size_t key_len, const void *v, const size_t val_len)
        {
            return db.putMeta(k, key_len, v, val_len);
        }

        std::pair<char *, size_t> getReplicated(const uint64_t k, const size_t key_len, AnnaError &error)
        {
            auto val = db.getReplicated(k, key_len);
            if (val.second == 0) {
                error = AnnaError::KEY_DNE;
            }

            return val;
        }

        void putReplicated(const uint64_t k, const size_t key_len, const void *v, const size_t val_len)
        {
            return db.putReplicated(k, key_len, v, val_len, false);
        }

        void updateReplicated(const uint64_t k, const size_t key_len, const void *v, const size_t val_len)
        {
            return db.putReplicated(k, key_len, v, val_len, true);
        }

        void merge(bool UNUSED(clear_cache)) {}

        void flush() {}

        void failover(int UNUSED(failed_node_rank)) {}

        void invalidate(const uint64_t UNUSED(k)) {}

        void swap(const uint64_t UNUSED(k), const bool UNUSED(replicated)) {}

        unsigned size(const uint64_t k)
        {
            return 0;
        }

        uint64_t value_cache_size()
        {
            return 0;
        }
        
        void update_cache_miss_cost() { return ; }

        uint64_t cache_miss_cost() { return 0; }

        uint64_t value_cache_hit_counter() { return 0; }

        uint64_t shortcut_cache_hit_counter() { return db.shortcut_cache_hit_counter(); }

        uint64_t local_log_hit_counter() { return 0; }

        uint64_t cache_miss_counter() { return db.cache_miss_counter(); }

        uint64_t rdma_read_counter() { return db.remote_read_counter(); }

        uint64_t rdma_read_payload() { return db.remote_read_payload(); }

        uint64_t rdma_write_counter() { return db.remote_write_counter(); }

        uint64_t rdma_write_payload() { return db.remote_write_payload(); }

        uint64_t rdma_send_counter() { return db.remote_send_counter(); }

        uint64_t rdma_send_payload() { return db.remote_send_payload(); }

        uint64_t rdma_recv_counter() { return db.remote_recv_counter(); }

        uint64_t rdma_recv_payload() { return db.remote_recv_payload(); }

        uint64_t rdma_cas_counter() { return db.remote_cas_counter(); }

        uint64_t rdma_cas_payload() { return db.remote_cas_payload(); }

        uint64_t rdma_faa_counter() { return db.remote_faa_counter(); }

        uint64_t rdma_faa_payload() { return db.remote_faa_payload(); }
};
#endif
#endif

#endif // INCLUDE_KVS_BASE_KV_STORE_HPP_
