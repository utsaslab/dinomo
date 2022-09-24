extern "C" void clover_init(int num_initial_memory_nodes, int rank, char **clover_memc_ips);

extern "C" int clover_put(const uint64_t key, const size_t key_len, const void *val, 
        const size_t val_len, const uint16_t thread_id);

extern "C" int clover_update(const uint64_t key, const size_t key_len, const void *val, 
        const size_t val_len, const uint16_t thread_id, bool &cacheHit);

extern "C" void *clover_get(const uint64_t key, const size_t key_len, uint32_t *read_size,
        const uint16_t thread_id, bool &cacheHit);

extern "C" void *clover_get_meta(const char *key, const size_t key_len, int *val_len);

extern "C" void clover_put_meta(const char *key, const size_t key_len, void *value, const size_t val_len);

