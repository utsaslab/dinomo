#include "kvs/clover.hpp"

unsigned Clover::put(const uint64_t key, const size_t key_len, const void *value, const uint64_t val_len, bool do_update)
{
    if (do_update) {
        bool cacheHit;
        clover_update(key, key_len, value, val_len, thread_id, cacheHit);
        if (cacheHit == true)
            shortcut_hit_counter++;
        else
            miss_counter++;
    } else {
        clover_put(key, key_len, value, val_len, thread_id);
    }

    return 0;
}

void Clover::putReplicated(const uint64_t key, const size_t key_len, const void *value, const uint64_t val_len, bool do_update)
{
    if (do_update) {
        bool cacheHit;
        clover_update(key, key_len, value, val_len, thread_id, cacheHit);
        if (cacheHit == true)
            shortcut_hit_counter++;
        else
            miss_counter++;
    } else {
        clover_put(key, key_len, value, val_len, thread_id);
    }
}

void Clover::putMeta(const char *key, const size_t key_len, const void *value, const uint64_t val_len)
{
    clover_put_meta(key, key_len, const_cast<void *>(value), val_len);
}

std::pair<char *, size_t> Clover::get(const uint64_t key, const size_t key_len)
{
    bool cacheHit;
    uint32_t read_size = 0;
    char *val = (char *) clover_get(key, key_len, &read_size, thread_id, cacheHit);

    if (cacheHit == true)
        shortcut_hit_counter++;
    else
        miss_counter++;

    if (read_size != 0)
        return std::make_pair<char *, size_t>((char *)val, (size_t)read_size);
    else
        return std::make_pair<char *, size_t>(NULL, 0);
}

std::pair<char *, size_t> Clover::getReplicated(const uint64_t key, const size_t key_len)
{
    bool cacheHit;
    uint32_t read_size = 0;
    char *val = (char *) clover_get(key, key_len, &read_size, thread_id, cacheHit);

    if (cacheHit == true)
        shortcut_hit_counter++;
    else
        miss_counter++;

    if (read_size != 0)
        return std::make_pair<char *, size_t>((char *)val, (size_t)read_size);
    else
        return std::make_pair<char *, size_t>(NULL, 0);
}

std::pair<char *, size_t> Clover::getMeta(const char *key, const size_t key_len)
{
    int read_size = 0;
    char *val = (char *) clover_get_meta(key, key_len, &read_size);
    if (read_size != 0)
        return std::make_pair<char *, size_t>((char *)val, (size_t)read_size);
    else
        return std::make_pair<char *, size_t>(NULL, 0);
}