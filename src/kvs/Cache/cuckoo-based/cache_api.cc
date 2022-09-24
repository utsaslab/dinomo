#include <malloc.h>
#include <string.h>
#include <string>
#include <iostream>

#include "cache_api.h"
#include "masstree.h"

using namespace std;

#define ENABLE_PM_INDEX     1

typedef tstarling::ThreadSafeScalableCache<string, uint64_t> ScalableCache;

ScalableCache *cache;
masstree::masstree *tree;

void Init(size_t cache_size, int num_threads)
{
    cache = new ScalableCache(cache_size, num_threads);
#if ENABLE_PM_INDEX == 1
    tree = new masstree::masstree();
#endif
#ifdef ENABLE_PREALLOCATE
    tree->preallocate(cache_size / 2, 1000000);
#endif
}

void Final()
{
    cache->clear();
#if ENABLE_PM_INDEX == 1
    tree->~masstree();
#endif
}

uint64_t Warmup(char *key, size_t key_len, char *val, size_t val_len)
{
#if ENABLE_PM_INDEX == 1
    auto t = tree->getThreadInfo();
    uint64_t version = tree->put(key, (uint64_t)val, val_len, t);
#else
    uint64_t version = 0;
#endif

    cache->insert(string(key), (uint64_t)val, val_len, version);
    return version;
}

uint64_t Put(char *key, size_t key_len, char *val, size_t val_len)
{
#if ENABLE_PM_INDEX == 1
    auto t = tree->getThreadInfo();
    uint64_t version = tree->put(key, (uint64_t)val, val_len, t);
#else
    uint64_t version = 0;
#endif

    cache->remove(string(key));
    return version;
}

static int Fetch(char *key, size_t key_len, char *val, size_t val_len, uint64_t version)
{
    if (cache->insert(string(key), (uint64_t)val, val_len, version))
        return 1;
    else
        return 0;
}

int Get(char *key, size_t key_len, char *val, size_t &val_len, uint64_t version)
{
    uint64_t c_val, c_version;
    if (cache->find(string(key), c_val, val_len, c_version)) {
        if (c_version == version) {
            memcpy(val, (char *)c_val, val_len);
            return 0;
        } else {
#if ENABLE_PM_INDEX == 1
            auto t = tree->getThreadInfo();
            void *ret = tree->get(key, val_len, version, t);
            if (ret != NULL) {
                memcpy(val, (char *)ret, val_len);
                Fetch(key, key_len, (char *)ret, val_len, version);
            }
#endif
            return -1;
        }
    } else {
#if ENABLE_PM_INDEX == 1
        auto t = tree->getThreadInfo();
        void *ret = tree->get(key, val_len, version, t);
        if (ret != NULL) {
            memcpy(val, (char *)(ret), val_len);
            Fetch(key, key_len, (char *)ret, val_len, version);
        }
#endif
        return -1;
    }
}

int Del(char *key, size_t key_len, uint64_t version)
{
    cache->remove(string(key));
#if ENABLE_PM_INDEX == 1
    auto t = tree->getThreadInfo();
    tree->del(key, version, t);
#endif
    return 0;
}
