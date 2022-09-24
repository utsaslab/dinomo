#ifndef ADAPTIVE_HYBRID_CACHE_H
#define ADAPTIVE_HYBRID_CACHE_H
#include <atomic>
#include <mutex>
#include <new>
#include <thread>
#include <vector>
#include <functional>
#include <cmath>

#include <cache.hpp>
#include <lru_cache_policy.hpp>
#include <lfu_cache_policy.hpp>

namespace sekwonlee {
    template <class TKey, class TValue>
        class AdaptiveHybridCache {
            struct ListNode {
                ListNode(const TKey& key, const TValue& value, const size_t& valueLen, const uint64_t& shortCut, const uint64_t& weight = 1)
                    : m_key(key), m_value(value), m_valLen(valueLen), m_shortcut(shortCut), m_weight(weight)
                {
                }

                TKey m_key;
                TValue m_value;
                size_t m_valLen;
                uint64_t m_shortcut;
                uint64_t m_weight;
            };

            typedef caches::fixed_sized_cache<TKey, ListNode *, caches::LRUCachePolicy<TKey>> ValueCache;
            typedef caches::fixed_sized_cache<TKey, ListNode *, caches::LFUCachePolicy<TKey>> ShortcutCache;

            public:

            explicit AdaptiveHybridCache(size_t maxCacheSize, size_t valueEntrySize, size_t shortcutEntrySize, uint64_t costPerMiss);

            AdaptiveHybridCache(const AdaptiveHybridCache& other) = delete;
            AdaptiveHybridCache& operator=(const AdaptiveHybridCache&) = delete;

            ~AdaptiveHybridCache() {
            }

            bool find(const TKey& key, TValue& value, size_t& valLen, uint64_t& shortCut, std::function<char *(char *, size_t, uint64_t)> CopyCachedValue);

            bool insert(const TKey& key, const TValue& value, const size_t &valLen, const uint64_t& shortCut);

            bool update(const TKey& key, const TValue& value, const size_t &valLen, const uint64_t& shortCut);

            bool remove(const TKey& key);

            void clear();
            
            void debugInfo();

            void updateMissCost(uint64_t newMissCost);

            uint64_t MissCost();

            uint64_t valueCacheSize();

            uint64_t shortcutCacheSize();

            std::function<void(const TKey &key, const ListNode *value)> EraseCallBack = [&] (const TKey &key, const ListNode *value) {
                if (value->m_value != NULL)
                    free(value->m_value);
                delete value;
            };

            private:

            ValueCache v_map;
            ShortcutCache s_map;

            size_t maxCacheSizeInBytes;
            size_t valueSize;
            size_t shortcutSize;
            uint64_t missCost;
            uint64_t shortcutsPerValue;
#ifdef SHARED_CACHE
            typedef std::mutex CacheMutex;
            CacheMutex cacheMutex;
#endif
        };
    
    template <class TKey, class TValue>
        AdaptiveHybridCache<TKey, TValue>::
        AdaptiveHybridCache(size_t maxCacheSize, size_t valueEntrySize, size_t shortcutEntrySize, uint64_t costPerMiss)
        : maxCacheSizeInBytes(maxCacheSize), valueSize(valueEntrySize), shortcutSize(shortcutEntrySize),
        missCost(costPerMiss), shortcutsPerValue(ceil((double)valueSize/(double)shortcutSize)), v_map(0, EraseCallBack), s_map(0, EraseCallBack)
    {
    }

    template <class TKey, class TValue>
        bool AdaptiveHybridCache<TKey, TValue>::
        find(const TKey& key, TValue& value, size_t& valLen, uint64_t& shortCut, std::function<char *(char *, size_t, uint64_t)> CopyCachedValue) {
            ListNode *node = NULL, *evictedNode = NULL;
            char *remotelyReadValue = NULL;
            size_t currentCacheSizeInBytes = 0;
            uint64_t minHits = 0;

#ifdef SHARED_CACHE
            std::unique_lock<CacheMutex> lock(cacheMutex);
#endif

            node = v_map.Get(key);
            if (node != NULL) {
                value = node->m_value;
                valLen = node->m_valLen;
                shortCut = node->m_shortcut;
                node->m_weight = node->m_weight + 1;
                CopyCachedValue(value, valLen, shortCut);
            } else {
                node = s_map.Get(key);
                if (node != NULL) {
                    value = node->m_value;
                    valLen = node->m_valLen;
                    shortCut = node->m_shortcut;
                    node->m_weight = node->m_weight + 1;
                    remotelyReadValue = CopyCachedValue(value, valLen, shortCut);

                    currentCacheSizeInBytes = v_map.Size() * valueSize + s_map.Size() * shortcutSize;
                    if (currentCacheSizeInBytes + valueSize - shortcutSize <= maxCacheSizeInBytes) {
                        char *val = (char *) malloc(node->m_valLen);
                        memcpy(val, remotelyReadValue, node->m_valLen);
                        node->m_value = val;
                        if (node != s_map.Remove(key))
                            fprintf(stderr, "[ERROR] Removed and prior search are different #1\n");
                        v_map.Put(key, node);
                    } else if (s_map.hitCounter(key) > ((minHits = s_map.AggregatedMinHits(shortcutsPerValue - 1)) * missCost)) {
                        for (uint64_t i = 0; i < (shortcutsPerValue - 1); i++) {
                            evictedNode = s_map.Evict();
                            delete evictedNode;
                        }

                        if (node != s_map.Remove(key))
                            fprintf(stderr, "[ERROR] Removed and prior search are different #2\n");

                        char *val = (char *) malloc(node->m_valLen);
                        memcpy(val, remotelyReadValue, node->m_valLen);
                        node->m_value = val;
                        v_map.Put(key, node);
                    }
                } else {
#ifdef SHARED_CACHE
                    lock.unlock();
#endif
                    return false;
                }
            }

#ifdef SHARED_CACHE
            lock.unlock();
#endif
            return true;
        }

    template <class TKey, class TValue>
        bool AdaptiveHybridCache<TKey, TValue>::
        insert(const TKey& key, const TValue& value, const size_t &valLen, const uint64_t& shortCut) {
            ListNode *node = NULL, *evictedNode = NULL, *retValueCache = NULL, *retShortcutCache = NULL;
            size_t currentCacheSizeInBytes = 0;
            uint64_t minHits = 0;

#ifdef SHARED_CACHE
            std::unique_lock<CacheMutex> lock(cacheMutex);
#endif

            if ((retValueCache = v_map.Get(key)) || (retShortcutCache = s_map.Get(key))) {
                if (retValueCache != NULL) {
                    node = new ListNode(key, value, valLen, shortCut, retValueCache->m_weight + 1);
                    v_map.Put(key, node);
                    free(retValueCache->m_value);
                    delete retValueCache;
                } else {
                    currentCacheSizeInBytes = v_map.Size() * valueSize + s_map.Size() * shortcutSize;
                    if (currentCacheSizeInBytes + valueSize - shortcutSize <= maxCacheSizeInBytes) {
                        if (retShortcutCache != s_map.Remove(key))
                            fprintf(stderr, "[ERROR] Removed and prior search are different #3\n");
                        retShortcutCache->m_value = value;
                        retShortcutCache->m_valLen = valLen;
                        retShortcutCache->m_weight = retShortcutCache->m_weight + 1;
                        v_map.Put(key, retShortcutCache);
                    } else if (s_map.hitCounter(key) > ((minHits = s_map.AggregatedMinHits(shortcutsPerValue - 1)) * missCost)) {
                        for (uint64_t i = 0; i < (shortcutsPerValue - 1); i++) {
                            evictedNode = s_map.Evict();
                            delete evictedNode;
                        }

                        if (retShortcutCache != s_map.Remove(key))
                            fprintf(stderr, "[ERROR] Removed and prior search are different #4\n");
                        retShortcutCache->m_value = value;
                        retShortcutCache->m_valLen = valLen;
                        retShortcutCache->m_weight = retShortcutCache->m_weight + 1;
                        v_map.Put(key, retShortcutCache);
                    } else {
                        if (value != NULL) free(value);
                        node = new ListNode(key, NULL, valLen, shortCut, retShortcutCache->m_weight + 1);
                        s_map.Put(key, node);
                        delete retShortcutCache;
                    }
                }
            } else {
                currentCacheSizeInBytes = v_map.Size() * valueSize + s_map.Size() * shortcutSize;
                if (currentCacheSizeInBytes + valueSize <= maxCacheSizeInBytes) {
                    node = new ListNode(key, value, valLen, shortCut);
                    v_map.Put(key, node);
                } else if (currentCacheSizeInBytes + shortcutSize <= maxCacheSizeInBytes) {
                    free(value);
                    node = new ListNode(key, NULL, valLen, shortCut);
                    s_map.Put(key, node);
                } else if (v_map.Size() != 0) {
                    free(value);
                    node = new ListNode(key, NULL, valLen, shortCut);

                    evictedNode = v_map.Evict();
                    free(evictedNode->m_value);
                    evictedNode->m_value = NULL;
                    s_map.weightPut(evictedNode->m_key, evictedNode, evictedNode->m_weight);
                    s_map.Put(key, node);
                } else {
                    free(value);
                    node = new ListNode(key, NULL, valLen, shortCut);

                    evictedNode = s_map.Evict();
                    delete evictedNode;

                    s_map.Put(key, node);
                }
            }

#ifdef SHARED_CACHE
            lock.unlock();
#endif
            return true;
        }

    template <class TKey, class TValue>
        bool AdaptiveHybridCache<TKey, TValue>::
        update(const TKey& key, const TValue& value, const size_t &valLen, const uint64_t& shortCut) {
            return insert(key, value, valLen, shortCut);
        }

    template <class TKey, class TValue>
        bool AdaptiveHybridCache<TKey, TValue>::
        remove(const TKey& key) {
            ListNode *node = NULL;
#ifdef SHARED_CACHE
            std::unique_lock<CacheMutex> lock(cacheMutex);
#endif
            node = v_map.Remove(key);
            if (node != NULL) {
                free(node->m_value);
                delete node;
            }
            node = s_map.Remove(key);
            if (node != NULL)
                delete node;
#ifdef SHARED_CACHE
            lock.unlock();
#endif
            return true;
        }

    template <class TKey, class TValue>
        void AdaptiveHybridCache<TKey, TValue>::
        clear() {
#ifdef SHARED_CACHE
            std::unique_lock<CacheMutex> lock(cacheMutex);
#endif
            v_map.Clear();
            s_map.Clear();
        }

    template <class TKey, class TValue>
        void AdaptiveHybridCache<TKey, TValue>::
        debugInfo() {
            printf("v_map.Size() = %lu, s_map.Size() = %lu\n", v_map.Size(), s_map.Size());
            printf("Total size in bytes = %lu\n", v_map.Size()*valueSize + s_map.Size()*shortcutSize);
        }

    template <class TKey, class TValue>
        void AdaptiveHybridCache<TKey, TValue>::
        updateMissCost(uint64_t newMissCost) {
            missCost = newMissCost;
        }

    template <class TKey, class TValue>
        uint64_t AdaptiveHybridCache<TKey, TValue>::
        MissCost() {
            return missCost;
        }

    template <class TKey, class TValue>
        uint64_t AdaptiveHybridCache<TKey, TValue>::
        valueCacheSize() {
#ifdef SHARED_CACHE
            std::unique_lock<CacheMutex> lock(cacheMutex);
#endif
            return (uint64_t)(v_map.Size() * valueSize);
        }

    template <class TKey, class TValue>
        uint64_t AdaptiveHybridCache<TKey, TValue>::
        shortcutCacheSize() {
#ifdef SHARED_CACHE
            std::unique_lock<CacheMutex> lock(cacheMutex);
#endif
            return (uint64_t)(s_map.Size() * shortcutSize);
        }
}
#endif
