#ifndef HYBRID_CACHE_H
#define HYBRID_CACHE_H
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
        class HybridCache {
            struct ListNode {
                ListNode(const TKey& key, const TValue& value, const size_t& valueLen, const uint64_t& shortCut)
                    : m_key(key), m_value(value), m_valLen(valueLen), m_shortcut(shortCut)
                {
                }

                TKey m_key;
                TValue m_value;
                size_t m_valLen;
                uint64_t m_shortcut;
            };

            typedef caches::fixed_sized_cache<TKey, ListNode *, caches::LRUCachePolicy<TKey>> ValueCache;
            typedef caches::fixed_sized_cache<TKey, ListNode *, caches::LRUCachePolicy<TKey>> ShortcutCache;

            public:

            explicit HybridCache(size_t maxCacheSize, size_t valueEntrySize, size_t shortcutEntrySize, double valueCacheRatio);

            HybridCache(const HybridCache& other) = delete;
            HybridCache& operator=(const HybridCache&) = delete;

            ~HybridCache() {
            }

            bool find(const TKey& key, TValue& value, size_t& valLen, uint64_t& shortCut, std::function<char *(char *, size_t, uint64_t)> CopyCachedValue);

            bool insert(const TKey& key, const TValue& value, const size_t &valLen, const uint64_t& shortCut);

            bool update(const TKey& key, const TValue& value, const size_t &valLen, const uint64_t& shortCut);

            bool remove(const TKey& key);

            void clear();

            bool debugInfo(const TKey& key);

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
            size_t maxValueCacheSizeInNums;
            size_t maxShortcutCacheSizeInNums;
#ifdef SHARED_CACHE
            typedef std::mutex CacheMutex;
            CacheMutex cacheMutex;
#endif
        };

    template <class TKey, class TValue>
        HybridCache<TKey, TValue>::
        HybridCache(size_t maxCacheSize, size_t valueEntrySize, size_t shortcutEntrySize, double valueCacheRatio)
        : maxCacheSizeInBytes(maxCacheSize), valueSize(valueEntrySize), shortcutSize(shortcutEntrySize), v_map(0, EraseCallBack), s_map(0, EraseCallBack),
        maxValueCacheSizeInNums(ceil((double)((double)(maxCacheSize) * valueCacheRatio)/(double)valueEntrySize)),
        maxShortcutCacheSizeInNums(ceil((double)((double)(maxCacheSize) * (1.0 - valueCacheRatio))/(double)shortcutEntrySize))
    {

    }

    template <class TKey, class TValue>
        bool HybridCache<TKey, TValue>::
        find(const TKey& key, TValue& value, size_t& valLen, uint64_t& shortCut, std::function<char *(char *, size_t, uint64_t)> CopyCachedValue) {
            ListNode *node = NULL, *evictedNode = NULL;
            char *remotelyReadValue = NULL;

#ifdef SHARED_CACHE
            std::unique_lock<CacheMutex> lock(cacheMutex);
#endif

            node = v_map.Get(key);
            if (node != NULL) {
                value = node->m_value;
                valLen = node->m_valLen;
                shortCut = node->m_shortcut;
                CopyCachedValue(value, valLen, shortCut);
            } else {
                node = s_map.Get(key);
                if (node != NULL) {
                    value = node->m_value;
                    valLen = node->m_valLen;
                    shortCut = node->m_shortcut;
                    remotelyReadValue = CopyCachedValue(value, valLen, shortCut);

                    if (maxValueCacheSizeInNums != 0) {
                        char *val = (char *) malloc(node->m_valLen);
                        memcpy(val, remotelyReadValue, node->m_valLen);
                        node->m_value = val;

                        if (node != s_map.Remove(key))
                            fprintf(stderr, "[ERROR, %s] Removed and prior search are different\n", __func__);

                        if (v_map.Size() + 1 > maxValueCacheSizeInNums) {
                            evictedNode = v_map.Evict();
                            free(evictedNode->m_value);
                            evictedNode->m_value = NULL;
                            s_map.Put(evictedNode->m_key, evictedNode);
                        }

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
        bool HybridCache<TKey, TValue>::
        insert(const TKey& key, const TValue& value, const size_t &valLen, const uint64_t& shortCut) {
            ListNode *node = NULL, *evictedNode = NULL, *retValueCache = NULL, *retShortcutCache = NULL;

#ifdef SHARED_CACHE
            std::unique_lock<CacheMutex> lock(cacheMutex);
#endif

            if ((retValueCache = v_map.Get(key)) || (retShortcutCache = s_map.Get(key))) {
                if (retValueCache != NULL) {
                    node = new ListNode(key, value, valLen, shortCut);
                    v_map.Put(key, node);
                    free(retValueCache->m_value);
                    delete retValueCache;
                } else {
                    if (maxValueCacheSizeInNums == 0) {
                        if (value != NULL) free(value);
                        node = new ListNode(key, NULL, valLen, shortCut);
                        s_map.Put(key, node);
                        delete retShortcutCache;
                    } else {
                        if (retShortcutCache != s_map.Remove(key))
                            fprintf(stderr, "[ERROR, %s] Removed and prior search are different\n", __func__);
                        retShortcutCache->m_value = value;
                        retShortcutCache->m_valLen = valLen;
                        retShortcutCache->m_shortcut = shortCut;

                        if (v_map.Size() + 1 <= maxValueCacheSizeInNums) {
                            v_map.Put(key, retShortcutCache);
                        } else {
                            evictedNode = v_map.Evict();
                            free(evictedNode->m_value);
                            evictedNode->m_value = NULL;

                            v_map.Put(key, retShortcutCache);
                            s_map.Put(evictedNode->m_key, evictedNode);
                        }
                    }
                }
            } else {
                if (maxValueCacheSizeInNums != 0 && maxShortcutCacheSizeInNums != 0) {
                    node = new ListNode(key, value, valLen, shortCut);
                    if (v_map.Size() + 1 <= maxValueCacheSizeInNums) {
                        v_map.Put(key, node);
                    } else {
                        evictedNode = v_map.Evict();
                        free(evictedNode->m_value);
                        evictedNode->m_value = NULL;

                        v_map.Put(key, node);
                        if (s_map.Size() + 1 > maxShortcutCacheSizeInNums)
                            delete s_map.Evict();
                        s_map.Put(evictedNode->m_key, evictedNode);
                    }
                } else {
                    if (maxValueCacheSizeInNums != 0) {
                        node = new ListNode(key, value, valLen, shortCut);
                        if (v_map.Size() + 1 <= maxValueCacheSizeInNums) {
                            v_map.Put(key, node);
                        } else {
                            evictedNode = v_map.Evict();
                            v_map.Put(key, node);
                            free(evictedNode->m_value);
                            delete evictedNode;
                        }
                    } else {
                        if (value != NULL)
                            free(value);

                        node = new ListNode(key, NULL, valLen, shortCut);
                        if (s_map.Size() + 1 <= maxShortcutCacheSizeInNums) {
                            s_map.Put(key, node);
                        } else {
                            evictedNode = s_map.Evict();
                            s_map.Put(key, node);
                            delete evictedNode;
                        }
                    }
                }
            }

#ifdef SHARED_CACHE
            lock.unlock();
#endif
            return true;
        }

    template <class TKey, class TValue>
        bool HybridCache<TKey, TValue>::
        update(const TKey& key, const TValue& value, const size_t &valLen, const uint64_t& shortCut) {
            return insert(key, value, valLen, shortCut);
        }

    template <class TKey, class TValue>
        bool HybridCache<TKey, TValue>::
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
        void HybridCache<TKey, TValue>::
        clear() {
#ifdef SHARED_CACHE
            std::unique_lock<CacheMutex> lock(cacheMutex);
#endif
            v_map.Clear();
            s_map.Clear();
        }

    template <class TKey, class TValue>
        bool HybridCache<TKey, TValue>::
        debugInfo(const TKey& key) {
            printf("v_map.Size() = %lu, s_map.Size() = %lu\n", v_map.Size(), s_map.Size());
            printf("Total size in bytes = %lu\n", v_map.Size()*valueSize + s_map.Size()*shortcutSize);
            return false;
        }

    template <class TKey, class TValue>
        void HybridCache<TKey, TValue>::
        updateMissCost(uint64_t newMissCost) {
            return ;
        }

    template <class TKey, class TValue>
        uint64_t HybridCache<TKey, TValue>::
        MissCost() {
            return 0;
        }

    template <class TKey, class TValue>
        uint64_t HybridCache<TKey, TValue>::
        valueCacheSize() {
#ifdef SHARED_CACHE
            std::unique_lock<CacheMutex> lock(cacheMutex);
#endif
            return (uint64_t)(v_map.Size() * valueSize);
        }

    template <class TKey, class TValue>
        uint64_t HybridCache<TKey, TValue>::
        shortcutCacheSize() {
#ifdef SHARED_CACHE
            std::unique_lock<CacheMutex> lock(cacheMutex);
#endif
            return (uint64_t)(s_map.Size() * shortcutSize);
        }
}
#endif
