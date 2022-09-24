#ifndef EMPTY_CACHE_H
#define EMPTY_CACHE_H
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
        class EmptyCache {
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

            explicit EmptyCache(size_t maxCacheSize, size_t valueEntrySize, size_t shortcutEntrySize, double valueCacheRatio);

            EmptyCache(const EmptyCache& other) = delete;
            EmptyCache& operator=(const EmptyCache&) = delete;

            ~EmptyCache() {
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
        };

    template <class TKey, class TValue>
        EmptyCache<TKey, TValue>::
        EmptyCache(size_t maxCacheSize, size_t valueEntrySize, size_t shortcutEntrySize, double valueCacheRatio)
        : maxCacheSizeInBytes(maxCacheSize), valueSize(valueEntrySize), shortcutSize(shortcutEntrySize), v_map(0, EraseCallBack), s_map(0, EraseCallBack),
        maxValueCacheSizeInNums(ceil((double)((double)(maxCacheSize) * valueCacheRatio)/(double)valueEntrySize)),
        maxShortcutCacheSizeInNums(ceil((double)((double)(maxCacheSize) * (1.0 - valueCacheRatio))/(double)shortcutEntrySize))
    {

    }

    template <class TKey, class TValue>
        bool EmptyCache<TKey, TValue>::
        find(const TKey& key, TValue& value, size_t& valLen, uint64_t& shortCut, std::function<char *(char *, size_t, uint64_t)> CopyCachedValue) {
            return false;
        }

    template <class TKey, class TValue>
        bool EmptyCache<TKey, TValue>::
        insert(const TKey& key, const TValue& value, const size_t &valLen, const uint64_t& shortCut) {
            return true;
        }

    template <class TKey, class TValue>
        bool EmptyCache<TKey, TValue>::
        update(const TKey& key, const TValue& value, const size_t &valLen, const uint64_t& shortCut) {
            return insert(key, value, valLen, shortCut);
        }

    template <class TKey, class TValue>
        bool EmptyCache<TKey, TValue>::
        remove(const TKey& key) {
            return true;
        }

    template <class TKey, class TValue>
        void EmptyCache<TKey, TValue>::
        clear() {
            v_map.Clear();
            s_map.Clear();
        }

    template <class TKey, class TValue>
        bool EmptyCache<TKey, TValue>::
        debugInfo(const TKey& key) {
            printf("v_map.Size() = %lu, s_map.Size() = %lu\n", v_map.Size(), s_map.Size());
            printf("Total size in bytes = %lu\n", v_map.Size()*valueSize + s_map.Size()*shortcutSize);
            return false;
        }

    template <class TKey, class TValue>
        void EmptyCache<TKey, TValue>::
        updateMissCost(uint64_t newMissCost) {
            return ;
        }

    template <class TKey, class TValue>
        uint64_t EmptyCache<TKey, TValue>::
        MissCost() {
            return 0;
        }

    template <class TKey, class TValue>
        uint64_t EmptyCache<TKey, TValue>::
        valueCacheSize() {
            return (uint64_t)(v_map.Size() * valueSize);
        }

    template <class TKey, class TValue>
        uint64_t EmptyCache<TKey, TValue>::
        shortcutCacheSize() {
            return (uint64_t)(s_map.Size() * shortcutSize);
        }
}
#endif
