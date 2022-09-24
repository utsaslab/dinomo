#ifndef LFU_CACHE_POLICY_HPP
#define LFU_CACHE_POLICY_HPP

#include "cache_policy.hpp"
#include <cstddef>
#include <iostream>
#include <map>
#include <unordered_map>

namespace caches
{
template <typename Key> class LFUCachePolicy : public ICachePolicy<Key>
{
  public:
    using lfu_iterator = typename std::multimap<std::size_t, Key>::iterator;

    LFUCachePolicy() = default;
    ~LFUCachePolicy() override = default;

    void Insert(const Key &key) override
    {
        constexpr std::size_t INIT_VAL = 1;
        // all new value initialized with the frequency 1
        lfu_storage[key] = frequency_storage.emplace_hint(
            frequency_storage.cbegin(), INIT_VAL, key);
    }

    void Touch(const Key &key) override
    {
        // get the previous frequency value of a key
        auto elem_for_update = lfu_storage[key];
        auto updated_elem =
            std::make_pair(elem_for_update->first + 1, elem_for_update->second);
        // update the previous value
        frequency_storage.erase(elem_for_update);
        lfu_storage[key] = frequency_storage.emplace_hint(
            frequency_storage.cend(), std::move(updated_elem));
    }

    void weightInsert(const Key &key, const uint64_t weight) override
    {
        // all new value initialized with the frequency 1
        lfu_storage[key] = frequency_storage.emplace_hint(
            frequency_storage.cend(), weight, key);
    }

    void weightTouch(const Key &key, const uint64_t weight) override
    {
        // get the previous frequency value of a key
        auto elem_for_update = lfu_storage[key];
        auto updated_elem =
            std::make_pair(weight, elem_for_update->second);
        // update the previous value
        frequency_storage.erase(elem_for_update);
        lfu_storage[key] = frequency_storage.emplace_hint(
            frequency_storage.cend(), std::move(updated_elem));
    }

    void Erase(const Key &key) override
    {
        frequency_storage.erase(lfu_storage[key]);
        lfu_storage.erase(key);
    }

    const Key &ReplCandidate() const override
    {
        // at the beginning of the frequency_storage we have the
        // least frequency used value
        return frequency_storage.cbegin()->second;
    }

    size_t SumMinHits(uint64_t num_elements) override
    {
        uint64_t iterCounter = 0;
        size_t sumHits = 0;

        for (auto it = frequency_storage.begin(); it != frequency_storage.end(); it++) {
            sumHits += it->first;
            iterCounter++;
            if (num_elements == iterCounter)
                break;
        }

        //printf("[%s] sumHits = %lu\n", __func__, sumHits);
        return (iterCounter == num_elements) ? sumHits : UINT64_MAX;
    }

    size_t Hits(const Key &key) override
    {
        auto elem_it = lfu_storage[key];
        return elem_it->first;
    }

  private:
    std::multimap<std::size_t, Key> frequency_storage;
    std::unordered_map<Key, lfu_iterator> lfu_storage;
};
} // namespace caches

#endif // LFU_CACHE_POLICY_HPP
