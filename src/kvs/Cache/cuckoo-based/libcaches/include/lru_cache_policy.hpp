#ifndef LRU_CACHE_POLICY_HPP
#define LRU_CACHE_POLICY_HPP

#include "cache_policy.hpp"
#include <list>
#include <unordered_map>

namespace caches
{
template <typename Key> class LRUCachePolicy : public ICachePolicy<Key>
{
  public:
    using lru_iterator = typename std::list<Key>::iterator;

    LRUCachePolicy() = default;
    ~LRUCachePolicy() = default;

    void Insert(const Key &key) override
    {
        lru_queue.emplace_front(key);
        key_finder[key] = lru_queue.begin();
    }

    void Touch(const Key &key) override
    {
        // move the touched element at the beginning of the lru_queue
        lru_queue.splice(lru_queue.begin(), lru_queue, key_finder[key]);
    }

    void weightInsert(const Key &key, const uint64_t weight) override
    {

    }
    
    void weightTouch(const Key &key, const uint64_t weight) override
    {

    }

    void Erase(const Key &key) override
    {
        // remove the least recently used element
        //key_finder.erase(lru_queue.back());
        //lru_queue.pop_back();
        lru_queue.erase(key_finder[key]);
        key_finder.erase(key);
    }

    // return a key of a displacement candidate
    const Key &ReplCandidate() const override
    {
        return lru_queue.back();
    }

    size_t SumMinHits(uint64_t num_elements) override
    {
    }

    size_t Hits(const Key &key) override
    {
    }

  private:
    std::list<Key> lru_queue;
    std::unordered_map<Key, lru_iterator> key_finder;
};
} // namespace caches

#endif // LRU_CACHE_POLICY_HPP
