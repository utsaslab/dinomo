#ifndef MASSTREE_H_
#define MASSTREE_H_

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <math.h>
#include <iostream>
#include <mutex>
#include <atomic>
#include <assert.h>
#include <emmintrin.h>

#include <libpmemobj.h>
#include "Epoche.h"

#include "log_blocks.h"

namespace masstree {

#define LEAF_WIDTH          15
#define LEAF_THRESHOLD      1
#define LEAF_HEADER_SIZE    64

#define INITIAL_VALUE       0x0123456789ABCDE0ULL
#define FULL_VALUE          0xEDCBA98765432100ULL

#define LV_BITS             (1ULL << 0)
#define IS_LV(x)            ((uintptr_t)x & LV_BITS)
#define LV_PTR(x)           (leafvalue*)((void*)((uintptr_t)x & ~LV_BITS))
#define SET_LV(x)           ((void*)((uintptr_t)x | LV_BITS))

enum state {UNLOCKED = 0, LOCKED = 1, OBSOLETE = 2};

class kv {
    private:
        uint64_t key;
        uint64_t value;
    public:
        kv() {
            key = UINT64_MAX;
            value = 0;
        }

        friend class leafnode;
};

typedef struct leafvalue {
    uint64_t value;
    uint32_t val_len;
    uint32_t key_len;
    uint64_t fkey[];
} leafvalue;

typedef struct key_indexed_position {
    int i;
    int p;
    inline key_indexed_position() {
    }
    inline constexpr key_indexed_position(int i_, int p_)
        : i(i_), p(p_) {
    }
} key_indexed_position;

class masstree {
    private:
        uint64_t root_;

        uint64_t remote_start_addr;
    public:
        masstree();

        masstree (void *new_root);

        ~masstree() {
        }

        void *operator new(size_t size);

        void operator delete(void *addr);

        uint64_t root() {return root_;}

        uint64_t *root_dp() {return &root_;}

        uint64_t raddr_from_off(uint64_t off);

        void setNewRoot(void *new_root);

        void put(uint64_t key, void *value);// MASS::ThreadInfo &threadEpocheInfo);

        void put(char *key, size_t key_len, uint64_t value, uint32_t val_len, log_block *curr_log_block);// MASS::ThreadInfo &threadEpocheInfo);

        void del(uint64_t key, MASS::ThreadInfo &threadEpocheInfo);

        void del(char *key, MASS::ThreadInfo &threadEpocheInfo);

        std::tuple<bool, void *, uint64_t> get(uint64_t key, uint32_t lkey, struct ibv_qp *qp, char *buf_ptr,
                uint32_t rkey, struct ibv_cq *cq, uint64_t root_raddr, int rank); //ThreadInfo &threadEpocheInfo

        std::tuple<bool, void *, uint64_t, uint32_t> get(char *key, size_t key_len, uint32_t lkey, struct ibv_qp *qp, 
                char *buf_ptr, uint32_t rkey, struct ibv_cq *cq, uint64_t root_raddr, int rank); //ThreadInfo &threadEpocheInfo

        void split(void *left, void *root, uint32_t depth, leafvalue *lv, uint64_t key, void *right, uint32_t level, void *child, bool isOverWrite);

        int merge(void *left, void *root, uint32_t depth, leafvalue *lv, uint64_t key, uint32_t level, MASS::ThreadInfo &threadInfo);

        leafvalue *make_leaf(char *key, size_t key_len, uint64_t value, uint32_t val_len);

        int scan(uint64_t min, int num, uint64_t *buf, uint64_t lkey, struct ibv_qp *qp, char *buf_ptr, 
                uint32_t rkey, struct ibv_cq *cq, uint64_t root_raddr, int rank); //ThreadInfo &threadEpocheInfo

        int scan(char *min, size_t key_len, int num, std::pair<uint64_t, uint32_t> *buf, uint32_t lkey, struct ibv_qp *qp, char *buf_ptr, 
                uint32_t rkey, struct ibv_cq *cq, uint64_t root_raddr, int rank); //ThreadInfo &threadEpocheInfo
};

class permuter {
    public:
        permuter() {
            x_ = 0ULL;
        }

        permuter(uint64_t x) : x_(x) {
        }

        /** @brief Return an empty permuter with size 0.

          Elements will be allocated in order 0, 1, ..., @a width - 1. */
        static inline uint64_t make_empty() {
            uint64_t p = (uint64_t) INITIAL_VALUE;
            return p & ~(uint64_t) (LEAF_WIDTH);
        }
        /** @brief Return a permuter with size @a n.

          The returned permutation has size() @a n. For 0 <= i < @a n,
          (*this)[i] == i. Elements n through @a width - 1 are free, and will be
          allocated in that order. */
        static inline uint64_t make_sorted(int n) {
            uint64_t mask = (n == LEAF_WIDTH ? (uint64_t) 0 : (uint64_t) 16 << (n << 2)) - 1;
            return (make_empty() << (n << 2))
                | ((uint64_t) FULL_VALUE & mask)
                | n;
        }

        /** @brief Return the permuter's size. */
        int size() const {
            return x_ & LEAF_WIDTH;
        }

        /** @brief Return the permuter's element @a i.
          @pre 0 <= i < width */
        int operator[](int i) const {
            return (x_ >> ((i << 2) + 4)) & LEAF_WIDTH;
        }

        int back() const {
            return (*this)[LEAF_WIDTH - 1];
        }

        uint64_t value() const {
            return x_;
        }

        uint64_t value_from(int i) const {
            return x_ >> ((i + 1) << 2);
        }

        void set_size(int n) {
            x_ = (x_ & ~(uint64_t)LEAF_WIDTH) | n;
        }

        /** @brief Allocate a new element and insert it at position @a i.
          @pre 0 <= @a i < @a width
          @pre size() < @a width
          @return The newly allocated element.

          Consider the following code:
          <code>
          kpermuter<...> p = ..., q = p;
          int x = q.insert_from_back(i);
          </code>

          The modified permuter, q, has the following properties.
          <ul>
          <li>q.size() == p.size() + 1</li>
          <li>Given j with 0 <= j < i, q[j] == p[j] && q[j] != x</li>
          <li>Given j with j == i, q[j] == x</li>
          <li>Given j with i < j < q.size(), q[j] == p[j-1] && q[j] != x</li>
          </ul> */
        int insert_from_back(int i) {
            int value = back();
            // increment size, leave lower slots unchanged
            x_ = ((x_ + 1) & (((uint64_t) 16 << (i << 2)) - 1))
                // insert slot
                | ((uint64_t) value << ((i << 2) + 4))
                // shift up unchanged higher entries & empty slots
                | ((x_ << 4) & ~(((uint64_t) 256 << (i << 2)) - 1));
            return value;
        }

        /** @brief Insert an unallocated element from position @a si at position @a di.
          @pre 0 <= @a di < @a width
          @pre size() < @a width
          @pre size() <= @a si
          @return The newly allocated element. */
        void insert_selected(int di, int si) {
            int value = (*this)[si];
            uint64_t mask = ((uint64_t) 256 << (si << 2)) - 1;
            // increment size, leave lower slots unchanged
            x_ = ((x_ + 1) & (((uint64_t) 16 << (di << 2)) - 1))
                // insert slot
                | ((uint64_t) value << ((di << 2) + 4))
                // shift up unchanged higher entries & empty slots
                | ((x_ << 4) & mask & ~(((uint64_t) 256 << (di << 2)) - 1))
                // leave uppermost slots alone
                | (x_ & ~mask);
        }
        /** @brief Remove the element at position @a i.
          @pre 0 <= @a i < @a size()
          @pre size() < @a width

          Consider the following code:
          <code>
          kpermuter<...> p = ..., q = p;
          q.remove(i);
          </code>

          The modified permuter, q, has the following properties.
          <ul>
          <li>q.size() == p.size() - 1</li>
          <li>Given j with 0 <= j < i, q[j] == p[j]</li>
          <li>Given j with i <= j < q.size(), q[j] == p[j+1]</li>
          <li>q[q.size()] == p[i]</li>
          </ul> */
        void remove(int i) {
            if (int(x_ & 15) == i + 1)
                --x_;
            else {
                int rot_amount = ((x_ & 15) - i - 1) << 2;
                uint64_t rot_mask =
                    (((uint64_t) 16 << rot_amount) - 1) << ((i + 1) << 2);
                // decrement size, leave lower slots unchanged
                x_ = ((x_ - 1) & ~rot_mask)
                    // shift higher entries down
                    | (((x_ & rot_mask) >> 4) & rot_mask)
                    // shift value up
                    | (((x_ & rot_mask) << rot_amount) & rot_mask);
            }
        }
        /** @brief Remove the element at position @a i to the back.
          @pre 0 <= @a i < @a size()
          @pre size() < @a width

          Consider the following code:
          <code>
          kpermuter<...> p = ..., q = p;
          q.remove_to_back(i);
          </code>

          The modified permuter, q, has the following properties.
          <ul>
          <li>q.size() == p.size() - 1</li>
          <li>Given j with 0 <= j < i, q[j] == p[j]</li>
          <li>Given j with i <= j < @a width - 1, q[j] == p[j+1]</li>
          <li>q.back() == p[i]</li>
          </ul> */
        void remove_to_back(int i) {
            uint64_t mask = ~(((uint64_t) 16 << (i << 2)) - 1);
            // clear unused slots
            uint64_t x = x_ & (((uint64_t) 16 << (LEAF_WIDTH << 2)) - 1);
            // decrement size, leave lower slots unchanged
            x_ = ((x - 1) & ~mask)
                // shift higher entries down
                | ((x >> 4) & mask)
                // shift removed element up
                | ((x & mask) << ((LEAF_WIDTH - i - 1) << 2));
        }
        /** @brief Rotate the permuter's elements between @a i and size().
          @pre 0 <= @a i <= @a j <= size()

          Consider the following code:
          <code>
          kpermuter<...> p = ..., q = p;
          q.rotate(i, j);
          </code>

          The modified permuter, q, has the following properties.
          <ul>
          <li>q.size() == p.size()</li>
          <li>Given k with 0 <= k < i, q[k] == p[k]</li>
          <li>Given k with i <= k < q.size(), q[k] == p[i + (k - i + j - i) mod (size() - i)]</li>
          </ul> */
        void rotate(int i, int j) {
            uint64_t mask = (i == LEAF_WIDTH ? (uint64_t) 0 : (uint64_t) 16 << (i << 2)) - 1;
            // clear unused slots
            uint64_t x = x_ & (((uint64_t) 16 << (LEAF_WIDTH << 2)) - 1);
            x_ = (x & mask)
                | ((x >> ((j - i) << 2)) & ~mask)
                | ((x & ~mask) << ((LEAF_WIDTH - j) << 2));
        }
        /** @brief Exchange the elements at positions @a i and @a j. */
        void exchange(int i, int j) {
            uint64_t diff = ((x_ >> (i << 2)) ^ (x_ >> (j << 2))) & 240;
            x_ ^= (diff << (i << 2)) | (diff << (j << 2));
        }
        /** @brief Exchange positions of values @a x and @a y. */
        void exchange_values(int x, int y) {
            uint64_t diff = 0, p = x_;
            for (int i = 0; i < LEAF_WIDTH; ++i, diff <<= 4, p <<= 4) {
                int v = (p >> (LEAF_WIDTH << 2)) & 15;
                diff ^= -((v == x) | (v == y)) & (x ^ y);
            }
            x_ ^= diff;
        }

        bool operator==(const permuter& x) const {
            return x_ == x.x_;
        }
        bool operator!=(const permuter& x) const {
            return !(*this == x);
        }

        int operator&(uint64_t mask) {
            return x_ & mask;
        }

        void operator>>=(uint64_t mask) {
            x_ = (x_ >> mask);
        }

        static inline int size(uint64_t p) {
            return p & 15;
        }

    private:
        uint64_t x_;
};

class leafnode {
    private:
        permuter permutation;                                   // 8bytes
        uint64_t next;                                          // 8bytes
        std::atomic<uint64_t> typeVersionLockObsolete{0b100};   // 8bytes
        uint64_t leftmost_ptr;                                  // 8bytes
        uint64_t highest;                                       // 8bytes
        uint32_t level_;                                        // 4bytes
        uint32_t dummy[5];                                      // 20bytes
        kv entry[LEAF_WIDTH];                                   // 240bytes

    public:
        leafnode(uint32_t level);

        leafnode(void *left, uint64_t key, void *right, uint32_t level);

        ~leafnode () {}

        void *operator new(size_t size);

        void operator delete(void *addr);

        permuter permute();

        key_indexed_position key_lower_bound_by(uint64_t key);

        key_indexed_position key_lower_bound(uint64_t key);

        bool isLocked(uint64_t version) const;

        void writeLock();

        void writeLockOrRestart(int &needRestart);

        bool tryLock(int &needRestart);

        void upgradeToWriteLockOrRestart(uint64_t &version, int &needRestart);

        void writeUnlock(bool isOverWrite);

        uint64_t readLockOrRestart(int &needRestart) const;

        void checkOrRestart(uint64_t startRead, int &needRestart) const;

        void readUnlockOrRestart(uint64_t startRead, int &needRestart) const;

        static bool isObsolete(uint64_t version);

        void writeUnlockObsolete() {
            typeVersionLockObsolete.fetch_add(0b11);
        }

        int compare_key(const uint64_t a, const uint64_t b);

        leafnode *advance_to_key(const uint64_t& key);

        leafnode *advance_to_key(masstree *t, const uint64_t& key, uint64_t &raddr, int &buf_idx,
                uint32_t lkey, struct ibv_qp *qp, leafnode *recv_buf_ptr, uint32_t rkey, struct ibv_cq *cq);

        void assign(int p, const uint64_t& key, void *value);

        void assign_value(int p, void *value);

        inline void assign_initialize(int p, const uint64_t& key, void *value);

        inline void assign_initialize(int p, leafnode *x, int xp);

        inline void assign_initialize_for_layer(int p, const uint64_t& key);

        int split_into(leafnode *nr, int p, const uint64_t& key, void *value, uint64_t& split_key);

        void split_into_inter(leafnode *nr, uint64_t& split_key);

        void *leaf_insert(masstree *t, void *root, uint32_t depth, leafvalue *lv, uint64_t key, void *value, key_indexed_position &kx_);
 
        void *leaf_delete(masstree *t, void *root, uint32_t depth, leafvalue *lv, key_indexed_position &kx_, MASS::ThreadInfo &threadInfo);

        void *inter_insert(masstree *t, void *root, uint32_t depth, leafvalue *lv, uint64_t key, void *value, key_indexed_position &kx_, leafnode *child, bool child_isOverWrite);

        int inter_delete(masstree *t, void *root, uint32_t depth, leafvalue *lv, key_indexed_position &kx_, MASS::ThreadInfo &threadInfo);

        void prefetch() const;

        uint32_t level() {return level_;}

        uint64_t key(int i) {return entry[i].key;}

        uint64_t value(int i) {return entry[i].value;}

        uint64_t leftmost() {return leftmost_ptr;}

        uint64_t next_() {return next;}

        uint64_t highest_() {return highest;}

        void make_new_layer(leafnode *p, key_indexed_position &kx_, leafvalue *olv, leafvalue *nlv, uint32_t depth);

        leafnode *correct_layer_root(void *root, leafvalue *lv, uint32_t depth, key_indexed_position &pkx_);

        void *entry_addr(int p);

        void check_for_recovery(masstree *t, leafnode *left, leafnode *right, void *root, uint32_t depth, leafvalue *lv);

        void get_range(masstree *t, leafvalue * &lv, int num, int &count, std::pair<uint64_t, uint32_t> *buf, 
                leafnode *root, uint32_t depth, uint32_t lkey, struct ibv_qp *qp, char *buf_ptr,
                uint32_t rkey, struct ibv_cq *cq);

        leafvalue *smallest_leaf(size_t key_len, uint64_t value);

        leafnode *search_for_leftsibling(uint64_t *root, uint64_t key, uint32_t level, leafnode *right);
};

}
#endif
