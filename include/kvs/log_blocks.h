#ifndef LOG_BLOCKS_H_
#define LOG_BLOCKS_H_

#include "bloom.h"
#include <list>
#include <functional>
#include <atomic>

#include "ib.h"
#include "tbb/concurrent_queue.h"

extern tbb::concurrent_queue<uint64_t> *reserved_alloc_queue;

typedef struct bloom bloomFilter;
extern std::list<bloomFilter *> *filterList;

extern uint64_t LOG_BLOCK_HEADER_LEN;
extern uint64_t MAX_LOG_BLOCK_LEN;
extern uint64_t MAX_PREALLOC_NUM;

enum optype {OP_READ = 0, OP_SCAN = 1, OP_INSERT = 2, OP_UPDATE = 3, OP_DELETE = 4};

enum merge_state {INACTIVE = 0, ACTIVE = 1, MERGED = 2};

typedef struct log_block {
    std::atomic<uint32_t> merged_offset;
    std::atomic<uint32_t> offset;
    std::atomic<uint64_t> valid_counter;
    std::atomic<uint64_t> local_invalid_counter;
    std::atomic<uint64_t> global_invalid_counter;
    std::atomic<uint64_t> next;
    char buffer[];
} log_block;

typedef struct oplogs {
    uint16_t op_type;
    uint16_t key_len;
    uint32_t val_len;
    uint32_t offset;
    uint32_t commit;
    uint64_t key;
    char value[];
} oplogs;

#endif /* LOG_BLOCKS_H_ */
