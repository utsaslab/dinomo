#include <iostream>
#include <chrono>
#include <random>
#include "tbb/tbb.h"
#if 1
#include "scalable-cache.h"
#else
#include <libcuckoo/cuckoohash_map.hh>
#endif

using namespace std;

void run(char **argv) {
    std::cout << "Simple Example of P-Masstree" << std::endl;

    uint64_t n = std::atoll(argv[1]);
#if 0
    uint64_t *keys = new uint64_t[n];
    uint64_t *vals = new uint64_t[n];
#else
    uint64_t *vals = new uint64_t[n];
    std::vector<string> keys;
    keys.reserve(n);
#endif

    // Generate keys
//    for (uint64_t i = 0; i < n; i++) {
//        keys[i] = i + 1;
//    }

    std::default_random_engine generator;
    std::uniform_int_distribution<uint64_t> distribution(0, UINT64_MAX);
    for (uint64_t i = 0; i < n; i++) {
#if 0
        keys[i] = distribution(generator);
        vals[i] = keys[i];
#else
        vals[i] = distribution(generator);
        keys.push_back(std::to_string(vals[i]));
#endif
    }

    int num_thread = atoi(argv[2]);
    tbb::task_scheduler_init init(num_thread);

    printf("operation,n,ops/s\n");
#if 1
    tstarling::ThreadSafeScalableCache<string, uint64_t> *cache = 
        new tstarling::ThreadSafeScalableCache<string, uint64_t> (n, num_thread);
#else
    libcuckoo::cuckoohash_map<string, uint64_t> *cache = new libcuckoo::cuckoohash_map<string, uint64_t> (64000000);
#endif

    {
        // Build tree
        auto starttime = std::chrono::system_clock::now();
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            for (uint64_t i = range.begin(); i != range.end(); i++) {
#if 1
                cache->insert(keys[i], vals[i], sizeof(uint64_t), 0);
#else
                cache->insert(keys[i], vals[i]);
#endif
            }
        });
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: insert,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: insert,%ld,%f sec\n", n, duration.count() / 1000000.0);
    }

    {
        // Lookup
        std::atomic <uint64_t> success{0}, failure{0};
        auto starttime = std::chrono::system_clock::now();
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            uint64_t valRet, valSize, verSion;
            for (uint64_t i = range.begin(); i != range.end(); i++) {
#if 1
                cache->find(keys[i], valRet, valSize, verSion);
//                if (cache->find(keys[i], valRet, valSize))
//                    success++;
//                else
//                    failure++;
#else
                cache->find(keys[i], valRet);
#endif
            }
        });
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: lookup,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: lookup,%ld,%f sec\n", n, duration.count() / 1000000.0);
        printf("Success = %lu, Failure = %lu\n", success.load(), failure.load());
    }

    {
        // Remove
        auto starttime = std::chrono::system_clock::now();
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            for (uint64_t i = range.begin(); i != range.end(); i++) {
#if 1
                cache->remove(keys[i]);
//                if (cache->find(keys[i], valRet, valSize))
//                    success++;
//                else
//                    failure++;
#else
                cache->find(keys[i], valRet);
#endif
            }
        });
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: remove,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: remove,%ld,%f sec\n", n, duration.count() / 1000000.0);
    }

    keys.clear();
//    delete[] keys;
    delete[] vals;
}

int main(int argc, char **argv) {
    if (argc != 3) {
        printf("usage: %s [n] [nthreads]\nn: number of keys (integer)\nnthreads: number of threads (integer)\n", argv[0]);
        return 1;
    }

    run(argv);
    return 0;
}