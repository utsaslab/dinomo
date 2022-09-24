/*
 * code from : http://c.learncodethehardway.org/book/ex20.html
 */

#ifndef DEBUG_H_
#define DEBUG_H_

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <atomic>
#include <chrono>

#define LOG_HEADER     "\n================ %s ================\n"
#define LOG_SUB_HEADER "\n************ %s ************\n"

extern FILE *log_fp;

#ifdef DH_DEBUG
extern std::atomic<uint64_t> RDMA_READ_COUNTER;
extern std::atomic<uint64_t> RDMA_WRITE_COUNTER;
extern std::atomic<uint64_t> RDMA_SEND_COUNTER;
extern std::atomic<uint64_t> RDMA_CAS_COUNTER;

extern std::atomic<uint64_t> RDMA_READ_PAYLOAD;
extern std::atomic<uint64_t> RDMA_WRITE_PAYLOAD;
extern std::atomic<uint64_t> RDMA_SEND_PAYLOAD;
extern std::atomic<uint64_t> RDMA_CAS_PAYLOAD;
#endif

#define clean_errno() (errno == 0 ? "None" : strerror(errno))

#define log_err(M, ...) fprintf(stderr, "[ERROR] (%s:%d:%s: errno: %s) " M "\n",\
        __FILE__, __LINE__, __func__, clean_errno(), ##__VA_ARGS__)

#define log_warn(M, ...) fprintf(stderr, "[WARN] (%s:%d:%s errno: %s) " M "\n",\
        __FILE__, __LINE__, __func__, clean_errno(), ##__VA_ARGS__)

#define log_info(M, ...) fprintf(stderr, "" M "\n", ##__VA_ARGS__)

#define log_file(M, ...) {fprintf(log_fp, "" M "\n", ##__VA_ARGS__);fflush(log_fp);}

#define sentinel(M, ...) {log_err(M, ##__VA_ARGS__); errno=0; goto error;}

#define check(A, M, ...) if(!(A)) {log_err(M, ##__VA_ARGS__); errno=0; goto error;}

#ifdef DEBUG
#define debug_detail(M, ...) fprintf(stderr, "[DEBUG] (%s:%d:%s) " M "\n",\
        __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define debug(M, ...) fprintf(stderr, "[DEBUG] " M "\n", ##__VA_ARGS__)
#define LOG(M, ...) {log_info (M, ##__VA_ARGS__); log_file (M, ##__VA_ARGS__);}
#else
//#define debug(M, ...)
#define LOG(M, ...) {log_info (M, ##__VA_ARGS__); log_file (M, ##__VA_ARGS__);}
#endif

#endif /* DEBUG_H_ */
