#ifndef SETUP_IB_H_
#define SETUP_IB_H_

#include <infiniband/verbs.h>

struct IBRes {
    struct ibv_context      *ctx;
    struct ibv_pd           *pd;
    struct ibv_mr           *mr_pool;
    struct ibv_mr           *mr_buf;
    struct ibv_cq           **cq;
    struct ibv_cq           **send_cq;
    struct ibv_qp           **qp;
    struct ibv_srq          *srq;
    struct ibv_port_attr    port_attr;
    struct ibv_device_attr  dev_attr;

    int num_qps;

#ifndef SHARED_NOTHING
    char *ib_pool;
#else
    union {
        char *ib_pool;
        char **ib_partitioned_pool;
    };
#endif
    size_t ib_pool_size;

    char *ib_buf;
    size_t ib_buf_size;

    uint32_t *rkey_pool;
    uint64_t *raddr_pool;

    uint32_t *rkey_buf;
    uint64_t *raddr_buf;
};

extern struct IBRes ib_res;

void ib_init(int total_available_memory_nodes, int num_initial_memory_nodes, 
        int num_initial_storage_nodes, int threads_per_memory, int threads_per_storage, 
        int num_storage_managers, int msg_size, char **storage_node_ips, 
        char *sock_port, int rank, bool is_server, uint64_t storage_capacities);

int setup_ib();

void close_ib_connection();

int connect_qp_server();
int connect_qp_client();

#endif //setup_ib.h
