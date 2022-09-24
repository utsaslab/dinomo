#include <arpa/inet.h>
#include <unistd.h>
#include <malloc.h>

#include "kvs/sock.h"
#include "kvs/ib.h"
#include "kvs/debug.h"
#include "kvs/ib_config.h"
#include "kvs/setup_ib.h"

struct IBRes ib_res;

#ifndef SHARED_NOTHING
clht_t *farIdx;
#else
clht_t **farIdx;
#endif

int connect_qp_server()
{
    int ret = 0, n = 0, i = 0, j = 0;
    int num_peers = config_info.num_initial_memory_nodes;
    int num_concurr_msgs = config_info.threads_per_memory;
    int sockfd = 0;
    int *peer_sockfd = NULL;
    struct sockaddr_in peer_addr;
    socklen_t peer_addr_len = sizeof(struct sockaddr_in);
    char sock_buf[64] = {'\0'};
    struct QPInfo *local_qp_info = NULL;
    struct QPInfo *remote_qp_info = NULL;

#ifdef SHARED_NOTHING
    std::vector<std::string> tmp_peer_ip_addresses;
#endif

    sockfd = sock_create_bind(config_info.sock_port);
    check(sockfd > 0, "Failed to create server socket");

    LOG("%s: start to listen", __func__);

    listen(sockfd, 5);

    peer_sockfd = (int *) calloc(num_peers, sizeof(int));
    check(peer_sockfd != NULL, "Failed to allocate peer_sockfd");

    for (i = 0; i < num_peers; i++) {
        peer_sockfd[i] = accept(sockfd, (struct sockaddr *)&peer_addr, &peer_addr_len);
        check(peer_sockfd[i] > 0, "Failed to create peer_sockfd[%d]", i);
#ifdef SHARED_NOTHING
        tmp_peer_ip_addresses.push_back(std::string(inet_ntoa(peer_addr.sin_addr)));
#endif
    }

    // init local qp_info
    local_qp_info = (struct QPInfo *) calloc(ib_res.num_qps, sizeof(struct QPInfo));
    check(local_qp_info != NULL, "Failed to allocate local_qp_info");

    for (i = 0; i < ib_res.num_qps; i++) {
        local_qp_info[i].lid = ib_res.port_attr.lid;
        local_qp_info[i].qp_num = ib_res.qp[i]->qp_num;
        local_qp_info[i].rank = config_info.rank;
        local_qp_info[i].rkey_pool = ib_res.mr_pool->rkey;
#ifndef SHARED_NOTHING
        local_qp_info[i].raddr_pool = (uintptr_t) ib_res.ib_pool;
#else
        local_qp_info[i].raddr_pool = (uintptr_t) ib_res.ib_partitioned_pool[i];
#endif
        local_qp_info[i].rkey_buf = ib_res.mr_buf->rkey;
        local_qp_info[i].raddr_buf = (uintptr_t) ib_res.ib_buf + (i * config_info.msg_size);
    }

    // get qp_info from client
    remote_qp_info = (struct QPInfo *) calloc(config_info.num_initial_memory_nodes * 
            config_info.threads_per_memory, sizeof(struct QPInfo));
    check(remote_qp_info != NULL, "Failed to allocate remote_qp_info");
    for (i = 0; i < num_peers; i++) {
        ret = sock_get_qp_info(peer_sockfd[i], &remote_qp_info[i * num_concurr_msgs], num_concurr_msgs);
        check(ret == 0, "Failed to get qp_info from client[%d]", i);
#ifdef SHARED_NOTHING
        peer_ip_addresses[remote_qp_info[i * num_concurr_msgs].rank] = tmp_peer_ip_addresses[i];
#endif
    }

    // send qp_info to client
    for (i = 0; i < num_peers; i++) {
        ret = sock_set_qp_info(peer_sockfd[i],
                &local_qp_info[remote_qp_info[i * num_concurr_msgs].rank * num_concurr_msgs],
                num_concurr_msgs);
        check(ret == 0, "Failed to send qp_info to client[%d]", remote_qp_info[i * num_concurr_msgs].rank);
    }

    // change send QP state to RTS
    LOG(LOG_SUB_HEADER, "Start of IB Config");
    for (i = 0; i < num_peers; i++) {
        for (j = 0; j < num_concurr_msgs; j++) {
            ret = modify_qp_to_rts(ib_res.qp[(remote_qp_info[i * num_concurr_msgs].rank * num_concurr_msgs) + j],
                    remote_qp_info[(i * num_concurr_msgs) + j].qp_num, remote_qp_info[(i * num_concurr_msgs) + j].lid);
            check(ret == 0, "Failed to modify qp[%d] to rts", (remote_qp_info[i * num_concurr_msgs].rank * num_concurr_msgs) + j);
            LOG("\tqp[%" PRIu32 "] <-> qp[%" PRIu32 "]", ib_res.qp[(remote_qp_info[i * num_concurr_msgs].rank * num_concurr_msgs) + j]->qp_num,
                    remote_qp_info[(i * num_concurr_msgs) + j].qp_num);
        }
    }
    LOG(LOG_SUB_HEADER, "End of IB Config");

    // sync with clients
    for (i = 0; i < num_peers; i++) {
        n = sock_read(peer_sockfd[i], sock_buf, sizeof(SOCK_SYNC_MSG));
        check(n == sizeof(SOCK_SYNC_MSG), "Failed to receive sync from client");
    }

    for (i = 0; i < num_peers; i++) {
        n = sock_write(peer_sockfd[i], sock_buf, sizeof(SOCK_SYNC_MSG));
        check(n == sizeof(SOCK_SYNC_MSG), "Failed to write sync to client");
    }

    for (i = 0; i < num_peers; i++)
        close(peer_sockfd[i]);
    free(peer_sockfd);
    close(sockfd);

    free(local_qp_info);
    free(remote_qp_info);

    return 0;

error:
    if (peer_sockfd != NULL) {
        for (i = 0; i < num_peers; i++) {
            if (peer_sockfd[i] > 0)
                close(peer_sockfd[i]);
        }
        free(peer_sockfd);
    }

    if (sockfd > 0)
        close(sockfd);

    return -1;
}

int connect_qp_client()
{
    int ret = 0, n = 0, i = 0, j = 0, peer_sock_idx = -1;
    int num_peers = config_info.num_initial_storage_nodes;
    int num_concurr_msgs = config_info.threads_per_memory;
    int *peer_sockfd = NULL;
    char sock_buf[64] = {'\0'};

    struct QPInfo *local_qp_info = NULL;
    struct QPInfo *remote_qp_info = NULL;

    peer_sockfd = (int *) calloc(num_peers, sizeof(int));
    check(peer_sockfd != NULL, "Failed to allocate peer_sockfd");

    for (i = 0; i < num_peers; i++) {
        peer_sockfd[i] = sock_create_connect(config_info.storage_node_ips[i], config_info.sock_port);
        check(peer_sockfd[i] > 0, "Failed to create peer_sockfd[%d]", i);
    }

    // init local qp_info
    local_qp_info = (struct QPInfo *) calloc(ib_res.num_qps, sizeof(struct QPInfo));
    check(local_qp_info != NULL, "Failed to allocate local_qp_info");

    for (i = 0; i < ib_res.num_qps; i++) {
        local_qp_info[i].lid = ib_res.port_attr.lid;
        local_qp_info[i].qp_num = ib_res.qp[i]->qp_num;
        local_qp_info[i].rank = config_info.rank;
        local_qp_info[i].rkey_pool = ib_res.mr_pool->rkey;
        local_qp_info[i].raddr_pool = (uintptr_t) ib_res.ib_pool;
        local_qp_info[i].rkey_buf = ib_res.mr_buf->rkey;
        local_qp_info[i].raddr_buf = (uintptr_t) ib_res.ib_buf + (i * config_info.msg_size);
    }

    // send qp_info to server
    for (i = 0; i < num_peers; i++) {
        ret = sock_set_qp_info(peer_sockfd[i], &local_qp_info[i * num_concurr_msgs], num_concurr_msgs);
        check(ret == 0, "Failed to send qp_info[%d] to server", i);
    }

    // get qp_info from server
    remote_qp_info = (struct QPInfo *) calloc(ib_res.num_qps, sizeof(struct QPInfo));
    check(remote_qp_info != NULL, "Failed to allocate remote_qp_info");
    for (i = 0; i < num_peers; i++) {
        ret = sock_get_qp_info(peer_sockfd[i], &remote_qp_info[i * num_concurr_msgs], num_concurr_msgs);
        check(ret == 0, "Failed to get qp_info[%d] from server", i);
    }

    ib_res.rkey_pool = (uint32_t *) calloc(ib_res.num_qps, sizeof(uint32_t));
    ib_res.raddr_pool = (uint64_t *) calloc(ib_res.num_qps, sizeof(uint64_t));
    ib_res.rkey_buf = (uint32_t *) calloc(ib_res.num_qps, sizeof(uint32_t));
    ib_res.raddr_buf = (uint64_t *) calloc(ib_res.num_qps, sizeof(uint64_t));

    // change QP state to RTS
    // send qp_info to client
    peer_sock_idx = -1;
    j = 0;
    LOG(LOG_SUB_HEADER, "Start of IB Config");
    for (i = 0; i < num_peers; i++) {
        peer_sock_idx = -1;
        for (j = 0; j < ib_res.num_qps; j++) {
            if (remote_qp_info[j].rank == i) {
                peer_sock_idx = j;
                break;
            }
        }

        for (j = 0; j < num_concurr_msgs; j++) {
            ret = modify_qp_to_rts(ib_res.qp[(i * num_concurr_msgs) + j], 
                    remote_qp_info[peer_sock_idx + j].qp_num,
                    remote_qp_info[peer_sock_idx + j].lid);
            check(ret == 0, "Failed to modify qp[%d] to rts", (i * num_concurr_msgs) + j);
            LOG("\tqp[%" PRIu32 "] <-> qp[%" PRIu32 "]", ib_res.qp[(i * num_concurr_msgs) + j]->qp_num,
                    remote_qp_info[peer_sock_idx + j].qp_num);

            ib_res.rkey_pool[(i * num_concurr_msgs) + j] = remote_qp_info[peer_sock_idx + j].rkey_pool;
            ib_res.raddr_pool[(i * num_concurr_msgs) + j] = remote_qp_info[peer_sock_idx + j].raddr_pool;
            ib_res.rkey_buf[(i * num_concurr_msgs) + j] = remote_qp_info[peer_sock_idx + j].rkey_buf;
            ib_res.raddr_buf[(i * num_concurr_msgs) + j] = remote_qp_info[peer_sock_idx + j].raddr_buf;
        }
    }
    LOG(LOG_SUB_HEADER, "End of IB Config");

    // sync with server
    for (i = 0; i < num_peers; i++) {
        n = sock_write(peer_sockfd[i], sock_buf, sizeof(SOCK_SYNC_MSG));
        check(n == sizeof(SOCK_SYNC_MSG), "Failed to write sync to client[%d]", i);
    }

    for (i = 0; i < num_peers; i++) {
        n = sock_read(peer_sockfd[i], sock_buf, sizeof(SOCK_SYNC_MSG));
        check(n == sizeof(SOCK_SYNC_MSG), "Failed to receive sync from client");
    }

    for (i = 0; i < num_peers; i++)
        close(peer_sockfd[i]);
    free(peer_sockfd);

    free(local_qp_info);
    free(remote_qp_info);
    return 0;

error:
    return -1;
}

void ib_init(int total_available_memory_nodes, int num_initial_memory_nodes, 
        int num_initial_storage_nodes, int threads_per_memory, int threads_per_storage, 
        int num_storage_managers, int msg_size, char **storage_node_ips, 
        char *sock_port, int rank, bool is_server, uint64_t storage_capacities)
{
    int ret = 0;

    ret = ib_config(total_available_memory_nodes, num_initial_memory_nodes,
            num_initial_storage_nodes, threads_per_memory, threads_per_storage,
            num_storage_managers, msg_size, storage_node_ips, sock_port, rank,
            is_server, storage_capacities);
    check(ret == 0, "Failed to config");

    ret = init_env();
    check(ret == 0, "Failed to init env");

    ret = setup_ib();
    check(ret == 0, "Failed to setup IB");

    return ;
error:
    LOG("%s: failure happens", __func__);
    return ;
}

int setup_ib()
{
    int ret = 0;
    int i = 0;
    struct ibv_device **dev_list = NULL;
    memset(&ib_res, 0, sizeof(struct IBRes));

    if (config_info.is_server)
        ib_res.num_qps = config_info.total_available_memory_nodes * config_info.threads_per_memory;
    else
        ib_res.num_qps = config_info.num_initial_storage_nodes * config_info.threads_per_memory;

#ifndef DINOMO_LOCAL_TEST
    // get IB device list
    dev_list = ibv_get_device_list(NULL);
    check(dev_list != NULL, "Failed to get ib device list");

    // create IB context
    ib_res.ctx = ibv_open_device(*dev_list);
    check(ib_res.ctx != NULL, "Failed to open ib device");

    // allocate protection domain
    ib_res.pd = ibv_alloc_pd(ib_res.ctx);
    check(ib_res.pd != NULL, "Failed to allocate protection domain");

    // query IB port attribute
    ret = ibv_query_port(ib_res.ctx, IB_PORT, &ib_res.port_attr);
    check(ret == 0, "Failed to query IB port information");

    // Register mr
    if (config_info.is_server) {
        // TODO: Index initialization
        // 1) Initialize the basic index structure through a root object.
        // 2) Initialize the operation log buffers
        // *) The root offset should be conveyed to each compute node in advance through sockets
        farIdx = clht_create(512, config_info.total_available_memory_nodes, 
                config_info.threads_per_memory, config_info.storage_capacities);
        ib_res.ib_pool_size = pool_size;
        ib_res.ib_pool = (char *)pop;
        check(ib_res.ib_pool != NULL, "Failed to allocate ib_pool in a server (pool)");

        // Multiple threads shares the same queue pairs, but uses different recv buffers
        // We are reserving the number of buffers to be same with the max # of threads
        // The thread scheduled for memory allocations exclusively uses a buffer
        ib_res.ib_buf_size = config_info.msg_size * ib_res.num_qps;
        ib_res.ib_buf = (char *) memalign(4096, ib_res.ib_buf_size);
        check(ib_res.ib_buf != NULL, "Failed to allocate ib_buf in a server (pool)");
        memset(ib_res.ib_buf, 0, ib_res.ib_buf_size);

        ib_res.mr_pool = ibv_reg_mr(ib_res.pd, (void *)ib_res.ib_pool, ib_res.ib_pool_size,
                IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
        check(ib_res.mr_pool != NULL, "Failed to register PM pool");

        ib_res.mr_buf = ibv_reg_mr(ib_res.pd, (void *)ib_res.ib_buf, ib_res.ib_buf_size,
                IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
        check(ib_res.mr_buf != NULL, "Failed to register local buffer in a server");

#ifndef SHARED_NOTHING
        ib_res.ib_pool = (char *)farIdx;
#else
        ib_res.ib_partitioned_pool = (char **)farIdx;
#endif
    } else {
        // This pool is used for the batching buffer in the client
        ib_res.ib_pool_size = (sizeof(log_block) + MAX_LOG_BLOCK_LEN) * config_info.threads_per_memory;
        ib_res.ib_pool = (char *) memalign(4096, ib_res.ib_pool_size);
        check(ib_res.ib_pool != NULL, "Failed to allocate ib_pool in clients");
        memset(ib_res.ib_pool, 0, ib_res.ib_pool_size);

        ib_res.ib_buf_size = config_info.msg_size * ib_res.num_qps;
        ib_res.ib_buf = (char *) memalign(4096, ib_res.ib_buf_size);
        check(ib_res.ib_buf != NULL, "Failed to allocate ib_buf in clients");
        memset(ib_res.ib_buf, 0, ib_res.ib_buf_size);

        ib_res.mr_pool = ibv_reg_mr(ib_res.pd, (void *)ib_res.ib_pool, ib_res.ib_pool_size,
                IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
        check(ib_res.mr_pool != NULL, "Failed to register the pool for batching");

        ib_res.mr_buf = ibv_reg_mr(ib_res.pd, (void *)ib_res.ib_buf, ib_res.ib_buf_size,
                IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
        check(ib_res.mr_buf != NULL, "Failed to register local buffer in clients");
    }

    // query IB device attr
    ret = ibv_query_device(ib_res.ctx, &ib_res.dev_attr);
    check(ret == 0, "Failed to query device");

    if (config_info.is_server) {
        ib_res.cq = (struct ibv_cq **) calloc(config_info.num_storage_managers, sizeof(struct ibv_cq *));
        check(ib_res.cq != NULL, "Failed to allocate cq");

        // Create completion queue as many as the number of manager threads
        for (i = 0; i < config_info.num_storage_managers; i++) {
            ib_res.cq[i] = ibv_create_cq(ib_res.ctx, ib_res.dev_attr.max_cqe, NULL, NULL, 0);
            check(ib_res.cq[i] != NULL, "Failed to create cq");
        }

#ifdef SINGLE_DPM_MANAGER_THREAD
        ib_res.send_cq = (struct ibv_cq **) calloc(config_info.num_storage_managers, sizeof(struct ibv_cq *));
#else
        ib_res.send_cq = (struct ibv_cq **) calloc(ib_res.num_qps, sizeof(struct ibv_cq *));
#endif
        check(ib_res.send_cq != NULL, "Failed to allocate cq");

#ifdef SINGLE_DPM_MANAGER_THREAD
        for (i = 0; i < config_info.num_storage_managers; i++) {
#else
        for (i = 0; i < ib_res.num_qps; i++) {
#endif
            ib_res.send_cq[i] = ibv_create_cq(ib_res.ctx, ib_res.dev_attr.max_cqe, NULL, NULL, 0);
            check(ib_res.send_cq[i] != NULL, "Failed to create cq");
        }

        // Create srq
        struct ibv_srq_init_attr srq_init_attr;
        memset(&srq_init_attr, 0, sizeof(struct ibv_srq_init_attr));
        srq_init_attr.attr.max_wr = ib_res.dev_attr.max_srq_wr;
        srq_init_attr.attr.max_sge = 1;

        ib_res.srq = ibv_create_srq(ib_res.pd, &srq_init_attr);

        // Create qps
        ib_res.qp = (struct ibv_qp **) calloc(ib_res.num_qps, sizeof(struct ibv_qp *));
        check(ib_res.qp != NULL, "Failed to allocate qp");

        for (i = 0; i < ib_res.num_qps; i++) {
            struct ibv_qp_init_attr qp_init_attr;
            memset(&qp_init_attr, 0, sizeof(struct ibv_qp_init_attr));
#ifdef SINGLE_DPM_MANAGER_THREAD
            qp_init_attr.send_cq = ib_res.send_cq[i % config_info.num_storage_managers];
#else
            qp_init_attr.send_cq = ib_res.send_cq[i];
#endif
            qp_init_attr.recv_cq = ib_res.cq[i % config_info.num_storage_managers];
            qp_init_attr.srq = ib_res.srq;
#ifdef ENABLE_MAX_QP_WR
            qp_init_attr.cap.max_send_wr = ib_res.dev_attr.max_qp_wr;
            qp_init_attr.cap.max_recv_wr = ib_res.dev_attr.max_qp_wr;
#else
            qp_init_attr.cap.max_send_wr = 1000;
            qp_init_attr.cap.max_recv_wr = 1000;
#endif
            qp_init_attr.cap.max_send_sge = 1;
            qp_init_attr.cap.max_recv_sge = 1;
            qp_init_attr.qp_type = IBV_QPT_RC;

            ib_res.qp[i] = ibv_create_qp(ib_res.pd, &qp_init_attr);
            check(ib_res.qp[i] != NULL, "Failed to create qp[%d]", i);
        }
    } else {
        ib_res.cq = (struct ibv_cq **) calloc(ib_res.num_qps, sizeof(struct ibv_cq *));
        check(ib_res.cq != NULL, "Failed to allocate cq");

        // create completion queue per qp (per thread)
        for (i = 0; i < ib_res.num_qps; i++) {
            ib_res.cq[i] = ibv_create_cq(ib_res.ctx, ib_res.dev_attr.max_cqe, NULL, NULL, 0);
            check(ib_res.cq[i] != NULL, "Failed to create cq");
        }

        ib_res.qp = (struct ibv_qp **) calloc(ib_res.num_qps, sizeof(struct ibv_qp *));
        check(ib_res.qp != NULL, "Failed to allocate qp");

        for (i = 0; i < ib_res.num_qps; i++) {
            struct ibv_qp_init_attr qp_init_attr;
            memset(&qp_init_attr, 0, sizeof(struct ibv_qp_init_attr));
            qp_init_attr.send_cq = ib_res.cq[i];
            qp_init_attr.recv_cq = ib_res.cq[i];
#ifdef ENABLE_MAX_QP_WR
            qp_init_attr.cap.max_send_wr = ib_res.dev_attr.max_qp_wr;
            qp_init_attr.cap.max_recv_wr = ib_res.dev_attr.max_qp_wr;
#else
            qp_init_attr.cap.max_send_wr = 1000;
            qp_init_attr.cap.max_recv_wr = 1000;
#endif
            qp_init_attr.cap.max_send_sge = 1;
            qp_init_attr.cap.max_recv_sge = 1;
            qp_init_attr.qp_type = IBV_QPT_RC;

            ib_res.qp[i] = ibv_create_qp(ib_res.pd, &qp_init_attr);
            check(ib_res.qp[i] != NULL, "Failed to create qp[%d]", i);
        }
    }

    // connect QP
    if (config_info.is_server)
        ret = connect_qp_server();
    else
        ret = connect_qp_client();
    check(ret == 0, "Failed to connect qp");

    ibv_free_device_list(dev_list);
#else
#ifdef PREPOPULATE_HASHTABLE
#ifndef SHARED_NOTHING
    farIdx = clht_create(33554432, config_info.total_available_memory_nodes, 
            config_info.threads_per_memory, config_info.storage_capacities);
#else
    farIdx = clht_create((16 * 1024 * 1024) / (config_info.total_available_memory nodes 
                * config_info.threads_per_memory), config_info.total_available_memory_nodes, 
            config_info.threads_per_memory, config_info.storage_capacities);
#endif
#else
    farIdx = clht_create(512, config_info.total_available_memory_nodes, 
            config_info.threads_per_memory, config_info.storage_capacities);
#endif
#endif
    return 0;

error:
    if (dev_list != NULL)
        ibv_free_device_list(dev_list);
    return -1;
}

void close_ib_connection()
{
    int i;

    if (ib_res.qp != NULL) {
        for (i = 0; i < ib_res.num_qps; i++) {
            if (ib_res.qp[i] != NULL)
                ibv_destroy_qp(ib_res.qp[i]);
        }
        free(ib_res.qp);
    }

    if (ib_res.srq != NULL)
        ibv_destroy_srq(ib_res.srq);

    if (ib_res.cq != NULL) {
        for (i = 0; i < ib_res.num_qps; i++) {
            if (ib_res.cq[i] != NULL)
                ibv_destroy_cq(ib_res.cq[i]);
        }
        free(ib_res.cq);
    }

    if (ib_res.mr_pool != NULL)
        ibv_dereg_mr(ib_res.mr_pool);

    if (ib_res.mr_buf != NULL)
        ibv_dereg_mr(ib_res.mr_buf);

    if (ib_res.pd != NULL)
        ibv_dealloc_pd(ib_res.pd);

    if (ib_res.ctx != NULL)
        ibv_close_device(ib_res.ctx);

    if (ib_res.ib_buf != NULL)
        free(ib_res.ib_buf);
}
