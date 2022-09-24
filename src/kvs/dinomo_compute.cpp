#include "kvs/dinomo_compute.hpp"

FILE *log_fp = NULL;

void ib_finalize()
{
    close_ib_connection();
    destroy_env();
}

void ib_sync_cross(uint16_t thread_unique_id)
{
    int ret = 0, n = 0;
    uint64_t i = 0, j = 0, k = 0;
    uint64_t thread_id = thread_unique_id;
    int msg_size = config_info.msg_size;
    int num_concurr_msgs = config_info.threads_per_memory;
    int num_peers = config_info.num_initial_storage_nodes;

    int num_wc = 1;
    struct ibv_qp *qp = ib_res.qp[thread_id];
    struct ibv_cq *cq = ib_res.cq[thread_id];
    struct ibv_wc *wc = NULL;
    struct ibv_srq *srq = ib_res.srq;
    uint32_t lkey_buf = ib_res.mr_buf->lkey;
    uint32_t lkey_pool = ib_res.mr_pool->lkey;

    char *buf_ptr = ib_res.ib_buf + (msg_size * thread_id);
    size_t buf_size = msg_size;

    uint32_t imm_data = 0;
    int num_acked_peers = 0;
    bool start_sending = false;

    // pre-post recvs
    wc = (struct ibv_wc *) calloc(num_wc, sizeof(struct ibv_wc));
    check(wc != NULL, "Thread[%ld]: failed to allocate wc", thread_id);

    // Each buffer is assigned to each thread exclusively
    ret = post_recv((uint32_t)msg_size, lkey_buf, (uint64_t)buf_ptr, qp, buf_ptr);

    // wait for start signal
    while (start_sending != true) {
        do {
            n = ibv_poll_cq(cq, num_wc, wc);
        } while (n < 1);
        check(n > 0, "Thread[%ld]: failed to poll cq", thread_id);

        for (i = 0; i < n; i++) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                check(0, "Thread[%ld]: wc failed status: %s",
                        thread_id, ibv_wc_status_str(wc[i].status));
            }

            if (wc[i].opcode == IBV_WC_RECV) {
                // post a receive
                post_recv((uint32_t)msg_size, lkey_buf, wc[i].wr_id, qp, (char *)wc[i].wr_id);
                if (ntohl(wc[i].imm_data) == MSG_CTL_START) {
                    num_acked_peers += 1;
                    if (num_acked_peers == num_peers) {
                        start_sending = true;
                        break;
                    }
                }
            }
        }
    }

    LOG("Client thread[%ld]: ready to send", thread_id);

    free(wc);
    return ;
error:
    LOG("%s: failure happens", __func__);
    free(wc);
    return ;
}