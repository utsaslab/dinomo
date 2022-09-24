#include <arpa/inet.h>
#include <unistd.h>

#include "kvs/ib.h"
#include "kvs/debug.h"

#ifdef DH_DEBUG
std::atomic<uint64_t> RDMA_READ_COUNTER;
std::atomic<uint64_t> RDMA_WRITE_COUNTER;
std::atomic<uint64_t> RDMA_SEND_COUNTER;
std::atomic<uint64_t> RDMA_CAS_COUNTER;

std::atomic<uint64_t> RDMA_READ_LATENCY;
std::atomic<uint64_t> RDMA_WRITE_LATENCY;
std::atomic<uint64_t> RDMA_SEND_LATENCY;
std::atomic<uint64_t> RDMA_CAS_LATENCY;

std::atomic<uint64_t> RDMA_READ_PAYLOAD;
std::atomic<uint64_t> RDMA_WRITE_PAYLOAD;
std::atomic<uint64_t> RDMA_SEND_PAYLOAD;
std::atomic<uint64_t> RDMA_CAS_PAYLOAD;
#endif

int modify_qp_to_rts(struct ibv_qp *qp, uint32_t target_qp_num, uint16_t target_lid)
{
    int ret = 0;

    // change QP state to INIT
    {
        struct ibv_qp_attr qp_attr;
        memset(&qp_attr, 0, sizeof(ibv_qp_attr));
        qp_attr.qp_state = IBV_QPS_INIT;
        qp_attr.pkey_index = 0;
        qp_attr.port_num = IB_PORT;
        qp_attr.qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;

        ret = ibv_modify_qp (qp, &qp_attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX |
                IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
        check(ret == 0, "Failed to modify qp to INIT");
    }

    // Change QP state to RTR
    {
        struct ibv_qp_attr qp_attr;
        memset(&qp_attr, 0, sizeof(ibv_qp_attr));
        qp_attr.qp_state               = IBV_QPS_RTR;
        qp_attr.path_mtu               = IB_MTU;
        qp_attr.rq_psn                 = 0;
        qp_attr.max_dest_rd_atomic     = 1;
        qp_attr.min_rnr_timer          = 12;
        qp_attr.ah_attr.is_global      = 0;
        qp_attr.ah_attr.sl             = IB_SL;
        qp_attr.ah_attr.src_path_bits  = 0;
        qp_attr.ah_attr.port_num       = IB_PORT;
        qp_attr.dest_qp_num            = target_qp_num;
        qp_attr.ah_attr.dlid           = target_lid;

        ret = ibv_modify_qp(qp, &qp_attr, IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
                IBV_QP_MIN_RNR_TIMER);
        check(ret == 0, "Failed to change qp to RTR");
    }

    // Change QP state to RTS
    {
        struct ibv_qp_attr qp_attr;
        memset(&qp_attr, 0, sizeof(ibv_qp_attr));
        qp_attr.qp_state               = IBV_QPS_RTS;
        qp_attr.timeout                = 14;
        qp_attr.retry_cnt              = 7;
        qp_attr.rnr_retry              = 7;
        qp_attr.sq_psn                 = 0;
        qp_attr.max_rd_atomic          = 1;

        ret = ibv_modify_qp(qp, &qp_attr, IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);
        check(ret == 0, "Failed to modify qp to RTS");
    }

    return 0;

error:
    return -1;
}

int poll_cq(struct ibv_cq *cq)
{
    int num_comp;
    struct ibv_wc wc;
    memset(&wc, 0, sizeof(struct ibv_wc));

    do {
        num_comp = ibv_poll_cq(cq, 1, &wc);
    } while (num_comp == 0);

    if (num_comp < 0) {
        fprintf(stderr, "%s: ibv_poll_cq() failed\n", __func__);
        return -1;
    }

    if (wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "%s: Failed status %s (%d) for wr_id %d\n", __func__,
                ibv_wc_status_str(wc.status), wc.status, (int)wc.wr_id);
        return -1;
    }

    return 0;
}

int post_write_signaled_blocking(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey, struct ibv_cq *cq)
{
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list;
    list.addr = (uintptr_t) buf;
    list.length = req_size;
    list.lkey = lkey;

    struct ibv_send_wr send_wr;
    memset(&send_wr, 0, sizeof(struct ibv_send_wr));
    send_wr.wr_id = wr_id;
    send_wr.sg_list = &list;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_RDMA_WRITE;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.wr.rdma.remote_addr = raddr;
    send_wr.wr.rdma.rkey = rkey;

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    if (ret != 0) {
        fprintf(stderr, "%s: ibv_post_send() failed %d\n", __func__, ret);
        return ret;
    }

    ret = poll_cq(cq);

    return ret;
}

int post_write_signaled_blocking_profile(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey, struct ibv_cq *cq,
        uint64_t *rdma_write_counter, uint64_t *rdma_write_payload)
{
    (*rdma_write_counter)++;
    *rdma_write_payload = *rdma_write_payload + (uint64_t)req_size;

    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list;
    list.addr = (uintptr_t) buf;
    list.length = req_size;
    list.lkey = lkey;

    struct ibv_send_wr send_wr;
    memset(&send_wr, 0, sizeof(struct ibv_send_wr));
    send_wr.wr_id = wr_id;
    send_wr.sg_list = &list;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_RDMA_WRITE;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.wr.rdma.remote_addr = raddr;
    send_wr.wr.rdma.rkey = rkey;

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    if (ret != 0) {
        fprintf(stderr, "%s: ibv_post_send() failed %d\n", __func__, ret);
        return ret;
    }

    ret = poll_cq(cq);

    return ret;
}

int post_write_signaled_nonblocking(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey)
{
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list;
    list.addr = (uintptr_t) buf;
    list.length = req_size;
    list.lkey = lkey;

    struct ibv_send_wr send_wr;
    memset(&send_wr, 0, sizeof(struct ibv_send_wr));
    send_wr.wr_id = wr_id;
    send_wr.sg_list = &list;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_RDMA_WRITE;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.wr.rdma.remote_addr = raddr;
    send_wr.wr.rdma.rkey = rkey;

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    return ret;
}

int post_write_unsignaled(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey)
{
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list;
    list.addr = (uintptr_t) buf;
    list.length = req_size;
    list.lkey = lkey;

    struct ibv_send_wr send_wr;
    memset(&send_wr, 0, sizeof(struct ibv_send_wr));
    send_wr.wr_id = wr_id;
    send_wr.sg_list = &list;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_RDMA_WRITE;
    send_wr.wr.rdma.remote_addr = raddr;
    send_wr.wr.rdma.rkey = rkey;

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    return ret;
}

int post_cas_signaled_blocking(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey, struct ibv_cq *cq,
        uint64_t expected, uint64_t value)
{
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list;
    list.addr = (uintptr_t) buf;
    list.length = req_size;
    list.lkey = lkey;

    struct ibv_send_wr send_wr;
    memset(&send_wr, 0, sizeof(struct ibv_send_wr));
    send_wr.wr_id = wr_id;
    send_wr.sg_list = &list;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.wr.atomic.remote_addr = raddr;
    send_wr.wr.atomic.rkey = rkey;
    send_wr.wr.atomic.compare_add = expected;
    send_wr.wr.atomic.swap = value;

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    if (ret != 0) {
        fprintf(stderr, "%s: ibv_post_send() failed %d\n", __func__, ret);
        return ret;
    }

    ret = poll_cq(cq);

    return ret;
}

int post_cas_signaled_blocking_profile(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey, struct ibv_cq *cq,
        uint64_t expected, uint64_t value, uint64_t *rdma_cas_counter, uint64_t *rdma_cas_payload)
{
    (*rdma_cas_counter)++;
    *rdma_cas_payload = *rdma_cas_payload + (uint64_t)req_size + sizeof(uint32_t);
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list;
    list.addr = (uintptr_t) buf;
    list.length = req_size;
    list.lkey = lkey;

    struct ibv_send_wr send_wr;
    memset(&send_wr, 0, sizeof(struct ibv_send_wr));
    send_wr.wr_id = wr_id;
    send_wr.sg_list = &list;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.wr.atomic.remote_addr = raddr;
    send_wr.wr.atomic.rkey = rkey;
    send_wr.wr.atomic.compare_add = expected;
    send_wr.wr.atomic.swap = value;

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    if (ret != 0) {
        fprintf(stderr, "%s: ibv_post_send() failed %d\n", __func__, ret);
        return ret;
    }

    ret = poll_cq(cq);

    return ret;
}

int post_cas_signaled_nonblocking(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey, struct ibv_cq *cq,
        uint64_t expected, uint64_t value)
{
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list;
    list.addr = (uintptr_t) buf;
    list.length = req_size;
    list.lkey = lkey;

    struct ibv_send_wr send_wr;
    memset(&send_wr, 0, sizeof(struct ibv_send_wr));
    send_wr.wr_id = wr_id;
    send_wr.sg_list = &list;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.wr.atomic.remote_addr = raddr;
    send_wr.wr.atomic.rkey = rkey;
    send_wr.wr.atomic.compare_add = expected;
    send_wr.wr.atomic.swap = value;

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    return ret;
}

int post_fetch_add_signaled_blocking(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey, struct ibv_cq *cq, uint64_t value)
{
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list;
    list.addr = (uintptr_t) buf;
    list.length = req_size;
    list.lkey = lkey;

    struct ibv_send_wr send_wr;
    memset(&send_wr, 0, sizeof(struct ibv_send_wr));
    send_wr.wr_id = wr_id;
    send_wr.sg_list = &list;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.wr.atomic.remote_addr = raddr;
    send_wr.wr.atomic.rkey = rkey;
    send_wr.wr.atomic.compare_add = value;

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    if (ret != 0) {
        fprintf(stderr, "%s: ibv_post_send() failed %d\n", __func__, ret);
        return ret;
    }

    ret = poll_cq(cq);
    return ret;
}

int post_fetch_add_signaled_blocking_profile(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey, struct ibv_cq *cq, uint64_t value,
uint64_t *rdma_faa_counter, uint64_t *rdma_faa_payload)
{
    (*rdma_faa_counter)++;
    *rdma_faa_payload = *rdma_faa_payload + (uint64_t)req_size + sizeof(uint32_t);
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list;
    list.addr = (uintptr_t) buf;
    list.length = req_size;
    list.lkey = lkey;

    struct ibv_send_wr send_wr;
    memset(&send_wr, 0, sizeof(struct ibv_send_wr));
    send_wr.wr_id = wr_id;
    send_wr.sg_list = &list;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.wr.atomic.remote_addr = raddr;
    send_wr.wr.atomic.rkey = rkey;
    send_wr.wr.atomic.compare_add = value;

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    if (ret != 0) {
        fprintf(stderr, "%s: ibv_post_send() failed %d\n", __func__, ret);
        return ret;
    }

    ret = poll_cq(cq);
    return ret;
}

int post_fetch_add_signaled_nonblocking(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey, struct ibv_cq *cq, uint64_t value)
{
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list;
    list.addr = (uintptr_t) buf;
    list.length = req_size;
    list.lkey = lkey;

    struct ibv_send_wr send_wr;
    memset(&send_wr, 0, sizeof(struct ibv_send_wr));
    send_wr.wr_id = wr_id;
    send_wr.sg_list = &list;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.wr.atomic.remote_addr = raddr;
    send_wr.wr.atomic.rkey = rkey;
    send_wr.wr.atomic.compare_add = value;

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    if (ret != 0) {
        fprintf(stderr, "%s: ibv_post_send() failed %d\n", __func__, ret);
        return ret;
    }
}

int post_read_signaled_blocking(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey, struct ibv_cq *cq)
{
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list;
    list.addr = (uintptr_t) buf;
    list.length = req_size;
    list.lkey = lkey;

    struct ibv_send_wr send_wr;
    memset(&send_wr, 0, sizeof(struct ibv_send_wr));
    send_wr.wr_id = wr_id;
    send_wr.sg_list = &list;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_RDMA_READ;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.wr.rdma.remote_addr = raddr;
    send_wr.wr.rdma.rkey = rkey;

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    if (ret != 0) {
        fprintf(stderr, "%s: ibv_post_send() failed %d\n", __func__, ret);
        return ret;
    }

    ret = poll_cq(cq);

    return ret;
}

int post_read_signaled_blocking_profile(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey, struct ibv_cq *cq,
        uint64_t *rdma_read_counter, uint64_t *rdma_read_payload)
{
    (*rdma_read_counter)++;
    *rdma_read_payload = *rdma_read_payload + (uint64_t)req_size;
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list;
    list.addr = (uintptr_t) buf;
    list.length = req_size;
    list.lkey = lkey;

    struct ibv_send_wr send_wr;
    memset(&send_wr, 0, sizeof(struct ibv_send_wr));
    send_wr.wr_id = wr_id;
    send_wr.sg_list = &list;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_RDMA_READ;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.wr.rdma.remote_addr = raddr;
    send_wr.wr.rdma.rkey = rkey;

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    if (ret != 0) {
        fprintf(stderr, "%s: ibv_post_send() failed %d\n", __func__, ret);
        return ret;
    }

    ret = poll_cq(cq);

    return ret;
}

int post_read_signaled_nonblocking(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey)
{
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list;
    list.addr = (uintptr_t) buf;
    list.length = req_size;
    list.lkey = lkey;

    struct ibv_send_wr send_wr;
    memset(&send_wr, 0, sizeof(struct ibv_send_wr));
    send_wr.wr_id = wr_id;
    send_wr.sg_list = &list;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_RDMA_READ;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.wr.rdma.remote_addr = raddr;
    send_wr.wr.rdma.rkey = rkey;

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    return ret;
}

int post_read_unsignaled(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey)
{
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list;
    list.addr = (uintptr_t) buf;
    list.length = req_size;
    list.lkey = lkey;

    struct ibv_send_wr send_wr;
    memset(&send_wr, 0, sizeof(struct ibv_send_wr));
    send_wr.wr_id = wr_id;
    send_wr.sg_list = &list;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_RDMA_READ;
    send_wr.wr.rdma.remote_addr = raddr;
    send_wr.wr.rdma.rkey = rkey;

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    return ret;
}

int post_send(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_qp *qp, char *buf)
{
#ifdef DH_DEBUG
    RDMA_SEND_COUNTER.fetch_add(1);
    RDMA_SEND_PAYLOAD.fetch_add((uint64_t)req_size);
#endif
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list;
    list.addr = (uintptr_t) buf;
    list.length = req_size;
    list.lkey = lkey;

    struct ibv_send_wr send_wr;
    memset(&send_wr, 0, sizeof(struct ibv_send_wr));
    send_wr.wr_id = wr_id;
    send_wr.sg_list = &list;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_SEND;
    //        send_wr.opcode = IBV_WR_SEND_WITH_IMM;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    //        send_wr.imm_data = htonl(imm_data);

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    if (ret != 0) {
        fprintf(stderr, "%s: ibv_post_send() failed %d\n", __func__, ret);
        return ret;
    }

    return ret;
}

int post_send_imm(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        uint32_t imm_data, struct ibv_qp *qp, char *buf)
{
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list;
    list.addr = (uintptr_t) buf;
    list.length = req_size;
    list.lkey = lkey;

    struct ibv_send_wr send_wr;
    memset(&send_wr, 0, sizeof(struct ibv_send_wr));
    send_wr.wr_id = wr_id;
    send_wr.sg_list = &list;
    if (req_size != 0) send_wr.num_sge = 1;
    else send_wr.num_sge = 0;
    send_wr.opcode = IBV_WR_SEND_WITH_IMM;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.imm_data = htonl(imm_data);

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    if (ret != 0) {
        fprintf(stderr, "%s: ibv_post_send() failed %d\n", __func__, ret);
        return ret;
    }

    return ret;
}

int post_send_imm_profile(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        uint32_t imm_data, struct ibv_qp *qp, char *buf,
        uint64_t *rdma_send_counter, uint64_t *rdma_send_payload)
{
    (*rdma_send_counter)++;
    *rdma_send_payload = *rdma_send_payload + (uint64_t)req_size + sizeof(uint32_t);
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list;
    list.addr = (uintptr_t) buf;
    list.length = req_size;
    list.lkey = lkey;

    struct ibv_send_wr send_wr;
    memset(&send_wr, 0, sizeof(struct ibv_send_wr));
    send_wr.wr_id = wr_id;
    send_wr.sg_list = &list;
    if (req_size != 0) send_wr.num_sge = 1;
    else send_wr.num_sge = 0;
    send_wr.opcode = IBV_WR_SEND_WITH_IMM;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.imm_data = htonl(imm_data);

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    if (ret != 0) {
        fprintf(stderr, "%s: ibv_post_send() failed %d\n", __func__, ret);
        return ret;
    }

    return ret;
}

int post_send_poll(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_qp *qp, char *buf, struct ibv_cq *cq)
{
#ifdef DH_DEBUG
    RDMA_SEND_COUNTER.fetch_add(1);
    RDMA_SEND_PAYLOAD.fetch_add((uint64_t)req_size);
#endif
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list;
    list.addr = (uintptr_t) buf;
    list.length = req_size;
    list.lkey = lkey;

    struct ibv_send_wr send_wr;
    memset(&send_wr, 0, sizeof(struct ibv_send_wr));
    send_wr.wr_id = wr_id;
    send_wr.sg_list = &list;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_SEND;
    //        send_wr.opcode = IBV_WR_SEND_WITH_IMM;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    //        send_wr.imm_data = htonl(imm_data);

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    if (ret != 0) {
        fprintf(stderr, "%s: ibv_post_send() failed %d\n", __func__, ret);
        return ret;
    }

    ret = poll_cq(cq);

    return ret;
}

int post_send_imm_poll(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        uint32_t imm_data, struct ibv_qp *qp, char *buf, struct ibv_cq *cq)
{
#ifdef DH_DEBUG
    RDMA_SEND_COUNTER.fetch_add(1);
    RDMA_SEND_PAYLOAD.fetch_add((uint64_t)(req_size + sizeof(uint32_t)));
#endif
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list;
    list.addr = (uintptr_t) buf;
    list.length = req_size;
    list.lkey = lkey;

    struct ibv_send_wr send_wr;
    memset(&send_wr, 0, sizeof(struct ibv_send_wr));
    send_wr.wr_id = wr_id;
    send_wr.sg_list = &list;
    if (req_size != 0) send_wr.num_sge = 1;
    else send_wr.num_sge = 0;
    send_wr.opcode = IBV_WR_SEND_WITH_IMM;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.imm_data = htonl(imm_data);

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    if (ret != 0) {
        fprintf(stderr, "%s: ibv_post_send() failed %d\n", __func__, ret);
        return ret;
    }

    ret = poll_cq(cq);

    return ret;
}

int post_recv(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_qp *qp, char *buf) {
    int ret = 0;
    struct ibv_recv_wr *bad_recv_wr;

    struct ibv_sge list;
    list.addr = (uintptr_t) buf;
    list.length = req_size;
    list.lkey = lkey;

    struct ibv_recv_wr recv_wr;
    memset(&recv_wr, 0, sizeof(struct ibv_recv_wr));
    recv_wr.wr_id = wr_id;
    recv_wr.sg_list = &list;
    recv_wr.num_sge = 1;

    ret = ibv_post_recv(qp, &recv_wr, &bad_recv_wr);
    return ret;
}

int post_recv_profile(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_qp *qp, char *buf, uint64_t *rdma_recv_counter, uint64_t *rdma_recv_payload) {
    (*rdma_recv_counter)++;
    *rdma_recv_payload = *rdma_recv_payload + (uint64_t)req_size + sizeof(uint32_t);
    int ret = 0;
    struct ibv_recv_wr *bad_recv_wr;

    struct ibv_sge list;
    list.addr = (uintptr_t) buf;
    list.length = req_size;
    list.lkey = lkey;

    struct ibv_recv_wr recv_wr;
    memset(&recv_wr, 0, sizeof(struct ibv_recv_wr));
    recv_wr.wr_id = wr_id;
    recv_wr.sg_list = &list;
    recv_wr.num_sge = 1;

    ret = ibv_post_recv(qp, &recv_wr, &bad_recv_wr);
    return ret;
}

int post_srq_recv(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_srq *srq, char *buf)
{
    int ret = 0;
    struct ibv_recv_wr *bad_recv_wr;

    struct ibv_sge list;
    list.addr = (uintptr_t) buf;
    list.length = req_size;
    list.lkey = lkey;

    struct ibv_recv_wr recv_wr;
    memset(&recv_wr, 0, sizeof(struct ibv_recv_wr));
    recv_wr.wr_id = wr_id;
    recv_wr.sg_list = &list;
    recv_wr.num_sge = 1;

    ret = ibv_post_srq_recv(srq, &recv_wr, &bad_recv_wr);
    return ret;
}
