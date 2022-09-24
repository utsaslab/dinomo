#ifndef IB_H_
#define IB_H_

#include <inttypes.h>
#include <sys/types.h>
#include <endian.h>
#include <byteswap.h>
#include <infiniband/verbs.h>
#include <arpa/inet.h>

#ifdef __cplusplus
extern "C" {
#endif

#define IB_MTU              IBV_MTU_4096
#define IB_PORT             1
#define IB_SL               0
#define IB_WR_ID_STOP       0xE000000000000000
#define NUM_WARMING_UP_OPS  500000
#define TOT_NUM_OPS         10000000
#define SIG_INTERVAL        1000

#if __BYTE_ORDER == __LITTLE_ENDIAN
static inline uint64_t htonll(uint64_t x) {return bswap_64(x);}
static inline uint64_t ntohll(uint64_t x) {return bswap_64(x);}
#elif __BYTE_ORDER == __BIG_ENDIAN
static inline uint64_t htonll(uint64_t x) {return x;}
static inline uint64_t ntohll(uint64_t x) {return x;}
#else
#error __BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN
#endif

struct QPInfo {
    uint16_t lid;
    uint32_t qp_num;
    uint32_t rank;
    uint32_t rkey_pool;
    uint64_t raddr_pool;
    uint32_t rkey_buf;
    uint64_t raddr_buf;
} __attribute__ ((packed));

enum MsgType {
    MSG_CTL_START = 100,
    MSG_CTL_STOP,
    MSG_REGULAR,
    MSG_CTL_COMMIT,
};

int modify_qp_to_rts(struct ibv_qp *qp, uint32_t target_qp_num, uint16_t target_lid);

int poll_cq(struct ibv_cq *cq);

int post_write_signaled_blocking(uint32_t req_size, uint32_t lkey, uint64_t wr_id, 
        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey, struct ibv_cq *cq);

int post_write_signaled_blocking_profile(uint32_t req_size, uint32_t lkey, uint64_t wr_id, 
        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey, struct ibv_cq *cq,
        uint64_t *rdma_write_counter, uint64_t *rdma_write_payload);

int post_write_signaled_nonblocking(uint32_t req_size, uint32_t lkey, uint64_t wr_id, 
        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey);

int post_write_unsignaled(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey);

int post_cas_signaled_blocking(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey, struct ibv_cq *cq,
        uint64_t expected, uint64_t value);

int post_cas_signaled_blocking_profile(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey, struct ibv_cq *cq,
        uint64_t expected, uint64_t value, uint64_t *rdma_cas_counter, uint64_t *rdma_cas_payload);

int post_cas_signaled_nonblocking(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey, struct ibv_cq *cq,
        uint64_t expected, uint64_t value);

int post_fetch_add_signaled_blocking(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey, struct ibv_cq *cq, uint64_t value);

int post_fetch_add_signaled_blocking_profile(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey, struct ibv_cq *cq, uint64_t value,
uint64_t *rdma_faa_counter, uint64_t *rdma_faa_payload);

int post_fetch_add_signaled_nonblocking(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey, struct ibv_cq *cq, uint64_t value);

int post_read_signaled_blocking(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey, struct ibv_cq *cq);

int post_read_signaled_blocking_profile(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey, struct ibv_cq *cq,
        uint64_t *rdma_read_counter, uint64_t *rdma_read_payload);

int post_read_signaled_nonblocking(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey);

int post_read_unsignaled(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey);

int post_send(uint32_t req_size, uint32_t lkey, uint64_t wr_id, struct ibv_qp *qp, char *buf);

int post_send_imm(uint32_t req_size, uint32_t lkey, uint64_t wr_id, uint32_t imm_data, struct ibv_qp *qp, char *buf);

int post_send_imm_profile(uint32_t req_size, uint32_t lkey, uint64_t wr_id, uint32_t imm_data, 
        struct ibv_qp *qp, char *buf, uint64_t *rdma_send_counter, uint64_t *rdma_send_payload);

int post_send_poll(uint32_t req_size, uint32_t lkey, uint64_t wr_id, struct ibv_qp *qp, char *buf, struct ibv_cq *cq);

int post_send_imm_poll(uint32_t req_size, uint32_t lkey, uint64_t wr_id, uint32_t imm_data, struct ibv_qp *qp, char *buf, struct ibv_cq *cq);

int post_recv(uint32_t req_size, uint32_t lkey, uint64_t wr_id, struct ibv_qp *qp, char *buf);

int post_recv_profile(uint32_t req_size, uint32_t lkey, uint64_t wr_id, struct ibv_qp *qp, char *buf,
        uint64_t *rdma_recv_counter, uint64_t *rdma_recv_payload);

int post_srq_recv(uint32_t req_size, uint32_t lkey, uint64_t wr_id, struct ibv_srq *srq, char *buf);

#ifdef __cplusplus
}
#endif
#endif // ib.h
