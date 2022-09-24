#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>

#include "kvs/debug.h"
#include "kvs/sock.h"

ssize_t sock_read(int sock_fd, void *buffer, size_t len)
{
    ssize_t nr, tot_read;
    char *buf = (char *)buffer;
    tot_read = 0;

    while (len != 0 && (nr = read(sock_fd, buf, len)) != 0) {
        if (nr < 0) {
            if (errno == EINTR)
                continue;
            else
                return -1;
        }

        len -= nr;
        buf += nr;
        tot_read += nr;
    }

    return tot_read;
}

ssize_t sock_write(int sock_fd, void *buffer, size_t len)
{
    ssize_t nw, tot_written;
    const char *buf = (const char *)buffer;

    for (tot_written = 0; tot_written < len; ) {
        nw = write(sock_fd, buf, len - tot_written);
        if (nw <= 0) {
            if (nw == -1 && errno == EINTR)
                continue;
            else
                return -1;
        }

        tot_written += nw;
        buf += nw;
    }

    return tot_written;
}

int sock_create_bind(char *port)
{
    struct addrinfo hints;
    struct addrinfo *result, *rp;
    int sock_fd = -1, ret = 0, enable = 1;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_UNSPEC;
    hints.ai_flags = AI_PASSIVE;

    ret = getaddrinfo(NULL, port, &hints, &result);
    check(ret == 0, "getaddrinfo error");

    for (rp = result; rp != NULL; rp = rp->ai_next) {
        sock_fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sock_fd < 0)
            continue;

        if (setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0)
                perror("setsockopt(SO_REUSEADDR) failed");

        ret = bind(sock_fd, rp->ai_addr, rp->ai_addrlen);
        if (ret == 0)
            break;

        close(sock_fd);
        sock_fd = -1;
    }

    check(rp != NULL, "creating socket");

    freeaddrinfo(result);
    return sock_fd;

error:
    if (result) {
        freeaddrinfo(result);
    }
    if (sock_fd > 0) {
        close(sock_fd);
    }
    return -1;
}

int sock_create_connect(char *server_name, char *port)
{
    struct addrinfo hints;
    struct addrinfo *result, *rp;
    int sock_fd = -1, ret = 0;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_UNSPEC;

    ret = getaddrinfo(server_name, port, &hints, &result);
    check(ret == 0, "[ERROR] %s", gai_strerror(ret));

    for (rp = result; rp != NULL; rp = rp->ai_next) {
        sock_fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sock_fd == -1)
            continue;

        ret = connect(sock_fd, rp->ai_addr, rp->ai_addrlen);
        if (ret == 0)
            break;

        close(sock_fd);
        sock_fd = -1;
    }

    check(rp != NULL, "Could not connect");

    freeaddrinfo(result);
    return sock_fd;

error:
    if (result)
        freeaddrinfo(result);
    if (sock_fd != -1)
        close(sock_fd);
    return -1;
}

//int sock_set_qp_info(int sock_fd, struct QPInfo *qp_info)
//{
//    int n;
//    struct QPInfo tmp_qp_info;
//
//    tmp_qp_info.lid = htons(qp_info->lid);
//    tmp_qp_info.qp_num = htonl(qp_info->qp_num);
//    tmp_qp_info.rkey_pool = htonl(qp_info->rkey_pool);
//    tmp_qp_info.raddr_pool = htonll(qp_info->raddr_pool);
//    tmp_qp_info.rkey_buf = htonl(qp_info->rkey_buf);
//    tmp_qp_info.raddr_buf = htonll(qp_info->raddr_buf);
//
//    n = sock_write(sock_fd, (char *)&tmp_qp_info, sizeof(struct QPInfo));
//    check(n == sizeof(struct QPInfo), "write qp_info to socket");
//
//    return 0;
//
//error:
//    return -1;
//}

int sock_set_qp_info(int sock_fd, struct QPInfo *qp_info, int num_concurr_msgs)
{
    int n, i;
    struct QPInfo tmp_qp_info[num_concurr_msgs];

    for (i = 0; i < num_concurr_msgs; i++) {
        tmp_qp_info[i].lid = htons(qp_info[i].lid);
        tmp_qp_info[i].qp_num = htonl(qp_info[i].qp_num);
        tmp_qp_info[i].rank = htonl(qp_info[i].rank);
        tmp_qp_info[i].rkey_pool = htonl(qp_info[i].rkey_pool);
        tmp_qp_info[i].raddr_pool = htonll(qp_info[i].raddr_pool);
        tmp_qp_info[i].rkey_buf = htonl(qp_info[i].rkey_buf);
        tmp_qp_info[i].raddr_buf = htonll(qp_info[i].raddr_buf);
    }

    n = sock_write(sock_fd, (char *)&tmp_qp_info, sizeof(struct QPInfo) * num_concurr_msgs);
    check(n == sizeof(struct QPInfo) * num_concurr_msgs, "write qp_info to socket");

    return 0;

error:
    return -1;
}

//int sock_get_qp_info(int sock_fd, struct QPInfo *qp_info)
//{
//    int n;
//    struct QPInfo tmp_qp_info;
//
//    n = sock_read(sock_fd, (char *)&tmp_qp_info, sizeof(struct QPInfo));
//    check(n == sizeof(struct QPInfo), "read qp_info from socket");
//
//    qp_info->lid = ntohs(tmp_qp_info.lid);
//    qp_info->qp_num = ntohl(tmp_qp_info.qp_num);
//    qp_info->rkey_pool = ntohl(tmp_qp_info.rkey_pool);
//    qp_info->raddr_pool = ntohll(tmp_qp_info.raddr_pool);
//    qp_info->rkey_buf = ntohl(tmp_qp_info.rkey_buf);
//    qp_info->raddr_buf = ntohll(tmp_qp_info.raddr_buf);
//
//    return 0;
//
//error:
//    return -1;
//}

int sock_get_qp_info(int sock_fd, struct QPInfo *qp_info, int num_concurr_msgs)
{
    int n, i;
    struct QPInfo tmp_qp_info[num_concurr_msgs];

    n = sock_read(sock_fd, (char *)&tmp_qp_info, sizeof(struct QPInfo) * num_concurr_msgs);
    check(n == sizeof(struct QPInfo) * num_concurr_msgs, "read qp_info from socket");

    for (i = 0; i < num_concurr_msgs; i++) {
        qp_info[i].lid = ntohs(tmp_qp_info[i].lid);
        qp_info[i].qp_num = ntohl(tmp_qp_info[i].qp_num);
        qp_info[i].rank = ntohl(tmp_qp_info[i].rank);
        qp_info[i].rkey_pool = ntohl(tmp_qp_info[i].rkey_pool);
        qp_info[i].raddr_pool = ntohll(tmp_qp_info[i].raddr_pool);
        qp_info[i].rkey_buf = ntohl(tmp_qp_info[i].rkey_buf);
        qp_info[i].raddr_buf = ntohll(tmp_qp_info[i].raddr_buf);
    }

    return 0;

error:
    return -1;
}
