/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "jfs_xds.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <sys/un.h>
#include <unistd.h>

#include "xds_p2p_user.h"

#define JFS_XDS_MAX_WAITERS   128u
#define JFS_XDS_ENDPOINT_MAX  65536u
#define JFS_XDS_MAX_REPLY_PKT (sizeof(jfs_xds_read_direct_reply) + \
			       JFS_XDS_MAX_HOLES * sizeof(jfs_xds_hole))

struct jfs_xds_waiter {
	uint64_t       req_id;
	void          *rep_buf;
	size_t         rep_cap;
	size_t         rep_len;
	int            rc;
	int            done;
	pthread_cond_t cond;
};

struct jfs_xds_client {
	int                        sock_fd;
	pthread_mutex_t            send_mu;
	pthread_mutex_t            rx_mu;
	pthread_cond_t             slot_cond;
	int                        rx_active;
	int                        rx_err;
	uint64_t                   next_req_id;
	struct jfs_xds_waiter     *waiters[JFS_XDS_MAX_WAITERS];
	uint32_t                   waiter_count;
	struct jfs_xds_client_info info;
};

struct jfs_xds_buf {
	uint64_t              token;
	uint64_t              size_bytes;
	struct xds_p2p_ctx    p2p_ctx;
	uint32_t              peer_count;
	struct xds_p2p_handle handles[JFS_XDS_MAX_PEERS];
};

static int send_msg_with_fd(int sock_fd, const void *buf, size_t len, int send_fd)
{
	struct msghdr msg = {0};
	struct iovec iov = { .iov_base = (void *)buf, .iov_len = len };
	char cmsg_buf[CMSG_SPACE(sizeof(int))];

	msg.msg_iov = &iov;
	msg.msg_iovlen = 1;

	if (send_fd >= 0) {
		memset(cmsg_buf, 0, sizeof(cmsg_buf));
		msg.msg_control = cmsg_buf;
		msg.msg_controllen = sizeof(cmsg_buf);
		struct cmsghdr *cm = CMSG_FIRSTHDR(&msg);
		cm->cmsg_level = SOL_SOCKET;
		cm->cmsg_type = SCM_RIGHTS;
		cm->cmsg_len = CMSG_LEN(sizeof(int));
		memcpy(CMSG_DATA(cm), &send_fd, sizeof(int));
	}

	ssize_t n;
	do {
		n = sendmsg(sock_fd, &msg, MSG_NOSIGNAL);
	} while (n < 0 && errno == EINTR);
	if (n < 0)
		return -errno;
	return (size_t)n == len ? 0 : -EIO;
}

static int recv_msg(int sock_fd, void *buf, size_t cap, size_t *out_len)
{
	struct msghdr msg = {0};
	struct iovec iov = { .iov_base = buf, .iov_len = cap };
	msg.msg_iov = &iov;
	msg.msg_iovlen = 1;

	ssize_t n;
	do {
		n = recvmsg(sock_fd, &msg, 0);
	} while (n < 0 && errno == EINTR);
	if (n < 0)
		return -errno;
	if (n == 0)
		return -ECONNRESET;
	if (msg.msg_flags & MSG_TRUNC)
		return -EMSGSIZE;
	*out_len = (size_t)n;
	return 0;
}

/*
 * Leader-follower RPC multiplexer over SOCK_SEQPACKET:
 * Multiple threads may invoke rpc_roundtrip concurrently on one client.
 * send_mu serializes only the non-blocking sendmsg; one waiting thread at a
 * time executes recvmsg and routes each reply by hdr.req_id to its owner.
 */
static int rpc_roundtrip(struct jfs_xds_client *cli,
			 void *req_buf, size_t req_len, int send_fd,
			 void *rep_buf, size_t rep_cap, size_t *rep_len_out)
{
	struct jfs_xds_waiter w = {0};
	w.rep_buf = rep_buf;
	w.rep_cap = rep_cap;
	pthread_cond_init(&w.cond, NULL);

	/* 1. Reserve a waiter slot and unique req_id under rx_mu. */
	pthread_mutex_lock(&cli->rx_mu);
	while (cli->waiter_count >= JFS_XDS_MAX_WAITERS && cli->rx_err == 0)
		pthread_cond_wait(&cli->slot_cond, &cli->rx_mu);
	if (cli->rx_err != 0) {
		int err = cli->rx_err;
		pthread_mutex_unlock(&cli->rx_mu);
		pthread_cond_destroy(&w.cond);
		return err;
	}
	w.req_id = cli->next_req_id++;
	cli->waiters[cli->waiter_count++] = &w;
	pthread_mutex_unlock(&cli->rx_mu);

	jfs_xds_msg_hdr hdr;
	memcpy(&hdr, req_buf, sizeof(hdr));
	hdr.req_id = w.req_id;
	memcpy(req_buf, &hdr, sizeof(hdr));

	/* 2. Send request packet without blocking other threads' recv loop. */
	pthread_mutex_lock(&cli->send_mu);
	int rc = send_msg_with_fd(cli->sock_fd, req_buf, req_len, send_fd);
	pthread_mutex_unlock(&cli->send_mu);
	if (rc != 0) {
		pthread_mutex_lock(&cli->rx_mu);
		for (uint32_t i = 0; i < cli->waiter_count; i++) {
			if (cli->waiters[i] == &w) {
				cli->waiters[i] = cli->waiters[--cli->waiter_count];
				pthread_cond_signal(&cli->slot_cond);
				break;
			}
		}
		pthread_mutex_unlock(&cli->rx_mu);
		pthread_cond_destroy(&w.cond);
		return rc;
	}

	/* 3. Leader-follower receive loop matched strictly by hdr.req_id. */
	pthread_mutex_lock(&cli->rx_mu);
	while (!w.done) {
		if (cli->rx_err != 0) {
			w.rc = cli->rx_err;
			w.done = 1;
			break;
		}
		if (!cli->rx_active) {
			cli->rx_active = 1;
			pthread_mutex_unlock(&cli->rx_mu);

			uint8_t pkt[JFS_XDS_MAX_REPLY_PKT];
			size_t rlen = 0;
			int rrc = recv_msg(cli->sock_fd, pkt, sizeof(pkt), &rlen);
			if (rrc == 0 && rlen < sizeof(jfs_xds_msg_hdr))
				rrc = -EPROTO;

			pthread_mutex_lock(&cli->rx_mu);
			cli->rx_active = 0;
			if (rrc != 0) {
				cli->rx_err = rrc;
				for (uint32_t i = 0; i < cli->waiter_count; i++) {
					cli->waiters[i]->rc = rrc;
					cli->waiters[i]->done = 1;
					pthread_cond_signal(&cli->waiters[i]->cond);
				}
				cli->waiter_count = 0;
				pthread_cond_broadcast(&cli->slot_cond);
			} else {
				jfs_xds_msg_hdr rhdr;
				memcpy(&rhdr, pkt, sizeof(rhdr));
				for (uint32_t i = 0; i < cli->waiter_count; i++) {
					if (cli->waiters[i]->req_id == rhdr.req_id) {
						struct jfs_xds_waiter *dst = cli->waiters[i];
						cli->waiters[i] = cli->waiters[--cli->waiter_count];
						if (rlen > dst->rep_cap) {
							dst->rc = -EMSGSIZE;
						} else {
							memcpy(dst->rep_buf, pkt, rlen);
							dst->rep_len = rlen;
							dst->rc = 0;
						}
						dst->done = 1;
						pthread_cond_signal(&dst->cond);
						pthread_cond_signal(&cli->slot_cond);
						break;
					}
				}
				/* Promote one remaining waiter to become the next reader. */
				if (cli->waiter_count > 0)
					pthread_cond_signal(&cli->waiters[0]->cond);
			}
		} else {
			pthread_cond_wait(&w.cond, &cli->rx_mu);
		}
	}
	for (uint32_t i = 0; i < cli->waiter_count; i++) {
		if (cli->waiters[i] == &w) {
			cli->waiters[i] = cli->waiters[--cli->waiter_count];
			pthread_cond_signal(&cli->slot_cond);
			break;
		}
	}
	pthread_mutex_unlock(&cli->rx_mu);
	pthread_cond_destroy(&w.cond);

	if (w.rc == 0 && rep_len_out)
		*rep_len_out = w.rep_len;
	return w.rc;
}

/* Minimal JSON string extractor for "socketPath": "..." inside .xds/.config. */
static int extract_json_string(const char *json, const char *key,
			       char *out, size_t cap)
{
	char pat[64];
	int pn = snprintf(pat, sizeof(pat), "\"%s\"", key);
	if (pn <= 0 || (size_t)pn >= sizeof(pat))
		return -EINVAL;

	const char *p = strstr(json, pat);
	if (!p)
		return -ENOENT;
	p += pn;
	while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')
		p++;
	if (*p != ':')
		return -EPROTO;
	p++;
	while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')
		p++;
	if (*p != '"')
		return -EPROTO;
	p++;

	size_t i = 0;
	while (*p && *p != '"') {
		if (*p == '\\' && p[1] != '\0')
			p++;
		if (i + 1 >= cap)
			return -ENAMETOOLONG;
		out[i++] = *p++;
	}
	if (*p != '"' || i == 0)
		return -ENOENT;
	out[i] = '\0';
	return 0;
}

static int read_endpoint_file(const char *path, char *sock_out, size_t sock_cap)
{
	int fd = open(path, O_RDONLY | O_CLOEXEC);
	if (fd < 0)
		return -errno;

	char *buf = malloc(JFS_XDS_ENDPOINT_MAX);
	if (!buf) {
		close(fd);
		return -ENOMEM;
	}
	size_t total = 0;
	while (total + 1 < JFS_XDS_ENDPOINT_MAX) {
		ssize_t n = read(fd, buf + total, JFS_XDS_ENDPOINT_MAX - 1 - total);
		if (n < 0) {
			if (errno == EINTR)
				continue;
			int err = -errno;
			free(buf);
			close(fd);
			return err;
		}
		if (n == 0)
			break;
		total += (size_t)n;
	}
	close(fd);
	if (total == 0) {
		free(buf);
		return -ENOENT;
	}
	buf[total] = '\0';
	int rc = extract_json_string(buf, "socketPath", sock_out, sock_cap);
	free(buf);
	return rc;
}

/* Unescapes Linux /proc/self/mountinfo octal sequences (e.g. \040 -> space). */
static void unescape_mountinfo(const char *src, char *dst, size_t cap)
{
	size_t di = 0;
	for (size_t si = 0; src[si] != '\0' && di + 1 < cap; si++) {
		if (src[si] == '\\' &&
		    src[si + 1] >= '0' && src[si + 1] <= '3' &&
		    src[si + 2] >= '0' && src[si + 2] <= '7' &&
		    src[si + 3] >= '0' && src[si + 3] <= '7') {
			unsigned int val =
				(unsigned int)(src[si + 1] - '0') * 64u +
				(unsigned int)(src[si + 2] - '0') * 8u +
				(unsigned int)(src[si + 3] - '0');
			dst[di++] = (char)val;
			si += 3;
		} else {
			dst[di++] = src[si];
		}
	}
	dst[di] = '\0';
}

int jfs_xds_discover_socket(int fd, char *sock_out, size_t sock_cap)
{
	if (!sock_out || sock_cap == 0)
		return -EINVAL;

	const char *env = getenv("JFS_XDS_SOCKET");
	if (env && env[0] != '\0') {
		size_t len = strlen(env);
		if (len >= sock_cap)
			return -ENAMETOOLONG;
		memcpy(sock_out, env, len + 1);
		return 0;
	}
	if (fd < 0)
		return -EINVAL;

	struct stat st;
	if (fstat(fd, &st) != 0)
		return -errno;

	unsigned int want_maj = major(st.st_dev);
	unsigned int want_min = minor(st.st_dev);

	FILE *fp = fopen("/proc/self/mountinfo", "re");
	if (!fp)
		return -errno;

	char line[8192];
	char raw_mp[PATH_MAX];
	char mp[PATH_MAX];
	int found = 0;
	while (fgets(line, sizeof(line), fp)) {
		unsigned int maj = 0, min = 0;
		if (sscanf(line, "%*d %*d %u:%u %*s %4095s",
			   &maj, &min, raw_mp) == 3) {
			if (maj == want_maj && min == want_min) {
				unescape_mountinfo(raw_mp, mp, sizeof(mp));
				found = 1;
				break;
			}
		}
	}
	fclose(fp);
	if (!found)
		return -ENOENT;

	static const char *const candidates[] = {
		".xds", ".jfs.xds", ".config", ".jfs.config"
	};
	for (size_t i = 0; i < sizeof(candidates) / sizeof(candidates[0]); i++) {
		char full[PATH_MAX];
		int n = snprintf(full, sizeof(full), "%s/%s", mp, candidates[i]);
		if (n > 0 && (size_t)n < sizeof(full)) {
			if (read_endpoint_file(full, sock_out, sock_cap) == 0)
				return 0;
		}
	}
	return -ENOENT;
}

int jfs_xds_connect(const char *socket_path, struct jfs_xds_client **out)
{
	if (!socket_path || !socket_path[0] || !out)
		return -EINVAL;

	struct sockaddr_un addr = {0};
	if (strlen(socket_path) >= sizeof(addr.sun_path))
		return -ENAMETOOLONG;

	int fd = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
	if (fd < 0)
		return -errno;

	addr.sun_family = AF_UNIX;
	memcpy(addr.sun_path, socket_path, strlen(socket_path) + 1);
	if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
		int err = -errno;
		close(fd);
		return err;
	}

	jfs_xds_hello hello = {0};
	hello.hdr.abi_version = JFS_XDS_ABI_VERSION;
	hello.hdr.type = JFS_XDS_MSG_HELLO;
	hello.hdr.msg_size = (uint32_t)sizeof(hello);
	hello.hdr.req_id = 1;
	hello.client_pid = (uint32_t)getpid();

	int rc = send_msg_with_fd(fd, &hello, sizeof(hello), -1);
	if (rc != 0) {
		close(fd);
		return rc;
	}

	jfs_xds_hello_reply rep = {0};
	size_t rlen = 0;
	rc = recv_msg(fd, &rep, sizeof(rep), &rlen);
	if (rc != 0) {
		close(fd);
		return rc;
	}
	if (rlen < sizeof(jfs_xds_msg_hdr) || rep.hdr.req_id != hello.hdr.req_id) {
		close(fd);
		return -EPROTO;
	}
	if (rep.hdr.status != 0) {
		close(fd);
		return rep.hdr.status;
	}
	if (rep.hdr.type != JFS_XDS_MSG_HELLO_REPLY || rlen != sizeof(rep) ||
	    rep.peer_count > JFS_XDS_MAX_PEERS) {
		close(fd);
		return -EPROTO;
	}

	struct jfs_xds_client *cli = calloc(1, sizeof(*cli));
	if (!cli) {
		close(fd);
		return -ENOMEM;
	}
	cli->sock_fd = fd;
	pthread_mutex_init(&cli->send_mu, NULL);
	pthread_mutex_init(&cli->rx_mu, NULL);
	pthread_cond_init(&cli->slot_cond, NULL);
	cli->next_req_id = 2;
	cli->info.mount_generation = rep.mount_generation;
	cli->info.io_unit_size = rep.io_unit_size;
	cli->info.block_size = rep.block_size;
	cli->info.max_inflight = rep.max_inflight;
	cli->info.device_backend = rep.device_backend;
	cli->info.peer_count = rep.peer_count;
	memcpy(cli->info.peer_bdfs, rep.peer_bdfs, sizeof(cli->info.peer_bdfs));

	*out = cli;
	return 0;
}

void jfs_xds_close(struct jfs_xds_client *cli)
{
	if (!cli)
		return;
	if (cli->sock_fd >= 0) {
		close(cli->sock_fd);
		cli->sock_fd = -1;
	}
	pthread_cond_destroy(&cli->slot_cond);
	pthread_mutex_destroy(&cli->rx_mu);
	pthread_mutex_destroy(&cli->send_mu);
	free(cli);
}

int jfs_xds_get_info(const struct jfs_xds_client *cli,
		     struct jfs_xds_client_info *out)
{
	if (!cli || !out)
		return -EINVAL;
	*out = cli->info;
	return 0;
}

int jfs_xds_bind_file(struct jfs_xds_client *cli, int fd,
		      uint64_t *file_token_out, uint64_t *file_size_out)
{
	if (!cli || fd < 0 || !file_token_out)
		return -EINVAL;

	jfs_xds_bind_file_req req = {0};
	req.hdr.abi_version = JFS_XDS_ABI_VERSION;
	req.hdr.type = JFS_XDS_MSG_BIND_FILE;
	req.hdr.msg_size = (uint32_t)sizeof(req);

	jfs_xds_bind_file_reply rep = {0};
	size_t rlen = 0;
	int rc = rpc_roundtrip(cli, &req, sizeof(req), fd, &rep, sizeof(rep), &rlen);
	if (rc != 0)
		return rc;
	if (rlen < sizeof(jfs_xds_msg_hdr))
		return -EPROTO;
	if (rep.hdr.status != 0)
		return rep.hdr.status;
	if (rep.hdr.type != JFS_XDS_MSG_BIND_FILE_REPLY || rlen != sizeof(rep))
		return -EPROTO;

	*file_token_out = rep.file_token;
	if (file_size_out)
		*file_size_out = rep.file_size;
	return 0;
}

int jfs_xds_unbind_file(struct jfs_xds_client *cli, uint64_t file_token)
{
	if (!cli || file_token == 0)
		return -EINVAL;

	jfs_xds_token_msg req = {0};
	req.hdr.abi_version = JFS_XDS_ABI_VERSION;
	req.hdr.type = JFS_XDS_MSG_UNBIND_FILE;
	req.hdr.msg_size = (uint32_t)sizeof(req);
	req.token = file_token;

	jfs_xds_msg_hdr rep = {0};
	size_t rlen = 0;
	int rc = rpc_roundtrip(cli, &req, sizeof(req), -1, &rep, sizeof(rep), &rlen);
	if (rc != 0)
		return rc;
	return (rlen >= sizeof(rep)) ? rep.status : -EPROTO;
}

int jfs_xds_register_buffer(struct jfs_xds_client *cli,
			    void *device_ptr, uint64_t size_bytes,
			    uint32_t dev_id, struct jfs_xds_buf **out)
{
	if (!cli || !device_ptr || size_bytes == 0 || !out ||
	    cli->info.peer_count == 0) {
		return -EINVAL;
	}

	struct jfs_xds_buf *buf = calloc(1, sizeof(*buf));
	if (!buf)
		return -ENOMEM;
	buf->p2p_ctx.fd = -1;
	buf->size_bytes = size_bytes;

	enum xds_p2p_backend backend =
		(enum xds_p2p_backend)cli->info.device_backend;
	int rc = xds_p2p_open(&buf->p2p_ctx, backend, NULL);
	if (rc != 0) {
		free(buf);
		return rc;
	}

	jfs_xds_register_buf req = {0};
	req.hdr.abi_version = JFS_XDS_ABI_VERSION;
	req.hdr.type = JFS_XDS_MSG_REGISTER_BUF;
	req.hdr.msg_size = (uint32_t)sizeof(req);
	req.device_va = (uint64_t)(uintptr_t)device_ptr;
	req.size_bytes = size_bytes;
	req.peer_count = cli->info.peer_count;

	for (uint32_t i = 0; i < cli->info.peer_count; i++) {
		struct xds_p2p_target target = {0};
		target.backend = backend;
		if (backend == XDS_P2P_BACKEND_ASCEND)
			target.dev_id = dev_id;
		else
			target.peer_bdf = cli->info.peer_bdfs[i];

		rc = xds_p2p_register(&buf->p2p_ctx, device_ptr, size_bytes,
				      &target, 0, &buf->handles[i]);
		if (rc != 0)
			goto fail_p2p;
		buf->peer_count = i + 1;

		struct xds_p2p_export exp = {0};
		rc = xds_p2p_export_handle(&buf->handles[i], &exp);
		if (rc != 0)
			goto fail_p2p;

		req.peers[i].handle = exp.handle;
		req.peers[i].backend = (uint32_t)exp.backend;
		req.peers[i].page_size = exp.page_size;
		req.peers[i].mapped_len = exp.mapped_len;
		req.peers[i].segment_count = exp.segment_count;
		req.peers[i].first_dma_addr = exp.first_dma_addr;
	}

	jfs_xds_token_msg rep = {0};
	size_t rlen = 0;
	rc = rpc_roundtrip(cli, &req, sizeof(req), buf->p2p_ctx.fd,
			   &rep, sizeof(rep), &rlen);
	if (rc == 0) {
		if (rlen < sizeof(jfs_xds_msg_hdr)) {
			rc = -EPROTO;
		} else if (rep.hdr.status != 0) {
			rc = rep.hdr.status;
		} else if (rep.hdr.type != JFS_XDS_MSG_REGISTER_BUF_REPLY ||
			   rlen != sizeof(rep)) {
			rc = -EPROTO;
		} else {
			buf->token = rep.token;
		}
	}
	if (rc != 0)
		goto fail_p2p;

	*out = buf;
	return 0;

fail_p2p:
	for (uint32_t i = 0; i < buf->peer_count; i++)
		(void)xds_p2p_unregister(&buf->p2p_ctx, &buf->handles[i]);
	xds_p2p_close(&buf->p2p_ctx);
	free(buf);
	return rc;
}

int jfs_xds_unregister_buffer(struct jfs_xds_client *cli,
			      struct jfs_xds_buf *buf)
{
	if (!cli || !buf)
		return -EINVAL;

	jfs_xds_token_msg req = {0};
	req.hdr.abi_version = JFS_XDS_ABI_VERSION;
	req.hdr.type = JFS_XDS_MSG_UNREGISTER_BUF;
	req.hdr.msg_size = (uint32_t)sizeof(req);
	req.token = buf->token;

	jfs_xds_msg_hdr rep = {0};
	size_t rlen = 0;
	int rc = rpc_roundtrip(cli, &req, sizeof(req), -1, &rep, sizeof(rep), &rlen);
	if (rc == 0)
		rc = (rlen >= sizeof(rep)) ? rep.status : -EPROTO;

	/* Only unregister kernel P2P handles when server confirmed DMA drain. */
	if (rc == 0 || rc == -ENOENT) {
		for (uint32_t i = 0; i < buf->peer_count; i++)
			(void)xds_p2p_unregister(&buf->p2p_ctx, &buf->handles[i]);
	}
	xds_p2p_close(&buf->p2p_ctx);
	free(buf);
	return rc;
}

int jfs_xds_readv(struct jfs_xds_client *cli,
		  uint64_t file_token, uint64_t file_offset, uint64_t length,
		  const struct jfs_xds_iov *iovs, uint32_t iovcnt,
		  jfs_xds_hole *holes_out, uint32_t *hole_count_inout,
		  uint64_t *bytes_read_out)
{
	if (!cli || file_token == 0 || length == 0 || !iovs ||
	    iovcnt == 0 || iovcnt > JFS_XDS_MAX_SLICES) {
		return -EINVAL;
	}

	uint8_t req_pkt[sizeof(jfs_xds_read_direct_req) +
			JFS_XDS_MAX_SLICES * sizeof(jfs_xds_slice)];
	uint8_t rep_pkt[JFS_XDS_MAX_REPLY_PKT];

	jfs_xds_read_direct_req req = {0};
	req.hdr.abi_version = JFS_XDS_ABI_VERSION;
	req.hdr.type = JFS_XDS_MSG_READ_DIRECT;
	req.hdr.msg_size = (uint32_t)(sizeof(req) + iovcnt * sizeof(jfs_xds_slice));
	req.file_token = file_token;
	req.file_offset = file_offset;
	req.length = length;
	req.slice_count = iovcnt;
	memcpy(req_pkt, &req, sizeof(req));

	jfs_xds_slice *dst_slices = (jfs_xds_slice *)(req_pkt + sizeof(req));
	for (uint32_t i = 0; i < iovcnt; i++) {
		if (!iovs[i].buf || iovs[i].length == 0)
			return -EINVAL;
		dst_slices[i].buf_token = iovs[i].buf->token;
		dst_slices[i].buf_offset = iovs[i].offset;
		dst_slices[i].length = iovs[i].length;
	}

	size_t rlen = 0;
	int rc = rpc_roundtrip(cli, req_pkt, req.hdr.msg_size, -1,
			       rep_pkt, sizeof(rep_pkt), &rlen);
	if (rc != 0)
		return rc;
	if (rlen < sizeof(jfs_xds_msg_hdr))
		return -EPROTO;

	jfs_xds_msg_hdr hdr;
	memcpy(&hdr, rep_pkt, sizeof(hdr));
	if (hdr.status != 0)
		return hdr.status;
	if (hdr.type != JFS_XDS_MSG_READ_DIRECT_REPLY ||
	    rlen < sizeof(jfs_xds_read_direct_reply)) {
		return -EPROTO;
	}

	jfs_xds_read_direct_reply rep;
	memcpy(&rep, rep_pkt, sizeof(rep));
	size_t expected = sizeof(rep) +
		(size_t)rep.hole_count * sizeof(jfs_xds_hole);
	if (rep.hole_count > JFS_XDS_MAX_HOLES || rlen != expected)
		return -EPROTO;

	if (bytes_read_out)
		*bytes_read_out = rep.bytes_read;

	if (rep.hole_count > 0) {
		if (!holes_out || !hole_count_inout) {
			if (hole_count_inout)
				*hole_count_inout = rep.hole_count;
			return -ENOSPC;
		}
		uint32_t cap = *hole_count_inout;
		uint32_t copy_n = rep.hole_count < cap ? rep.hole_count : cap;
		if (copy_n > 0) {
			memcpy(holes_out, rep_pkt + sizeof(rep),
			       (size_t)copy_n * sizeof(jfs_xds_hole));
		}
		*hole_count_inout = rep.hole_count;
		if (cap < rep.hole_count)
			return -ENOSPC;
	} else if (hole_count_inout) {
		*hole_count_inout = 0;
	}
	return 0;
}

int jfs_xds_read(struct jfs_xds_client *cli,
		 uint64_t file_token, uint64_t file_offset,
		 struct jfs_xds_buf *buf, uint64_t buf_offset, uint64_t length,
		 jfs_xds_hole *holes_out, uint32_t *hole_count_inout,
		 uint64_t *bytes_read_out)
{
	struct jfs_xds_iov iov = {
		.buf = buf,
		.offset = buf_offset,
		.length = length,
	};
	return jfs_xds_readv(cli, file_token, file_offset, length, &iov, 1,
			     holes_out, hole_count_inout, bytes_read_out);
}
