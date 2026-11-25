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

#ifndef JFS_XDS_CLIENT_H
#define JFS_XDS_CLIENT_H

#include <stddef.h>
#include <stdint.h>

#include "proto.h"

#ifdef __cplusplus
extern "C" {
#endif

struct jfs_xds_client;
struct jfs_xds_buf;

/* Static endpoint geometry returned by HELLO handshake. */
struct jfs_xds_client_info {
	uint64_t mount_generation;
	uint64_t io_unit_size;
	uint64_t block_size;
	uint32_t max_inflight;
	uint32_t device_backend;
	uint32_t peer_count;
	char     peer_bdfs[JFS_XDS_MAX_PEERS][JFS_XDS_BDF_LEN];
};

/* One destination slice inside a registered device buffer for vector readv. */
struct jfs_xds_iov {
	struct jfs_xds_buf *buf;
	uint64_t            offset;
	uint64_t            length;
};

/*
 * Auto-discovers the JuiceFS xDS socket path for an open file descriptor:
 * checks JFS_XDS_SOCKET env first, then resolves fd's mountpoint from
 * /proc/self/mountinfo (unescaping mountinfo octal escapes) and reads
 * <mountpoint>/.xds (or .jfs.xds / .config).
 * Returns 0 on success or a negative errno.
 */
int jfs_xds_discover_socket(int fd, char *sock_out, size_t sock_cap);

/*
 * Connects to JuiceFS xDS Unix SOCK_SEQPACKET server and completes HELLO.
 * A single jfs_xds_client safely multiplexes concurrent RPCs across threads
 * using req_id matching so callers can pipeline up to max_inflight reads.
 */
int jfs_xds_connect(const char *socket_path, struct jfs_xds_client **out);

/*
 * Closes the client connection (server auto-drains & unregisters buffers).
 * Must not be called concurrently with active RPCs on the same client.
 */
void jfs_xds_close(struct jfs_xds_client *cli);

/* Copies static handshake geometry into out. */
int jfs_xds_get_info(const struct jfs_xds_client *cli,
		     struct jfs_xds_client_info *out);

/* Binds an opened readable JuiceFS file descriptor into a reusable file token. */
int jfs_xds_bind_file(struct jfs_xds_client *cli, int fd,
		      uint64_t *file_token_out, uint64_t *file_size_out);

/* Releases a previously bound file token. */
int jfs_xds_unbind_file(struct jfs_xds_client *cli, uint64_t file_token);

/*
 * Registers device memory (NVIDIA/Ascend/Iluvatar/AMD) via xDS P2P kernel
 * driver and imports it into the JuiceFS server over SCM_RIGHTS.
 * dev_id is used only by backends that address peers by device ID (e.g. Ascend).
 */
int jfs_xds_register_buffer(struct jfs_xds_client *cli,
			    void *device_ptr, uint64_t size_bytes,
			    uint32_t dev_id, struct jfs_xds_buf **out);

/*
 * Requests server DMA drain + unregister, then releases local P2P handles and
 * frees buf. On return == 0 (or -ENOENT), the server confirmed no DMA targets
 * this buffer and the caller may safely free the underlying device memory.
 * On any other error, server drain is unconfirmed; close the client before
 * freeing device memory.
 */
int jfs_xds_unregister_buffer(struct jfs_xds_client *cli,
			      struct jfs_xds_buf *buf);

/*
 * Performs a scalar NVMe-to-device P2P direct read into a single registered
 * buffer. If the file range contains sparse holes (hole_count > 0), holes_out
 * and hole_count_inout MUST be non-NULL with sufficient capacity so the caller
 * can zero-fill those device spans; otherwise -ENOSPC is returned (with the
 * required count stored in *hole_count_inout if non-NULL).
 */
int jfs_xds_read(struct jfs_xds_client *cli,
		 uint64_t file_token, uint64_t file_offset,
		 struct jfs_xds_buf *buf, uint64_t buf_offset, uint64_t length,
		 jfs_xds_hole *holes_out, uint32_t *hole_count_inout,
		 uint64_t *bytes_read_out);

/*
 * Performs a scatter NVMe-to-device P2P direct read across 1..JFS_XDS_MAX_SLICES
 * destination slices in a single RPC. Sparse-hole contract matches jfs_xds_read.
 */
int jfs_xds_readv(struct jfs_xds_client *cli,
		  uint64_t file_token, uint64_t file_offset, uint64_t length,
		  const struct jfs_xds_iov *iovs, uint32_t iovcnt,
		  jfs_xds_hole *holes_out, uint32_t *hole_count_inout,
		  uint64_t *bytes_read_out);

#ifdef __cplusplus
}
#endif

#endif /* JFS_XDS_CLIENT_H */
