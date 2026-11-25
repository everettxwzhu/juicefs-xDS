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

#ifndef JFS_XDS_PROTO_H
#define JFS_XDS_PROTO_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#if defined(__cplusplus) && __cplusplus >= 201103L
#define JFS_XDS_STATIC_ASSERT(cond, msg) static_assert(cond, msg)
#elif defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
#define JFS_XDS_STATIC_ASSERT(cond, msg) _Static_assert(cond, msg)
#else
#define JFS_XDS_STATIC_ASSERT(cond, msg)
#endif

#define JFS_XDS_ABI_VERSION     1u
#define JFS_XDS_MAX_PEERS       8u
#define JFS_XDS_BDF_LEN         16u
#define JFS_XDS_MAX_SLICES      64u
#define JFS_XDS_MAX_HOLES       32u

/* Unix SOCK_SEQPACKET message types (supports req_id pipelining). */
enum jfs_xds_msg_type {
	JFS_XDS_MSG_HELLO              = 1,
	JFS_XDS_MSG_HELLO_REPLY        = 2,
	JFS_XDS_MSG_BIND_FILE          = 3,
	JFS_XDS_MSG_BIND_FILE_REPLY    = 4,
	JFS_XDS_MSG_UNBIND_FILE        = 5,
	JFS_XDS_MSG_REGISTER_BUF       = 6,
	JFS_XDS_MSG_REGISTER_BUF_REPLY = 7,
	JFS_XDS_MSG_UNREGISTER_BUF     = 8,
	JFS_XDS_MSG_READ_DIRECT        = 9,
	JFS_XDS_MSG_READ_DIRECT_REPLY  = 10,
	JFS_XDS_MSG_STATUS             = 11,
};

typedef struct jfs_xds_msg_hdr {
	uint32_t abi_version;
	uint32_t type;
	uint32_t msg_size;
	int32_t  status;
	uint64_t req_id;
} jfs_xds_msg_hdr;

typedef struct jfs_xds_hello {
	jfs_xds_msg_hdr hdr;
	uint32_t        client_pid;
	uint32_t        reserved;
} jfs_xds_hello;

typedef struct jfs_xds_hello_reply {
	jfs_xds_msg_hdr hdr;
	uint64_t        mount_generation;
	uint64_t        io_unit_size;
	uint64_t        block_size;
	uint32_t        max_inflight;
	uint32_t        device_backend;
	uint32_t        peer_count;
	uint32_t        reserved;
	char            peer_bdfs[JFS_XDS_MAX_PEERS][JFS_XDS_BDF_LEN];
} jfs_xds_hello_reply;

/* Sent with 1 fd via SCM_RIGHTS: client's opened readable file descriptor. */
typedef struct jfs_xds_bind_file {
	jfs_xds_msg_hdr hdr;
} jfs_xds_bind_file;

typedef struct jfs_xds_bind_file_reply {
	jfs_xds_msg_hdr hdr;
	uint64_t        file_token;
	uint64_t        inode;
	uint64_t        file_size;
} jfs_xds_bind_file_reply;

/* Used by UNBIND_FILE, REGISTER_BUF_REPLY, and UNREGISTER_BUF. */
typedef struct jfs_xds_token_msg {
	jfs_xds_msg_hdr hdr;
	uint64_t        token;
} jfs_xds_token_msg;

/* Mirrors xds_ctrl_gpu_peer_export without modifying xDS headers. */
typedef struct jfs_xds_peer_export {
	int32_t  handle;
	uint32_t backend;
	uint64_t page_size;
	uint64_t mapped_len;
	uint32_t segment_count;
	uint32_t reserved;
	uint64_t first_dma_addr;
} jfs_xds_peer_export;

/* Sent with 1 fd via SCM_RIGHTS: xDS P2P device fd owning exported handles. */
typedef struct jfs_xds_register_buf {
	jfs_xds_msg_hdr     hdr;
	uint64_t            device_va;
	uint64_t            size_bytes;
	uint32_t            peer_count;
	uint32_t            reserved;
	jfs_xds_peer_export peers[JFS_XDS_MAX_PEERS];
} jfs_xds_register_buf;

typedef struct jfs_xds_slice {
	uint64_t buf_token;
	uint64_t buf_offset;
	uint64_t length;
} jfs_xds_slice;

typedef struct jfs_xds_hole {
	uint64_t dst_offset;
	uint64_t length;
	uint32_t slice_index;
	uint32_t reserved;
} jfs_xds_hole;

/*
 * Followed immediately on the wire by slice_count * jfs_xds_slice entries
 * (1 <= slice_count <= JFS_XDS_MAX_SLICES), unified for scalar and scatter reads.
 */
typedef struct jfs_xds_read_direct_req {
	jfs_xds_msg_hdr hdr;
	uint64_t        file_token;
	uint64_t        file_offset;
	uint64_t        length;
	uint32_t        slice_count;
	uint32_t        flags;
} jfs_xds_read_direct_req;

/*
 * Followed immediately on the wire by hole_count * jfs_xds_hole entries
 * (0 <= hole_count <= JFS_XDS_MAX_HOLES).
 */
typedef struct jfs_xds_read_direct_reply {
	jfs_xds_msg_hdr hdr;
	uint64_t        bytes_read;
	uint32_t        hole_count;
	uint32_t        reserved;
} jfs_xds_read_direct_reply;

JFS_XDS_STATIC_ASSERT(sizeof(jfs_xds_msg_hdr) == 24, "msg_hdr size mismatch");
JFS_XDS_STATIC_ASSERT(sizeof(jfs_xds_hello) == 32, "hello size mismatch");
JFS_XDS_STATIC_ASSERT(sizeof(jfs_xds_hello_reply) == 192, "hello_reply size mismatch");
JFS_XDS_STATIC_ASSERT(sizeof(jfs_xds_bind_file_reply) == 48, "bind_file_reply size mismatch");
JFS_XDS_STATIC_ASSERT(sizeof(jfs_xds_token_msg) == 32, "token_msg size mismatch");
JFS_XDS_STATIC_ASSERT(sizeof(jfs_xds_peer_export) == 40, "peer_export size mismatch");
JFS_XDS_STATIC_ASSERT(sizeof(jfs_xds_register_buf) == 368, "register_buf size mismatch");
JFS_XDS_STATIC_ASSERT(sizeof(jfs_xds_slice) == 24, "slice size mismatch");
JFS_XDS_STATIC_ASSERT(sizeof(jfs_xds_hole) == 24, "hole size mismatch");
JFS_XDS_STATIC_ASSERT(sizeof(jfs_xds_read_direct_req) == 56, "read_direct_req size mismatch");
JFS_XDS_STATIC_ASSERT(sizeof(jfs_xds_read_direct_reply) == 40, "read_direct_reply size mismatch");

#ifdef __cplusplus
}
#endif

#endif /* JFS_XDS_PROTO_H */
