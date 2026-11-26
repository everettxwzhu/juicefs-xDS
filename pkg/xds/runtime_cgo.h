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

#ifndef JFS_XDS_RUNTIME_CGO_H
#define JFS_XDS_RUNTIME_CGO_H

#include <stddef.h>
#include <stdint.h>

struct xds_dma_import_peer;

#define JFS_XDS_CQ_CAP      4096u
#define JFS_XDS_INLINE_IOVS 64u

struct jfs_xds_iov {
	uintptr_t base;
	uint64_t  len;
};

struct jfs_xds_desc {
	uintptr_t blob_handle;
	uintptr_t payload;
	uint64_t  offset_bytes;
	uint64_t  size_bytes;
	uint32_t  iov_offset;
	uint32_t  iovcnt;
};

struct jfs_xds_cqe {
	uint32_t slot_id;
	int32_t  status;
};

int jfs_xds_cq_init(int *evfd_out);
int jfs_xds_cq_fini(void);
uint32_t jfs_xds_cq_drain(struct jfs_xds_cqe *out, uint32_t max_out);
int jfs_xds_cq_has_ready(void);
int jfs_xds_cq_arm(void);

int jfs_xds_get_blob(uintptr_t *out);
int jfs_xds_release_blob(uintptr_t h);
int jfs_xds_import_buf(int p2p_fd, uint64_t device_va, uint64_t size,
	const struct xds_dma_import_peer *peers, size_t peer_count,
	uint64_t *reg_out, uintptr_t *io_ptr_out);

int jfs_xds_submit_write_cpu(uint32_t slot_id, uintptr_t blob_handle,
	uint64_t blob_off, const void *src, uint64_t len);
void jfs_xds_free_slot_dma(uint32_t slot_id);

int jfs_xds_submit_direct_batch(uint32_t slot_id,
	const struct jfs_xds_desc *descs, uint32_t desc_count,
	const struct jfs_xds_iov *iovs, uint32_t iov_count);
void jfs_xds_free_slot_iovs(uint32_t slot_id);

#endif /* JFS_XDS_RUNTIME_CGO_H */
