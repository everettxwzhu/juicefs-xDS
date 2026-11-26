//go:build linux && xds

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

#include "runtime_cgo.h"

#include <errno.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/uio.h>
#include <unistd.h>

#include "xds/blob_pool.h"
#include "xds/dma_registry.h"
#include "xds/io.h"
#include "xds/runtime.h"

#define JFS_XDS_CQ_MASK     (JFS_XDS_CQ_CAP - 1u)
#define JFS_XDS_STACK_DESCS 32u

struct jfs_xds_cq_slot {
	_Atomic uint64_t seq;
	uint32_t         slot_id;
	int32_t          status;
};

struct jfs_xds_io_slot {
	struct iovec  inline_iovs[JFS_XDS_INLINE_IOVS];
	struct iovec *dyn_iovs; // heap fallback only when total_iovs > 64
	void         *dma_buf;  // SPDK hugepage buffer for WriteCPU
};

// Process-internal wait-free MPSC completion queue shared between SPDK polling
// reactor threads (producers) and one Go netpoller dispatcher goroutine
// (consumer). Because active slots are bounded by JFS_XDS_CQ_CAP and a slot is
// only recycled after its CQE is drained, the ring can never overflow.
static struct {
	struct jfs_xds_cq_slot *entries;
	struct jfs_xds_io_slot *slots;
	_Atomic uint64_t        prod_tail;
	uint64_t                cons_head;
	_Atomic uint32_t        armed;
	int                     evfd;
} g_cq = { .evfd = -1 };

static uint64_t jfs_xds_align_up(uint64_t v, uint64_t align)
{
	return (v + align - 1) & ~(align - 1);
}

int jfs_xds_cq_init(int *evfd_out)
{
	if (!evfd_out)
		return -EINVAL;
	if (g_cq.entries != NULL || g_cq.slots != NULL || g_cq.evfd >= 0)
		return -EBUSY;

	g_cq.entries = calloc(JFS_XDS_CQ_CAP, sizeof(*g_cq.entries));
	g_cq.slots = calloc(JFS_XDS_CQ_CAP, sizeof(*g_cq.slots));
	if (!g_cq.entries || !g_cq.slots) {
		free(g_cq.entries);
		free(g_cq.slots);
		g_cq.entries = NULL;
		g_cq.slots = NULL;
		return -ENOMEM;
	}
	for (uint32_t i = 0; i < JFS_XDS_CQ_CAP; i++)
		atomic_init(&g_cq.entries[i].seq, (uint64_t)i);

	atomic_init(&g_cq.prod_tail, 0u);
	g_cq.cons_head = 0u;
	atomic_init(&g_cq.armed, 0u);

	int fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
	if (fd < 0) {
		int err = -errno;
		free(g_cq.entries);
		free(g_cq.slots);
		g_cq.entries = NULL;
		g_cq.slots = NULL;
		return err;
	}
	g_cq.evfd = fd;
	*evfd_out = fd;
	return 0;
}

// Called only after SPDK stopped and Go's cqFile closed evfd.
int jfs_xds_cq_fini(void)
{
	g_cq.evfd = -1;
	free(g_cq.entries);
	free(g_cq.slots);
	g_cq.entries = NULL;
	g_cq.slots = NULL;
	return 0;
}

// SPDK reactor completion callback: wait-free MPSC enqueue (~10ns) with
// coalesced eventfd wakeup (0 syscalls under steady load while armed == 0).
static void jfs_xds_cq_cb(void *cb_arg, int status,
	enum xds_completion_result_type result_type, uint64_t result)
{
	uint32_t slot_id = (uint32_t)(uintptr_t)cb_arg;
	(void)result_type;
	(void)result;

	uint64_t pos = atomic_fetch_add_explicit(&g_cq.prod_tail, 1u,
		memory_order_relaxed);
	struct jfs_xds_cq_slot *s = &g_cq.entries[pos & JFS_XDS_CQ_MASK];

	s->slot_id = slot_id;
	s->status = status;
	atomic_store_explicit(&s->seq, pos + 1u, memory_order_release);

	// Dekker seq_cst fence pairs with jfs_xds_cq_arm to prevent missed wakeups.
	atomic_thread_fence(memory_order_seq_cst);
	if (atomic_load_explicit(&g_cq.armed, memory_order_relaxed) != 0 &&
	    atomic_exchange_explicit(&g_cq.armed, 0u, memory_order_acq_rel) != 0) {
		uint64_t one = 1;
		ssize_t ignored = write(g_cq.evfd, &one, sizeof(one));
		(void)ignored;
	}
}

uint32_t jfs_xds_cq_drain(struct jfs_xds_cqe *out, uint32_t max_out)
{
	uint32_t n = 0;
	uint64_t head = g_cq.cons_head;

	while (n < max_out) {
		struct jfs_xds_cq_slot *s = &g_cq.entries[head & JFS_XDS_CQ_MASK];
		if (atomic_load_explicit(&s->seq, memory_order_acquire) != head + 1u)
			break;
		out[n].slot_id = s->slot_id;
		out[n].status = s->status;
		head++;
		n++;
	}
	g_cq.cons_head = head;
	return n;
}

int jfs_xds_cq_has_ready(void)
{
	uint64_t head = g_cq.cons_head;
	struct jfs_xds_cq_slot *s = &g_cq.entries[head & JFS_XDS_CQ_MASK];
	return atomic_load_explicit(&s->seq, memory_order_acquire) == head + 1u;
}

// Arms eventfd notifications and returns 1 if a completion is already ready.
int jfs_xds_cq_arm(void)
{
	atomic_store_explicit(&g_cq.armed, 1u, memory_order_relaxed);
	atomic_thread_fence(memory_order_seq_cst);
	if (jfs_xds_cq_has_ready()) {
		atomic_store_explicit(&g_cq.armed, 0u, memory_order_relaxed);
		return 1;
	}
	return 0;
}

int jfs_xds_get_blob(uintptr_t *out)
{
	xds_blob_handle_t h = NULL;
	int rc = xds_blob_pool_get_blob(0, 5000, &h);
	if (rc == 0)
		*out = (uintptr_t)h;
	return rc;
}

int jfs_xds_release_blob(uintptr_t h)
{
	return xds_blob_pool_release_blob(0, (xds_blob_handle_t)h);
}

int jfs_xds_import_buf(int p2p_fd, uint64_t device_va, uint64_t size,
	const struct xds_dma_import_peer *peers, size_t peer_count,
	uint64_t *reg_out, uintptr_t *io_ptr_out)
{
	uint64_t reg = xds_direct_import_buffer(p2p_fd,
		(void *)(uintptr_t)device_va, size, peers, peer_count);
	if (reg == 0)
		return -ENODEV;
	void *io_ptr = xds_direct_buffer_io_ptr(reg);
	if (!io_ptr) {
		(void)xds_direct_unregister_buffer(reg);
		return -ENODEV;
	}
	*reg_out = reg;
	*io_ptr_out = (uintptr_t)io_ptr;
	return 0;
}

int jfs_xds_submit_write_cpu(uint32_t slot_id, uintptr_t blob_handle,
	uint64_t blob_off, const void *src, uint64_t len)
{
	uint64_t unit = xds_runtime_get_io_unit_size();
	if (slot_id >= JFS_XDS_CQ_CAP || !blob_handle || !src || len == 0 ||
	    unit == 0 || (blob_off % unit) != 0) {
		return -EINVAL;
	}

	uint64_t aligned_len = jfs_xds_align_up(len, unit);
	void *dma_buf = xds_alloc_io_buffer(aligned_len);
	if (!dma_buf)
		return -ENOMEM;

	memcpy(dma_buf, src, (size_t)len);
	if (aligned_len > len)
		memset((char *)dma_buf + len, 0, (size_t)(aligned_len - len));

	g_cq.slots[slot_id].dma_buf = dma_buf;
	struct xds_io_desc desc = {
		.handle = (void *)blob_handle,
		.payload = dma_buf,
		.offset_bytes = blob_off,
		.size_bytes = aligned_len,
	};
	int rc = xds_submit_write(&desc, jfs_xds_cq_cb, (void *)(uintptr_t)slot_id);
	if (rc != 0) {
		g_cq.slots[slot_id].dma_buf = NULL;
		xds_free_io_buffer(dma_buf);
	}
	return rc;
}

void jfs_xds_free_slot_dma(uint32_t slot_id)
{
	void *dma_buf = g_cq.slots[slot_id].dma_buf;
	g_cq.slots[slot_id].dma_buf = NULL;
	if (dma_buf)
		xds_free_io_buffer(dma_buf);
}

int jfs_xds_submit_direct_batch(uint32_t slot_id,
	const struct jfs_xds_desc *descs, uint32_t desc_count,
	const struct jfs_xds_iov *iovs, uint32_t iov_count)
{
	struct xds_io_desc stack_descs[JFS_XDS_STACK_DESCS];
	struct xds_io_desc *xds_descs = stack_descs;
	struct jfs_xds_io_slot *slot;
	struct iovec *xds_iovs = NULL;
	size_t total_iovs = 0, iov_cursor = 0;

	if (slot_id >= JFS_XDS_CQ_CAP || !descs || desc_count == 0)
		return -EINVAL;

	for (uint32_t i = 0; i < desc_count; i++) {
		if (descs[i].iovcnt > 0) {
			if (!iovs ||
			    descs[i].iov_offset > iov_count ||
			    descs[i].iovcnt > iov_count - descs[i].iov_offset) {
				return -EINVAL;
			}
			total_iovs += descs[i].iovcnt;
		}
	}

	slot = &g_cq.slots[slot_id];
	if (total_iovs > 0) {
		if (total_iovs <= JFS_XDS_INLINE_IOVS) {
			xds_iovs = slot->inline_iovs;
		} else {
			xds_iovs = calloc(total_iovs, sizeof(*xds_iovs));
			if (!xds_iovs)
				return -ENOMEM;
			slot->dyn_iovs = xds_iovs;
		}
	}

	if (desc_count > JFS_XDS_STACK_DESCS) {
		xds_descs = calloc(desc_count, sizeof(*xds_descs));
		if (!xds_descs) {
			free(slot->dyn_iovs);
			slot->dyn_iovs = NULL;
			return -ENOMEM;
		}
	} else {
		memset(stack_descs, 0, (size_t)desc_count * sizeof(*xds_descs));
	}

	for (uint32_t i = 0; i < desc_count; i++) {
		xds_descs[i].handle = (void *)descs[i].blob_handle;
		xds_descs[i].payload = (void *)descs[i].payload;
		xds_descs[i].offset_bytes = descs[i].offset_bytes;
		xds_descs[i].size_bytes = descs[i].size_bytes;
		if (descs[i].iovcnt > 0) {
			struct iovec *dst_iov = &xds_iovs[iov_cursor];
			const struct jfs_xds_iov *src_iov = &iovs[descs[i].iov_offset];
			for (uint32_t j = 0; j < descs[i].iovcnt; j++) {
				dst_iov[j].iov_base = (void *)src_iov[j].base;
				dst_iov[j].iov_len = (size_t)src_iov[j].len;
			}
			xds_descs[i].iovs = dst_iov;
			xds_descs[i].iovcnt = descs[i].iovcnt;
			iov_cursor += descs[i].iovcnt;
		}
	}

	// xds_submit_read_batch copies xds_descs synchronously, while retaining
	// iovs pointers inside slot->inline_iovs / slot->dyn_iovs until cb fires.
	int rc = xds_submit_read_batch(xds_descs, desc_count, jfs_xds_cq_cb,
		(void *)(uintptr_t)slot_id);
	if (xds_descs != stack_descs)
		free(xds_descs);
	if (rc != 0) {
		free(slot->dyn_iovs);
		slot->dyn_iovs = NULL;
	}
	return rc;
}

void jfs_xds_free_slot_iovs(uint32_t slot_id)
{
	if (g_cq.slots[slot_id].dyn_iovs) {
		free(g_cq.slots[slot_id].dyn_iovs);
		g_cq.slots[slot_id].dyn_iovs = NULL;
	}
}
