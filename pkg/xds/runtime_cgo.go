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

package xds

/*
#cgo CFLAGS: -I${SRCDIR}/../../xDS/include -I${SRCDIR}/../../xDS/kernels/include -std=gnu11 -D_GNU_SOURCE
#cgo LDFLAGS: -L${SRCDIR}/../../dist/xds -ljfs_xds_runtime -pthread

#include <errno.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/uio.h>
#include <unistd.h>

#include "xds/blob_pool.h"
#include "xds/dma_registry.h"
#include "xds/io.h"
#include "xds/runtime.h"

#define JFS_XDS_CQ_CAP      4096u
#define JFS_XDS_CQ_MASK     (JFS_XDS_CQ_CAP - 1u)
#define JFS_XDS_STACK_DESCS 32u
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

struct jfs_xds_cq_slot {
	_Atomic uint64_t seq;
	uint32_t         slot_id;
	int32_t          status;
};

struct jfs_xds_io_slot {
	struct iovec  inline_iovs[JFS_XDS_INLINE_IOVS];
	struct iovec *dyn_iovs; // heap fallback only when total_iovs > 64
	void         *dma_buf;  // SPDK hugepage buffer for ReadCPU/WriteCPU
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

static int jfs_xds_cq_init(int *evfd_out)
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
static int jfs_xds_cq_fini(void)
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

static uint32_t jfs_xds_cq_drain(struct jfs_xds_cqe *out, uint32_t max_out)
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

static int jfs_xds_cq_has_ready(void)
{
	uint64_t head = g_cq.cons_head;
	struct jfs_xds_cq_slot *s = &g_cq.entries[head & JFS_XDS_CQ_MASK];
	return atomic_load_explicit(&s->seq, memory_order_acquire) == head + 1u;
}

// Arms eventfd notifications and returns 1 if a completion is already ready.
static int jfs_xds_cq_arm(void)
{
	atomic_store_explicit(&g_cq.armed, 1u, memory_order_relaxed);
	atomic_thread_fence(memory_order_seq_cst);
	if (jfs_xds_cq_has_ready()) {
		atomic_store_explicit(&g_cq.armed, 0u, memory_order_relaxed);
		return 1;
	}
	return 0;
}

static inline int jfs_xds_get_blob(uintptr_t *out)
{
	xds_blob_handle_t h = NULL;
	int rc = xds_blob_pool_get_blob(0, 5000, &h);
	if (rc == 0)
		*out = (uintptr_t)h;
	return rc;
}

static inline int jfs_xds_release_blob(uintptr_t h)
{
	return xds_blob_pool_release_blob(0, (xds_blob_handle_t)h);
}

static int jfs_xds_import_buf(int p2p_fd, uint64_t device_va, uint64_t size,
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

static int jfs_xds_submit_read_cpu(uint32_t slot_id, uintptr_t blob_handle,
	uint64_t blob_off, uint64_t len)
{
	uint64_t unit = xds_runtime_get_io_unit_size();
	if (slot_id >= JFS_XDS_CQ_CAP || !blob_handle || len == 0 ||
	    unit == 0 || (blob_off % unit) != 0) {
		return -EINVAL;
	}

	uint64_t aligned_len = jfs_xds_align_up(len, unit);
	void *dma_buf = xds_alloc_io_buffer(aligned_len);
	if (!dma_buf)
		return -ENOMEM;

	g_cq.slots[slot_id].dma_buf = dma_buf;
	struct xds_io_desc desc = {
		.handle = (void *)blob_handle,
		.payload = dma_buf,
		.offset_bytes = blob_off,
		.size_bytes = aligned_len,
	};
	int rc = xds_submit_read(&desc, jfs_xds_cq_cb, (void *)(uintptr_t)slot_id);
	if (rc != 0) {
		g_cq.slots[slot_id].dma_buf = NULL;
		xds_free_io_buffer(dma_buf);
	}
	return rc;
}

static void jfs_xds_finish_read_cpu(uint32_t slot_id, void *dst,
	uint64_t len, int copy_ok)
{
	void *dma_buf = g_cq.slots[slot_id].dma_buf;
	g_cq.slots[slot_id].dma_buf = NULL;
	if (!dma_buf)
		return;
	if (copy_ok && dst && len > 0)
		memcpy(dst, dma_buf, (size_t)len);
	xds_free_io_buffer(dma_buf);
}

static int jfs_xds_submit_write_cpu(uint32_t slot_id, uintptr_t blob_handle,
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

static void jfs_xds_free_slot_dma(uint32_t slot_id)
{
	void *dma_buf = g_cq.slots[slot_id].dma_buf;
	g_cq.slots[slot_id].dma_buf = NULL;
	if (dma_buf)
		xds_free_io_buffer(dma_buf);
}

static int jfs_xds_submit_direct_batch(uint32_t slot_id,
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

static void jfs_xds_free_slot_iovs(uint32_t slot_id)
{
	if (g_cq.slots[slot_id].dyn_iovs) {
		free(g_cq.slots[slot_id].dyn_iovs);
		g_cq.slots[slot_id].dyn_iovs = NULL;
	}
}
*/
import "C"

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

const (
	cgoCQCap      = uint32(C.JFS_XDS_CQ_CAP)
	cgoInlineIOVs = int(C.JFS_XDS_INLINE_IOVS)
	cgoDrainBatch = 64
)

// cgoRuntime implements Runtime over the embedded xDS SPDK C API using an
// internal lock-free MPSC completion queue and a single Go netpoller dispatcher
// so callers never pin OS threads in blocking CGo calls.
type cgoRuntime struct {
	freeSlots chan uint32
	slotCh    []chan error
	cqFile    *os.File
	cqStop    atomic.Bool
	closingCh chan struct{}
	cqWG      sync.WaitGroup

	ioMu    sync.RWMutex
	ioWG    sync.WaitGroup
	started bool
}

// NewDefaultRuntime returns the embedded xDS SPDK Runtime on Linux builds with
// the xds build tag enabled.
func NewDefaultRuntime() Runtime {
	return &cgoRuntime{}
}

func (r *cgoRuntime) Start(cfg *Config) (RuntimeInfo, error) {
	cBdev := C.CString(cfg.BdevName)
	cJSON := C.CString(cfg.JSONConfigFile)
	cMask := C.CString(cfg.ReactorMask)
	cRPC := C.CString(cfg.RPCAddr)
	defer func() {
		C.free(unsafe.Pointer(cBdev))
		C.free(unsafe.Pointer(cJSON))
		C.free(unsafe.Pointer(cMask))
		C.free(unsafe.Pointer(cRPC))
	}()

	// Allocate peer_bdfs pointer table in C memory so rtCfg contains zero Go pointers.
	var cPeers **C.char
	if n := len(cfg.PeerBDFs); n > 0 {
		cPeers = (**C.char)(C.calloc(C.size_t(n), C.size_t(unsafe.Sizeof((*C.char)(nil)))))
		if cPeers == nil {
			return RuntimeInfo{}, syscall.ENOMEM
		}
		peerArr := unsafe.Slice(cPeers, n)
		defer func() {
			for i := range peerArr {
				if peerArr[i] != nil {
					C.free(unsafe.Pointer(peerArr[i]))
				}
			}
			C.free(unsafe.Pointer(cPeers))
		}()
		for i, bdf := range cfg.PeerBDFs {
			peerArr[i] = C.CString(bdf)
		}
	}

	rtCfg := C.struct_xds_runtime_config{
		bdev_name:           cBdev,
		json_config_file:    cJSON,
		reactor_mask:        cMask,
		sock:                cRPC,
		main_core:           C.uint32_t(cfg.MainCore),
		cluster_size_bytes:  C.uint32_t(cfg.BlockSize),
		metadata_page_count: C.uint32_t(cfg.MetadataPageCount),
		peer_bdfs:           cPeers,
		peer_bdf_count:      C.size_t(len(cfg.PeerBDFs)),
		device_backend:      C.enum_xds_device_backend(cfg.DeviceBackend),
	}
	if err := cErr("runtime_start", C.xds_runtime_start(&rtCfg)); err != nil {
		return RuntimeInfo{}, err
	}

	poolCfg := C.struct_xds_blob_pool_config{
		blob_size_bytes: C.uint64_t(cfg.BlobSize),
		blob_count:      C.uint32_t(cfg.BlobCount),
		queue_count:     1,
		enable_fast_map: C.bool(cfg.EnableFastMap),
	}
	rc := C.xds_blob_pool_set_config(&poolCfg)
	if rc == 0 {
		rc = C.xds_blob_pool_init()
	}
	if rc != 0 {
		_ = C.xds_runtime_stop()
		return RuntimeInfo{}, cErr("blob_pool_init", rc)
	}

	var evfd C.int
	if err := cErr("cq_init", C.jfs_xds_cq_init(&evfd)); err != nil {
		_ = C.xds_blob_pool_teardown()
		_ = C.xds_runtime_stop()
		return RuntimeInfo{}, err
	}

	r.ioMu.Lock()
	r.cqStop.Store(false)
	r.closingCh = make(chan struct{})
	r.freeSlots = make(chan uint32, cgoCQCap)
	r.slotCh = make([]chan error, cgoCQCap)
	for i := range cgoCQCap {
		r.slotCh[i] = make(chan error, 1)
		r.freeSlots <- i
	}
	// Non-blocking eventfd registers cleanly with Go's epoll netpoller.
	r.cqFile = os.NewFile(uintptr(evfd), "xds-cq")
	r.started = true
	r.ioMu.Unlock()

	r.cqWG.Add(1)
	go r.cqLoop()

	return RuntimeInfo{
		IOUnitSize:  uint64(C.xds_runtime_get_io_unit_size()),
		ClusterSize: uint64(C.xds_runtime_get_cluster_size()),
		WorkerCount: uint32(C.xds_runtime_get_worker_count()),
		DirectReady: bool(C.xds_direct_is_enabled()),
	}, nil
}

func (r *cgoRuntime) cqLoop() {
	defer r.cqWG.Done()
	var batch [cgoDrainBatch]C.struct_jfs_xds_cqe
	var drain [8]byte

	for {
		drained := false
		for {
			n := int(C.jfs_xds_cq_drain(&batch[0], C.uint32_t(len(batch))))
			if n == 0 {
				break
			}
			drained = true
			for i := range n {
				id := uint32(batch[i].slot_id)
				if id < cgoCQCap {
					r.slotCh[id] <- cErr("io", batch[i].status)
				}
			}
		}
		if r.cqStop.Load() {
			return
		}
		if drained {
			runtime.Gosched()
			if C.jfs_xds_cq_has_ready() != 0 {
				continue
			}
		}
		if C.jfs_xds_cq_arm() != 0 {
			continue
		}
		if _, err := r.cqFile.Read(drain[:]); err != nil {
			return
		}
	}
}

func (r *cgoRuntime) acquireSlot() (uint32, error) {
	r.ioMu.RLock()
	if !r.started {
		r.ioMu.RUnlock()
		return 0, ErrClosed
	}
	r.ioWG.Add(1)
	closing := r.closingCh
	r.ioMu.RUnlock()

	select {
	case id := <-r.freeSlots:
		return id, nil
	case <-closing:
		r.ioWG.Done()
		return 0, ErrClosed
	}
}

func (r *cgoRuntime) releaseSlot(id uint32) {
	r.freeSlots <- id
	r.ioWG.Done()
}

func (r *cgoRuntime) Stop() error {
	// 1. Stop admitting new I/Os and wake goroutines waiting for a free slot.
	r.ioMu.Lock()
	wasStarted := r.started
	r.started = false
	if wasStarted {
		close(r.closingCh)
	}
	r.ioMu.Unlock()

	// 2. Drain all in-flight I/Os (including xds_free_io_buffer) while SPDK
	//    and cqLoop are still running, preventing UAF or hangs.
	if wasStarted {
		r.ioWG.Wait()
	}

	// 3. Tear down SPDK blob pool & runtime once zero I/Os remain in flight.
	rc1 := C.xds_blob_pool_teardown()
	rc2 := C.xds_runtime_stop()

	// 4. Stop cqLoop and release CQ memory only if SPDK stopped cleanly.
	if wasStarted && r.cqStop.CompareAndSwap(false, true) {
		if r.cqFile != nil {
			_ = r.cqFile.Close()
		}
		r.cqWG.Wait()
		if rc2 == 0 {
			_ = C.jfs_xds_cq_fini()
		}
	}
	if rc1 != 0 {
		return cErr("blob_pool_teardown", rc1)
	}
	return cErr("runtime_stop", rc2)
}

func (r *cgoRuntime) LeaseBlobs(count uint32) ([]BlobHandle, error) {
	if count == 0 {
		return nil, ErrInvalidArg
	}
	out := make([]BlobHandle, count)
	for i := range count {
		var h C.uintptr_t
		if rc := C.jfs_xds_get_blob(&h); rc != 0 {
			for j := range i {
				_ = C.jfs_xds_release_blob(C.uintptr_t(out[j]))
			}
			return nil, cErr("lease_blobs", rc)
		}
		out[i] = BlobHandle(h)
	}
	return out, nil
}

func (r *cgoRuntime) ReleaseBlobs(handles []BlobHandle) error {
	var firstErr error
	for _, h := range handles {
		if h == 0 {
			continue
		}
		if err := cErr("release_blobs", C.jfs_xds_release_blob(C.uintptr_t(h))); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (r *cgoRuntime) ImportBuffer(p2pFD int, deviceVA, size uint64, peers []PeerExport) (RegHandle, uintptr, error) {
	if len(peers) == 0 || len(peers) > C.XDS_CTRL_MAX_PEERS {
		if p2pFD >= 0 {
			_ = syscall.Close(p2pFD)
		}
		return 0, 0, ErrInvalidArg
	}

	xdsPeers := make([]C.struct_xds_dma_import_peer, len(peers))
	for i, p := range peers {
		n := min(len(p.PeerBDF), C.XDS_CTRL_BDF_LEN-1)
		for j := range n {
			xdsPeers[i].peer_bdf[j] = C.char(p.PeerBDF[j])
		}
		xdsPeers[i].exported = C.struct_xds_ctrl_gpu_peer_export{
			handle:         C.int32_t(p.Handle),
			backend:        C.uint32_t(p.Backend),
			page_size:      C.uint64_t(p.PageSize),
			mapped_len:     C.uint64_t(p.MappedLen),
			segment_count:  C.uint32_t(p.SegmentCount),
			first_dma_addr: C.uint64_t(p.FirstDMAAddr),
		}
	}

	var reg C.uint64_t
	var ioPtr C.uintptr_t
	rc := C.jfs_xds_import_buf(
		C.int(p2pFD), C.uint64_t(deviceVA), C.uint64_t(size),
		&xdsPeers[0], C.size_t(len(xdsPeers)), &reg, &ioPtr,
	)
	if rc != 0 {
		return 0, 0, cErr("import_buf", rc)
	}
	return RegHandle(reg), uintptr(ioPtr), nil
}

func (r *cgoRuntime) UnregisterBuffer(reg RegHandle) error {
	return cErr("unregister_buf", C.xds_direct_unregister_buffer(C.uint64_t(reg)))
}

func (r *cgoRuntime) ReadCPU(blob BlobHandle, blobOff uint64, dst []byte) error {
	if len(dst) == 0 {
		return nil
	}
	slotID, err := r.acquireSlot()
	if err != nil {
		return err
	}
	defer r.releaseSlot(slotID)

	rc := C.jfs_xds_submit_read_cpu(C.uint32_t(slotID), C.uintptr_t(blob),
		C.uint64_t(blobOff), C.uint64_t(len(dst)))
	if rc != 0 {
		return cErr("read_cpu", rc)
	}

	err = <-r.slotCh[slotID]
	copyOK := C.int(0)
	if err == nil {
		copyOK = 1
	}
	C.jfs_xds_finish_read_cpu(C.uint32_t(slotID), unsafe.Pointer(&dst[0]),
		C.uint64_t(len(dst)), copyOK)
	return err
}

func (r *cgoRuntime) WriteCPU(blob BlobHandle, blobOff uint64, src []byte) error {
	if len(src) == 0 {
		return nil
	}
	slotID, err := r.acquireSlot()
	if err != nil {
		return err
	}
	defer r.releaseSlot(slotID)

	rc := C.jfs_xds_submit_write_cpu(C.uint32_t(slotID), C.uintptr_t(blob),
		C.uint64_t(blobOff), unsafe.Pointer(&src[0]), C.uint64_t(len(src)))
	if rc != 0 {
		return cErr("write_cpu", rc)
	}

	err = <-r.slotCh[slotID]
	C.jfs_xds_free_slot_dma(C.uint32_t(slotID))
	return err
}

func (r *cgoRuntime) ReadDirectBatch(descs []DirectReadDesc) error {
	if len(descs) == 0 {
		return ErrInvalidArg
	}
	totalIovs := 0
	for i := range descs {
		totalIovs += len(descs[i].Iovs)
	}

	slotID, err := r.acquireSlot()
	if err != nil {
		return err
	}
	defer r.releaseSlot(slotID)

	cDescs := make([]C.struct_jfs_xds_desc, len(descs))
	var cIovs []C.struct_jfs_xds_iov
	var iovsPtr *C.struct_jfs_xds_iov
	if totalIovs > 0 {
		cIovs = make([]C.struct_jfs_xds_iov, 0, totalIovs)
	}

	for i, d := range descs {
		cDescs[i] = C.struct_jfs_xds_desc{
			blob_handle:  C.uintptr_t(d.Blob),
			payload:      C.uintptr_t(d.Payload),
			offset_bytes: C.uint64_t(d.BlobOffset),
			size_bytes:   C.uint64_t(d.Size),
		}
		if len(d.Iovs) > 0 {
			cDescs[i].iov_offset = C.uint32_t(len(cIovs))
			cDescs[i].iovcnt = C.uint32_t(len(d.Iovs))
			for _, v := range d.Iovs {
				cIovs = append(cIovs, C.struct_jfs_xds_iov{
					base: C.uintptr_t(v.Base),
					len:  C.uint64_t(v.Len),
				})
			}
		}
	}
	if len(cIovs) > 0 {
		iovsPtr = &cIovs[0]
	}

	rc := C.jfs_xds_submit_direct_batch(
		C.uint32_t(slotID),
		&cDescs[0], C.uint32_t(len(cDescs)),
		iovsPtr, C.uint32_t(len(cIovs)),
	)
	if rc != 0 {
		return cErr("read_direct_batch", rc)
	}

	err = <-r.slotCh[slotID]
	if totalIovs > cgoInlineIOVs {
		C.jfs_xds_free_slot_iovs(C.uint32_t(slotID))
	}
	return err
}

func cErr(op string, rc C.int) error {
	switch {
	case rc < 0:
		return fmt.Errorf("xds %s: %w", op, syscall.Errno(-rc))
	case rc > 0:
		return fmt.Errorf("xds %s: rc=%d", op, int(rc))
	}
	return nil
}
