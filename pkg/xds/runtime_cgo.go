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

#include <stdlib.h>

#include "xds/blob_pool.h"
#include "xds/dma_registry.h"
#include "xds/runtime.h"

#include "runtime_cgo.h"
*/
import "C"

import (
	"errors"
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
	return errors.Join(cErr("blob_pool_teardown", rc1), cErr("runtime_stop", rc2))
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
