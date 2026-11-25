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
#include "bridge.h"
*/
import "C"

import (
	"fmt"
	"syscall"
	"unsafe"
)

// cgoRuntime implements Runtime over the embedded xDS SPDK C runtime.
type cgoRuntime struct{}

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

	var cPeers **C.char
	var peerPtrs []*C.char
	if len(cfg.PeerBDFs) > 0 {
		peerPtrs = make([]*C.char, len(cfg.PeerBDFs))
		for i, bdf := range cfg.PeerBDFs {
			peerPtrs[i] = C.CString(bdf)
			defer C.free(unsafe.Pointer(peerPtrs[i]))
		}
		cPeers = &peerPtrs[0]
	}

	cCfg := C.struct_jfs_xds_rt_config{
		bdev_name:           cBdev,
		json_config_file:    cJSON,
		reactor_mask:        cMask,
		rpc_addr:            cRPC,
		main_core:           C.uint32_t(cfg.MainCore),
		cluster_size_bytes:  C.uint32_t(cfg.BlockSize),
		metadata_page_count: C.uint32_t(cfg.MetadataPageCount),
		peer_bdfs:           cPeers,
		peer_bdf_count:      C.size_t(len(peerPtrs)),
		device_backend:      C.uint32_t(cfg.DeviceBackend),
		blob_size_bytes:     C.uint64_t(cfg.BlobSize),
		blob_count:          C.uint32_t(cfg.BlobCount),
		enable_fast_map:     C.bool(cfg.EnableFastMap),
	}

	var cInfo C.struct_jfs_xds_rt_info
	if err := cErr("start", C.jfs_xds_rt_start(&cCfg, &cInfo)); err != nil {
		return RuntimeInfo{}, err
	}

	return RuntimeInfo{
		IOUnitSize:  uint64(cInfo.io_unit_size),
		ClusterSize: uint64(cInfo.cluster_size),
		WorkerCount: uint32(cInfo.worker_count),
		DirectReady: cInfo.direct_ready != 0,
	}, nil
}

func (r *cgoRuntime) Stop() error {
	return cErr("stop", C.jfs_xds_rt_stop())
}

func (r *cgoRuntime) LeaseBlobs(count uint32) ([]BlobHandle, error) {
	raw := make([]C.uintptr_t, count)
	if rc := C.jfs_xds_rt_lease_blobs(C.uint32_t(count), &raw[0]); rc != 0 {
		return nil, cErr("lease_blobs", rc)
	}
	out := make([]BlobHandle, count)
	for i := range raw {
		out[i] = BlobHandle(raw[i])
	}
	return out, nil
}

func (r *cgoRuntime) ReleaseBlobs(handles []BlobHandle) error {
	if len(handles) == 0 {
		return nil
	}
	raw := make([]C.uintptr_t, len(handles))
	for i, h := range handles {
		raw[i] = C.uintptr_t(h)
	}
	return cErr("release_blobs", C.jfs_xds_rt_release_blobs(&raw[0], C.uint32_t(len(raw))))
}

func (r *cgoRuntime) ImportBuffer(p2pFD int, deviceVA, size uint64, peers []PeerExport) (RegHandle, uintptr, error) {
	l := len(peers)
	if l == 0 {
		if p2pFD >= 0 {
			_ = syscall.Close(p2pFD)
		}
		return 0, 0, ErrInvalidArg
	}
	bdfPtrs := make([]*C.char, l)
	cPeers := make([]C.struct_jfs_xds_peer_export, l)
	for i, p := range peers {
		bdfPtrs[i] = C.CString(p.PeerBDF)
		defer C.free(unsafe.Pointer(bdfPtrs[i]))
		cPeers[i] = C.struct_jfs_xds_peer_export{
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
	rc := C.jfs_xds_rt_import_buf(
		C.int(p2pFD), C.uint64_t(deviceVA), C.uint64_t(size),
		&bdfPtrs[0], &cPeers[0], C.size_t(l), &reg, &ioPtr,
	)
	if rc != 0 {
		return 0, 0, cErr("import_buf", rc)
	}
	return RegHandle(reg), uintptr(ioPtr), nil
}

func (r *cgoRuntime) UnregisterBuffer(reg RegHandle) error {
	return cErr("unregister_buf", C.jfs_xds_rt_unregister_buf(C.uint64_t(reg)))
}

func (r *cgoRuntime) ReadCPU(blob BlobHandle, blobOff uint64, dst []byte) error {
	if len(dst) == 0 {
		return nil
	}
	rc := C.jfs_xds_rt_read_cpu(C.uintptr_t(blob), C.uint64_t(blobOff),
		unsafe.Pointer(&dst[0]), C.uint64_t(len(dst)))
	return cErr("read_cpu", rc)
}

func (r *cgoRuntime) WriteCPU(blob BlobHandle, blobOff uint64, src []byte) error {
	if len(src) == 0 {
		return nil
	}
	rc := C.jfs_xds_rt_write_cpu(C.uintptr_t(blob), C.uint64_t(blobOff),
		unsafe.Pointer(&src[0]), C.uint64_t(len(src)))
	return cErr("write_cpu", rc)
}

func (r *cgoRuntime) ReadDirectBatch(descs []DirectReadDesc) error {
	if len(descs) == 0 {
		return ErrInvalidArg
	}
	totalIovs := 0
	for i := range descs {
		totalIovs += len(descs[i].Iovs)
	}

	cDescs := make([]C.struct_jfs_xds_bridge_desc, len(descs))
	var cIovs []C.struct_jfs_xds_bridge_iov
	var iovsPtr *C.struct_jfs_xds_bridge_iov
	if totalIovs > 0 {
		cIovs = make([]C.struct_jfs_xds_bridge_iov, 0, totalIovs)
	}

	for i, d := range descs {
		cDescs[i] = C.struct_jfs_xds_bridge_desc{
			blob_handle:  C.uintptr_t(d.Blob),
			payload:      C.uintptr_t(d.Payload),
			offset_bytes: C.uint64_t(d.BlobOffset),
			size_bytes:   C.uint64_t(d.Size),
		}
		if len(d.Iovs) > 0 {
			cDescs[i].iov_offset = C.uint32_t(len(cIovs))
			cDescs[i].iovcnt = C.uint32_t(len(d.Iovs))
			for _, v := range d.Iovs {
				cIovs = append(cIovs, C.struct_jfs_xds_bridge_iov{
					base: C.uintptr_t(v.Base),
					len:  C.uint64_t(v.Len),
				})
			}
		}
	}
	if len(cIovs) > 0 {
		iovsPtr = &cIovs[0]
	}

	rc := C.jfs_xds_rt_read_direct_batch(
		&cDescs[0], C.uint32_t(len(cDescs)),
		iovsPtr, C.uint32_t(len(cIovs)),
	)
	return cErr("read_direct_batch", rc)
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
