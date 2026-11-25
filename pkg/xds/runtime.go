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

// BlobHandle is an opaque SPDK blob handle leased from the xDS blob pool.
type BlobHandle uintptr

// RegHandle is an opaque DMA registration handle returned by xDS buffer import.
type RegHandle uintptr

// RuntimeInfo describes static geometry queried from the active xDS runtime.
type RuntimeInfo struct {
	IOUnitSize  uint64
	ClusterSize uint64
	WorkerCount uint32
	DirectReady bool
}

// PeerExport carries one NVMe peer's exported P2P DMA registration from an
// external client process.
type PeerExport struct {
	PeerBDF      string
	Handle       int32
	Backend      uint32
	PageSize     uint64
	MappedLen    uint64
	SegmentCount uint32
	FirstDMAAddr uint64
}

// IOVec describes one destination memory segment for vector P2P readv.
type IOVec struct {
	Base uintptr
	Len  uint64
}

// DirectReadDesc describes one NVMe-to-device P2P read operation inside a batch.
// Either Payload is set (scalar contiguous destination) or Iovs is non-empty
// (scatter destination within a single contiguous blob source range).
type DirectReadDesc struct {
	Blob       BlobHandle
	BlobOffset uint64
	Size       uint64
	Payload    uintptr
	Iovs       []IOVec
}

// HoleSpan describes a logical zero-filled sparse region in the client target.
// Its binary layout matches struct jfs_xds_hole in proto.h.
type HoleSpan struct {
	DstOffset  uint64
	Length     uint64
	SliceIndex uint32
	_          uint32
}

// Runtime abstracts the underlying xDS SPDK C runtime so that the cache engine
// and IPC server remain 100% testable without NVMe or GPU hardware.
type Runtime interface {
	// Start initializes the embedded xDS runtime and blob pool.
	Start(cfg *Config) (RuntimeInfo, error)

	// Stop tears down the blob pool and xDS runtime.
	Stop() error

	// LeaseBlobs acquires count pre-allocated blobs from the xDS blob pool.
	LeaseBlobs(count uint32) ([]BlobHandle, error)

	// ReleaseBlobs returns leased blobs to the xDS blob pool.
	ReleaseBlobs(handles []BlobHandle) error

	// ImportBuffer imports an external client's P2P device fd and peer mappings
	// into the xDS DMA registry, returning the registration handle and reserved
	// SPDK I/O alias pointer. It always consumes p2pFD on both success and error.
	ImportBuffer(p2pFD int, deviceVA, size uint64, peers []PeerExport) (RegHandle, uintptr, error)

	// UnregisterBuffer unmaps a previously imported buffer after all I/O drains.
	UnregisterBuffer(reg RegHandle) error

	// ReadCPU reads len(dst) bytes starting at blobOff (which must be IOUnit
	// aligned) from blob into CPU memory dst.
	ReadCPU(blob BlobHandle, blobOff uint64, dst []byte) error

	// WriteCPU writes src to blob at blobOff (IOUnit aligned), zero-padding the
	// trailing partial IOUnit automatically.
	WriteCPU(blob BlobHandle, blobOff uint64, src []byte) error

	// ReadDirectBatch executes a batch of NVMe-to-device P2P reads concurrently
	// on SPDK workers and returns once all descriptors complete.
	ReadDirectBatch(descs []DirectReadDesc) error
}
