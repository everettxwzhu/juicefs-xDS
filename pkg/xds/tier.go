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

import (
	"syscall"
	"unsafe"
)

// slotRef identifies one BlockSize-byte region in a leased blob. blob is an
// opaque native handle, not a memory address that Go may dereference.
type slotRef struct {
	blob uintptr
	off  uint64
}

// stager pairs a borrowed xDS CPU buffer with an exclusive scratch slot.
// Borrowers must retain both until all submitted I/O referencing them has
// completed. EIO from a broken engine does not establish physical completion.
// Neither buf nor ref may be used after the owning client closes.
type stager struct {
	buf     []byte
	ref     uint64
	scratch slotRef
}

type tier struct {
	// Immutable address table shared by all slot users. Slot 0 is permanently
	// zero; [1, cacheStart) is scratch; [cacheStart, len(slots)) is cache.
	// Zero and scratch slots must never enter the cache eviction set.
	slots      []slotRef
	cacheStart int32

	// A stager is in this channel only while available for exclusive use.
	// Do not close the channel during teardown: borrowers may still return
	// stagers. The Volume lifecycle protects the underlying native mappings.
	stagers chan *stager
}

// slotLayout contains checked geometry only; constructing it acquires no
// native resources. Keep all arithmetic unsigned until its bounds are checked.
type slotLayout struct {
	blobCount   int
	perBlob     uint64
	slotCount   int
	stagerCount int
	cacheStart  int32
}

func makeLayout(cm clientMeta, blockSize int) (slotLayout, error) {
	if blockSize <= 0 || cm.blobSize == 0 || cm.ioUnit == 0 || cm.capacity == 0 {
		return slotLayout{}, syscall.EINVAL
	}
	bs := uint64(blockSize)
	if cm.capacity%cm.blobSize != 0 || cm.blobSize%bs != 0 || bs%cm.ioUnit != 0 {
		return slotLayout{}, syscall.EINVAL
	}

	stagerCount := min(uint64(maxStagers), uint64(stagerMemoryBudget)/bs)
	if stagerCount == 0 {
		return slotLayout{}, syscall.EINVAL
	}

	// capacity is already the capacity of this rank. The divisibility checks
	// above let us divide directly instead of multiplying blob and slot counts.
	blobCount := cm.capacity / cm.blobSize
	slotCount := cm.capacity / bs
	if slotCount < 1+2*stagerCount {
		return slotLayout{}, syscall.EINVAL
	}

	// Cache slot IDs use int32. Also check allocation byte counts before any
	// int conversion, including on 32-bit builds. unsafe.Sizeof is used only
	// for compile-time Go type sizes; no pointers are constructed or retained.
	maxInt := uint64(^uint(0) >> 1)
	if slotCount > uint64(1<<31-1) ||
		slotCount > maxInt/uint64(unsafe.Sizeof(slotRef{})) ||
		blobCount > maxInt/uint64(unsafe.Sizeof(uintptr(0))) {
		return slotLayout{}, syscall.EOVERFLOW
	}

	return slotLayout{
		blobCount:   int(blobCount),
		perBlob:     cm.blobSize / bs,
		slotCount:   int(slotCount),
		stagerCount: int(stagerCount),
		cacheStart:  int32(1 + stagerCount),
	}, nil
}

// newTier constructs addresses and allocates CPU buffers after all blob leases
// have been acquired. l must come from makeLayout for the same metadata and
// blockSize. The caller retains ownership of c and must close it on failure;
// that also releases any buffers allocated before a later allocation failed.
//
// The returned pool is initially empty. The caller owns the returned stagers
// until it has written the zero slot and can publish all stagers to the pool.
func newTier(c client, blobs []uintptr, l slotLayout, blockSize int) (*tier, []*stager, error) {
	if len(blobs) != l.blobCount {
		return nil, nil, syscall.EINVAL
	}
	t := &tier{
		slots:      make([]slotRef, l.slotCount),
		cacheStart: l.cacheStart,
		stagers:    make(chan *stager, l.stagerCount),
	}
	bs := uint64(blockSize)
	for i := range t.slots {
		j := uint64(i)
		t.slots[i] = slotRef{
			blob: blobs[j/l.perBlob],
			off:  (j % l.perBlob) * bs,
		}
	}

	stagers := make([]*stager, l.stagerCount)
	for i := range stagers {
		buf, ref, err := c.allocBuffer(bs)
		if err != nil {
			return nil, nil, err
		}
		if len(buf) != blockSize || ref == 0 {
			return nil, nil, syscall.EIO
		}
		stagers[i] = &stager{
			buf:     buf,
			ref:     ref,
			scratch: t.slots[1+i],
		}
	}
	return t, stagers, nil
}
