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
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"syscall"
	"unsafe"

	"github.com/juicedata/juicefs/pkg/chunk"
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

// blockKey identifies an immutable block within a slice object. File offsets
// and inode numbers are excluded because several mappings can share this data.
type blockKey struct {
	id   uint64
	size uint32
	indx uint32
}

type slotState uint8

const (
	slotFree slotState = iota
	slotFilling
	slotReady
	slotQuarantined
)

// slotMeta is protected by tier.mu. Only ready slots occur in index, and only
// ready slots without references may be evicted. A filling slot holds one
// reference for its fill owner; success transfers that reference to the caller.
type slotMeta struct {
	key   blockKey
	state slotState
	refs  uint32
	atime uint64
}

// fillCall coalesces both host reads and NVMe writes. err becomes immutable
// before done closes. Waiting does not grant a slot reference: after success a
// waiter must look up and pin the entry under tier.mu again.
type fillCall struct {
	done chan struct{}
	err  error
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

	// Protects cache metadata only. Never hold this lock over chunk reads,
	// memcpy, engine.do, or a blocking wait. Volume.mu precedes this lock.
	mu      sync.Mutex
	metas   []slotMeta
	index   map[blockKey]int32
	free    []int32
	fills   map[blockKey]*fillCall
	clock   uint64
	changed chan struct{} // Created lazily only when allocation must wait.

	// Initialized once before Open publishes the Volume. The native client is
	// deliberately not retained here: all data-plane operations use the engine.
	store     chunk.ChunkStore
	e         *engine
	blockSize int
	ioUnit    int
	readahead int
	bgCtx     context.Context
	bg        *sync.WaitGroup

	// Only prefetch uses these credits. Reserving at most half the stagers for
	// background fills prevents prefetch alone from exhausting the staging pool.
	prefetchTokens chan struct{}
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

	// Cache slot IDs use int32. Check the byte count of each slice allocation
	// before converting to int, including on 32-bit targets. Checking slotRef
	// alone is insufficient now that mutable metadata has a larger element.
	maxInt := uint64(^uint(0) >> 1)
	if slotCount > uint64(1<<31-1) ||
		slotCount > maxInt/uint64(unsafe.Sizeof(slotRef{})) ||
		slotCount > maxInt/uint64(unsafe.Sizeof(slotMeta{})) ||
		slotCount > maxInt/uint64(unsafe.Sizeof(int32(0))) ||
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

// initCache binds immutable runtime dependencies after the zero slot is ready,
// before Open publishes v. It acquires no native resources and starts no work.
func (t *tier) initCache(v *Volume) {
	t.store, t.e = v.store, v.e
	t.blockSize, t.ioUnit, t.readahead = v.blockSize, v.ioUnit, v.readahead
	t.bgCtx, t.bg = v.bgCtx, &v.bg
	t.metas = make([]slotMeta, len(t.slots))
	t.index = make(map[blockKey]int32)
	t.fills = make(map[blockKey]*fillCall)
	t.free = make([]int32, 0, len(t.slots)-int(t.cacheStart))
	for sid := len(t.slots) - 1; sid >= int(t.cacheStart); sid-- {
		t.free = append(t.free, int32(sid))
	}
	t.prefetchTokens = make(chan struct{}, cap(t.stagers)/2)
}

// check prioritizes sticky I/O failure over cancellation. All callers protect
// native resources through either a foreground lifecycle lock or admitted
// background work. In particular, this helper must not inspect v.closed.
func (t *tier) check(ctx context.Context) error {
	if t.e.broken.Load() {
		return syscall.EIO
	}
	if ctx == nil {
		return syscall.EINVAL
	}
	select {
	case <-t.e.stopping:
		return syscall.EBADF
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return t.bgCtx.Err()
}

// blockRange validates the key before subtraction or int conversion. A zero
// slice ID represents a hole and belongs to the permanent zero slot instead.
func (t *tier) blockRange(k blockKey) (off, blen, alen int, err error) {
	if k.id == 0 || k.size == 0 {
		return 0, 0, 0, syscall.EINVAL
	}
	if uint64(k.size) > uint64(^uint(0)>>1) {
		return 0, 0, 0, syscall.EOVERFLOW
	}
	start := uint64(k.indx) * uint64(t.blockSize)
	if start >= uint64(k.size) {
		return 0, 0, 0, syscall.EINVAL
	}
	n := min(uint64(t.blockSize), uint64(k.size)-start)
	u := uint64(t.ioUnit)
	aligned := ((n + u - 1) / u) * u
	return int(start), int(n), int(aligned), nil
}

// acquire returns a ready cache slot with one reference owned by the caller.
// The caller must retain it until its DMA physically completes. Foreground
// callers must already hold Volume.mu.RLock; this method never reacquires it.
func (t *tier) acquire(ctx context.Context, k blockKey) (int32, error) {
	for {
		if err := t.check(ctx); err != nil {
			return -1, err
		}
		t.mu.Lock()
		if err := t.check(ctx); err != nil {
			t.mu.Unlock()
			return -1, err
		}
		if sid, ok := t.index[k]; ok {
			m := &t.metas[sid]
			if m.refs == ^uint32(0) {
				t.mu.Unlock()
				return -1, syscall.EOVERFLOW
			}
			m.refs++
			t.clock++
			m.atime = t.clock
			t.mu.Unlock()
			return sid, nil
		}
		if call, ok := t.fills[k]; ok {
			t.mu.Unlock()
			select {
			case <-call.done:
			case <-ctx.Done():
				return -1, t.check(ctx)
			case <-t.bgCtx.Done():
				return -1, t.check(ctx)
			case <-t.e.failed:
				return -1, syscall.EIO
			case <-t.e.stopping:
				return -1, t.check(ctx)
			}
			if err := t.check(ctx); err != nil {
				return -1, err
			}
			if call.err != nil && !errors.Is(call.err, context.Canceled) &&
				!errors.Is(call.err, context.DeadlineExceeded) {
				return -1, call.err
			}
			// Success must be pinned through index again. If the owner alone
			// cancelled, a waiter with a live context may become the next owner.
			continue
		}
		// Published entries and existing flights already have validated keys.
		// Keep geometry divisions off the cache-hit path.
		if _, _, _, err := t.blockRange(k); err != nil {
			t.mu.Unlock()
			return -1, err
		}
		call := &fillCall{done: make(chan struct{})}
		t.fills[k] = call
		t.mu.Unlock()

		st, err := t.getStager(ctx)
		sid := int32(-1)
		if err == nil {
			sid, err = t.allocSlot(ctx, k)
			if err == nil {
				err = t.fill(ctx, k, st, sid)
			}
		}
		err = t.completeFill(k, call, sid, err)
		if st != nil {
			t.putStager(st)
		}
		if err != nil {
			return -1, err
		}
		return sid, nil
	}
}

// fill writes a caller-reserved filling slot. It owns neither the slot nor the
// stager: the owner must call completeFill exactly once and then putStager.
// Keeping publication in completeFill makes slot state and flight completion
// one atomic metadata transition, shared by foreground and prefetch fills.
func (t *tier) fill(ctx context.Context, k blockKey, st *stager, sid int32) error {
	if err := t.check(ctx); err != nil {
		return err
	}
	off, blen, alen, err := t.blockRange(k)
	if err != nil {
		return err
	}
	page, err := readSliceRange(ctx, t.store, k.id, k.size, off, blen)
	if err != nil {
		if checkErr := t.check(ctx); checkErr != nil {
			return checkErr
		}
		return syscall.EIO
	}
	// Chunk/cache workers may retain page references. Copy once into borrowed
	// xDS memory; never let those workers observe a recyclable stager buffer.
	copy(st.buf[:blen], page.Data)
	clear(st.buf[blen:alen])
	page.Release()
	if err := t.check(ctx); err != nil {
		return err
	}
	slot := t.ref(sid)
	if err := t.e.do(&ioOp{
		write: true, blob: slot.blob, ref: st.ref,
		blobOff: slot.off, n: uint64(alen), ctx: ctx,
	}); err != nil {
		// An admission timeout has submitted nothing, while a broken engine
		// may still have live DMA. completeFill/putStager distinguish them.
		// Preserve cancellation as well: independent singleflight waiters may
		// retry an abandoned fill instead of inheriting an unrelated EIO.
		if checkErr := t.check(ctx); checkErr != nil {
			return checkErr
		}
		return syscall.EIO
	}
	return nil
}

// completeFill publishes either success or failure exactly once. sid is -1 if
// the owner failed before reserving a slot. err is stored before closing done,
// providing the synchronization needed by waiters reading call.err.
func (t *tier) completeFill(k blockKey, call *fillCall, sid int32, err error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.fills[k] != call {
		panic("xds: completing an unowned fill")
	}
	broken := t.e.broken.Load()
	if broken {
		err = syscall.EIO
	}
	if sid >= 0 {
		m := &t.metas[sid]
		if m.state != slotFilling || m.key != k || m.refs != 1 {
			panic("xds: completing an unreserved cache slot")
		}
		switch {
		case err == nil:
			m.state = slotReady
			t.clock++
			m.atime = t.clock
			t.index[k] = sid
		case broken:
			// EIO is not a DMA completion barrier. Preserve the pin and never
			// reuse this region before the client drains or tears down.
			m.state = slotQuarantined
		default:
			*m = slotMeta{}
			t.free = append(t.free, sid)
			t.notifySlotLocked()
		}
	} else if err == nil {
		panic("xds: successful fill without a cache slot")
	}
	delete(t.fills, k)
	call.err = err
	close(call.done)
	return err
}

// allocSlot waits only after checking availability and capturing changed under
// the same lock. Notifications are hints; every wakeup rechecks the predicate.
func (t *tier) allocSlot(ctx context.Context, k blockKey) (int32, error) {
	for {
		if err := t.check(ctx); err != nil {
			return -1, err
		}
		t.mu.Lock()
		if err := t.check(ctx); err != nil {
			t.mu.Unlock()
			return -1, err
		}
		if sid, ok := t.tryAllocSlotLocked(k); ok {
			t.mu.Unlock()
			return sid, nil
		}
		if t.changed == nil {
			t.changed = make(chan struct{})
		}
		changed := t.changed
		t.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
		case <-t.bgCtx.Done():
		case <-t.e.failed:
		case <-t.e.stopping:
		}
	}
}

// tryAllocSlotLocked never waits. Its caller holds mu and synchronizes flight
// registration with reservation before unlocking. A reservation removes the old
// key before any bytes can be overwritten.
func (t *tier) tryAllocSlotLocked(k blockKey) (int32, bool) {
	sid := int32(-1)
	if n := len(t.free); n != 0 {
		sid = t.free[n-1]
		t.free = t.free[:n-1]
	} else {
		eligible := func(i int32) bool {
			m := &t.metas[i]
			return m.state == slotReady && m.refs == 0
		}
		n := len(t.slots) - int(t.cacheStart)
		for tries := 0; tries < 8; tries++ {
			a := t.cacheStart + int32(rand.IntN(n))
			b := t.cacheStart + int32(rand.IntN(n))
			if eligible(a) {
				sid = a
			}
			if eligible(b) && (sid < 0 || t.metas[b].atime < t.metas[sid].atime) {
				sid = b
			}
			if sid >= 0 {
				break
			}
		}
		if sid < 0 {
			for i := t.cacheStart; int(i) < len(t.slots); i++ {
				if eligible(i) {
					sid = i
					break
				}
			}
		}
		if sid < 0 {
			return -1, false
		}
		delete(t.index, t.metas[sid].key)
	}
	t.metas[sid] = slotMeta{key: k, state: slotFilling, refs: 1}
	return sid, true
}

// notifySlotLocked is called only when a slot becomes reusable. Lazily creating
// the next channel avoids allocations on uncontended hit/release paths.
func (t *tier) notifySlotLocked() {
	if t.changed != nil {
		close(t.changed)
		t.changed = nil
	}
}

// release ends one caller's pin after DMA completion. Once broken, the engine
// may have notified callers while their DMA is still live, so preserve all pins.
func (t *tier) release(sid int32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.e.broken.Load() {
		return
	}
	m := &t.metas[sid]
	if sid < t.cacheStart || m.state != slotReady || m.refs == 0 {
		panic("xds: releasing an unowned cache slot")
	}
	m.refs--
	if m.refs == 0 {
		t.notifySlotLocked()
	}
}

// ref reads only the immutable address table. The caller must already own the
// corresponding cache pin or the exclusive stager paired with a scratch slot.
func (t *tier) ref(sid int32) slotRef {
	return t.slots[sid]
}

func (t *tier) getStager(ctx context.Context) (*stager, error) {
	if err := t.check(ctx); err != nil {
		return nil, err
	}
	select {
	case st := <-t.stagers:
		// A resource and failure/cancellation can become ready together.
		if err := t.check(ctx); err != nil {
			t.putStager(st)
			return nil, err
		}
		return st, nil
	case <-ctx.Done():
	case <-t.bgCtx.Done():
	case <-t.e.failed:
	case <-t.e.stopping:
	}
	return nil, t.check(ctx)
}

// tryGetStager is for best-effort background work and never blocks.
func (t *tier) tryGetStager() *stager {
	if t.check(t.bgCtx) != nil {
		return nil
	}
	select {
	case st := <-t.stagers:
		if t.check(t.bgCtx) != nil {
			t.putStager(st)
			return nil
		}
		return st
	default:
		return nil
	}
}

// putStager returns exclusive ownership exactly once. Broken-engine buffers are
// quarantined instead: discarding this Go wrapper does not free native memory,
// which remains owned by the client until teardown. Never clear such a buffer.
func (t *tier) putStager(st *stager) {
	if t.e.broken.Load() {
		return
	}
	select {
	case t.stagers <- st:
	default:
		panic("xds: stager returned more than once")
	}
}
