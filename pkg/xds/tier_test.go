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
	"bytes"
	"context"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/juicedata/juicefs/pkg/chunk"
)

func TestMakeLayout(t *testing.T) {
	tests := []struct {
		name string
		meta clientMeta
		bs   int
		want slotLayout
	}{
		{
			name: "four MiB blocks",
			meta: clientMeta{
				blobSize: 64 << 20, ioUnit: 4096, capacity: 320 << 20,
			},
			bs: 4 << 20,
			want: slotLayout{
				blobCount: 5, perBlob: 16, slotCount: 80,
				stagerCount: 32, cacheStart: 33,
			},
		},
		{
			name: "stagers limited by byte budget",
			meta: clientMeta{
				blobSize: 64 << 20, ioUnit: 4096, capacity: 320 << 20,
			},
			bs: 8 << 20,
			want: slotLayout{
				blobCount: 5, perBlob: 8, slotCount: 40,
				stagerCount: 16, cacheStart: 17,
			},
		},
		{
			name: "one stager",
			meta: clientMeta{
				blobSize: 128 << 20, ioUnit: 4096, capacity: 384 << 20,
			},
			bs: 128 << 20,
			want: slotLayout{
				blobCount: 3, perBlob: 1, slotCount: 3,
				stagerCount: 1, cacheStart: 2,
			},
		},
		{
			name: "exact minimum slot count",
			meta: clientMeta{
				blobSize: 4096, ioUnit: 4096, capacity: 65 * 4096,
			},
			bs: 4096,
			want: slotLayout{
				blobCount: 65, perBlob: 1, slotCount: 65,
				stagerCount: 32, cacheStart: 33,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := makeLayout(tt.meta, tt.bs)
			if err != nil || got != tt.want {
				t.Fatalf("makeLayout: %+v, %v; want %+v, nil", got, err, tt.want)
			}
		})
	}
}

func TestMakeLayoutInvalid(t *testing.T) {
	valid := clientMeta{
		blobSize: 64 << 20, ioUnit: 4096, capacity: 320 << 20,
	}
	tests := []struct {
		name string
		meta clientMeta
		bs   int
	}{
		{"zero block size", valid, 0},
		{"negative block size", valid, -1},
		{
			"zero blob size",
			clientMeta{ioUnit: 4096, capacity: 320 << 20},
			4 << 20,
		},
		{
			"zero I/O unit",
			clientMeta{blobSize: 64 << 20, capacity: 320 << 20},
			4 << 20,
		},
		{
			"zero capacity",
			clientMeta{blobSize: 64 << 20, ioUnit: 4096},
			4 << 20,
		},
		{
			"capacity contains a partial blob",
			clientMeta{
				blobSize: 64 << 20, ioUnit: 4096,
				capacity: (320 << 20) + 1,
			},
			4 << 20,
		},
		{
			"blob contains a partial block",
			clientMeta{
				blobSize: (64 << 20) + 4096, ioUnit: 4096,
				capacity: 5 * ((64 << 20) + 4096),
			},
			4 << 20,
		},
		{
			"block is not I/O aligned",
			clientMeta{
				blobSize: 16 * ((4 << 20) + 1), ioUnit: 4096,
				capacity: 80 * ((4 << 20) + 1),
			},
			(4 << 20) + 1,
		},
		{
			"block exceeds stager byte budget",
			clientMeta{
				blobSize: 129 << 20, ioUnit: 4096, capacity: 387 << 20,
			},
			129 << 20,
		},
		{
			"one slot short",
			clientMeta{
				blobSize: 64 << 20, ioUnit: 4096, capacity: 256 << 20,
			},
			4 << 20,
		},
		{
			"capacity smaller than one blob",
			clientMeta{
				blobSize: 64 << 20, ioUnit: 4096, capacity: 4 << 20,
			},
			4 << 20,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := makeLayout(tt.meta, tt.bs); err != syscall.EINVAL {
				t.Fatalf("makeLayout: %+v, %v; want EINVAL", got, err)
			}
		})
	}
}

func TestMakeLayoutOverflow(t *testing.T) {
	// These inputs are geometrically valid. Only their representation is too
	// large, so a failed divisibility check must not mask the overflow case.
	for _, capacity := range []uint64{1 << 31, 1 << 63, ^uint64(0)} {
		cm := clientMeta{blobSize: 1, ioUnit: 1, capacity: capacity}
		if got, err := makeLayout(cm, 1); err != syscall.EOVERFLOW {
			t.Fatalf("capacity=%d: %+v, %v; want EOVERFLOW", capacity, got, err)
		}
	}

	// A slice length may fit int and int32 while its backing allocation does
	// not fit int on a 32-bit target. This calculation never allocates memory.
	maxInt := uint64(^uint(0) >> 1)
	firstOversizedSlice := maxInt/uint64(unsafe.Sizeof(slotRef{})) + 1
	if firstOversizedSlice <= 1<<31-1 {
		cm := clientMeta{
			blobSize: 1, ioUnit: 1, capacity: firstOversizedSlice,
		}
		if got, err := makeLayout(cm, 1); err != syscall.EOVERFLOW {
			t.Fatalf("oversized slot slice: %+v, %v; want EOVERFLOW", got, err)
		}
	}
}

// The mutable metadata can exceed the address-table allocation on 32-bit
// targets. This is a geometry-only check and never allocates the large slices.
func TestMakeLayoutMetadataOverflow(t *testing.T) {
	maxInt := uint64(^uint(0) >> 1)
	capacity := maxInt/uint64(unsafe.Sizeof(slotMeta{})) + 1
	if capacity > 1<<31-1 {
		t.Skip("int32 slot IDs impose the earlier limit on this target")
	}
	cm := clientMeta{blobSize: 1, ioUnit: 1, capacity: capacity}
	if got, err := makeLayout(cm, 1); err != syscall.EOVERFLOW {
		t.Fatalf("oversized metadata slice: %+v, %v; want EOVERFLOW", got, err)
	}
}

const cacheTestBlockSize = 4096

// cacheTestClient delays both the transfer and its completion when requested.
// Delaying only the completion would miss premature reuse of a DMA source.
// Submission snapshots contain scalars only; the engine may release ioOp after
// reporting a sticky failure while the test still holds a pending transfer.
type cacheTestClient struct {
	*fakeClient
	blobs   map[uintptr][]byte
	buffers map[uint64][]byte
	mu      sync.Mutex
	delay   atomic.Bool
	badPoll atomic.Bool
	closed  atomic.Bool
}

func (c *cacheTestClient) submit(op *ioOp) (uint64, error) {
	id, err := c.fakeClient.submit(op)
	if err != nil {
		return 0, err
	}
	if !c.delay.Load() {
		c.complete(submission{
			result: result{id: id, completed: op.n}, write: op.write,
			blob: op.blob, ref: op.ref, bufOff: op.bufOff, blobOff: op.blobOff,
		})
	}
	return id, nil
}

func (c *cacheTestClient) poll(out []result) (int, error) {
	if c.badPoll.Load() {
		return 0, syscall.EIO
	}
	return c.fakeClient.poll(out)
}

func (c *cacheTestClient) close() { c.closed.Store(true) }

func (c *cacheTestClient) complete(s submission) {
	c.mu.Lock()
	blob, buf := c.blobs[s.blob], c.buffers[s.ref]
	r := s.result
	if s.blobOff > uint64(len(blob)) || s.completed > uint64(len(blob))-s.blobOff ||
		s.bufOff > uint64(len(buf)) || s.completed > uint64(len(buf))-s.bufOff {
		r.status = -int32(syscall.EINVAL)
		r.completed = 0
	} else {
		b := blob[s.blobOff : s.blobOff+s.completed]
		p := buf[s.bufOff : s.bufOff+s.completed]
		if s.write {
			copy(b, p)
		} else {
			copy(p, b)
		}
	}
	c.mu.Unlock()
	c.ready <- r
}

func (c *cacheTestClient) contents(ref slotRef, n int) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return bytes.Clone(c.blobs[ref.blob][ref.off : ref.off+uint64(n)])
}

type cacheTestStore struct {
	chunk.ChunkStore
	data   map[uint64][]byte
	reads  atomic.Int32
	onRead func(context.Context, uint64, int) error
}

type cacheTestReader struct {
	s  *cacheTestStore
	id uint64
}

func (s *cacheTestStore) NewReader(id uint64, _ int) chunk.Reader {
	return &cacheTestReader{s: s, id: id}
}

func (r *cacheTestReader) ReadAt(ctx context.Context, p *chunk.Page, off int) (int, error) {
	r.s.reads.Add(1)
	if r.s.onRead != nil {
		if err := r.s.onRead(ctx, r.id, off); err != nil {
			return 0, err
		}
	}
	data := r.s.data[r.id]
	if off >= len(data) {
		return 0, io.EOF
	}
	n := copy(p.Data, data[off:])
	if n != len(p.Data) {
		return n, io.EOF
	}
	return n, nil
}

type cacheFixture struct {
	v       *Volume
	c       *cacheTestClient
	store   *cacheTestStore
	stagers []*stager
}

// The tiny layout exercises exhaustion without allocating production-sized
// staging memory. Native allocation is already covered by the Volume tests.
func newCacheFixture(t testing.TB, cacheSlots, stagerCount int, store *cacheTestStore) *cacheFixture {
	t.Helper()
	if store == nil {
		store = &cacheTestStore{data: map[uint64][]byte{
			1: bytes.Repeat([]byte{1}, cacheTestBlockSize),
			2: bytes.Repeat([]byte{2}, cacheTestBlockSize),
			3: bytes.Repeat([]byte{3}, cacheTestBlockSize),
		}}
	}
	c := &cacheTestClient{
		fakeClient: newFakeClient(),
		blobs:      make(map[uintptr][]byte),
		buffers:    make(map[uint64][]byte),
	}
	n := 1 + stagerCount + cacheSlots
	c.blobs[1] = bytes.Repeat([]byte{0xa5}, n*cacheTestBlockSize)
	tr := &tier{
		slots: make([]slotRef, n), cacheStart: int32(1 + stagerCount),
		stagers: make(chan *stager, stagerCount),
	}
	for i := range tr.slots {
		tr.slots[i] = slotRef{blob: 1, off: uint64(i * cacheTestBlockSize)}
	}
	stagers := make([]*stager, stagerCount)
	for i := range stagers {
		ref := uint64(i + 1)
		buf := bytes.Repeat([]byte{0xcc}, cacheTestBlockSize)
		c.buffers[ref] = buf
		stagers[i] = &stager{buf: buf, ref: ref, scratch: tr.slots[i+1]}
		tr.stagers <- stagers[i]
	}
	ctx, cancel := context.WithCancel(context.Background())
	v := &Volume{
		c: c, e: newEngineWithTimeout(c, time.Second), tier: tr, store: store,
		blockSize: cacheTestBlockSize, ioUnit: 1024, readahead: 2 * cacheTestBlockSize,
		bgCtx: ctx, cancel: cancel,
	}
	tr.initCache(v)
	f := &cacheFixture{v: v, c: c, store: store, stagers: stagers}
	t.Cleanup(func() { _ = v.Close() })
	return f
}

func cacheTestKey(id uint64) blockKey {
	return blockKey{id: id, size: cacheTestBlockSize}
}

type cacheAcquireResult struct {
	sid int32
	err error
}

func cacheAcquire(v *Volume, ctx context.Context, k blockKey) cacheAcquireResult {
	v.mu.RLock()
	defer v.mu.RUnlock()
	sid, err := v.tier.acquire(ctx, k)
	return cacheAcquireResult{sid: sid, err: err}
}

func asyncCacheAcquire(v *Volume, ctx context.Context, k blockKey) <-chan cacheAcquireResult {
	out := make(chan cacheAcquireResult, 1)
	go func() { out <- cacheAcquire(v, ctx, k) }()
	return out
}

func releaseCache(v *Volume, sid int32) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	v.tier.release(sid)
}

func prefetchCache(v *Volume, k blockKey) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	v.tier.prefetch(k)
}

func TestTierCacheHitAndPadding(t *testing.T) {
	data := bytes.Repeat([]byte{0x2a}, 1300)
	f := newCacheFixture(t, 2, 2, &cacheTestStore{data: map[uint64][]byte{1: data}})
	k := blockKey{id: 1, size: uint32(len(data))}
	a := cacheAcquire(f.v, context.Background(), k)
	if a.err != nil {
		t.Fatal(a.err)
	}
	b := cacheAcquire(f.v, context.Background(), k)
	if b.err != nil || b.sid != a.sid {
		t.Fatalf("hit = %+v, first = %+v", b, a)
	}
	got := f.c.contents(f.v.tier.ref(a.sid), 3072)
	if !bytes.Equal(got[:1300], data) || !bytes.Equal(got[1300:2048], make([]byte, 748)) {
		t.Fatal("block contents or aligned padding changed")
	}
	if !bytes.Equal(got[2048:], bytes.Repeat([]byte{0xa5}, 1024)) {
		t.Fatal("fill wrote beyond its aligned length")
	}
	if f.store.reads.Load() != 1 || f.c.submits.Load() != 1 {
		t.Fatalf("reads=%d writes=%d, want 1 each", f.store.reads.Load(), f.c.submits.Load())
	}
	releaseCache(f.v, a.sid)
	releaseCache(f.v, b.sid)
}

func TestTierCacheFillPublicationAndDeduplication(t *testing.T) {
	f := newCacheFixture(t, 2, 2, nil)
	f.c.delay.Store(true)
	k := cacheTestKey(1)
	owner := asyncCacheAcquire(f.v, context.Background(), k)
	s := receive(t, f.c.submitted)
	f.v.tier.mu.Lock()
	_, published := f.v.tier.index[k]
	f.v.tier.mu.Unlock()
	if published || len(f.v.tier.stagers) != 1 {
		t.Fatal("fill published or recycled its stager before DMA completion")
	}
	const waiters = 8
	results := make([]<-chan cacheAcquireResult, waiters)
	for i := range results {
		results[i] = asyncCacheAcquire(f.v, context.Background(), k)
	}
	f.c.complete(s)
	a := receive(t, owner)
	if a.err != nil {
		t.Fatal(a.err)
	}
	for _, ch := range results {
		r := receive(t, ch)
		if r.err != nil || r.sid != a.sid {
			t.Fatalf("duplicate acquisition = %+v, first = %+v", r, a)
		}
		releaseCache(f.v, r.sid)
	}
	releaseCache(f.v, a.sid)
	if f.store.reads.Load() != 1 || f.c.submits.Load() != 1 || len(f.v.tier.stagers) != 2 {
		t.Fatalf("reads=%d writes=%d available=%d", f.store.reads.Load(), f.c.submits.Load(), len(f.v.tier.stagers))
	}
}

// cacheWaitContext reports entry into a select that observes cancellation.
// acquire checks Err before its metadata lookup; a waiter for an existing
// flight reaches Done only when constructing the flight wait select.
type cacheWaitContext struct {
	context.Context
	once     sync.Once
	observed chan struct{}
}

func (c *cacheWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

func TestTierStagerWaitCancellation(t *testing.T) {
	f := newCacheFixture(t, 2, 2, nil)
	f.v.mu.RLock()
	a, err := f.v.tier.getStager(context.Background())
	if err != nil {
		f.v.mu.RUnlock()
		t.Fatal(err)
	}
	b, err := f.v.tier.getStager(context.Background())
	f.v.mu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	defer f.v.tier.putStager(a)
	defer f.v.tier.putStager(b)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waitCtx := &cacheWaitContext{Context: ctx, observed: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		f.v.mu.RLock()
		defer f.v.mu.RUnlock()
		_, err := f.v.tier.getStager(waitCtx)
		done <- err
	}()
	receive(t, waitCtx.observed)
	cancel()
	if err := receive(t, done); err != context.Canceled {
		t.Fatalf("stager wait cancellation = %v", err)
	}
}

func TestTierCacheWaiterCancellationDoesNotCancelOwner(t *testing.T) {
	f := newCacheFixture(t, 2, 2, nil)
	f.c.delay.Store(true)
	k := cacheTestKey(1)
	owner := asyncCacheAcquire(f.v, context.Background(), k)
	s := receive(t, f.c.submitted)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waitCtx := &cacheWaitContext{Context: ctx, observed: make(chan struct{})}
	waiter := asyncCacheAcquire(f.v, waitCtx, k)
	receive(t, waitCtx.observed)
	cancel()
	if r := receive(t, waiter); r.err != context.Canceled {
		t.Fatalf("waiter cancellation = %+v", r)
	}
	f.c.complete(s)
	r := receive(t, owner)
	if r.err != nil {
		t.Fatalf("owner after independent waiter cancellation = %+v", r)
	}
	releaseCache(f.v, r.sid)
}

func TestTierCacheSharesFillFailure(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce sync.Once
	releaseRead := sync.OnceFunc(func() { close(release) })
	store := &cacheTestStore{data: map[uint64][]byte{
		1: bytes.Repeat([]byte{1}, cacheTestBlockSize),
	}, onRead: func(ctx context.Context, _ uint64, _ int) error {
		enteredOnce.Do(func() { close(entered) })
		select {
		case <-release:
			return syscall.ETIMEDOUT
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	f := newCacheFixture(t, 2, 2, store)
	t.Cleanup(releaseRead)
	k := cacheTestKey(1)
	owner := asyncCacheAcquire(f.v, context.Background(), k)
	receive(t, entered)
	const n = 8
	waiters := make([]<-chan cacheAcquireResult, n)
	for i := range waiters {
		ctx := &cacheWaitContext{Context: context.Background(), observed: make(chan struct{})}
		waiters[i] = asyncCacheAcquire(f.v, ctx, k)
		receive(t, ctx.observed)
	}
	releaseRead()
	for _, ch := range append(waiters, owner) {
		if r := receive(t, ch); r.err != syscall.EIO {
			t.Fatalf("shared fill failure = %+v", r)
		}
	}
	if store.reads.Load() != 1 || f.v.e.broken.Load() || len(f.v.tier.stagers) != 2 {
		t.Fatalf("reads=%d broken=%v available=%d", store.reads.Load(), f.v.e.broken.Load(), len(f.v.tier.stagers))
	}
}

func TestTierCacheOwnerCancellationAllowsWaiterRetry(t *testing.T) {
	entered := make(chan struct{})
	var first atomic.Bool
	store := &cacheTestStore{data: map[uint64][]byte{
		1: bytes.Repeat([]byte{1}, cacheTestBlockSize),
	}, onRead: func(ctx context.Context, _ uint64, _ int) error {
		if first.CompareAndSwap(false, true) {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}}
	f := newCacheFixture(t, 2, 2, store)
	k := cacheTestKey(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	owner := asyncCacheAcquire(f.v, ctx, k)
	receive(t, entered)
	waitCtx := &cacheWaitContext{Context: context.Background(), observed: make(chan struct{})}
	waiter := asyncCacheAcquire(f.v, waitCtx, k)
	receive(t, waitCtx.observed)
	cancel()
	if r := receive(t, owner); r.err != context.Canceled {
		t.Fatalf("owner cancellation = %+v", r)
	}
	r := receive(t, waiter)
	if r.err != nil {
		t.Fatalf("independent waiter = %+v", r)
	}
	releaseCache(f.v, r.sid)
	if store.reads.Load() != 2 || f.c.submits.Load() != 1 {
		t.Fatalf("reads=%d writes=%d", store.reads.Load(), f.c.submits.Load())
	}
}

// waitForCacheSlotWaiter observes the allocator's actual wait predicate rather
// than guessing when another goroutine has run. No sleep is used to order work.
func waitForCacheSlotWaiter(t *testing.T, tr *tier) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		tr.mu.Lock()
		waiting := tr.changed != nil
		tr.mu.Unlock()
		if waiting {
			return
		}
		select {
		case <-deadline:
			t.Fatal("allocator did not begin waiting for a slot")
		default:
			runtime.Gosched()
		}
	}
}

func TestTierCachePinnedEvictionAndCancellation(t *testing.T) {
	f := newCacheFixture(t, 1, 2, nil)
	a := cacheAcquire(f.v, context.Background(), cacheTestKey(1))
	if a.err != nil {
		t.Fatal(a.err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if r := cacheAcquire(f.v, ctx, cacheTestKey(2)); r.err != context.Canceled {
		t.Fatalf("cancelled acquire = %+v", r)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	pending := asyncCacheAcquire(f.v, ctx, cacheTestKey(2))
	waitForCacheSlotWaiter(t, f.v.tier)
	cancel()
	if r := receive(t, pending); r.err != context.Canceled {
		t.Fatalf("waiting acquire = %+v", r)
	}
	if !bytes.Equal(f.c.contents(f.v.tier.ref(a.sid), cacheTestBlockSize), f.store.data[1]) {
		t.Fatal("pinned block was overwritten")
	}
	releaseCache(f.v, a.sid)
	b := cacheAcquire(f.v, context.Background(), cacheTestKey(2))
	if b.err != nil || b.sid != a.sid {
		t.Fatalf("eviction after release = %+v", b)
	}
	releaseCache(f.v, b.sid)
	f.v.tier.mu.Lock()
	_, old := f.v.tier.index[cacheTestKey(1)]
	f.v.tier.mu.Unlock()
	if old {
		t.Fatal("evicted block remains indexed")
	}
}

func TestTierCacheReleaseWakesSlotWaiter(t *testing.T) {
	f := newCacheFixture(t, 1, 2, nil)
	a := cacheAcquire(f.v, context.Background(), cacheTestKey(1))
	if a.err != nil {
		t.Fatal(a.err)
	}
	releasePin := sync.OnceFunc(func() { releaseCache(f.v, a.sid) })
	defer releasePin()
	waiter := asyncCacheAcquire(f.v, context.Background(), cacheTestKey(2))
	waitForCacheSlotWaiter(t, f.v.tier)
	releasePin()
	b := receive(t, waiter)
	if b.err != nil || b.sid != a.sid {
		t.Fatalf("acquisition after release = %+v", b)
	}
	releaseCache(f.v, b.sid)
}

func TestTierCacheFailureQuarantinesDMAResources(t *testing.T) {
	f := newCacheFixture(t, 2, 2, nil)
	f.c.delay.Store(true)
	a := asyncCacheAcquire(f.v, context.Background(), cacheTestKey(1))
	b := asyncCacheAcquire(f.v, context.Background(), cacheTestKey(2))
	pending := []submission{receive(t, f.c.submitted), receive(t, f.c.submitted)}
	want := make(map[uint64][]byte)
	for _, s := range pending {
		want[s.ref] = bytes.Clone(f.c.buffers[s.ref][:s.completed])
	}
	waitCtx := &cacheWaitContext{Context: context.Background(), observed: make(chan struct{})}
	waiting := asyncCacheAcquire(f.v, waitCtx, cacheTestKey(3))
	receive(t, waitCtx.observed)
	dupCtx := &cacheWaitContext{Context: context.Background(), observed: make(chan struct{})}
	duplicate := asyncCacheAcquire(f.v, dupCtx, cacheTestKey(1))
	receive(t, dupCtx.observed)
	f.c.badPoll.Store(true)
	for _, ch := range []<-chan cacheAcquireResult{a, b, waiting, duplicate} {
		if r := receive(t, ch); r.err != syscall.EIO {
			t.Fatalf("acquire after poll failure = %+v", r)
		}
	}
	if len(f.v.tier.stagers) != 0 {
		t.Fatal("a stager referenced by pending DMA was recycled")
	}
	f.v.tier.mu.Lock()
	for sid := f.v.tier.cacheStart; sid < int32(len(f.v.tier.metas)); sid++ {
		if f.v.tier.metas[sid].state != slotQuarantined {
			f.v.tier.mu.Unlock()
			t.Fatalf("slot %d was not quarantined", sid)
		}
	}
	f.v.tier.mu.Unlock()
	// The failure notification must prevent even explicit returns from making
	// dangerous CPU memory available again. Late transfers still read it below.
	f.v.tier.putStager(f.stagers[0])
	if len(f.v.tier.stagers) != 0 {
		t.Fatal("putStager recycled memory after sticky failure")
	}
	f.c.badPoll.Store(false)
	for _, s := range pending {
		f.c.complete(s)
		if !bytes.Equal(f.c.contents(slotRef{blob: s.blob, off: s.blobOff}, int(s.completed)), want[s.ref]) {
			t.Fatal("late DMA observed a prematurely reused staging buffer")
		}
	}
	if err := f.v.Close(); err != syscall.EIO {
		t.Fatalf("Close after uncertain poll = %v", err)
	}
}

func TestTierPrefetchDeduplicationAndRange(t *testing.T) {
	entered := make(chan int, 2)
	release := make(chan struct{})
	store := &cacheTestStore{data: map[uint64][]byte{
		1: bytes.Repeat([]byte{1}, 3*cacheTestBlockSize),
	}, onRead: func(ctx context.Context, _ uint64, off int) error {
		entered <- off
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	f := newCacheFixture(t, 4, 4, store)
	k := blockKey{id: 1, size: 3 * cacheTestBlockSize}
	prefetchCache(f.v, k)
	a, b := receive(t, entered), receive(t, entered)
	if a == b || a == 0 || b == 0 {
		t.Fatalf("prefetch offsets = %d, %d", a, b)
	}
	prefetchCache(f.v, k)
	close(release)
	f.v.bg.Wait()
	for i := uint32(1); i <= 2; i++ {
		key := blockKey{id: 1, size: k.size, indx: i}
		r := cacheAcquire(f.v, context.Background(), key)
		if r.err != nil {
			t.Fatal(r.err)
		}
		releaseCache(f.v, r.sid)
	}
	if store.reads.Load() != 2 || f.c.submits.Load() != 2 {
		t.Fatalf("prefetch reads=%d writes=%d, want 2 each", store.reads.Load(), f.c.submits.Load())
	}
	prefetchCache(f.v, blockKey{id: 1, size: k.size, indx: 2})
	f.v.bg.Wait()
	if store.reads.Load() != 2 {
		t.Fatal("prefetch crossed the slice boundary")
	}
}

func TestTierPrefetchSkipsUnavailableSlots(t *testing.T) {
	store := &cacheTestStore{data: map[uint64][]byte{
		1: bytes.Repeat([]byte{1}, 3*cacheTestBlockSize),
	}}
	f := newCacheFixture(t, 1, 2, store)
	k := blockKey{id: 1, size: 3 * cacheTestBlockSize}
	r := cacheAcquire(f.v, context.Background(), k)
	if r.err != nil {
		t.Fatal(r.err)
	}
	prefetchCache(f.v, k)
	f.v.bg.Wait()
	if store.reads.Load() != 1 || len(f.v.tier.stagers) != 2 {
		t.Fatal("prefetch waited for a pinned slot or retained a stager")
	}
	releaseCache(f.v, r.sid)
}

func TestTierCloseCancelsActualPrefetch(t *testing.T) {
	entered := make(chan struct{}, 1)
	cancelled := make(chan struct{}, 1)
	store := &cacheTestStore{data: map[uint64][]byte{
		1: bytes.Repeat([]byte{1}, 3*cacheTestBlockSize),
	}, onRead: func(ctx context.Context, _ uint64, _ int) error {
		entered <- struct{}{}
		<-ctx.Done()
		cancelled <- struct{}{}
		return ctx.Err()
	}}
	f := newCacheFixture(t, 2, 2, store)
	prefetchCache(f.v, blockKey{id: 1, size: 3 * cacheTestBlockSize})
	receive(t, entered)
	done := make(chan error, 1)
	go func() { done <- f.v.Close() }()
	receive(t, cancelled)
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
	if !f.c.closed.Load() {
		t.Fatal("Close did not close the client after its prefetch exited")
	}
	select {
	case <-f.v.e.exited:
	default:
		t.Fatal("client closed before the engine exited")
	}
}

func BenchmarkTierHit(b *testing.B) {
	f := newCacheFixture(b, 2, 2, nil)
	k := cacheTestKey(1)
	first := cacheAcquire(f.v, context.Background(), k)
	if first.err != nil {
		b.Fatal(first.err)
	}
	defer releaseCache(f.v, first.sid)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := cacheAcquire(f.v, context.Background(), k)
		if r.err != nil {
			b.Fatal(r.err)
		}
		releaseCache(f.v, r.sid)
	}
}

func BenchmarkTierHitParallel(b *testing.B) {
	f := newCacheFixture(b, 2, 2, nil)
	k := cacheTestKey(1)
	first := cacheAcquire(f.v, context.Background(), k)
	if first.err != nil {
		b.Fatal(first.err)
	}
	defer releaseCache(f.v, first.sid)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r := cacheAcquire(f.v, context.Background(), k)
			if r.err != nil {
				b.Error(r.err)
				return
			}
			releaseCache(f.v, r.sid)
		}
	})
}
