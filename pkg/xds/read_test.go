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
	"errors"
	"io"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/fs"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/vfs"
)

// readDeviceClient extends the existing byte-moving fake with registered GPU
// buffers. Device pointers remain opaque numbers; all map access uses the same
// mutex as delayed DMA so registration cannot race a real byte transfer.
type readDeviceClient struct {
	*cacheTestClient
	registered map[uintptr]uint64
	nextRef    uint64
}

func (c *readDeviceClient) registerGPU(ptr uintptr, size uint64) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextRef++
	ref := c.nextRef
	c.buffers[ref] = bytes.Repeat([]byte{0x7d}, int(size))
	c.registered[ptr] = ref
	return ref, nil
}

func (c *readDeviceClient) unregister(ref uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.buffers, ref)
	for ptr, registered := range c.registered {
		if registered == ref {
			delete(c.registered, ptr)
			break
		}
	}
	return nil
}

func (c *readDeviceClient) deviceBytes(ref uint64) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return bytes.Clone(c.buffers[ref])
}

// The real metadata engine owns the file handle and its length. Only the slice
// map is replaced, allowing precise overwrite/EOF layouts without exporting
// constructors for fs.File or depending on a particular writer's compaction.
type readTestMeta struct {
	meta.Meta
	slices        map[uint32][]meta.Slice
	err           syscall.Errno
	reads         atomic.Int32
	invalidations atomic.Int32
	onInvalidate  func(meta.Context, meta.Ino, uint32) syscall.Errno
}

func (m *readTestMeta) Read(_ meta.Context, _ meta.Ino, indx uint32, out *[]meta.Slice) syscall.Errno {
	m.reads.Add(1)
	if m.err != 0 {
		return m.err
	}
	*out = m.slices[indx]
	return 0
}

func (m *readTestMeta) InvalidateChunkCache(ctx meta.Context, ino meta.Ino, indx uint32) syscall.Errno {
	m.invalidations.Add(1)
	if m.onInvalidate != nil {
		return m.onInvalidate(ctx, ino, indx)
	}
	return m.Meta.InvalidateChunkCache(ctx, ino, indx)
}

type readFixture struct {
	*cacheFixture
	device *readDeviceClient
	m      *readTestMeta
	file   *fs.File
	ptr    uintptr
	ref    uint64
}

func readTestConfig() *vfs.Config {
	mc := meta.DefaultConf()
	mc.NoBGJob, mc.NoCompact = true, true
	return &vfs.Config{
		Meta: mc,
		Chunk: &chunk.Config{
			BlockSize: cacheTestBlockSize, MaxUpload: 2, MaxDownload: 4,
			BufferSize: 1 << 20, Compress: "none", CacheDir: "memory",
		},
	}
}

func readTestFileSystem(t testing.TB, conf *vfs.Config, m meta.Meta, store chunk.ChunkStore) *fs.FileSystem {
	t.Helper()
	if err := m.Init(&meta.Format{Name: "xds-read", BlockSize: cacheTestBlockSize >> 10}, true); err != nil {
		t.Fatal(err)
	}
	jfs, err := fs.NewFileSystem(conf, m, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = jfs.Close()
		_ = m.Shutdown()
	})
	return jfs
}

// attachReadDevice uses the tiny cache fixture, but binds its borrowed metadata
// and store to the FileSystem under test. The fixture's deliberately dirty zero
// slot is initialized here, matching Open's successful-initialization invariant.
func attachReadDevice(t testing.TB, m meta.Meta, store chunk.ChunkStore, cacheStore *cacheTestStore) (*cacheFixture, *readDeviceClient, uintptr, uint64) {
	t.Helper()
	f := newCacheFixture(t, 8, 2, cacheStore)
	f.v.m, f.v.store, f.v.tier.store = m, store, store
	f.v.readahead, f.v.tier.readahead = 0, 0
	clear(f.c.blobs[1][:cacheTestBlockSize])
	c := &readDeviceClient{cacheTestClient: f.c, registered: make(map[uintptr]uint64), nextRef: 100}
	f.v.c = c
	const ptr = uintptr(0x10000)
	if err := f.v.Register(ptr, 16*cacheTestBlockSize); err != nil {
		t.Fatal(err)
	}
	return f, c, ptr, c.registered[ptr]
}

func newReadFixture(t testing.TB, fileLen uint64, slices map[uint32][]meta.Slice, data map[uint64][]byte) *readFixture {
	t.Helper()
	conf := readTestConfig()
	m := &readTestMeta{Meta: meta.NewClient("memkv://", conf.Meta), slices: slices}
	store := &cacheTestStore{data: data}
	jfs := readTestFileSystem(t, conf, m, store)
	ctx := meta.Background()
	f, eno := jfs.Create(ctx, "/data", 0644, 0)
	if eno != 0 {
		t.Fatal(eno)
	}
	if fileLen != 0 {
		if eno := f.Truncate(ctx, fileLen); eno != 0 {
			t.Fatal(eno)
		}
	}
	if eno := f.Close(ctx); eno != 0 {
		t.Fatal(eno)
	}
	f, eno = jfs.Open(ctx, "/data", vfs.MODE_MASK_R)
	if eno != 0 {
		t.Fatal(eno)
	}
	t.Cleanup(func() { _ = f.Close(meta.Background()) })
	cache, device, ptr, ref := attachReadDevice(t, m, store, store)
	// File construction may invalidate metadata during truncate. Only count
	// recovery invalidations performed by the ReadAt operation under test.
	m.invalidations.Store(0)
	return &readFixture{cacheFixture: cache, device: device, m: m, file: f, ptr: ptr, ref: ref}
}

func readPattern(n int, seed byte) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = seed + byte(i*31+i/251)
	}
	return p
}

func checkReadDevice(t *testing.T, device *readDeviceClient, ref uint64, offset int, want []byte, u int) {
	t.Helper()
	got := device.deviceBytes(ref)
	end := offset + len(want)
	alignedEnd := offset + (len(want)+u-1)/u*u
	if !bytes.Equal(got[offset:end], want) {
		t.Fatalf("GPU bytes differ from CPU read: got=%x want=%x", got[offset:min(end, offset+64)], want[:min(len(want), 64)])
	}
	if !bytes.Equal(got[end:alignedEnd], make([]byte, alignedEnd-end)) {
		t.Fatal("EOF alignment padding contains nonzero data")
	}
	if !bytes.Equal(got[:offset], bytes.Repeat([]byte{0x7d}, offset)) ||
		!bytes.Equal(got[alignedEnd:], bytes.Repeat([]byte{0x7d}, len(got)-alignedEnd)) {
		t.Fatal("DMA modified bytes outside the requested result and its padding")
	}
}

func TestReadAtData(t *testing.T) {
	const bs = cacheTestBlockSize
	original, replacement := readPattern(2*bs, 13), readPattern(1511, 107)
	for _, tc := range []struct {
		name    string
		fileLen uint64
		slices  map[uint32][]meta.Slice
		data    map[uint64][]byte
		off     int64
		size    int
	}{
		{"aligned", 2 * bs, map[uint32][]meta.Slice{0: {{Id: 1, Size: 2 * bs, Len: 2 * bs}}}, map[uint64][]byte{1: original}, 0, 2 * bs},
		{"unaligned overwrite", 2 * bs, map[uint32][]meta.Slice{0: {
			{Id: 1, Size: 2 * bs, Len: 777},
			{Id: 2, Size: 1511, Len: 1511},
			{Id: 1, Size: 2 * bs, Off: 2288, Len: 2*bs - 2288},
		}}, map[uint64][]byte{1: original, 2: replacement}, 0, 2 * bs},
		{"nonzero source and request offsets", 2 * bs, map[uint32][]meta.Slice{0: {{Id: 1, Size: 2 * bs, Len: 2 * bs}}}, map[uint64][]byte{1: original}, 1024, bs},
		{"explicit and trailing holes", 2 * bs, map[uint32][]meta.Slice{0: {
			{Id: 1, Size: 2 * bs, Len: 1024}, {Len: 2300}, {Id: 2, Size: 1511, Len: 777},
		}}, map[uint64][]byte{1: original, 2: replacement}, 0, 2 * bs},
		{"all hole", 5123, nil, nil, 0, 2 * bs},
		{"short object tail", 1300, map[uint32][]meta.Slice{0: {{Id: 1, Size: 1300, Len: 1300}}}, map[uint64][]byte{1: original[:1300]}, 0, bs},
		// These two EOF cases have identical visible bytes. The second must
		// assemble instead of exposing the longer object's hidden suffix.
		{"EOF inside longer object", 1300, map[uint32][]meta.Slice{0: {{Id: 1, Size: 2 * bs, Len: 1300}}}, map[uint64][]byte{1: original}, 0, bs},
		{"cross chunk", meta.ChunkSize + bs, map[uint32][]meta.Slice{
			0: {{Len: meta.ChunkSize - 1024}, {Id: 1, Size: 2 * bs, Off: 2048, Len: 1024}},
			1: {{Id: 2, Size: 1511, Len: 1511}},
		}, map[uint64][]byte{1: original, 2: replacement}, meta.ChunkSize - 1024, 6 * 1024},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReadFixture(t, tc.fileLen, tc.slices, tc.data)
			cpu := make([]byte, tc.size)
			n, err := f.file.Pread(meta.Background(), cpu, tc.off)
			if err != nil {
				t.Fatal(err)
			}
			// A nonzero registration-relative offset also verifies that the
			// planner never accidentally incorporates dstBase into its pieces.
			dst := f.v.IOUnit()
			got, err := f.v.ReadAt(meta.Background(), f.file, f.ptr+uintptr(dst), tc.size, tc.off)
			if err != nil || got != n {
				t.Fatalf("ReadAt=(%d,%v); CPU=(%d,nil)", got, err, n)
			}
			checkReadDevice(t, f.device, f.ref, dst, cpu[:n], f.v.IOUnit())
		})
	}
}

func TestReadAtWarmCache(t *testing.T) {
	f := newReadFixture(t, cacheTestBlockSize,
		map[uint32][]meta.Slice{0: {{Id: 1, Size: cacheTestBlockSize, Len: cacheTestBlockSize}}},
		map[uint64][]byte{1: readPattern(cacheTestBlockSize, 17)})
	for i := 0; i < 2; i++ {
		before := f.c.submits.Load()
		n, err := f.v.ReadAt(meta.Background(), f.file, f.ptr, cacheTestBlockSize, 0)
		if n != cacheTestBlockSize || err != nil {
			t.Fatalf("ReadAt=(%d,%v)", n, err)
		}
		wantDMA := int64(2) // First read fills a slot and then reads it to GPU.
		if i != 0 {
			wantDMA = 1
		}
		if f.c.submits.Load()-before != wantDMA || f.store.reads.Load() != 1 {
			t.Fatalf("read %d: DMA=%d host reads=%d", i, f.c.submits.Load()-before, f.store.reads.Load())
		}
	}
}

func TestReadAtPrefetchStartsAfterLastRequestedBlock(t *testing.T) {
	data := readPattern(3*cacheTestBlockSize, 83)
	f := newReadFixture(t, uint64(len(data)),
		map[uint32][]meta.Slice{0: {{Id: 1, Size: uint32(len(data)), Len: uint32(len(data))}}},
		map[uint64][]byte{1: data})
	f.v.readahead, f.v.tier.readahead = 2*cacheTestBlockSize, 2*cacheTestBlockSize
	n, err := f.v.ReadAt(meta.Background(), f.file, f.ptr, 2*cacheTestBlockSize, 0)
	if err != nil || n != 2*cacheTestBlockSize {
		t.Fatalf("ReadAt=(%d,%v)", n, err)
	}
	// ReadAt finished all background admission before returning. No other
	// foreground operation can add a worker while this test waits for prefetch.
	f.v.bg.Wait()
	key := blockKey{id: 1, size: uint32(len(data)), indx: 2}
	f.v.tier.mu.Lock()
	sid, cached := f.v.tier.index[key]
	ready := cached && f.v.tier.metas[sid].state == slotReady && f.v.tier.metas[sid].refs == 0
	f.v.tier.mu.Unlock()
	if !ready || f.store.reads.Load() != 3 {
		t.Fatalf("following block ready=%t, storage reads=%d; want true,3", ready, f.store.reads.Load())
	}
	checkReadDevice(t, f.device, f.ref, 0, data[:n], f.v.IOUnit())
}

func TestReadAtInvalidArgumentsAndEOF(t *testing.T) {
	f := newReadFixture(t, cacheTestBlockSize, nil, nil)
	ctx := meta.Background()
	for _, tc := range []struct {
		name string
		ctx  meta.Context
		file *fs.File
		ptr  uintptr
		n    int
		off  int64
	}{
		{"nil context", nil, f.file, f.ptr, 1024, 0},
		{"nil file", ctx, nil, f.ptr, 1024, 0},
		{"zero size", ctx, f.file, f.ptr, 0, 0},
		{"negative size", ctx, f.file, f.ptr, -1024, 0},
		{"negative offset", ctx, f.file, f.ptr, 1024, -1024},
		{"unaligned size", ctx, f.file, f.ptr, 1025, 0},
		{"unaligned offset", ctx, f.file, f.ptr, 1024, 1},
		{"unaligned device offset", ctx, f.file, f.ptr + 1, 1024, 0},
		{"unregistered device", ctx, f.file, f.ptr - 1024, 1024, 0},
		{"registration overrun", ctx, f.file, f.ptr + 16*cacheTestBlockSize - 1024, 2048, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if n, err := f.v.ReadAt(tc.ctx, tc.file, tc.ptr, tc.n, tc.off); n != 0 || err != syscall.EINVAL {
				t.Fatalf("ReadAt=(%d,%v); want (0,EINVAL)", n, err)
			}
		})
	}
	if n, err := f.v.ReadAt(ctx, f.file, f.ptr, 1024, cacheTestBlockSize); n != 0 || err != io.EOF {
		t.Fatalf("at EOF: (%d,%v)", n, err)
	}
	if f.c.submits.Load() != 0 || f.m.reads.Load() != 0 {
		t.Fatal("invalid/EOF request issued storage or DMA operations")
	}
	cancelled := meta.Background()
	cancelled.Cancel()
	if n, err := f.v.ReadAt(cancelled, f.file, f.ptr, 1024, 0); n != 0 || err != syscall.EINTR {
		t.Fatalf("cancelled request: (%d,%v)", n, err)
	}
	if err := f.v.Close(); err != nil {
		t.Fatal(err)
	}
	if n, err := f.v.ReadAt(ctx, f.file, f.ptr, 1024, 0); n != 0 || err != syscall.EBADF {
		t.Fatalf("closed volume: (%d,%v)", n, err)
	}
}

func TestReadAtErrors(t *testing.T) {
	for _, metadata := range []bool{true, false} {
		t.Run(map[bool]string{true: "metadata", false: "storage"}[metadata], func(t *testing.T) {
			f := newReadFixture(t, cacheTestBlockSize,
				map[uint32][]meta.Slice{0: {{Id: 1, Size: cacheTestBlockSize, Len: cacheTestBlockSize}}},
				map[uint64][]byte{1: readPattern(cacheTestBlockSize, 7)})
			want := syscall.EIO
			if metadata {
				f.m.err = syscall.EACCES
				want = syscall.EACCES
			} else {
				f.store.onRead = func(context.Context, uint64, int) error { return errors.New("object unavailable") }
			}
			if n, err := f.v.ReadAt(meta.Background(), f.file, f.ptr, cacheTestBlockSize, 0); n != 0 || err != want {
				t.Fatalf("ReadAt=(%d,%v); want (0,%v)", n, err, want)
			}
			if err := f.v.Unregister(f.ptr); err != nil {
				t.Fatalf("non-DMA failure retained registration: %v", err)
			}
		})
	}
}

func TestReadAtStorageRetry(t *testing.T) {
	for _, assembled := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "assembled"}[assembled], func(t *testing.T) {
			data := readPattern(2*cacheTestBlockSize, 71)
			s := meta.Slice{Id: 1, Size: uint32(len(data)), Len: cacheTestBlockSize}
			if assembled {
				s.Off = 1
			}
			f := newReadFixture(t, cacheTestBlockSize, map[uint32][]meta.Slice{0: {s}}, map[uint64][]byte{1: data})
			f.v.maxRetries = 1
			f.store.onRead = func(context.Context, uint64, int) error {
				if f.store.reads.Load() == 1 {
					return errors.New("temporary object read failure")
				}
				return nil
			}
			n, err := f.v.ReadAt(meta.Background(), f.file, f.ptr, cacheTestBlockSize, 0)
			if n != cacheTestBlockSize || err != nil {
				t.Fatalf("recovered ReadAt=(%d,%v)", n, err)
			}
			wantReads := int32(2)
			if assembled {
				// The successful assembled attempt reads two pieces separated by
				// a source-block boundary; the failed attempt reads only the first.
				wantReads++
			}
			if f.store.reads.Load() != wantReads || f.m.reads.Load() != 2 || f.m.invalidations.Load() != 1 {
				t.Fatalf("storage reads=%d plans=%d invalidations=%d", f.store.reads.Load(), f.m.reads.Load(), f.m.invalidations.Load())
			}
			if f.v.e.broken.Load() || len(f.v.tier.stagers) != 2 {
				t.Fatal("recoverable storage failure broke the engine or retained its stager")
			}
			checkReadDevice(t, f.device, f.ref, 0, data[int(s.Off):int(s.Off)+n], f.v.IOUnit())
		})
	}
}

func TestReadAtRetryInvalidatesAndReplans(t *testing.T) {
	data := readPattern(cacheTestBlockSize, 101)
	f := newReadFixture(t, cacheTestBlockSize,
		map[uint32][]meta.Slice{0: {{Id: 1, Size: cacheTestBlockSize, Len: cacheTestBlockSize}}},
		map[uint64][]byte{2: data})
	f.v.maxRetries = 1
	f.store.onRead = func(_ context.Context, id uint64, _ int) error {
		if id == 1 {
			return errors.New("old slice was removed after compaction")
		}
		return nil
	}
	f.m.onInvalidate = func(_ meta.Context, ino meta.Ino, indx uint32) syscall.Errno {
		if ino != f.file.Inode() || indx != 0 {
			t.Errorf("invalidated inode=%d chunk=%d", ino, indx)
		}
		// This mutation is sequenced after all failed-attempt workers joined.
		// Reusing the old windows would continue asking for the missing slice.
		f.m.slices[0] = []meta.Slice{{Id: 2, Size: cacheTestBlockSize, Len: cacheTestBlockSize}}
		return 0
	}
	n, err := f.v.ReadAt(meta.Background(), f.file, f.ptr, cacheTestBlockSize, 0)
	if n != cacheTestBlockSize || err != nil {
		t.Fatalf("replanned ReadAt=(%d,%v)", n, err)
	}
	if f.store.reads.Load() != 2 || f.m.reads.Load() != 2 || f.m.invalidations.Load() != 1 {
		t.Fatalf("reads=%d plans=%d invalidations=%d; expected one fresh plan", f.store.reads.Load(), f.m.reads.Load(), f.m.invalidations.Load())
	}
	checkReadDevice(t, f.device, f.ref, 0, data, f.v.IOUnit())
}

func TestReadAtRetryInvalidatesCrossChunkRange(t *testing.T) {
	const n = 1024
	first, second := readPattern(n, 103), readPattern(n, 107)
	f := newReadFixture(t, meta.ChunkSize+cacheTestBlockSize,
		map[uint32][]meta.Slice{
			0: {{Len: meta.ChunkSize - n}, {Id: 1, Size: n, Len: n}},
			1: {{Id: 2, Size: n, Len: n}},
		}, map[uint64][]byte{1: first, 2: second})
	f.v.maxRetries = 1
	f.store.onRead = func(context.Context, uint64, int) error {
		if f.store.reads.Load() == 1 {
			return errors.New("temporary read failure at the previous chunk tail")
		}
		return nil
	}
	ctx := meta.NewContext(42, 7, []uint32{9})
	var invalidated []uint32
	f.m.onInvalidate = func(got meta.Context, ino meta.Ino, indx uint32) syscall.Errno {
		if got != ctx || got.Uid() != 7 || got.Gid() != 9 || ino != f.file.Inode() {
			t.Errorf("metadata recovery changed caller context or inode: uid=%d gid=%d inode=%d", got.Uid(), got.Gid(), ino)
		}
		invalidated = append(invalidated, indx)
		return 0
	}
	got, err := f.v.ReadAt(ctx, f.file, f.ptr, 2*n, meta.ChunkSize-n)
	if got != 2*n || err != nil {
		t.Fatalf("cross-chunk retry=(%d,%v)", got, err)
	}
	if len(invalidated) != 2 || invalidated[0] != 0 || invalidated[1] != 1 ||
		f.m.reads.Load() != 4 || f.store.reads.Load() != 3 {
		t.Fatalf("invalidated=%v plans=%d source reads=%d", invalidated, f.m.reads.Load(), f.store.reads.Load())
	}
	checkReadDevice(t, f.device, f.ref, 0, append(bytes.Clone(first), second...), f.v.IOUnit())
}

func TestReadAtRetryExhausted(t *testing.T) {
	f := newReadFixture(t, cacheTestBlockSize,
		map[uint32][]meta.Slice{0: {{Id: 1, Size: cacheTestBlockSize, Len: cacheTestBlockSize}}},
		map[uint64][]byte{1: readPattern(cacheTestBlockSize, 113)})
	f.v.maxRetries = 1
	f.store.onRead = func(context.Context, uint64, int) error { return errors.New("object remains unavailable") }
	if n, err := f.v.ReadAt(meta.Background(), f.file, f.ptr, cacheTestBlockSize, 0); n != 0 || err != syscall.EIO {
		t.Fatalf("exhausted ReadAt=(%d,%v)", n, err)
	}
	if f.store.reads.Load() != 2 || f.m.reads.Load() != 2 || f.m.invalidations.Load() != 1 {
		t.Fatalf("reads=%d plans=%d invalidations=%d; maxRetries=1 must allow exactly two attempts", f.store.reads.Load(), f.m.reads.Load(), f.m.invalidations.Load())
	}
	if f.c.submits.Load() != 0 || f.v.e.broken.Load() {
		t.Fatal("source failures issued DMA or broke the engine")
	}
	if err := f.v.Unregister(f.ptr); err != nil {
		t.Fatalf("exhausted source retries retained registration: %v", err)
	}
}

func TestReadAtRetryCancellation(t *testing.T) {
	f := newReadFixture(t, cacheTestBlockSize,
		map[uint32][]meta.Slice{0: {{Id: 1, Size: cacheTestBlockSize, Len: cacheTestBlockSize}}},
		map[uint64][]byte{1: readPattern(cacheTestBlockSize, 127)})
	f.v.maxRetries = 10
	f.store.onRead = func(context.Context, uint64, int) error { return errors.New("temporary object failure") }
	ctx := meta.Background()
	f.m.onInvalidate = func(_ meta.Context, _ meta.Ino, _ uint32) syscall.Errno {
		// Recovery has joined the failed attempt and reached its retry boundary.
		// Cancellation here must prevent backoff from starting another GET.
		ctx.Cancel()
		return 0
	}
	if n, err := f.v.ReadAt(ctx, f.file, f.ptr, cacheTestBlockSize, 0); n != 0 || err != syscall.EINTR {
		t.Fatalf("cancelled retry=(%d,%v)", n, err)
	}
	if f.store.reads.Load() != 1 || f.m.reads.Load() != 1 || f.m.invalidations.Load() != 1 {
		t.Fatalf("cancelled retry performed more work: reads=%d plans=%d invalidations=%d", f.store.reads.Load(), f.m.reads.Load(), f.m.invalidations.Load())
	}
	if err := f.v.Unregister(f.ptr); err != nil {
		t.Fatalf("cancelled source retry retained registration: %v", err)
	}
}

func TestReadRetryBackoffCancellation(t *testing.T) {
	f := newCacheFixture(t, 2, 2, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observed := &cacheWaitContext{Context: ctx, observed: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		f.v.mu.RLock()
		defer f.v.mu.RUnlock()
		// The capped retry would otherwise wait ten seconds. Observing Done
		// guarantees cancellation occurs after constructing the wait select.
		done <- f.v.waitReadRetry(observed, 30)
	}()
	receive(t, observed.observed)
	cancel()
	if err := receive(t, done); err != context.Canceled {
		t.Fatalf("cancelled retry backoff=%v", err)
	}
}

func TestReadRetryDelay(t *testing.T) {
	for _, tc := range []struct {
		retry int
		want  time.Duration
	}{
		{1, time.Millisecond},
		{2, 301 * time.Millisecond},
		{29, 8401 * time.Millisecond},
		{30, 10 * time.Second},
		{31, 10 * time.Second},
		{int(^uint(0) >> 1), 10 * time.Second},
	} {
		if got := readRetryDelay(tc.retry); got != tc.want {
			t.Errorf("readRetryDelay(%d)=%s; want %s", tc.retry, got, tc.want)
		}
	}
}

func TestReadAtMetadataFailureDoesNotRetry(t *testing.T) {
	f := newReadFixture(t, cacheTestBlockSize, nil, nil)
	f.v.maxRetries = 2
	f.m.err = syscall.EIO
	if n, err := f.v.ReadAt(meta.Background(), f.file, f.ptr, cacheTestBlockSize, 0); n != 0 || err != syscall.EIO {
		t.Fatalf("metadata failure=(%d,%v)", n, err)
	}
	if f.m.reads.Load() != 1 || f.m.invalidations.Load() != 0 || f.store.reads.Load() != 0 {
		t.Fatal("metadata EIO was mistaken for a retryable object read failure")
	}
}

func TestReadAtInvalidationFailureStopsRetry(t *testing.T) {
	f := newReadFixture(t, cacheTestBlockSize,
		map[uint32][]meta.Slice{0: {{Id: 1, Size: cacheTestBlockSize, Len: cacheTestBlockSize}}},
		map[uint64][]byte{1: readPattern(cacheTestBlockSize, 131)})
	f.v.maxRetries = 2
	f.store.onRead = func(context.Context, uint64, int) error { return errors.New("temporary object failure") }
	f.m.onInvalidate = func(meta.Context, meta.Ino, uint32) syscall.Errno { return syscall.EACCES }
	if n, err := f.v.ReadAt(meta.Background(), f.file, f.ptr, cacheTestBlockSize, 0); n != 0 || err != syscall.EACCES {
		t.Fatalf("failed invalidation=(%d,%v)", n, err)
	}
	if f.store.reads.Load() != 1 || f.m.reads.Load() != 1 || f.m.invalidations.Load() != 1 {
		t.Fatal("failed metadata invalidation did not terminate recovery")
	}
}

func TestReadAtEmptyAndForeignFile(t *testing.T) {
	empty := newReadFixture(t, 0, nil, nil)
	other := newReadFixture(t, cacheTestBlockSize, nil, nil)
	if n, err := empty.v.ReadAt(meta.Background(), empty.file, empty.ptr, 1024, 0); n != 0 || err != io.EOF {
		t.Fatalf("empty file=(%d,%v)", n, err)
	}
	if n, err := empty.v.ReadAt(meta.Background(), other.file, empty.ptr, 1024, 0); n != 0 || err != syscall.EINVAL {
		t.Fatalf("foreign file=(%d,%v)", n, err)
	}
}

type deviceReadResult struct {
	n   int
	err error
}

func asyncReadAt(f *readFixture, ctx meta.Context) <-chan deviceReadResult {
	done := make(chan deviceReadResult, 1)
	go func() {
		n, err := f.v.ReadAt(ctx, f.file, f.ptr, cacheTestBlockSize, 0)
		done <- deviceReadResult{n, err}
	}()
	return done
}

func discardReadSubmissions(c *cacheTestClient) {
	for {
		select {
		case <-c.submitted:
		default:
			return
		}
	}
}

func TestReadAtCancellationWaitsForDMAAndClose(t *testing.T) {
	data := readPattern(cacheTestBlockSize, 29)
	f := newReadFixture(t, cacheTestBlockSize,
		map[uint32][]meta.Slice{0: {{Id: 1, Size: cacheTestBlockSize, Len: cacheTestBlockSize}}},
		map[uint64][]byte{1: data})
	got := cacheAcquire(f.v, context.Background(), cacheTestKey(1))
	if got.err != nil {
		t.Fatal(got.err)
	}
	releaseCache(f.v, got.sid)
	discardReadSubmissions(f.c)
	f.c.delay.Store(true)
	ctx := meta.Background()
	done := asyncReadAt(f, ctx)
	pending := receive(t, f.c.submitted)
	if pending.ref != f.ref || pending.write {
		t.Fatalf("expected GPU read, got %+v", pending)
	}
	ctx.Cancel()
	select {
	case r := <-done:
		t.Fatalf("ReadAt returned before DMA completion: %+v", r)
	default:
	}
	if err := f.v.Unregister(f.ptr); err != syscall.EBUSY {
		t.Fatalf("Unregister during cancelled DMA=%v", err)
	}
	// Close removes the fake's registration map entry after the read exits.
	// Retain the test-owned Go backing array solely for post-close assertions;
	// real device allocations still obey ReadAt's native-lifetime contract.
	f.c.mu.Lock()
	deviceBytes := f.c.buffers[f.ref]
	f.c.mu.Unlock()
	closed := make(chan error, 1)
	go func() { closed <- f.v.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned before DMA completion: %v", err)
	default:
	}
	f.c.complete(pending)
	if r := receive(t, done); r.n != 0 || r.err != syscall.EINTR {
		t.Fatalf("cancelled read=%+v", r)
	}
	if err := receive(t, closed); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(deviceBytes[:len(data)], data) ||
		!bytes.Equal(deviceBytes[len(data):], bytes.Repeat([]byte{0x7d}, len(deviceBytes)-len(data))) {
		t.Fatal("cancelled DMA was not completed correctly before Close unregistered its buffer")
	}
}

func TestReadAtCancellationAfterScratchWrite(t *testing.T) {
	// Misaligned source bytes force assembly. Cancellation after scratch write
	// submission must wait for that write and skip the subsequent GPU read.
	f := newReadFixture(t, 1024,
		map[uint32][]meta.Slice{0: {{Id: 1, Size: cacheTestBlockSize, Off: 1, Len: 1024}}},
		map[uint64][]byte{1: readPattern(cacheTestBlockSize, 43)})
	f.c.delay.Store(true)
	ctx := meta.Background()
	done := asyncReadAt(f, ctx)
	pending := receive(t, f.c.submitted)
	if !pending.write || pending.ref == f.ref {
		t.Fatalf("expected scratch write, got %+v", pending)
	}
	ctx.Cancel()
	f.c.complete(pending)
	if r := receive(t, done); r.n != 0 || r.err != syscall.EINTR {
		t.Fatalf("cancelled assembled read=%+v", r)
	}
	if f.c.submits.Load() != 1 || len(f.v.tier.stagers) != 2 {
		t.Fatalf("DMA=%d available stagers=%d", f.c.submits.Load(), len(f.v.tier.stagers))
	}
	if got := f.device.deviceBytes(f.ref); !bytes.Equal(got, bytes.Repeat([]byte{0x7d}, len(got))) {
		t.Fatal("cancelled scratch write was followed by a GPU transfer")
	}
	if err := f.v.Unregister(f.ptr); err != nil {
		t.Fatalf("completed cancelled DMA retained registration: %v", err)
	}
}

func TestReadAtWindowFailureWaitsForOtherDMA(t *testing.T) {
	f := newReadFixture(t, 2*cacheTestBlockSize,
		map[uint32][]meta.Slice{0: {
			{Id: 1, Size: cacheTestBlockSize, Len: cacheTestBlockSize},
			{Id: 2, Size: cacheTestBlockSize, Len: cacheTestBlockSize},
		}}, map[uint64][]byte{
			1: readPattern(cacheTestBlockSize, 11),
			2: readPattern(cacheTestBlockSize, 67),
		})
	warm := cacheAcquire(f.v, context.Background(), cacheTestKey(1))
	if warm.err != nil {
		t.Fatal(warm.err)
	}
	releaseCache(f.v, warm.sid)
	discardReadSubmissions(f.c)
	f.c.delay.Store(true)
	readFailed, failRead := make(chan struct{}), make(chan struct{})
	f.store.onRead = func(context.Context, uint64, int) error {
		<-failRead
		close(readFailed)
		return errors.New("second window cannot read its object")
	}
	done := make(chan deviceReadResult, 1)
	go func() {
		n, err := f.v.ReadAt(meta.Background(), f.file, f.ptr, 2*cacheTestBlockSize, 0)
		done <- deviceReadResult{n, err}
	}()
	pending := receive(t, f.c.submitted)
	if pending.ref != f.ref || pending.bufOff != 0 {
		t.Fatalf("expected first window GPU transfer, got %+v", pending)
	}
	close(failRead)
	receive(t, readFailed)
	select {
	case r := <-done:
		t.Fatalf("window failure returned before sibling DMA completed: %+v", r)
	default:
	}
	f.c.complete(pending)
	if r := receive(t, done); r.n != 0 || r.err != syscall.EIO {
		t.Fatalf("partially transferred request=%+v; want zero usable bytes", r)
	}
	if err := f.v.Unregister(f.ptr); err != nil {
		t.Fatalf("joined sibling DMA retained registration: %v", err)
	}
}

func TestReadAtBrokenQuarantinesResources(t *testing.T) {
	for _, assembled := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "assembled"}[assembled], func(t *testing.T) {
			s := meta.Slice{Id: 1, Size: cacheTestBlockSize, Len: 1024}
			if assembled {
				s.Off = 1
			}
			f := newReadFixture(t, 1024, map[uint32][]meta.Slice{0: {s}},
				map[uint64][]byte{1: readPattern(cacheTestBlockSize, 53)})
			f.v.maxRetries = 2
			sid := int32(-1)
			if !assembled {
				got := cacheAcquire(f.v, context.Background(), cacheTestKey(1))
				if got.err != nil {
					t.Fatal(got.err)
				}
				sid = got.sid
				releaseCache(f.v, sid)
			}
			discardReadSubmissions(f.c)
			f.c.delay.Store(true)
			done := asyncReadAt(f, meta.Background())
			pending := receive(t, f.c.submitted)
			f.c.badPoll.Store(true)
			if r := receive(t, done); r.n != 0 || r.err != syscall.EIO {
				t.Fatalf("broken read=%+v", r)
			}
			if f.m.reads.Load() != 1 || f.m.invalidations.Load() != 0 {
				t.Fatal("broken DMA attempted metadata invalidation or replanning")
			}
			if err := f.v.Unregister(f.ptr); err != syscall.EBUSY {
				t.Fatalf("broken registration was released: %v", err)
			}
			f.v.regs.mu.Lock()
			pinned := f.v.regs.entries[0].inflight.Load()
			f.v.regs.mu.Unlock()
			if pinned != 1 {
				t.Fatalf("registration references=%d; want 1", pinned)
			}
			if assembled {
				if len(f.v.tier.stagers) != 1 {
					t.Fatal("scratch source stager was recycled while DMA remained outstanding")
				}
			} else {
				f.v.tier.mu.Lock()
				refs := f.v.tier.metas[sid].refs
				f.v.tier.mu.Unlock()
				if refs != 1 {
					t.Fatalf("cache slot references=%d; want 1", refs)
				}
			}
			f.c.badPoll.Store(false)
			f.c.complete(pending)
			if err := f.v.Close(); err != syscall.EIO {
				t.Fatalf("uncertain DMA drain=%v", err)
			}
		})
	}
}

// This differential test uses the actual writer and metadata mapping, rather
// than assuming that the hand-crafted layouts above match writer behavior.
func TestReadAtFileSystemDifferential(t *testing.T) {
	conf := readTestConfig()
	m := meta.NewClient("memkv://", conf.Meta)
	obj, err := object.CreateStorage("mem", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	store := chunk.NewCachedStore(obj, *conf.Chunk, nil)
	jfs := readTestFileSystem(t, conf, m, store)
	ctx := meta.Background()
	f, eno := jfs.Create(ctx, "/model", 0644, 0)
	if eno != 0 {
		t.Fatal(eno)
	}
	t.Cleanup(func() { _ = f.Close(meta.Background()) })
	for _, write := range []struct {
		off  int64
		data []byte
	}{
		{0, readPattern(2*cacheTestBlockSize, 19)},
		{777, readPattern(1511, 31)},
		{meta.ChunkSize - 1024, readPattern(3072, 97)},
	} {
		if n, eno := f.Pwrite(ctx, write.data, write.off); eno != 0 || n != len(write.data) {
			t.Fatalf("Pwrite=(%d,%v)", n, eno)
		}
		if eno := f.Flush(ctx); eno != 0 {
			t.Fatal(eno)
		}
	}
	if eno := f.Truncate(ctx, meta.ChunkSize+1300); eno != 0 {
		t.Fatal(eno)
	}
	if eno := f.Close(ctx); eno != 0 {
		t.Fatal(eno)
	}
	f, eno = jfs.Open(ctx, "/model", vfs.MODE_MASK_R)
	if eno != 0 {
		t.Fatal(eno)
	}
	cache, device, ptr, _ := attachReadDevice(t, m, store, nil)
	for _, off := range []int64{0, 3 * cacheTestBlockSize, meta.ChunkSize - 2048} {
		cpu := make([]byte, 2*cacheTestBlockSize)
		n, err := f.Pread(ctx, cpu, off)
		if err != nil {
			t.Fatal(err)
		}
		// Use a separate registration for each result to retain sentinel bytes.
		ptr += 1 << 20
		if err := cache.v.Register(ptr, 16*cacheTestBlockSize); err != nil {
			t.Fatal(err)
		}
		ref := device.registered[ptr]
		got, err := cache.v.ReadAt(ctx, f, ptr, len(cpu), off)
		if err != nil || got != n {
			t.Fatalf("offset %d: ReadAt=(%d,%v), CPU=(%d,nil)", off, got, err, n)
		}
		checkReadDevice(t, device, ref, 0, cpu[:n], cache.v.IOUnit())
	}
}

func BenchmarkReadAt(b *testing.B) {
	for _, layout := range []string{"warm", "assembled", "hole"} {
		b.Run(layout, func(b *testing.B) {
			slices := map[uint32][]meta.Slice{0: {{Id: 1, Size: 2 * cacheTestBlockSize, Len: cacheTestBlockSize}}}
			if layout == "assembled" {
				slices[0][0].Off = 1
			} else if layout == "hole" {
				slices = nil
			}
			f := newReadFixture(b, cacheTestBlockSize, slices,
				map[uint64][]byte{1: readPattern(2*cacheTestBlockSize, 17)})
			ctx := meta.Background()
			if _, err := f.v.ReadAt(ctx, f.file, f.ptr, cacheTestBlockSize, 0); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(cacheTestBlockSize)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := f.v.ReadAt(ctx, f.file, f.ptr, cacheTestBlockSize, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
