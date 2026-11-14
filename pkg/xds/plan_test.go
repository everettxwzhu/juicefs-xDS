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
	"reflect"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
)

// The planner depends only on metadata's resolved Read view. Unimplemented
// methods intentionally panic if planning starts acquiring unrelated resources.
type planTestMeta struct {
	meta.Meta
	chunks map[uint32][]meta.Slice
	calls  []uint32
	onRead func(meta.Context, meta.Ino, uint32) syscall.Errno
}

func (m *planTestMeta) Read(ctx meta.Context, ino meta.Ino, indx uint32, slices *[]meta.Slice) syscall.Errno {
	m.calls = append(m.calls, indx)
	if m.onRead != nil {
		if err := m.onRead(ctx, ino, indx); err != 0 {
			return err
		}
	}
	*slices = m.chunks[indx]
	return 0
}

func TestPlanMapping(t *testing.T) {
	const bs = 4096
	data := func(id uint64, size, indx, boff, n uint32, dst uint64) piece {
		return piece{key: blockKey{id: id, size: size, indx: indx}, boff: boff, n: n, dstOff: dst}
	}
	hole := func(dst uint64, n uint32) piece { return piece{hole: true, dstOff: dst, n: n} }
	for _, tc := range []struct {
		name        string
		chunks      map[uint32][]meta.Slice
		fileLen     uint64
		off, size   uint64
		wantN       uint64
		wantWindows []window
		wantCalls   []uint32
	}{
		{
			name: "aligned block", chunks: map[uint32][]meta.Slice{0: {{Id: 1, Size: bs, Len: bs}}},
			fileLen: bs, size: bs, wantN: bs, wantCalls: []uint32{0},
			wantWindows: []window{{n: bs, direct: true, pieces: []piece{data(1, bs, 0, 0, bs, 0)}}},
		},
		{
			name:    "nonzero request start and dual boundaries",
			chunks:  map[uint32][]meta.Slice{0: {{Id: 1, Size: 3 * bs, Len: 3 * bs}}},
			fileLen: 3 * bs, off: 1024, size: 2 * bs, wantN: 2 * bs, wantCalls: []uint32{0},
			wantWindows: []window{
				{n: bs, direct: true, pieces: []piece{data(1, 3*bs, 0, 1024, 3072, 0), data(1, 3*bs, 1, 0, 1024, 3072)}},
				{dstOff: bs, n: bs, direct: true, pieces: []piece{data(1, 3*bs, 1, 1024, 3072, bs), data(1, 3*bs, 2, 0, 1024, 7168)}},
			},
		},
		{
			name:    "nonzero slice object offset",
			chunks:  map[uint32][]meta.Slice{0: {{Id: 1, Size: 2 * bs, Off: 1024, Len: bs}}},
			fileLen: bs, size: bs, wantN: bs, wantCalls: []uint32{0},
			wantWindows: []window{{n: bs, direct: true, pieces: []piece{data(1, 2*bs, 0, 1024, 3072, 0), data(1, 2*bs, 1, 0, 1024, 3072)}}},
		},
		{
			name:    "explicit and trailing holes",
			chunks:  map[uint32][]meta.Slice{0: {{Len: 1024}, {Id: 1, Size: 2048, Len: 2048}}},
			fileLen: bs, size: bs, wantN: bs, wantCalls: []uint32{0},
			wantWindows: []window{{n: bs, direct: true, pieces: []piece{hole(0, 1024), data(1, 2048, 0, 0, 2048, 1024), hole(3072, 1024)}}},
		},
		{
			name: "empty chunk and short final window", fileLen: bs + 100, size: 2 * bs,
			wantN: bs + 100, wantCalls: []uint32{0},
			wantWindows: []window{
				{n: bs, direct: true, pieces: []piece{hole(0, bs)}},
				{dstOff: bs, n: 100, direct: true, pieces: []piece{hole(bs, 100)}},
			},
		},
		{
			name:    "adjacent holes merge",
			chunks:  map[uint32][]meta.Slice{0: {{Len: 123}, {Len: bs - 123}}},
			fileLen: bs, size: bs, wantN: bs, wantCalls: []uint32{0},
			wantWindows: []window{{n: bs, direct: true, pieces: []piece{hole(0, bs)}}},
		},
		{
			name:    "contiguous source entries merge",
			chunks:  map[uint32][]meta.Slice{0: {{Id: 1, Size: bs, Len: 123}, {Id: 1, Size: bs, Off: 123, Len: bs - 123}}},
			fileLen: bs, size: bs, wantN: bs, wantCalls: []uint32{0},
			wantWindows: []window{{n: bs, direct: true, pieces: []piece{data(1, bs, 0, 0, bs, 0)}}},
		},
		{
			name:    "unaligned overwrite assembles",
			chunks:  map[uint32][]meta.Slice{0: {{Id: 1, Size: bs, Len: 123}, {Id: 2, Size: bs, Off: 13, Len: bs - 123}}},
			fileLen: bs, size: bs, wantN: bs, wantCalls: []uint32{0},
			wantWindows: []window{{n: bs, pieces: []piece{data(1, bs, 0, 0, 123, 0), data(2, bs, 0, 13, bs-123, 123)}}},
		},
		{
			name:    "object tail permits padded DMA",
			chunks:  map[uint32][]meta.Slice{0: {{Id: 1, Size: 1300, Len: 1300}}},
			fileLen: 1300, size: bs, wantN: 1300, wantCalls: []uint32{0},
			wantWindows: []window{{n: 1300, direct: true, pieces: []piece{data(1, 1300, 0, 0, 1300, 0)}}},
		},
		{
			name:    "file EOF inside longer object requires explicit zeros",
			chunks:  map[uint32][]meta.Slice{0: {{Id: 1, Size: bs, Len: bs}}},
			fileLen: 1300, size: bs, wantN: 1300, wantCalls: []uint32{0},
			wantWindows: []window{{n: 1300, pieces: []piece{data(1, bs, 0, 0, 1300, 0)}}},
		},
		{
			name:    "unaligned nonfinal tail cannot overwrite next piece",
			chunks:  map[uint32][]meta.Slice{0: {{Id: 1, Size: 1300, Len: 1300}}},
			fileLen: bs, size: bs, wantN: bs, wantCalls: []uint32{0},
			wantWindows: []window{{n: bs, pieces: []piece{data(1, 1300, 0, 0, 1300, 0), hole(1300, bs-1300)}}},
		},
		{
			name: "cross chunk coordinates",
			chunks: map[uint32][]meta.Slice{
				0: {{Id: 1, Size: meta.ChunkSize, Len: meta.ChunkSize}},
				1: {{Id: 2, Size: 2048, Len: 2048}},
			},
			fileLen: meta.ChunkSize + 2048, off: meta.ChunkSize - 1024, size: 2048,
			wantN: 2048, wantCalls: []uint32{0, 1},
			wantWindows: []window{{n: 2048, direct: true, pieces: []piece{
				data(1, meta.ChunkSize, meta.ChunkSize/bs-1, bs-1024, 1024, 0), data(2, 2048, 0, 0, 1024, 1024),
			}}},
		},
		{
			name:    "request ends exactly at chunk boundary",
			chunks:  map[uint32][]meta.Slice{0: {{Id: 1, Size: meta.ChunkSize, Len: meta.ChunkSize}}},
			fileLen: meta.ChunkSize + bs, off: meta.ChunkSize - bs, size: bs, wantN: bs, wantCalls: []uint32{0},
			wantWindows: []window{{n: bs, direct: true, pieces: []piece{data(1, meta.ChunkSize, meta.ChunkSize/bs-1, 0, bs, 0)}}},
		},
		{name: "empty file", size: bs},
		{name: "at EOF", fileLen: bs, off: bs, size: bs},
		{name: "past EOF", fileLen: bs, off: 2 * bs, size: bs},
		{name: "zero request", fileLen: bs},
		{
			name: "original request end overflows but EOF clips it", fileLen: bs, size: ^uint64(0),
			off: 1024, wantN: bs - 1024, wantCalls: []uint32{0},
			wantWindows: []window{{n: bs - 1024, direct: true, pieces: []piece{hole(0, bs-1024)}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &planTestMeta{chunks: tc.chunks}
			v := &Volume{m: m, blockSize: bs, ioUnit: 1024}
			windows, n, err := v.plan(meta.Background(), 17, tc.fileLen, tc.off, tc.size)
			if err != nil || n != tc.wantN || !reflect.DeepEqual(windows, tc.wantWindows) {
				t.Fatalf("plan=(%+v, %d, %v), want (%+v, %d, nil)", windows, n, err, tc.wantWindows, tc.wantN)
			}
			if !reflect.DeepEqual(m.calls, tc.wantCalls) {
				t.Fatalf("metadata chunks=%v, want %v", m.calls, tc.wantCalls)
			}
			checkPlanCoverage(t, windows, n, tc.size, bs, 1024)
		})
	}
}

// Validate the externally relevant invariants independently of the planner's
// direct predicate, including the physical extent of padded source reads.
func checkPlanCoverage(t *testing.T, windows []window, n, size, bs, u uint64) {
	t.Helper()
	var dst uint64
	for i, w := range windows {
		if w.dstOff != dst || w.dstOff != uint64(i)*bs || w.n == 0 || w.n > bs {
			t.Fatalf("invalid window %d: %+v, next destination=%d", i, w, dst)
		}
		for _, p := range w.pieces {
			if p.dstOff != dst || p.n == 0 || uint64(p.n) > w.dstOff+w.n-dst {
				t.Fatalf("piece %+v does not continuously cover window %+v, next=%d", p, w, dst)
			}
			alen := (uint64(p.n) + u - 1) / u * u
			if !p.hole {
				start := uint64(p.key.indx) * bs
				if start >= uint64(p.key.size) || uint64(p.boff)+uint64(p.n) > min(bs, uint64(p.key.size)-start) {
					t.Fatalf("piece exceeds source block: %+v", p)
				}
				if w.direct {
					blen := min(bs, uint64(p.key.size)-start)
					if uint64(p.boff)%u != 0 || uint64(p.boff)+alen > (blen+u-1)/u*u {
						t.Fatalf("direct read exceeds initialized source bytes: %+v", p)
					}
				}
			}
			if w.direct && (p.dstOff%u != 0 || p.dstOff+alen > size ||
				p.dstOff+alen > n && p.dstOff+uint64(p.n) != n) {
				t.Fatalf("direct read overlaps another piece or exceeds request: %+v", p)
			}
			dst += uint64(p.n)
		}
		if dst != w.dstOff+w.n {
			t.Fatalf("window has uncovered bytes: %+v, covered through %d", w, dst)
		}
	}
	if dst != n {
		t.Fatalf("plan covers %d bytes, want %d", dst, n)
	}
}

func TestPlanErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		slices  []meta.Slice
		onRead  func(meta.Context, meta.Ino, uint32) syscall.Errno
		fileLen uint64
		off     uint64
		wantErr error
	}{
		{name: "metadata errno", fileLen: 4096, onRead: func(meta.Context, meta.Ino, uint32) syscall.Errno { return syscall.EACCES }, wantErr: syscall.EACCES},
		{name: "data exceeds object", fileLen: 4096, slices: []meta.Slice{{Id: 1, Size: 1024, Len: 2048}}, wantErr: syscall.EIO},
		{name: "object offset exceeds size", fileLen: 4096, slices: []meta.Slice{{Id: 1, Size: 1024, Off: 1025, Len: 1}}, wantErr: syscall.EIO},
		{name: "chunk extent exceeds limit", fileLen: 4096, slices: []meta.Slice{{Len: meta.ChunkSize + 1}}, wantErr: syscall.EIO},
		{name: "chunk index overflow", off: (uint64(1) << 32) * meta.ChunkSize, fileLen: ((uint64(1) << 32) * meta.ChunkSize) + 4096, wantErr: syscall.EOVERFLOW},
		{name: "last chunk index overflow", off: (uint64(1)<<32)*meta.ChunkSize - 1024, fileLen: ((uint64(1) << 32) * meta.ChunkSize) + 4096, wantErr: syscall.EOVERFLOW},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &planTestMeta{chunks: map[uint32][]meta.Slice{0: tc.slices}, onRead: tc.onRead}
			v := &Volume{m: m, blockSize: 4096, ioUnit: 1024}
			windows, n, err := v.plan(meta.Background(), 1, tc.fileLen, tc.off, 4096)
			if !errors.Is(err, tc.wantErr) || windows != nil || n != 0 {
				t.Fatalf("plan=(%v, %d, %v), want (nil, 0, %v)", windows, n, err, tc.wantErr)
			}
		})
	}
}

func TestPlanCancellation(t *testing.T) {
	for _, duringRead := range []bool{false, true} {
		t.Run(map[bool]string{false: "before metadata", true: "during metadata"}[duringRead], func(t *testing.T) {
			ctx := meta.NewContext(19, 23, []uint32{29})
			defer ctx.Cancel()
			m := &planTestMeta{}
			if duringRead {
				m.onRead = func(got meta.Context, ino meta.Ino, indx uint32) syscall.Errno {
					if got != ctx || got.Pid() != 19 || got.Uid() != 23 || got.Gid() != 29 || ino != 17 || indx != 0 {
						t.Fatalf("planning lost metadata identity: context=%v inode=%d chunk=%d", got, ino, indx)
					}
					ctx.Cancel()
					return syscall.EIO
				}
			} else {
				ctx.Cancel()
			}
			v := &Volume{m: m, blockSize: 4096, ioUnit: 1024}
			windows, n, err := v.plan(ctx, 17, meta.ChunkSize+4096, 0, meta.ChunkSize+4096)
			if windows != nil || n != 0 || !errors.Is(err, context.Canceled) {
				t.Fatalf("plan=(%v,%d,%v), want cancellation", windows, n, err)
			}
			wantCalls := 0
			if duringRead {
				wantCalls = 1
			}
			if len(m.calls) != wantCalls {
				t.Fatalf("metadata calls=%v, want %d calls", m.calls, wantCalls)
			}
		})
	}
}

func TestPlanFailureDiscardsEarlierChunks(t *testing.T) {
	m := &planTestMeta{onRead: func(_ meta.Context, _ meta.Ino, indx uint32) syscall.Errno {
		if indx == 1 {
			return syscall.ENOENT
		}
		return 0
	}}
	v := &Volume{m: m, blockSize: 4096, ioUnit: 1024}
	windows, n, err := v.plan(meta.Background(), 1, meta.ChunkSize+4096, meta.ChunkSize-4096, 8192)
	if windows != nil || n != 0 || err != syscall.ENOENT || !reflect.DeepEqual(m.calls, []uint32{0, 1}) {
		t.Fatalf("partial plan leaked after error: windows=%v n=%d err=%v calls=%v", windows, n, err, m.calls)
	}
}

func BenchmarkPlan(b *testing.B) {
	const (
		bs     = 4 << 20
		length = meta.ChunkSize
	)
	// Every fixture describes the same logical byte count. The fragmented view
	// models small overwrites with unaligned object offsets; the sparse view
	// exercises zero-slot planning without allocating any corresponding data.
	fragmented := make([]meta.Slice, length/4096)
	for i := range fragmented {
		fragmented[i] = meta.Slice{Id: uint64(i + 1), Size: 4097, Off: 1, Len: 4096}
	}
	for _, tc := range []struct {
		name   string
		slices []meta.Slice
	}{
		{name: "aligned", slices: []meta.Slice{{Id: 1, Size: length, Len: length}}},
		{name: "fragmented", slices: fragmented},
		{name: "sparse"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			m := &planTestMeta{chunks: map[uint32][]meta.Slice{0: tc.slices}, calls: make([]uint32, 0, 1)}
			v := &Volume{m: m, blockSize: bs, ioUnit: 4096}
			ctx := meta.Background()
			defer ctx.Cancel()
			b.ReportAllocs()
			b.SetBytes(length)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m.calls = m.calls[:0]
				windows, n, err := v.plan(ctx, 1, length, 0, length)
				if err != nil || n != length || len(windows) != length/bs {
					b.Fatalf("plan produced windows=%d n=%d err=%v", len(windows), n, err)
				}
			}
		})
	}
}
