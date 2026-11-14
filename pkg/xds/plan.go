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

	"github.com/juicedata/juicefs/pkg/meta"
)

// piece maps a contiguous request range to one immutable source block or a
// hole. Neither a source block boundary nor a destination window boundary may
// occur inside a piece. Device addresses do not participate in planning.
type piece struct {
	hole   bool
	key    blockKey // Ignored for holes.
	boff   uint32   // Offset within key's block, not within the file or slice.
	n      uint32   // Valid file bytes; DMA padding is computed by the executor.
	dstOff uint64   // Offset from the request start, excluding registration dstBase.
}

// window bounds host assembly to BlockSize bytes. Its pieces are ordered,
// disjoint, and cover [dstOff, dstOff+n) exactly, including explicit holes.
// direct means every piece can independently use aligned DMA; otherwise the
// executor must assemble the whole window before touching device memory.
type window struct {
	dstOff uint64
	n      uint64
	pieces []piece
	direct bool
}

// plan resolves a file range without acquiring cache slots or submitting DMA.
// The caller protects the Volume's borrowed metadata dependency. A failed plan
// returns no partial result, so metadata errors cannot cause partial execution.
// File-offset and request-size alignment are checked by ReadAt, not here.
func (v *Volume) plan(ctx meta.Context, ino meta.Ino, fileLen, off, size uint64) ([]window, uint64, error) {
	if ctx == nil || v.m == nil || v.blockSize <= 0 || v.ioUnit <= 0 || v.blockSize%v.ioUnit != 0 {
		return nil, 0, syscall.EINVAL
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if size == 0 || off >= fileLen {
		return nil, 0, nil
	}
	// Subtract before adding: an EOF-truncated request must not overflow even
	// when its original end cannot be represented as uint64.
	n := min(size, fileLen-off)
	end := off + n
	first, last := off/meta.ChunkSize, (end-1)/meta.ChunkSize
	bs, u := uint64(v.blockSize), uint64(v.ioUnit)
	if bs > uint64(^uint32(0)) || last > uint64(^uint32(0)) {
		return nil, 0, syscall.EOVERFLOW
	}
	nwindows := (n-1)/bs + 1
	if nwindows > uint64(^uint(0)>>1)/uint64(unsafe.Sizeof(window{})) {
		return nil, 0, syscall.EOVERFLOW
	}
	// Reserve the exact ordinary-request size, but do not allocate a giant plan
	// before metadata has even been read. Growth remains amortized linear for
	// unusually large device buffers and metadata errors can still return early.
	windows := make([]window, 0, int(min(nwindows, 1024)))
	for indx := first; indx <= last; indx++ {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		base := indx * meta.ChunkSize
		lo := max(off, base) - base
		hi := min(end, base+meta.ChunkSize) - base
		var slices []meta.Slice
		errno := v.m.Read(ctx, ino, uint32(indx), &slices)
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		if errno != 0 {
			return nil, 0, errno
		}

		// meta.Read already resolves overwrites and inserts internal holes.
		// pos, lo, and hi are chunk-relative; src is slice-object-relative;
		// only base+a-off converts back to request-relative destination space.
		var pos uint64
		for _, s := range slices {
			if err := ctx.Err(); err != nil {
				return nil, 0, err
			}
			next := pos + uint64(s.Len)
			if next > meta.ChunkSize || s.Id != 0 &&
				(uint64(s.Off) > uint64(s.Size) || uint64(s.Len) > uint64(s.Size)-uint64(s.Off)) {
				return nil, 0, syscall.EIO
			}
			a, b := max(lo, pos), min(hi, next)
			if a < b {
				src := uint64(s.Off) + a - pos
				windows = appendPlanSegment(windows, bs, s, src, base+a-off, b-a)
			}
			pos = next
			if pos >= hi {
				break
			}
		}
		// buildSlice stops at the last mapping, not at the chunk boundary.
		// Ordinary file reads zero the remaining range; represent it explicitly
		// so both direct and assembled execution have the same sparse semantics.
		if pos < hi {
			a := max(lo, pos)
			windows = appendPlanSegment(windows, bs, meta.Slice{}, 0, base+a-off, hi-a)
		}
	}
	for i := range windows {
		w := &windows[i]
		w.direct = true
		for _, p := range w.pieces {
			if !p.canReadDirect(bs, u, n) {
				w.direct = false
				break
			}
		}
	}
	return windows, n, nil
}

// appendPlanSegment splits both coordinate spaces in one pass. s describes an
// already-validated metadata segment; src is its current object offset and dst
// its request-relative offset. Holes have no meaningful source coordinates.
func appendPlanSegment(windows []window, bs uint64, s meta.Slice, src, dst, n uint64) []window {
	for n != 0 {
		take := min(n, bs-dst%bs)
		p := piece{hole: s.Id == 0, dstOff: dst}
		if !p.hole {
			take = min(take, bs-src%bs)
			p.key = blockKey{id: s.Id, size: s.Size, indx: uint32(src / bs)}
			p.boff = uint32(src % bs)
		}
		p.n = uint32(take)
		if len(windows) == 0 || windows[len(windows)-1].n == bs {
			windows = append(windows, window{dstOff: dst})
		}
		w := &windows[len(windows)-1]
		// Adjacent metadata entries may describe one contiguous source range.
		// Merging removes unnecessary small DMAs and may eliminate a merely
		// artificial unaligned boundary. Never merge across a block or window.
		merged := false
		if count := len(w.pieces); count != 0 {
			prev := &w.pieces[count-1]
			if prev.hole && p.hole || !prev.hole && !p.hole && prev.key == p.key &&
				uint64(prev.boff)+uint64(prev.n) == uint64(p.boff) {
				prev.n += p.n
				merged = true
			}
		}
		if !merged {
			w.pieces = append(w.pieces, p)
		}
		w.n += take
		src += take
		dst += take
		n -= take
	}
	return windows
}

// canReadDirect permits padding only at the request's valid-data end. A file
// EOF inside a longer slice object does not establish that subsequent object
// bytes are zero: that case must be assembled and explicitly cleared. Only an
// actual block-data tail can use the zero padding established by cache fill.
func (p piece) canReadDirect(bs, u, total uint64) bool {
	if uint64(p.dstOff)%u != 0 {
		return false
	}
	last := p.dstOff+uint64(p.n) == total
	if p.hole {
		return uint64(p.n)%u == 0 || last
	}
	if uint64(p.boff)%u != 0 {
		return false
	}
	if uint64(p.n)%u == 0 {
		return true
	}
	start := uint64(p.key.indx) * bs
	blen := min(bs, uint64(p.key.size)-start)
	return last && uint64(p.boff)+uint64(p.n) == blen
}
