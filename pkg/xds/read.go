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
	"io"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/juicedata/juicefs/pkg/fs"
	"github.com/juicedata/juicefs/pkg/meta"
)

// Limit scheduling independently of the stager pool: warm reads need no host
// buffers, even when a large BlockSize leaves only one stager. The engine has
// its own admission bound shared by all concurrent ReadAt calls.
const maxReadWorkers = 32

// ReadAt reads file data into caller-owned, previously registered device memory.
// f must be an open, readable regular file belonging to this Volume. Its open
// flags and closed state are not exposed by fs.File and remain caller obligations.
// Internal virtual files are not supported. Do not concurrently write, truncate,
// or close the same f; concurrent reads must use disjoint device ranges. File
// length comes from the handle's cached Stat, as in fs.File.Pread; this operation
// does not establish a snapshot against changes through other handles/clients.
//
// size must be positive. off, size, and the device address relative to its
// registration base must be multiples of IOUnit. The whole requested device
// range must fit one registration, including when EOF shortens the file data.
// Flush runs before planning. A positive short read at EOF returns (n, nil),
// while a starting offset at/past EOF returns (0, io.EOF). Bytes from n through
// its next IOUnit boundary are zeroed; subsequent device bytes are untouched.
//
// Other failures return (0, errno). Some windows may already have changed the
// destination, so the caller must discard that request's result. Cancellation
// skips operations not yet being submitted and waits for submitted DMA before
// returning EINTR. Sticky device failure returns EIO and retains registration
// ownership until Close: an error alone cannot prove physical DMA completion.
//
// The caller must finish device work touching this range before the call and
// keep its allocation alive. Observe the backend's device synchronization rules;
// in particular, AMD requires caller-side HIP synchronization before and after.
func (v *Volume) ReadAt(ctx meta.Context, f *fs.File, ptr uintptr, size int, off int64) (n int, err error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if err := v.deviceReadError(); err != nil {
		return 0, err
	}
	if ctx == nil || f == nil || f.FS() == nil || f.FS().Meta() != v.m ||
		size <= 0 || off < 0 || uint64(off)%uint64(v.ioUnit) != 0 || size%v.ioUnit != 0 {
		return 0, syscall.EINVAL
	}
	reg, dstBase, err := v.lookup(ptr, uint64(size))
	if err != nil {
		return 0, err
	}
	defer func() {
		// Join every worker before this defer. A child-context cancellation due
		// to a sibling error must not mask the first error; only the original
		// caller context and sticky failures can override that result here.
		err = v.readResultError(ctx, err)
		if err != nil {
			n = 0
		}
		v.releaseRegistration(reg)
	}()
	if err := v.checkRead(ctx); err != nil {
		return 0, err
	}
	// Flush takes f's mutex itself. Taking it around Flush would deadlock.
	// A separate short lock protects the cached size access, but cannot make
	// flush+planning atomic against writes; see the caller obligations above.
	if eno := f.Flush(ctx); eno != 0 {
		return 0, eno
	}
	f.Lock()
	info, statErr := f.Stat()
	var fileLen int64
	if statErr == nil && info != nil {
		fileLen = info.Size()
	}
	f.Unlock()
	if statErr != nil {
		return 0, statErr
	}
	if info == nil || fileLen < 0 {
		return 0, syscall.EIO
	}
	if off >= fileLen {
		return 0, io.EOF
	}
	windows, planned, err := v.plan(ctx, f.Inode(), uint64(fileLen), uint64(off), uint64(size))
	if err != nil {
		return 0, err
	}
	if err := v.executeWindows(ctx, reg, dstBase, windows); err != nil {
		return 0, err
	}
	if err := v.checkRead(ctx); err != nil {
		return 0, err
	}
	// Admission must finish under our lifecycle lock. Prefetch workers use
	// Volume.bg, never reacquire mu, and are cancelled/joined by Close.
	v.prefetchRead(ctx, windows)
	return int(planned), nil // plan clips to size, which is already an int.
}

// checkRead is used only while the foreground caller holds the lifecycle read
// lock. Workers share that caller's lifetime; reacquiring mu in a worker could
// deadlock once Close is waiting for its write lock.
func (v *Volume) checkRead(ctx context.Context) error {
	if err := v.deviceReadError(); err != nil {
		return err
	}
	return ctx.Err()
}

// readResultError is the public errno boundary. Metadata errno values survive;
// storage helpers below classify backend failures as EIO before returning here.
func (v *Volume) readResultError(ctx context.Context, err error) error {
	if stateErr := v.deviceReadError(); stateErr != nil {
		return stateErr
	}
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return syscall.EINTR
	}
	if err == nil || err == io.EOF {
		return err
	}
	var eno syscall.Errno
	if errors.As(err, &eno) && eno != 0 {
		return eno
	}
	return syscall.EIO
}

// executeWindows bounds goroutine count independently of request length. The
// first failure cancels unstarted work, but Wait is unconditional: a successful
// engine submission may still reference the registration, cache, or stager.
func (v *Volume) executeWindows(ctx context.Context, reg *registration, dstBase uint64, windows []window) error {
	if err := v.checkRead(ctx); err != nil {
		return err
	}
	readWindow := func(ctx context.Context, w window) error {
		if w.direct {
			return v.readDirectWindow(ctx, reg, dstBase, w)
		}
		return v.readAssembledWindow(ctx, reg, dstBase, w)
	}
	if len(windows) == 1 {
		return readWindow(ctx, windows[0])
	}
	if len(windows) == 0 {
		return nil
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var next atomic.Uint64
	var first sync.Once
	var firstErr error
	var workers sync.WaitGroup
	for range min(len(windows), maxReadWorkers) {
		workers.Go(func() {
			for {
				err := v.checkRead(workCtx)
				if err == nil {
					i := next.Add(1) - 1
					if i >= uint64(len(windows)) {
						return
					}
					err = readWindow(workCtx, windows[i])
				}
				if err != nil {
					first.Do(func() {
						firstErr = err
						cancel()
					})
					return
				}
			}
		})
	}
	workers.Wait()
	return firstErr
}

func (v *Volume) readDirectWindow(ctx context.Context, reg *registration, dstBase uint64, w window) error {
	for _, p := range w.pieces {
		if err := v.checkRead(ctx); err != nil {
			return err
		}
		if !p.hole {
			if err := v.readBlockPiece(ctx, reg, dstBase, p); err != nil {
				return err
			}
			continue
		}
		zero := v.tier.ref(0)
		if err := v.e.do(&ioOp{
			ctx: ctx, blob: zero.blob, blobOff: zero.off,
			ref: reg.ref, bufOff: dstBase + p.dstOff, n: v.alignedReadSize(uint64(p.n)),
		}); err != nil {
			return err
		}
	}
	return nil
}

// Scope the defer to one piece. Holding all acquired blocks until the whole
// window/request completes could exhaust the cache and wait for our own pins.
func (v *Volume) readBlockPiece(ctx context.Context, reg *registration, dstBase uint64, p piece) error {
	sid, err := v.tier.acquire(ctx, p.key)
	if err != nil {
		return err
	}
	defer v.tier.release(sid)
	if err := v.checkRead(ctx); err != nil {
		return err
	}
	slot := v.tier.ref(sid)
	return v.e.do(&ioOp{
		ctx: ctx, blob: slot.blob, blobOff: slot.off + uint64(p.boff),
		ref: reg.ref, bufOff: dstBase + p.dstOff, n: v.alignedReadSize(uint64(p.n)),
	})
}

func (v *Volume) readAssembledWindow(ctx context.Context, reg *registration, dstBase uint64, w window) error {
	st, err := v.tier.getStager(ctx)
	if err != nil {
		return err
	}
	// The stager and scratch are one exclusive resource. In particular, keep
	// scratch reserved through the GPU read, not merely through its host write.
	// putStager quarantines the native buffer if an error preceded completion.
	defer v.tier.putStager(st)
	for _, p := range w.pieces {
		if err := v.checkRead(ctx); err != nil {
			return err
		}
		a := p.dstOff - w.dstOff
		dst := st.buf[a : a+uint64(p.n)]
		if p.hole {
			clear(dst)
			continue
		}
		// Chunk timeout workers and asynchronous local-cache writes may retain
		// their Page after ReadAt returns. A private refcounted page followed by
		// one copy prevents them from touching this recyclable xDS buffer.
		src := uint64(p.key.indx)*uint64(v.blockSize) + uint64(p.boff)
		if src > uint64(^uint(0)>>1) {
			return syscall.EOVERFLOW
		}
		page, err := readSliceRange(ctx, v.store, p.key.id, p.key.size, int(src), int(p.n))
		if err != nil {
			if checkErr := v.checkRead(ctx); checkErr != nil {
				return checkErr
			}
			return syscall.EIO
		}
		copy(dst, page.Data)
		page.Release()
	}
	alen := v.alignedReadSize(w.n)
	clear(st.buf[w.n:alen])
	if err := v.checkRead(ctx); err != nil {
		return err
	}
	if err := v.e.do(&ioOp{
		ctx: ctx, write: true, blob: st.scratch.blob, blobOff: st.scratch.off,
		ref: st.ref, n: alen,
	}); err != nil {
		return err
	}
	// A completed scratch write is safe to recycle on cancellation. Once the
	// following read is submitted, do must wait rather than select on ctx.Done.
	if err := v.checkRead(ctx); err != nil {
		return err
	}
	return v.e.do(&ioOp{
		ctx: ctx, blob: st.scratch.blob, blobOff: st.scratch.off,
		ref: reg.ref, bufOff: dstBase + w.dstOff, n: alen,
	})
}

// All callers round a piece/window length bounded by BlockSize. Open validates
// that BlockSize is an IOUnit multiple, so this cannot overflow or exceed a slot.
func (v *Volume) alignedReadSize(n uint64) uint64 {
	u := uint64(v.ioUnit)
	return ((n + u - 1) / u) * u
}

// prefetchRead coalesces all mappings of one immutable slice. Starting only at
// its largest requested block avoids repeated prefetch scans for every window.
// It runs after foreground success and under Volume.mu.RLock for safe admission.
func (v *Volume) prefetchRead(ctx context.Context, windows []window) {
	if v.readahead < v.blockSize {
		return
	}
	type sliceKey struct {
		id   uint64
		size uint32
	}
	last := make(map[sliceKey]blockKey)
	for _, w := range windows {
		for _, p := range w.pieces {
			if p.hole {
				continue
			}
			k := sliceKey{id: p.key.id, size: p.key.size}
			if old, ok := last[k]; !ok || p.key.indx > old.indx {
				last[k] = p.key
			}
		}
	}
	for _, k := range last {
		if v.checkRead(ctx) != nil {
			return
		}
		v.tier.prefetch(k)
	}
}
