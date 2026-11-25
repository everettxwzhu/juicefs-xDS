//go:build linux

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

package vfs

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"syscall"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/xds"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

const xdsFillConcurrency = 8

// XDSResolver implements xds.FileResolver over a live JuiceFS VFS instance in
// sidecar mode. Normal POSIX/FUSE reads and writes remain on JuiceFS's native
// cache manager; XDSResolver only flushes pending writes, resolves chunk/block
// extents, and populates misses directly into the dedicated xds.BlockCache
// without writing into the POSIX disk/memory cache.
type XDSResolver struct {
	vfs      *VFS
	cache    *xds.BlockCache
	mountDev atomic.Uint64
}

// NewXDSResolver creates an xds.FileResolver backed by v and the dedicated
// sidecar cache. Passing mountDev == 0 defers st_dev discovery until the FUSE
// mountpoint is active so the FUSE device ID (not the host directory's) is cached.
func (v *VFS) NewXDSResolver(cache *xds.BlockCache, mountDev uint64) *XDSResolver {
	r := &XDSResolver{
		vfs:   v,
		cache: cache,
	}
	if mountDev != 0 {
		r.mountDev.Store(mountDev)
	}
	return r
}

// MountDev returns the Linux device number (st_dev) of the JuiceFS mount.
func (r *XDSResolver) MountDev() uint64 {
	if dev := r.mountDev.Load(); dev != 0 {
		return dev
	}
	mp := r.vfs.Conf.Mountpoint
	if mp == "" {
		return 0
	}
	var stx unix.Statx_t
	err := unix.Statx(unix.AT_FDCWD, mp, unix.AT_STATX_DONT_SYNC, unix.STATX_INO, &stx)
	if err != nil || stx.Ino != uint64(rootID) {
		return 0
	}
	dev := unix.Mkdev(stx.Dev_major, stx.Dev_minor)
	r.mountDev.Store(dev)
	return dev
}

type xdsBlockFill struct {
	key       string
	sliceID   uint64
	sliceSize uint32
	blkStart  uint32
	origSize  uint32
}

// ResolveRead flushes pending writes for ino, maps [offset, offset+length) into
// ordered BlockExtents (including coalesced sparse holes), and populates any
// missing blocks directly into the dedicated xds.BlockCache.
func (r *XDSResolver) ResolveRead(
	ctx context.Context, ino uint64, offset, length uint64,
) ([]xds.BlockExtent, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	mctx := meta.WrapContext(ctx)
	targetIno := meta.Ino(ino)
	if IsSpecialNode(targetIno) {
		return nil, 0, syscall.EINVAL
	}

	// Flush pending client writes so direct reads observe read-your-writes state.
	if errno := r.vfs.writer.Flush(mctx, targetIno); errno != 0 {
		return nil, 0, errno
	}

	var attr meta.Attr
	if errno := r.vfs.Meta.GetAttr(mctx, targetIno, &attr); errno != 0 {
		return nil, 0, errno
	}
	fileSize := attr.Length
	if offset >= fileSize || length == 0 {
		return nil, fileSize, nil
	}
	readLen := min(length, fileSize-offset)
	readEnd := offset + readLen

	chunkSize := uint64(meta.ChunkSize)
	blockSize := uint32(r.vfs.Conf.Chunk.BlockSize)
	hashPrefix := r.vfs.Conf.Chunk.HashPrefix

	firstChunk := offset / chunkSize
	lastChunk := (readEnd - 1) / chunkSize

	extents := make([]xds.BlockExtent, 0, int(readLen/uint64(blockSize))+2)
	var seenMissing map[string]struct{}
	var missing []xdsBlockFill

	appendHole := func(dstOff uint64, hLen uint32) {
		if hLen == 0 {
			return
		}
		if n := len(extents); n > 0 && extents[n-1].IsHole &&
			extents[n-1].DstOff+uint64(extents[n-1].Len) == dstOff {
			extents[n-1].Len += hLen
			return
		}
		extents = append(extents, xds.BlockExtent{
			DstOff: dstOff,
			Len:    hLen,
			IsHole: true,
		})
	}

	for cindx := firstChunk; cindx <= lastChunk; cindx++ {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		var chunkSlices []meta.Slice
		if errno := r.vfs.Meta.Read(mctx, targetIno, uint32(cindx), &chunkSlices); errno != 0 {
			return nil, 0, errno
		}

		chunkBase := cindx * chunkSize
		var winStart uint32
		if offset > chunkBase {
			winStart = uint32(offset - chunkBase)
		}
		winEnd := uint32(min(readEnd-chunkBase, chunkSize))

		var pos uint32
		for _, sl := range chunkSlices {
			slEnd := pos + sl.Len
			if slEnd <= winStart {
				pos = slEnd
				continue
			}
			if pos >= winEnd {
				break
			}

			ovStart := max(pos, winStart)
			ovEnd := min(slEnd, winEnd)
			ovLen := ovEnd - ovStart
			dstOff := (chunkBase + uint64(ovStart)) - offset

			if sl.Id == 0 {
				appendHole(dstOff, ovLen)
				pos = slEnd
				continue
			}

			slOff := sl.Off + (ovStart - pos)
			if uint64(slOff)+uint64(ovLen) > uint64(sl.Size) {
				return nil, 0, syscall.EIO
			}

			firstBlk := int(slOff / blockSize)
			lastBlk := int((slOff + ovLen - 1) / blockSize)
			for bi := firstBlk; bi <= lastBlk; bi++ {
				blkStart := uint32(bi) * blockSize
				origBlkSize := min(blockSize, sl.Size-blkStart)
				inBlkOff := max(slOff, blkStart) - blkStart
				inBlkEnd := min(slOff+ovLen, blkStart+origBlkSize) - blkStart
				take := inBlkEnd - inBlkOff

				key := chunk.BlockKey(hashPrefix, sl.Id, bi, int(origBlkSize))
				extents = append(extents, xds.BlockExtent{
					Key:      key,
					BlockOff: inBlkOff,
					Len:      take,
					DstOff:   dstOff,
				})
				if r.cache != nil && !r.cache.Exist(key) {
					if seenMissing == nil {
						seenMissing = make(map[string]struct{})
					}
					if _, dup := seenMissing[key]; !dup {
						seenMissing[key] = struct{}{}
						missing = append(missing, xdsBlockFill{
							key:       key,
							sliceID:   sl.Id,
							sliceSize: sl.Size,
							blkStart:  blkStart,
							origSize:  origBlkSize,
						})
					}
				}
				dstOff += uint64(take)
			}
			pos = slEnd
		}

		// Any unwritten tail inside [winStart, winEnd) is a sparse hole.
		if pos < winEnd {
			holeStart := max(pos, winStart)
			appendHole((chunkBase+uint64(holeStart))-offset, winEnd-holeStart)
		}
	}

	if len(missing) == 1 {
		if err := r.fillBlock(ctx, missing[0]); err != nil {
			return nil, 0, err
		}
	} else if len(missing) > 1 {
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(xdsFillConcurrency)
		for _, b := range missing {
			b := b
			g.Go(func() error {
				if err := gctx.Err(); err != nil {
					return err
				}
				return r.fillBlock(gctx, b)
			})
		}
		if err := g.Wait(); err != nil {
			return nil, 0, err
		}
	}

	return extents, fileSize, nil
}

func (r *XDSResolver) fillBlock(ctx context.Context, b xdsBlockFill) error {
	if r.cache.Exist(b.key) {
		return nil
	}
	if loader, ok := r.vfs.Store.(chunk.BlockLoader); ok {
		page, err := loader.LoadBlock(ctx, b.key, int(b.origSize))
		if err != nil {
			return err
		}
		defer page.Release()
		return r.cache.Put(b.key, page.Data)
	}

	page := chunk.NewOffPage(int(b.origSize))
	defer page.Release()
	n, err := r.vfs.Store.NewReader(b.sliceID, int(b.sliceSize)).ReadAt(ctx, page, int(b.blkStart))
	if err != nil && err != io.EOF {
		return err
	}
	if n != int(b.origSize) {
		return fmt.Errorf("xds fill %s: short read %d != %d", b.key, n, b.origSize)
	}
	return r.cache.Put(b.key, page.Data[:n])
}
