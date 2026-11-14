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

package xds_test

import (
	"errors"
	"io"
	"runtime"
	"syscall"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/fs"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/juicedata/juicefs/pkg/xds"
)

func ExampleOpen() {
	// The application provides an existing FileSystem and the exact metadata,
	// store, and chunk configuration used to construct it. devicePtr/capacity
	// describe a live device allocation owned by the application. activate and
	// synchronize wrap its CUDA/HIP driver calls; xds does not supply them.
	readModel := func(
		ctx meta.Context, jfs *fs.FileSystem,
		conf xds.Config, m meta.Meta, store chunk.ChunkStore, cc *chunk.Config,
		devicePtr uintptr, capacity uint64,
		activate, synchronize func() error,
	) (n int, err error, closeErr error) {
		// Keep context activation, registration, and device synchronization on
		// one OS thread. Close's later defer runs before this thread is unlocked.
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		if err := activate(); err != nil {
			return 0, err, nil
		}

		v, err := xds.Open(conf, m, store, cc)
		if err != nil {
			return 0, err, nil
		}
		// Always close, including when registration or reading fails. A read
		// EIO followed by successful Close differs from Close itself returning
		// EIO: the latter cannot establish that DMA has stopped.
		defer func() { closeErr = v.Close() }()

		// Read at most 256 MiB, with room for any EOF alignment padding inside
		// the registration. A native backend also checks registration alignment.
		u := v.IOUnit()
		size := int(min(capacity, uint64(256<<20)))
		size -= size % u
		if size == 0 {
			return 0, syscall.EINVAL, nil
		}
		if err := v.Register(devicePtr, capacity); err != nil {
			return 0, err, nil
		}

		file, eno := jfs.Open(ctx, "/model.safetensors", vfs.MODE_MASK_R)
		if eno != 0 {
			return 0, eno, nil
		}
		defer func() {
			if eno := file.Close(ctx); eno != 0 {
				err = errors.Join(err, eno)
			}
		}()

		// Wait for all prior work touching the buffer. In particular, AMD
		// requires HIP synchronization both before and after xDS I/O.
		if err := synchronize(); err != nil {
			return 0, err, nil
		}
		n, err = v.ReadAt(ctx, file, devicePtr, size, 0)
		if err != nil {
			return 0, err, nil // Do not consume partially changed data on failure.
		}
		if err := synchronize(); err != nil {
			return 0, err, nil
		}
		// Only the first n bytes are file data. A positive short read at EOF
		// succeeds; padding through the next IOUnit boundary contains zeros.
		if err := v.Unregister(devicePtr); err != nil {
			return 0, err, nil
		}
		return n, nil, nil
	}

	// Call readModel with, for example:
	//   conf := xds.Config{Service: "xds-svc", Rank: 0, WorldSize: 1}
	// Use the actual local rank and the same world size as other service clients.
	// Handle both returned errors. If closeErr != nil, keep the allocation alive
	// until the service exits or another mechanism proves DMA has stopped. A nil
	// closeErr confirms xDS drainage; the application must still account for its
	// own device work. Close the borrowed metadata/store only after this helper.
	_ = readModel

	// No Output directive: compiled for API checking, not executed against a GPU.
}

func ExampleVolume_ReadAt() {
	// This helper borrows an open Volume, a readable regular file, and a range
	// already registered with v. batchSize must fit that registration. The owner
	// remains responsible for Unregister and Close as in the Open example.
	readBatches := func(
		ctx meta.Context, v *xds.Volume, file *fs.File,
		devicePtr uintptr, batchSize int,
		activate, synchronize func() error,
		consume func(ptr uintptr, validBytes int) error,
	) (int64, error) {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		if err := activate(); err != nil {
			return 0, err
		}
		if batchSize <= 0 || batchSize%v.IOUnit() != 0 {
			return 0, syscall.EINVAL
		}

		var offset int64
		for {
			// consume may have queued a GPU kernel using the previous batch.
			// Finish it before the next DMA overwrites this same device range.
			if err := synchronize(); err != nil {
				return offset, err
			}
			n, err := v.ReadAt(ctx, file, devicePtr, batchSize, offset)
			if err == io.EOF {
				return offset, nil
			}
			if err != nil {
				return offset, err // This batch is unusable, including partial DMA.
			}
			if err := synchronize(); err != nil {
				return offset, err
			}
			if err := consume(devicePtr, n); err != nil {
				return offset, err
			}
			offset += int64(n)
			if n < batchSize {
				// EOF may produce an unaligned n. Stop here instead of using that
				// offset for another read, and finish the last consumer's work.
				return offset, synchronize()
			}
		}
	}

	// The callbacks activate the allocation's existing context, synchronize all
	// work touching it, and process only validBytes (not EOF padding). They are
	// application code, not xds methods. On errors retain the device allocation
	// through owner cleanup; do not free it merely because this helper returned.
	_ = readBatches

	// No Output directive: compiled for API checking, not executed against a GPU.
}
