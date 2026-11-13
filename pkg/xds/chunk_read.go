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
	"syscall"

	"github.com/juicedata/juicefs/pkg/chunk"
)

// readSliceRange reads an immutable slice range into a private, reference-counted
// page. On success, the caller owns one reference and must release it after use.
//
// A chunk read may return before a timeout worker has finished writing its page;
// asynchronous local-cache writes may also retain references after success.
// Never supply an xDS stager here or reuse a failed page for another attempt.
// Releasing our reference is safe: chunk/cache workers retain their own references.
func readSliceRange(ctx context.Context, store chunk.ChunkStore, id uint64, size uint32, off, n int) (*chunk.Page, error) {
	if ctx == nil || store == nil || id == 0 || off < 0 || n <= 0 {
		return nil, syscall.EINVAL
	}
	if uint64(size) > uint64(^uint(0)>>1) {
		return nil, syscall.EOVERFLOW
	}
	// Subtract only after checking off; neither validation nor conversion may
	// overflow, including on a 32-bit build.
	if uint64(off) > uint64(size) || uint64(n) > uint64(size)-uint64(off) {
		return nil, syscall.EINVAL
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	r := store.NewReader(id, int(size))
	if r == nil {
		return nil, syscall.EIO
	}
	page := chunk.NewOffPage(n)
	owned := true
	defer func() {
		if owned {
			page.Release()
		}
	}()

	for got := 0; got < n; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// The usual full-block read needs no child Page allocation. After a
		// short read, Slice keeps the parent alive for any retained child refs.
		part := page
		if got != 0 {
			part = page.Slice(got, n-got)
		}
		nr, err := r.ReadAt(ctx, part, off+got)
		if part != page {
			part.Release()
		}
		// Some chunk errors lose their wrapped cancellation cause. Preserve the
		// caller's cancellation before classifying the backend error.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if nr < 0 || nr > n-got {
			return nil, syscall.EIO
		}
		got += nr
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return nil, err
			}
			if got != n {
				return nil, io.ErrUnexpectedEOF
			}
		}
		if nr == 0 {
			return nil, io.ErrNoProgress
		}
	}
	owned = false
	return page, nil
}
