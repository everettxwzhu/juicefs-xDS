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
	"sync"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/chunk"
)

type rangeTestReader func(context.Context, *chunk.Page, int) (int, error)

func (r rangeTestReader) ReadAt(ctx context.Context, p *chunk.Page, off int) (int, error) {
	return r(ctx, p, off)
}

type rangeTestStore struct {
	chunk.ChunkStore
	reader chunk.Reader
	calls  int
	id     uint64
	size   int
}

func (s *rangeTestStore) NewReader(id uint64, size int) chunk.Reader {
	s.calls++
	s.id, s.size = id, size
	return s.reader
}

func TestReadSliceRange(t *testing.T) {
	data := []byte("0123456789abcdef")
	backendErr := errors.New("backend failed")
	for _, tc := range []struct {
		name     string
		maxRead  int
		endErr   error
		wantErr  error
		wantRead int
	}{
		{name: "full", maxRead: 8, wantRead: 1},
		{name: "short", maxRead: 3, wantRead: 3},
		{name: "full EOF", maxRead: 8, endErr: io.EOF, wantRead: 1},
		{name: "partial EOF", maxRead: 3, endErr: io.EOF, wantErr: io.ErrUnexpectedEOF, wantRead: 1},
		{name: "zero EOF", endErr: io.EOF, wantErr: io.ErrUnexpectedEOF, wantRead: 1},
		{name: "zero progress", wantErr: io.ErrNoProgress, wantRead: 1},
		{name: "backend error", maxRead: 3, endErr: backendErr, wantErr: backendErr, wantRead: 1},
		{name: "full backend error", maxRead: 8, endErr: backendErr, wantErr: backendErr, wantRead: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls, nextOff := 0, 4
			store := &rangeTestStore{reader: rangeTestReader(func(ctx context.Context, p *chunk.Page, off int) (int, error) {
				calls++
				if off != nextOff {
					t.Fatalf("off=%d, want %d", off, nextOff)
				}
				n := min(len(p.Data), tc.maxRead)
				copy(p.Data[:n], data[off:off+n])
				nextOff += n
				return n, tc.endErr
			})}
			p, err := readSliceRange(context.Background(), store, 17, uint32(len(data)), 4, 8)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err=%v, want %v", err, tc.wantErr)
			}
			if tc.wantErr != nil {
				if p != nil {
					p.Release()
					t.Fatal("failed read returned a page")
				}
			} else {
				if p == nil {
					t.Fatal("successful read returned nil")
				}
				defer p.Release()
				if !bytes.Equal(p.Data, data[4:12]) {
					t.Fatalf("data=%q, want %q", p.Data, data[4:12])
				}
			}
			if store.calls != 1 || store.id != 17 || store.size != len(data) || calls != tc.wantRead {
				t.Fatalf("reader constructions=%d id=%d size=%d reads=%d", store.calls, store.id, store.size, calls)
			}
		})
	}
}

func TestReadSliceRangeInvalid(t *testing.T) {
	store := &rangeTestStore{}
	for _, tc := range []struct {
		name   string
		ctx    context.Context
		store  chunk.ChunkStore
		id     uint64
		size   uint32
		off, n int
	}{
		{"nil context", nil, store, 1, 8, 0, 1},
		{"nil store", context.Background(), nil, 1, 8, 0, 1},
		{"hole", context.Background(), store, 0, 8, 0, 1},
		{"negative offset", context.Background(), store, 1, 8, -1, 1},
		{"zero length", context.Background(), store, 1, 8, 0, 0},
		{"negative length", context.Background(), store, 1, 8, 0, -1},
		{"zero object", context.Background(), store, 1, 0, 0, 1},
		{"past end", context.Background(), store, 1, 8, 9, 1},
		{"at end", context.Background(), store, 1, 8, 8, 1},
		{"range exceeds end", context.Background(), store, 1, 8, 7, 2},
		{"addition overflow", context.Background(), store, 1, 8, 7, int(^uint(0) >> 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := readSliceRange(tc.ctx, tc.store, tc.id, tc.size, tc.off, tc.n)
			if p != nil || err != syscall.EINVAL {
				t.Fatalf("page=%v err=%v, want nil EINVAL", p, err)
			}
		})
	}
	if store.calls != 0 {
		t.Fatal("invalid arguments constructed a reader")
	}
	if uint64(^uint(0)>>1) < uint64(^uint32(0)) {
		if p, err := readSliceRange(context.Background(), store, 1, ^uint32(0), 0, 1); p != nil || err != syscall.EOVERFLOW {
			t.Fatalf("oversized object: page=%v err=%v", p, err)
		}
	}
	if p, err := readSliceRange(context.Background(), store, 1, 8, 0, 1); p != nil || err != syscall.EIO {
		t.Fatalf("nil reader: page=%v err=%v", p, err)
	}
}

func TestReadSliceRangeInvalidCount(t *testing.T) {
	for _, nr := range []int{-1, 9} {
		store := &rangeTestStore{reader: rangeTestReader(func(context.Context, *chunk.Page, int) (int, error) {
			return nr, nil
		})}
		if p, err := readSliceRange(context.Background(), store, 1, 8, 0, 8); p != nil || err != syscall.EIO {
			t.Fatalf("nr=%d: page=%v err=%v", nr, p, err)
		}
	}
}

func TestReadSliceRangeCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := &rangeTestStore{}
	if p, err := readSliceRange(ctx, store, 1, 8, 0, 8); p != nil || err != context.Canceled || store.calls != 0 {
		t.Fatalf("pre-cancelled: page=%v err=%v constructions=%d", p, err, store.calls)
	}

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	var retained *chunk.Page
	write := make(chan struct{})
	finished := make(chan []byte, 1)
	workerDone := make(chan struct{})
	releaseWorker := sync.OnceFunc(func() { close(write) })
	defer func() {
		if retained != nil {
			releaseWorker()
			<-workerDone
		}
	}()
	store.reader = rangeTestReader(func(_ context.Context, p *chunk.Page, _ int) (int, error) {
		// Model WithTimeout: a worker retains the page, while its caller returns
		// cancellation before that worker performs its final memory access.
		p.Acquire()
		retained = p
		go func() {
			defer close(workerDone)
			<-write
			copy(p.Data, []byte("late GET"))
			got := append([]byte(nil), p.Data...)
			p.Release()
			finished <- got
		}()
		cancel()
		return 0, errors.New("backend lost cancellation cause")
	})
	p, err := readSliceRange(ctx, store, 1, 8, 0, 8)
	if p != nil || err != context.Canceled {
		t.Fatalf("cancelled: page=%v err=%v", p, err)
	}
	if retained == nil || len(retained.Data) != 8 {
		t.Fatal("caller released the worker's page ownership")
	}
	// A second successful read must own separate memory. A late write into the
	// abandoned page cannot modify the new result.
	store.reader = rangeTestReader(func(_ context.Context, p *chunk.Page, _ int) (int, error) {
		return copy(p.Data, []byte("new data")), nil
	})
	next, err := readSliceRange(context.Background(), store, 1, 8, 0, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Release()
	releaseWorker()
	if got := <-finished; string(got) != "late GET" {
		t.Fatalf("late worker data=%q", got)
	}
	if string(next.Data) != "new data" {
		t.Fatalf("late worker modified the next read: %q", next.Data)
	}
	if retained.Data != nil {
		t.Fatal("last worker reference did not release the abandoned page")
	}
}
