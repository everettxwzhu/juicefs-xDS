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
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
)

// Stage 3 only retains these interfaces; invoking their methods is a bug.
type volumeTestMeta struct{ meta.Meta }
type volumeTestStore struct{ chunk.ChunkStore }

// volumeClient adds lifecycle and byte-transfer behavior without weakening
// fakeClient's assertions that the engine itself never manages resources.
// Faults are configured before Open; maps have one owner during initialization
// and then one engine owner until Close. Channels synchronize test observations.
type volumeClient struct {
	*fakeClient
	cm               clientMeta
	metaErr          error
	getFailAt        int
	getErr           error
	allocFailAt      int
	allocErr         error
	badBuffer        bool
	completionStatus int32
	complete         bool
	gets             int
	allocs           int
	blobs            map[uintptr][]byte
	buffers          map[uint64][]byte
	closes           atomic.Int32
	onClose          func()
}

var _ client = (*volumeClient)(nil)

func newVolumeClient() *volumeClient {
	return &volumeClient{
		fakeClient: newFakeClient(),
		cm: clientMeta{
			blobSize: 16 * 4096, ioUnit: 4096,
			capacity: 5 * 16 * 4096, gpuBufferSize: 1 << 30,
		},
		complete: true,
		blobs:    make(map[uintptr][]byte),
		buffers:  make(map[uint64][]byte),
	}
}

func (c *volumeClient) metadata() (clientMeta, error) { return c.cm, c.metaErr }

func (c *volumeClient) getBlob() (uintptr, error) {
	c.gets++
	if c.gets == c.getFailAt {
		return 0, c.getErr
	}
	h := uintptr(100 + c.gets)
	// Nonzero initial contents catch missing or partial zero-slot writes.
	c.blobs[h] = bytes.Repeat([]byte{0xa5}, int(c.cm.blobSize))
	return h, nil
}

func (c *volumeClient) allocBuffer(size uint64) ([]byte, uint64, error) {
	c.allocs++
	if c.allocs == c.allocFailAt {
		return nil, 0, c.allocErr
	}
	if c.badBuffer {
		return make([]byte, int(size)-1), 0, nil
	}
	ref := uint64(200 + c.allocs)
	buf := bytes.Repeat([]byte{0x5a}, int(size))
	c.buffers[ref] = buf
	return buf, ref, nil
}

func (c *volumeClient) submit(op *ioOp) (uint64, error) {
	id, err := c.fakeClient.submit(op)
	if err != nil {
		return 0, err
	}
	r := result{id: id, status: c.completionStatus}
	if r.status == 0 {
		blob, buf := c.blobs[op.blob], c.buffers[op.ref]
		if op.blobOff > uint64(len(blob)) || op.n > uint64(len(blob))-op.blobOff ||
			op.bufOff > uint64(len(buf)) || op.n > uint64(len(buf))-op.bufOff {
			r.status = -int32(syscall.EINVAL)
		} else {
			b := blob[op.blobOff : op.blobOff+op.n]
			p := buf[op.bufOff : op.bufOff+op.n]
			if op.write {
				copy(b, p)
			} else {
				copy(p, b)
			}
			r.completed = op.n
		}
	}
	if c.complete {
		c.ready <- r
	}
	return id, nil
}

func (c *volumeClient) close() {
	if c.onClose != nil {
		c.onClose()
	}
	c.closes.Add(1)
}

func openTestVolume(t *testing.T, c *volumeClient) *Volume {
	t.Helper()
	v, err := openVolume(c, &volumeTestMeta{}, &volumeTestStore{}, 4096, 8192, 3)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = v.Close() })
	return v
}

func TestVolumeOpen(t *testing.T) {
	c := newVolumeClient()
	v := openTestVolume(t, c)
	if c.gets != 5 || c.allocs != 32 || len(v.tier.slots) != 80 ||
		v.tier.cacheStart != 33 || len(v.tier.stagers) != 32 {
		t.Fatalf("gets=%d allocs=%d slots=%d cacheStart=%d stagers=%d",
			c.gets, c.allocs, len(v.tier.slots), v.tier.cacheStart, len(v.tier.stagers))
	}
	if v.blockSize != 4096 || v.readahead != 8192 || v.maxRetries != 3 || v.IOUnit() != 4096 ||
		v.gpuBufferSize != c.cm.gpuBufferSize {
		t.Fatal("configuration or metadata snapshot is incorrect")
	}
	for i, ref := range v.tier.slots {
		if ref.blob != uintptr(101+i/16) || ref.off != uint64(i%16)*4096 {
			t.Fatalf("slot %d: %+v", i, ref)
		}
	}
	for i := 0; i < 32; i++ {
		st := <-v.tier.stagers
		if len(st.buf) != 4096 || st.ref != uint64(201+i) ||
			st.scratch != v.tier.slots[1+i] {
			t.Fatalf("stager %d: ref=%d len=%d scratch=%+v",
				i, st.ref, len(st.buf), st.scratch)
		}
	}
	zero := v.tier.slots[0]
	if !bytes.Equal(c.blobs[zero.blob][:4096], make([]byte, 4096)) {
		t.Fatal("zero slot contains nonzero bytes")
	}
	if c.blobs[zero.blob][4096] != 0xa5 || c.submits.Load() != 1 {
		t.Fatal("Open initialized more than the reserved zero slot")
	}
	if c.closes.Load() != 0 {
		t.Fatal("successful Open closed its client")
	}
}

func TestVolumeOpenWaitsForZeroSlot(t *testing.T) {
	c := newVolumeClient()
	c.complete = false
	type opened struct {
		v   *Volume
		err error
	}
	done := make(chan opened, 1)
	go func() {
		v, err := openVolume(c, &volumeTestMeta{}, &volumeTestStore{}, 4096, 0, 0)
		done <- opened{v, err}
	}()
	r := receive(t, c.submitted)
	completeZero := sync.OnceFunc(func() { c.ready <- r.result })
	defer completeZero()
	select {
	case result := <-done:
		completeZero()
		if result.v != nil {
			_ = result.v.Close()
		}
		t.Fatal("Open returned before zero-slot completion")
	default:
	}
	if !r.write || r.ref != 201 || r.blob != 101 || r.blobOff != 0 ||
		r.bufOff != 0 || r.completed != 4096 {
		t.Fatalf("unexpected zero-slot operation: %+v", r)
	}
	completeZero()
	result := receive(t, done)
	if result.err != nil {
		t.Fatal(result.err)
	}
	if err := result.v.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestVolumeOpenRollback(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*volumeClient)
		want      error
		gets      int
		allocs    int
		submits   int64
	}{
		{"metadata", func(c *volumeClient) {
			c.metaErr = syscall.ENODEV
		}, syscall.ENODEV, 0, 0, 0},
		{"layout", func(c *volumeClient) {
			c.cm.ioUnit = 0
		}, syscall.EINVAL, 0, 0, 0},
		{"acquisition timeout", func(c *volumeClient) {
			c.getFailAt, c.getErr = 3, syscall.ETIMEDOUT
		}, syscall.ETIMEDOUT, 3, 0, 0},
		{"buffer allocation", func(c *volumeClient) {
			c.allocFailAt, c.allocErr = 4, syscall.ENOMEM
		}, syscall.ENOMEM, 5, 4, 0},
		{"malformed buffer", func(c *volumeClient) {
			c.badBuffer = true
		}, syscall.EIO, 5, 1, 0},
		{"submit", func(c *volumeClient) {
			c.reject = func(int64) error { return syscall.ENODEV }
		}, syscall.ENODEV, 5, 32, 1},
		{"completion", func(c *volumeClient) {
			c.completionStatus = -int32(syscall.EIO)
		}, syscall.EIO, 5, 32, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newVolumeClient()
			tc.configure(c)
			v, err := openVolume(c, &volumeTestMeta{}, &volumeTestStore{}, 4096, 0, 0)
			if v != nil || err != tc.want {
				if v != nil {
					_ = v.Close()
				}
				t.Fatalf("Open: %v, %v; want nil, %v", v, err, tc.want)
			}
			if c.gets != tc.gets || c.allocs != tc.allocs ||
				c.submits.Load() != tc.submits ||
				c.closes.Load() != 1 || c.active.Load() != 0 {
				t.Fatalf("gets=%d allocs=%d submits=%d closes=%d active=%d",
					c.gets, c.allocs, c.submits.Load(),
					c.closes.Load(), c.active.Load())
			}
		})
	}
}

func TestVolumeOpenInvalidArguments(t *testing.T) {
	valid := Config{Service: "xds-test", WorldSize: 1}
	m, store := &volumeTestMeta{}, &volumeTestStore{}
	for _, tc := range []struct {
		name  string
		conf  Config
		m     meta.Meta
		store chunk.ChunkStore
		cc    *chunk.Config
	}{
		{"empty service", Config{WorldSize: 1},
			m, store, &chunk.Config{BlockSize: 4096}},
		{"embedded NUL", Config{Service: "xds\x00test", WorldSize: 1},
			m, store, &chunk.Config{BlockSize: 4096}},
		{"zero world", Config{Service: "xds-test"},
			m, store, &chunk.Config{BlockSize: 4096}},
		{"rank", Config{Service: "xds-test", Rank: 1, WorldSize: 1},
			m, store, &chunk.Config{BlockSize: 4096}},
		{"nil metadata", valid, nil, store, &chunk.Config{BlockSize: 4096}},
		{"nil store", valid, m, nil, &chunk.Config{BlockSize: 4096}},
		{"nil configuration", valid, m, store, nil},
		{"zero block", valid, m, store, &chunk.Config{}},
		{"negative block", valid, m, store, &chunk.Config{BlockSize: -1}},
		{"negative readahead", valid, m, store,
			&chunk.Config{BlockSize: 4096, Readahead: -1}},
		{"negative retries", valid, m, store,
			&chunk.Config{BlockSize: 4096, MaxRetries: -1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, err := Open(tc.conf, tc.m, tc.store, tc.cc)
			if v != nil || err != syscall.EINVAL {
				if v != nil {
					_ = v.Close()
				}
				t.Fatalf("Open: %v, %v; want nil, EINVAL", v, err)
			}
		})
	}
}

func TestVolumeClose(t *testing.T) {
	for _, complete := range []bool{true, false} {
		t.Run(map[bool]string{true: "drained", false: "uncertain DMA"}[complete], func(t *testing.T) {
			c := newVolumeClient()
			c.complete = false
			blob, _ := c.getBlob()
			_, ref, _ := c.allocBuffer(4096)
			timeout := 50 * time.Millisecond
			if complete {
				// The successful-drain case tests ordering, not the scheduler's
				// ability to return a completion within a short timeout.
				timeout = 5 * time.Second
			}
			e := newEngineWithTimeout(c, timeout)
			ctx, cancel := context.WithCancel(context.Background())
			v := &Volume{
				c: c, e: e, ioUnit: 4096,
				bgCtx: ctx, cancel: cancel,
			}
			t.Cleanup(func() { _ = v.Close() })
			c.onClose = func() {
				select {
				case <-e.exited:
				default:
					t.Error("client closed before engine exited")
				}
			}
			done := asyncDo(e, &ioOp{blob: blob, ref: ref, n: 4096})
			r := receive(t, c.submitted)
			closed := make(chan error, 8)
			for i := 0; i < cap(closed); i++ {
				go func() { closed <- v.Close() }()
			}
			receive(t, e.stopping)
			want := error(syscall.EIO)
			if complete {
				want = nil
				c.ready <- r.result
			}
			for i := 0; i < cap(closed); i++ {
				if err := receive(t, closed); err != want {
					t.Fatalf("Close=%v, want=%v", err, want)
				}
			}
			if err := receive(t, done); err != want {
				t.Fatalf("I/O=%v, want=%v", err, want)
			}
			if err := v.Close(); err != want ||
				c.closes.Load() != 1 || v.IOUnit() != 4096 {
				t.Fatalf("repeat Close=%v closes=%d unit=%d",
					err, c.closes.Load(), v.IOUnit())
			}
		})
	}
}

func TestVolumeCloseWaitsForBackgroundWork(t *testing.T) {
	c := newVolumeClient()
	v := openTestVolume(t, c)
	started := make(chan struct{})
	cancelled := make(chan struct{})
	release := make(chan struct{})
	releaseWorker := sync.OnceFunc(func() { close(release) })
	defer releaseWorker()
	v.mu.RLock()
	v.bg.Add(1)
	go func() {
		defer v.bg.Done()
		close(started)
		<-v.bgCtx.Done()
		close(cancelled)
		<-release
	}()
	v.mu.RUnlock()
	receive(t, started)
	done := make(chan error, 1)
	go func() { done <- v.Close() }()
	receive(t, cancelled)
	if c.closes.Load() != 0 {
		t.Fatal("client closed while a background worker still owns resources")
	}
	select {
	case <-v.e.stopping:
		t.Fatal("engine stopped before background workers finished")
	default:
	}
	releaseWorker()
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestVolumeCloseWaitsForReaders(t *testing.T) {
	c := newVolumeClient()
	v := openTestVolume(t, c)
	v.mu.RLock()
	locked := true
	defer func() {
		if locked {
			v.mu.RUnlock()
		}
	}()
	done := make(chan error, 1)
	go func() { done <- v.Close() }()
	deadline := time.After(5 * time.Second)
	// A pending writer prevents new TryRLock calls. Observe that transition
	// before checking that the already-admitted reader still protects resources.
	for v.mu.TryRLock() {
		v.mu.RUnlock()
		select {
		case <-done:
			t.Fatal("Close returned while a reader still holds the lifecycle lock")
		case <-deadline:
			t.Fatal("Close did not acquire the lifecycle lock")
		default:
			runtime.Gosched()
		}
	}
	select {
	case <-v.bgCtx.Done():
		t.Fatal("Close started teardown before admitted readers finished")
	default:
	}
	v.mu.RUnlock()
	locked = false
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
}
