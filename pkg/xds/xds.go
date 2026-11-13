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

// Package xds provides the in-process client adapter for xDS GPU reads.
//
// The native client is enabled only on Linux with cgo and the xds build tag.
// Other builds use a stub and do not require the xDS submodule.
//
// Build with make juicefs.xds to include the external native sources and
// headers in cgo's cache key. Client changes are then rebuilt automatically,
// without clearing the Go build cache or forcing all dependencies to rebuild.
package xds

import (
	"context"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
)

var logger = utils.GetLogger("juicefs")

// Config identifies the xDS queue exclusively owned by one Volume.
type Config struct {
	// Service is the service name passed to xds_service.
	Service string
	// Rank selects a queue. It must be unique among clients of this service.
	Rank uint32
	// WorldSize must be positive, identical for all connected clients, and
	// no greater than the service's configured queue count.
	WorldSize uint32
}

const (
	ioTimeout = 60 * time.Second
	pollWait  = 100 * time.Microsecond
	pollBurst = 64

	// get_blob uses the same timeout for service-side acquisition and for
	// waiting for its completion. Zero may lose a successfully acquired
	// handle when the completion has not arrived yet.
	blobAcquireTimeout = 5 * time.Second

	// Bound foreground staging concurrency and borrowed CPU-buffer bytes.
	maxStagers         = 32
	stagerMemoryBudget = 128 << 20
)

// Volume owns one xDS client, its I/O engine, and its private slot layout.
// Construct it with Open and do not copy it after first use.
//
// The metadata engine and chunk store are borrowed. Close the Volume before
// shutting down either dependency; Volume.Close does not close them.
type Volume struct {
	// Lifecycle lock: Register, Unregister, and every foreground operation
	// accessing c, e, tier, or regs must hold RLock for its entire lifetime.
	// Internal helpers assume that lock is already held; never reacquire it
	// recursively. Close holds Lock through teardown; background work follows
	// the admission protocol below. Registration state has its own short lock.
	mu       sync.RWMutex
	closed   bool
	closeErr error

	c    client
	e    *engine
	tier *tier

	// Zero-initialized device registry; native registrations belong to c.
	regs registrationTable

	m     meta.Meta
	store chunk.ChunkStore

	// Immutable snapshots; the caller may reuse its configuration after Open.
	blockSize     int
	readahead     int
	ioUnit        int
	gpuBufferSize uint64

	// Background workers must be admitted with bg.Add while an operation
	// holds mu.RLock, before launching the worker. Workers must not acquire
	// mu: Close holds mu.Lock while cancelling and waiting for them.
	bgCtx  context.Context
	cancel context.CancelFunc
	bg     sync.WaitGroup
}

// Open binds a private xDS rank to a JuiceFS volume. m, store, and cc must
// describe the same volume used to construct the application's fs.FileSystem.
//
// On success, all advertised blobs are leased, the stager pool is allocated,
// and the zero slot has been fully written. A failed acquisition aborts the
// entire initialization, including ETIMEDOUT: it does not cancel the request.
//
// Invalid arguments or geometry return EINVAL; unrepresentable layouts return
// EOVERFLOW. Other native errors are returned unchanged. Valid arguments on an
// unsupported build return ENOTSUP. The caller must not mutate cc during Open.
func Open(conf Config, m meta.Meta, store chunk.ChunkStore, cc *chunk.Config) (*Volume, error) {
	if conf.Service == "" || strings.IndexByte(conf.Service, 0) >= 0 ||
		conf.WorldSize == 0 || conf.Rank >= conf.WorldSize ||
		m == nil || store == nil || cc == nil ||
		cc.BlockSize <= 0 || cc.Readahead < 0 {
		return nil, syscall.EINVAL
	}
	blockSize, readahead := cc.BlockSize, cc.Readahead
	c, err := newClient(conf)
	if err != nil {
		return nil, err
	}
	return openVolume(c, m, store, blockSize, readahead)
}

// openVolume takes ownership of c on entry. On failure, it stops any started
// engine before closing c; on success, ownership transfers to the Volume.
// Keeping client construction outside permits isolated tests without mutable
// global factories or changes to the native binding.
func openVolume(c client, m meta.Meta, store chunk.ChunkStore, blockSize, readahead int) (*Volume, error) {
	if c == nil {
		return nil, syscall.EINVAL
	}
	var e *engine
	committed := false
	defer func() {
		if committed {
			return
		}
		if e != nil {
			if err := e.stop(); err != nil {
				// Preserve the original initialization errno. A drain failure is
				// additional diagnostic information, not proof of DMA completion.
				logger.Errorf("xDS initialization rollback: cannot confirm DMA drain: %s", err)
			}
		}
		c.close()
	}()
	if m == nil || store == nil || blockSize <= 0 || readahead < 0 {
		return nil, syscall.EINVAL
	}

	cm, err := c.metadata()
	if err != nil {
		return nil, err
	}
	l, err := makeLayout(cm, blockSize)
	if err != nil {
		return nil, err
	}

	// getBlob shares the queue/completion state used by the engine. All
	// acquisitions must finish successfully before that engine is started.
	blobs := make([]uintptr, l.blobCount)
	for i := range blobs {
		blobs[i], err = c.getBlob()
		if err != nil {
			return nil, err
		}
		if blobs[i] == 0 {
			return nil, syscall.EIO
		}
	}

	t, stagers, err := newTier(c, blobs, l, blockSize)
	if err != nil {
		return nil, err
	}
	e = newEngine(c)

	// Keep every stager private until this write physically completes. Only
	// the zero slot needs initialization; scratch/cache contents are invalid
	// until their later writes complete. Do not initialize whole blobs.
	clear(stagers[0].buf)
	zero := t.slots[0]
	if err := e.do(&ioOp{
		write: true, blob: zero.blob, ref: stagers[0].ref,
		blobOff: zero.off, n: uint64(blockSize),
	}); err != nil {
		return nil, err
	}
	for _, st := range stagers {
		t.stagers <- st
	}

	bgCtx, cancel := context.WithCancel(context.Background())
	v := &Volume{
		c: c, e: e, tier: t, m: m, store: store,
		blockSize: blockSize, readahead: readahead,
		ioUnit: int(cm.ioUnit), gpuBufferSize: cm.gpuBufferSize,
		bgCtx: bgCtx, cancel: cancel,
	}
	committed = true
	return v, nil
}

// IOUnit returns the immutable alignment unit obtained during Open. It remains
// available after Close and never accesses the native client.
func (v *Volume) IOUnit() int {
	return v.ioUnit
}

// Close waits for admitted operations and background workers, then stops the
// engine before closing its client. It is concurrent-safe and idempotent;
// repeated calls return the original drain result.
//
// Registration calls hold the lifecycle read lock, so they also finish before
// teardown. Do not wait for registration counters or unregister each record:
// failures deliberately retain references, and client.close releases the session.
// A control failure alone does not change Close's result; engine.stop determines
// whether all submitted DMA was observed complete.
//
// EIO means outstanding DMA could not be ruled out. Native client teardown is
// not a device-memory synchronization barrier; in that case device memory must
// remain alive until the service has exited or another mechanism proves DMA
// has stopped. ioTimeout bounds engine draining, not the entire native close.
func (v *Volume) Close() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return v.closeErr
	}
	v.closed = true

	if v.cancel != nil {
		v.cancel()
	}
	v.bg.Wait()

	// Workers may be waiting for submitted I/O, so retain the engine until
	// they have exited. No client method may overlap the subsequent close.
	if v.e != nil {
		v.closeErr = v.e.stop()
	}
	if v.c != nil {
		v.c.close()
		v.c = nil
	}
	return v.closeErr
}
