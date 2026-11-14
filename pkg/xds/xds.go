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
	maxRetries    int
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
// unsupported build return ENOTSUP. cc.MaxRetries is the additional source-read
// retry budget; zero disables retries. The caller must not mutate cc during Open.
func Open(conf Config, m meta.Meta, store chunk.ChunkStore, cc *chunk.Config) (*Volume, error) {
	if conf.Service == "" || strings.IndexByte(conf.Service, 0) >= 0 ||
		conf.WorldSize == 0 || conf.Rank >= conf.WorldSize ||
		m == nil || store == nil || cc == nil ||
		cc.BlockSize <= 0 || cc.Readahead < 0 || cc.MaxRetries < 0 {
		return nil, syscall.EINVAL
	}
	blockSize, readahead, maxRetries := cc.BlockSize, cc.Readahead, cc.MaxRetries
	c, err := newClient(conf)
	if err != nil {
		return nil, err
	}
	return openVolume(c, m, store, blockSize, readahead, maxRetries)
}

// openVolume takes ownership of c on entry. On failure, it stops any started
// engine before closing c; on success, ownership transfers to the Volume.
// Keeping client construction outside permits isolated tests without mutable
// global factories or changes to the native binding.
func openVolume(c client, m meta.Meta, store chunk.ChunkStore, blockSize, readahead, maxRetries int) (*Volume, error) {
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
	if m == nil || store == nil || blockSize <= 0 || readahead < 0 || maxRetries < 0 {
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
		blockSize: blockSize, readahead: readahead, maxRetries: maxRetries,
		ioUnit: int(cm.ioUnit), gpuBufferSize: cm.gpuBufferSize,
		bgCtx: bgCtx, cancel: cancel,
	}
	// Bind cache dependencies only after zero-slot initialization succeeded.
	// initCache starts no workers; the fully initialized Volume is published below.
	t.initCache(v)
	committed = true
	return v, nil
}

// IOUnit returns the immutable alignment unit obtained during Open. It remains
// available after Close and never accesses the native client.
func (v *Volume) IOUnit() int {
	return v.ioUnit
}

// Close waits for admitted operations and background workers, drains the engine,
// and synchronously unregisters remaining device allocations before closing the
// client. It is concurrent-safe and idempotent; repeated calls return the same
// teardown result. A nil result confirms both DMA drain and device revocation.
//
// Registration calls hold the lifecycle read lock, so they also finish before
// teardown. Retained registration counters may outlive failed requests; after a
// confirmed drain they do not prevent direct native unregister. Do not call the
// public Unregister here: it reacquires the lifecycle lock and rejects sticky
// failure, even when a later engine drain has established completion.
//
// EIO means DMA drain or device revocation could not be confirmed. A previous
// control failure, or any unregister error, makes further control RPCs unsafe;
// Close still disconnects but does not treat that as a revocation acknowledgement.
// Keep device memory alive until the service exits or another mechanism proves
// it is safe. ioTimeout bounds engine draining, not the entire Close call.
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
	if v.regs.failed.Load() {
		// A failed registration may have reached the service without producing
		// a usable local record. Even an empty table cannot prove revocation.
		v.closeErr = syscall.EIO
	}
	if v.c != nil {
		if v.closeErr == nil {
			// The lifecycle write lock excludes all registry users. Native
			// unregister waits for the service's buffer close and P2P revocation;
			// closing the socket alone only schedules session cleanup. Remove
			// each acknowledged record, keeping an uncertain tail on failure.
			for len(v.regs.entries) != 0 {
				reg := v.regs.entries[0]
				if err := v.c.unregister(reg.ref); err != nil {
					v.regs.failed.Store(true)
					v.closeErr = syscall.EIO
					break
				}
				v.regs.entries[0] = nil
				v.regs.entries = v.regs.entries[1:]
			}
			if len(v.regs.entries) == 0 {
				v.regs.entries = nil
			}
		}
		v.c.close()
		v.c = nil
	}
	return v.closeErr
}
