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
	"fmt"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// Engine owns the embedded xDS SPDK runtime lifecycle, leased blob pool,
// Prometheus metrics, P2P DMA buffer registration/reads, and local BlockCache.
type Engine struct {
	cfg     Config
	rt      Runtime
	info    RuntimeInfo
	metrics *Metrics
	blobs   []BlobHandle
	cache   *BlockCache
	closed  atomic.Bool
}

// NewEngine starts the xDS runtime, leases the configured slab blobs, and
// constructs the sharded BlockCache. Passing nil for rt selects
// NewDefaultRuntime().
func NewEngine(cfg *Config, rt Runtime, reg prometheus.Registerer) (*Engine, error) {
	if err := cfg.Normalize(); err != nil {
		return nil, err
	}
	if rt == nil {
		rt = NewDefaultRuntime()
	}

	info, err := rt.Start(cfg)
	if err != nil {
		return nil, err
	}
	if info.IOUnitSize == 0 || info.IOUnitSize&(info.IOUnitSize-1) != 0 || uint64(cfg.BlockSize)%info.IOUnitSize != 0 {
		_ = rt.Stop()
		return nil, fmt.Errorf("%w: block_size %d not aligned to power-of-two io_unit_size %d",
			ErrInvalidArg, cfg.BlockSize, info.IOUnitSize)
	}

	blobs, err := rt.LeaseBlobs(cfg.BlobCount)
	if err != nil {
		_ = rt.Stop()
		return nil, err
	}

	metrics := newMetrics(reg)
	cache, err := newBlockCache(rt, blobs, *cfg, info.IOUnitSize, metrics)
	if err != nil {
		_ = rt.ReleaseBlobs(blobs)
		_ = rt.Stop()
		return nil, err
	}

	return &Engine{
		cfg:     *cfg,
		rt:      rt,
		info:    info,
		metrics: metrics,
		blobs:   blobs,
		cache:   cache,
	}, nil
}

// Cache returns the NVMe slab block cache backed by this Engine.
func (e *Engine) Cache() *BlockCache {
	return e.cache
}

// Info returns static geometry reported by the active xDS runtime.
func (e *Engine) Info() RuntimeInfo {
	return e.info
}

// Config returns a copy of the normalized Engine configuration.
func (e *Engine) Config() Config {
	return e.cfg
}

// Metrics returns the Engine's Prometheus metrics collector set.
func (e *Engine) Metrics() *Metrics {
	return e.metrics
}

// ImportBuffer forwards a client's exported P2P device buffer into xDS.
func (e *Engine) ImportBuffer(p2pFD int, deviceVA, size uint64, peers []PeerExport) (RegHandle, uintptr, error) {
	if e.closed.Load() {
		return 0, 0, ErrClosed
	}
	reg, ioPtr, err := e.rt.ImportBuffer(p2pFD, deviceVA, size, peers)
	if err == nil {
		e.metrics.ImportedBuffers.Inc()
	}
	return reg, ioPtr, err
}

// UnregisterBuffer removes an imported P2P device buffer from xDS.
func (e *Engine) UnregisterBuffer(reg RegHandle) error {
	err := e.rt.UnregisterBuffer(reg)
	if err == nil {
		e.metrics.ImportedBuffers.Dec()
	}
	return err
}

// ReadDirectBatch executes a batch of NVMe-to-device P2P reads through xDS.
func (e *Engine) ReadDirectBatch(descs []DirectReadDesc) error {
	if e.closed.Load() {
		return ErrClosed
	}
	return e.rt.ReadDirectBatch(descs)
}

// Close stops accepting new operations, releases all leased blobs, and shuts
// down the underlying xDS runtime. Callers must stop any Server before closing
// Engine so no DMA remains in flight.
func (e *Engine) Close() error {
	if !e.closed.CompareAndSwap(false, true) {
		return nil
	}
	e.cache.close()
	err1 := e.rt.ReleaseBlobs(e.blobs)
	err2 := e.rt.Stop()
	if err1 != nil {
		return err1
	}
	return err2
}
