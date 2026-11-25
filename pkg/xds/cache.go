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
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/hashicorp/golang-lru/v2/simplelru"
)

const (
	cacheShardCount    = 64
	maxEvictProbeSteps = 32
)

// PinnedSlot represents a cached block pinned in NVMe memory against eviction
// until Unpin is called.
type PinnedSlot struct {
	SlotIndex  uint32
	Blob       BlobHandle
	BlobOffset uint64
	OrigLen    uint32
}

type cacheEntry struct {
	slotIdx  uint32
	origLen  uint32
	expireAt int64 // unix nano; 0 disables expiry
}

type cacheShard struct {
	mu  sync.Mutex
	lru *simplelru.LRU[string, *cacheEntry]
}

// BlockCache implements a sharded, pin-safe NVMe slab block cache where each
// fixed-size slot aligns 1:1 with one SPDK cluster for lock-free FastMap reads.
type BlockCache struct {
	rt           Runtime
	metrics      *Metrics
	blobs        []BlobHandle
	blockSize    int
	slotsPerBlob uint32
	totalSlots   uint32
	cacheExpire  time.Duration
	slotPins     []atomic.Uint32

	shards [cacheShardCount]cacheShard

	allocMu      sync.Mutex
	freeSlots    []uint32
	retiredSlots []uint32 // unlinked slots waiting for inflight DMA pins to hit 0
	evictCursor  atomic.Uint32

	usedSlots atomic.Int64
	usedBytes atomic.Int64
	closed    atomic.Bool
}

func newBlockCache(
	rt Runtime,
	blobs []BlobHandle,
	cfg Config,
	metrics *Metrics,
) (*BlockCache, error) {
	totalSlots := cfg.TotalSlots()
	c := &BlockCache{
		rt:           rt,
		metrics:      metrics,
		blobs:        blobs,
		blockSize:    cfg.BlockSize,
		slotsPerBlob: uint32(cfg.BlobSize / uint64(cfg.BlockSize)),
		totalSlots:   totalSlots,
		cacheExpire:  cfg.CacheExpire,
		slotPins:     make([]atomic.Uint32, totalSlots),
		freeSlots:    make([]uint32, totalSlots),
	}
	for i := range totalSlots {
		c.freeSlots[i] = totalSlots - 1 - i // pop from tail -> ascending order
	}

	// Capacity exceeds totalSlots so simplelru never auto-evicts without
	// respecting atomic DMA pins; BlockCache drives eviction explicitly.
	shardCap := int(totalSlots) + 1
	for i := range c.shards {
		l, err := simplelru.NewLRU[string, *cacheEntry](shardCap, nil)
		if err != nil {
			return nil, fmt.Errorf("xds: init cache shard: %w", err)
		}
		c.shards[i].lru = l
	}
	return c, nil
}

func (c *BlockCache) close() {
	c.closed.Store(true)
}

// Stats returns the current cached block count and cached payload bytes.
func (c *BlockCache) Stats() (count, bytes int64) {
	return c.usedSlots.Load(), c.usedBytes.Load()
}

func (c *BlockCache) shardFor(key string) *cacheShard {
	return &c.shards[xxhash.Sum64String(key)&(cacheShardCount-1)]
}

func (c *BlockCache) slotLocation(idx uint32) (BlobHandle, uint64) {
	blobIdx := idx / c.slotsPerBlob
	slotInBlob := idx % c.slotsPerBlob
	return c.blobs[blobIdx], uint64(slotInBlob) * uint64(c.blockSize)
}

func (c *BlockCache) recordRemove(origLen uint32, evicted bool) {
	c.usedSlots.Add(-1)
	c.usedBytes.Add(-int64(origLen))
	c.metrics.CacheSlotsUsed.Dec()
	c.metrics.CacheBytesUsed.Sub(float64(origLen))
	if evicted {
		c.metrics.CacheEvictions.Inc()
	}
}

// Pin looks up key and atomically pins its NVMe slot so concurrent eviction
// cannot overwrite the slot while DMA or CPU I/O is in flight.
func (c *BlockCache) Pin(key string) (PinnedSlot, bool) {
	if c.closed.Load() {
		return PinnedSlot{}, false
	}
	sh := c.shardFor(key)
	sh.mu.Lock()
	ent, ok := sh.lru.Get(key)
	if !ok {
		sh.mu.Unlock()
		c.metrics.CacheMisses.Inc()
		return PinnedSlot{}, false
	}
	if ent.expireAt > 0 && time.Now().UnixNano() > ent.expireAt {
		sh.lru.Remove(key)
		sh.mu.Unlock()
		c.recordRemove(ent.origLen, false)
		c.recycleSlot(ent.slotIdx)
		c.metrics.CacheMisses.Inc()
		return PinnedSlot{}, false
	}
	// Increment pin counter while holding shard lock to race-proof against eviction.
	c.slotPins[ent.slotIdx].Add(1)
	slotIdx := ent.slotIdx
	origLen := ent.origLen
	sh.mu.Unlock()

	blob, off := c.slotLocation(slotIdx)
	c.metrics.CacheHits.Inc()
	return PinnedSlot{
		SlotIndex:  slotIdx,
		Blob:       blob,
		BlobOffset: off,
		OrigLen:    origLen,
	}, true
}

// Unpin releases a slot pin previously acquired via Pin.
func (c *BlockCache) Unpin(slotIdx uint32) {
	if slotIdx < c.totalSlots {
		c.slotPins[slotIdx].Add(^uint32(0))
	}
}

// Exist reports whether key is currently cached and unexpired.
func (c *BlockCache) Exist(key string) bool {
	if c.closed.Load() {
		return false
	}
	sh := c.shardFor(key)
	sh.mu.Lock()
	ent, ok := sh.lru.Peek(key)
	expired := ok && ent.expireAt > 0 && time.Now().UnixNano() > ent.expireAt
	sh.mu.Unlock()
	return ok && !expired
}

// Put writes a plaintext JuiceFS block into an xDS NVMe slot and publishes key.
// NVMe I/O executes without holding any shard lock before indexing.
func (c *BlockCache) Put(key string, data []byte) error {
	if c.closed.Load() {
		return ErrClosed
	}
	if len(data) == 0 || len(data) > c.blockSize {
		return fmt.Errorf("%w: block length %d outside (0, %d]", ErrInvalidArg, len(data), c.blockSize)
	}
	if c.Exist(key) {
		return nil
	}

	slotIdx, err := c.allocSlot()
	if err != nil {
		return err
	}

	blob, off := c.slotLocation(slotIdx)
	if err := c.rt.WriteCPU(blob, off, data); err != nil {
		c.freeSlotDirect(slotIdx)
		return err
	}

	var expireAt int64
	if c.cacheExpire > 0 {
		expireAt = time.Now().UnixNano() + c.cacheExpire.Nanoseconds()
	}

	sh := c.shardFor(key)
	sh.mu.Lock()
	var expiredOld *cacheEntry
	if old, exists := sh.lru.Peek(key); exists {
		if old.expireAt == 0 || time.Now().UnixNano() <= old.expireAt {
			// Another goroutine won the race with a live entry; discard duplicate slot.
			sh.mu.Unlock()
			c.freeSlotDirect(slotIdx)
			return nil
		}
		expiredOld = old
		sh.lru.Remove(key)
	}
	sh.lru.Add(key, &cacheEntry{
		slotIdx:  slotIdx,
		origLen:  uint32(len(data)),
		expireAt: expireAt,
	})
	sh.mu.Unlock()

	if expiredOld != nil {
		c.recordRemove(expiredOld.origLen, false)
		c.recycleSlot(expiredOld.slotIdx)
	}
	c.usedSlots.Add(1)
	c.usedBytes.Add(int64(len(data)))
	c.metrics.CacheSlotsUsed.Inc()
	c.metrics.CacheBytesUsed.Add(float64(len(data)))
	return nil
}

// Remove unlinks key from the cache index and recycles its slot once all
// inflight DMA pins drain to zero.
func (c *BlockCache) Remove(key string) {
	sh := c.shardFor(key)
	sh.mu.Lock()
	ent, ok := sh.lru.Peek(key)
	if ok {
		sh.lru.Remove(key)
	}
	sh.mu.Unlock()
	if !ok {
		return
	}

	c.recordRemove(ent.origLen, false)
	c.recycleSlot(ent.slotIdx)
}

func (c *BlockCache) freeSlotDirect(idx uint32) {
	c.allocMu.Lock()
	c.freeSlots = append(c.freeSlots, idx)
	c.allocMu.Unlock()
}

func (c *BlockCache) recycleSlot(idx uint32) {
	c.allocMu.Lock()
	if c.slotPins[idx].Load() == 0 {
		c.freeSlots = append(c.freeSlots, idx)
	} else {
		c.retiredSlots = append(c.retiredSlots, idx)
	}
	c.allocMu.Unlock()
}

func (c *BlockCache) reclaimRetiredLocked() {
	if len(c.retiredSlots) == 0 {
		return
	}
	keep := c.retiredSlots[:0]
	for _, idx := range c.retiredSlots {
		if c.slotPins[idx].Load() == 0 {
			c.freeSlots = append(c.freeSlots, idx)
		} else {
			keep = append(keep, idx)
		}
	}
	c.retiredSlots = keep
}

func (c *BlockCache) allocSlot() (uint32, error) {
	c.allocMu.Lock()
	if len(c.freeSlots) == 0 {
		c.reclaimRetiredLocked()
	}
	if n := len(c.freeSlots); n > 0 {
		idx := c.freeSlots[n-1]
		c.freeSlots = c.freeSlots[:n-1]
		c.allocMu.Unlock()
		return idx, nil
	}
	c.allocMu.Unlock()

	return c.evictOne()
}

// evictOne scans shards round-robin in a single pass and evicts the oldest
// unpinned slot, never displacing any slot pinned by inflight DMA.
func (c *BlockCache) evictOne() (uint32, error) {
	start := c.evictCursor.Add(1)
	for step := range uint32(cacheShardCount) {
		if slotIdx, ok := c.evictFromShard(&c.shards[(start+step)&(cacheShardCount-1)]); ok {
			return slotIdx, nil
		}
	}
	return 0, ErrNoSpace
}

func (c *BlockCache) evictFromShard(sh *cacheShard) (uint32, bool) {
	sh.mu.Lock()
	for range maxEvictProbeSteps {
		k, ent, ok := sh.lru.GetOldest()
		if !ok {
			break
		}
		if c.slotPins[ent.slotIdx].Load() > 0 {
			// Actively pinned by DMA; promote to MRU and inspect next oldest.
			sh.lru.Get(k)
			continue
		}
		sh.lru.RemoveOldest()
		// Re-check pin count under shard lock: no new pin can occur once unlinked.
		reusable := c.slotPins[ent.slotIdx].Load() == 0
		sh.mu.Unlock()

		c.recordRemove(ent.origLen, true)
		if reusable {
			return ent.slotIdx, true
		}
		c.recycleSlot(ent.slotIdx)
		return 0, false
	}
	sh.mu.Unlock()
	return 0, false
}
