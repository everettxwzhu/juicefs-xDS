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

// prefetch opportunistically fills following blocks of the same immutable
// slice. The caller must hold Volume.mu.RLock until this method returns: it
// admits every worker to bg before Close can start waiting for that group.
// Workers never acquire Volume.mu and must not recursively start prefetches.
//
// Both stager and slot admission are nonblocking. A separate background limit
// prevents speculative work from occupying all staging buffers. This is a
// concurrency reservation, not a strict priority scheduler for foreground I/O.
func (t *tier) prefetch(k blockKey) {
	if cap(t.prefetchTokens) == 0 || t.readahead < t.blockSize || t.check(t.bgCtx) != nil {
		return
	}
	if _, _, _, err := t.blockRange(k); err != nil {
		return
	}
	last := (uint64(k.size) - 1) / uint64(t.blockSize)
	current := uint64(k.indx)
	if current >= last {
		return
	}
	end := min(last, current+uint64(t.readahead/t.blockSize))
	for next := current + 1; next <= end; next++ {
		key := blockKey{id: k.id, size: k.size, indx: uint32(next)}
		if !t.startPrefetch(key) {
			return
		}
	}
}

// startPrefetch returns false when cancellation or resource pressure means
// remaining speculative work should be skipped. An existing cache entry or
// fill is not resource pressure and does not stop inspection of later blocks.
func (t *tier) startPrefetch(k blockKey) bool {
	if t.check(t.bgCtx) != nil {
		return false
	}
	t.mu.Lock()
	_, cached := t.index[k]
	_, filling := t.fills[k]
	t.mu.Unlock()
	if cached || filling {
		return true
	}

	select {
	case t.prefetchTokens <- struct{}{}:
	default:
		return false
	}
	st := t.tryGetStager()
	if st == nil {
		<-t.prefetchTokens
		return false
	}

	// Recheck deduplication while reserving the slot and publishing the flight.
	// Foreground acquire may have claimed this key after the optimistic check.
	t.mu.Lock()
	if err := t.check(t.bgCtx); err != nil {
		t.mu.Unlock()
		t.putStager(st)
		<-t.prefetchTokens
		return false
	}
	_, cached = t.index[k]
	_, filling = t.fills[k]
	if cached || filling {
		t.mu.Unlock()
		t.putStager(st)
		<-t.prefetchTokens
		return true
	}
	sid, ok := t.tryAllocSlotLocked(k)
	if !ok {
		t.mu.Unlock()
		t.putStager(st)
		<-t.prefetchTokens
		return false
	}
	call := &fillCall{done: make(chan struct{})}
	t.fills[k] = call
	t.mu.Unlock()

	// The caller's lifecycle read lock excludes Close's bg.Wait until Add
	// completes. Do not move Add into the worker or acquire that lock there.
	t.bg.Go(func() {
		defer func() { <-t.prefetchTokens }()
		err := t.fill(t.bgCtx, k, st, sid)
		err = t.completeFill(k, call, sid, err)
		t.putStager(st)
		if err == nil {
			// Prefetch owns the initial reference only until publication.
			// Foreground waiters obtain their own references through acquire.
			t.release(sid)
		}
	})
	return true
}
