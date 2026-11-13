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
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
)

// registration describes caller-owned device memory. base, size and ref are
// immutable after publication. Store pointers to these records: moving slice
// elements must neither invalidate a reader's record nor copy atomic fields.
type registration struct {
	base uint64
	size uint64
	ref  uint64

	// One reference per ReadAt, retained until all its workers have finished.
	// Failed reads may retain references until Close because an error alone
	// does not establish physical DMA completion.
	inflight atomic.Int32

	// Protected by registrationTable.mu. Closing records still reserve their
	// address ranges, but lookup must not acquire a new reference to them.
	closing bool
}

// registrationTable separates slow control operations from read admission.
// Its zero value is an empty table; do not copy it after first use.
type registrationTable struct {
	// Lock order: Volume.mu.RLock -> controlMu -> mu. Only Register and
	// Unregister acquire controlMu; lookup never waits for a native RPC.
	controlMu sync.Mutex

	// Protects entries and closing. Release it before calling the client.
	mu      sync.Mutex
	entries []*registration // Sorted by base; address ranges do not overlap.

	// Sticky control failure. The client exposes only errno, so callers cannot
	// distinguish an unchanged registration from a lost reply or partial close.
	// Existing engine requests are still drained by Volume.Close.
	failed atomic.Bool
}

// deviceRange validates a nonempty device address range before any addition.
// Device addresses remain integers: they must never be dereferenced by Go.
func deviceRange(ptr uintptr, size uint64) (base, end uint64, err error) {
	if ptr == 0 || size == 0 {
		return 0, 0, syscall.EINVAL
	}
	base = uint64(ptr)
	if size > uint64(^uintptr(0))-base {
		return 0, 0, syscall.EOVERFLOW
	}
	return base, base + size, nil
}

// deviceReadError checks admission to the device path. The caller must hold
// Volume.mu.RLock; this helper deliberately does not reacquire the lifecycle
// lock, since a pending Close would make a recursive RLock deadlock.
func (v *Volume) deviceReadError() error {
	if v.closed {
		return syscall.EBADF
	}
	if v.e.broken.Load() || v.regs.failed.Load() {
		return syscall.EIO
	}
	return nil
}

// Register registers caller-owned device memory for this Volume. The caller
// must LockOSThread before activating the allocation's device context and keep
// that thread locked through this call. Registration runs on that same thread.
//
// Keep the allocation alive until successful Unregister or a successful Close.
// Address/size alignment is checked by the native backend, independently of
// IOUnit. Overlap returns EEXIST; empty ranges return EINVAL; an unrepresentable
// exclusive end returns EOVERFLOW. Plan total registered bytes within the
// service's advertised gpu_buffer_size; this method does not enforce that budget.
//
// Native errors are returned unchanged and make the device path unusable until
// Close. The current client API cannot prove whether a failed control operation
// changed service state. Local validation errors do not fail the Volume.
func (v *Volume) Register(ptr uintptr, size uint64) error {
	v.mu.RLock()
	defer v.mu.RUnlock()

	t := &v.regs
	t.controlMu.Lock()
	defer t.controlMu.Unlock()
	if err := v.deviceReadError(); err != nil {
		return err
	}
	base, end, err := deviceRange(ptr, size)
	if err != nil {
		return err
	}

	t.mu.Lock()
	i := sort.Search(len(t.entries), func(i int) bool {
		return t.entries[i].base >= base
	})
	if i > 0 && t.entries[i-1].base+t.entries[i-1].size > base ||
		i < len(t.entries) && t.entries[i].base < end {
		t.mu.Unlock()
		return syscall.EEXIST
	}
	t.mu.Unlock()

	// controlMu reserves the insertion point against all other table writers.
	// No pending record or second overlap check is needed; existing readers
	// remain free to look up unrelated registrations during the native call.
	ref, err := v.c.registerGPU(ptr, size)
	if err == nil && ref == 0 {
		err = syscall.EIO
	}
	if err != nil {
		t.failed.Store(true)
		return err
	}

	// The engine may fail while the native registration blocks. Preserve a
	// successful native registration for teardown, but do not report it usable
	// or attempt another control operation to roll it back.
	reg := &registration{base: base, size: size, ref: ref}
	t.mu.Lock()
	err = v.deviceReadError()
	reg.closing = err != nil
	t.entries = append(t.entries, nil)
	copy(t.entries[i+1:], t.entries[i:])
	t.entries[i] = reg
	t.mu.Unlock()
	return err
}

// Unregister removes the registration whose base is exactly ptr. It returns
// ENOENT for an unknown base and EBUSY while a read owns the registration, a
// close is in progress, or the device path has failed. Closed Volumes return
// EBADF. A native failure is returned unchanged and fails the device path.
//
// A failed native unregister may already have removed the service's buffer ref.
// Keep that record closing until Volume teardown; never republish it to readers.
func (v *Volume) Unregister(ptr uintptr) error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.closed {
		return syscall.EBADF
	}

	t := &v.regs
	t.controlMu.Lock()
	defer t.controlMu.Unlock()

	base := uint64(ptr)
	t.mu.Lock()
	i := sort.Search(len(t.entries), func(i int) bool {
		return t.entries[i].base >= base
	})
	if i == len(t.entries) || t.entries[i].base != base {
		t.mu.Unlock()
		return syscall.ENOENT
	}
	reg := t.entries[i]
	if reg.closing || reg.inflight.Load() != 0 ||
		v.e.broken.Load() || t.failed.Load() {
		t.mu.Unlock()
		return syscall.EBUSY
	}

	// The check and transition share lookup's lock: either the reader pins
	// first, or it sees closing. No scalar DMA can slip between the two.
	reg.closing = true
	t.mu.Unlock()

	if err := v.c.unregister(reg.ref); err != nil {
		// Publish failure before releasing controlMu, so the next control
		// operation cannot reuse a connection with uncertain protocol state.
		t.failed.Store(true)
		return err
	}
	t.mu.Lock()
	copy(t.entries[i:], t.entries[i+1:])
	t.entries[len(t.entries)-1] = nil
	t.entries = t.entries[:len(t.entries)-1]
	t.mu.Unlock()
	return nil
}

// lookup pins one registration containing the entire requested address range.
// The caller must already hold Volume.mu.RLock and keep it until all workers
// using the result have finished. Do not acquire that lock recursively here.
// A successful call owns exactly one reference, released by releaseRegistration.
// Request length and file-offset alignment belong to the future ReadAt method;
// this helper checks registration-relative address alignment only.
func (v *Volume) lookup(ptr uintptr, size uint64) (*registration, uint64, error) {
	t := &v.regs
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := v.deviceReadError(); err != nil {
		return nil, 0, err
	}
	base, _, err := deviceRange(ptr, size)
	if err != nil {
		return nil, 0, err
	}
	i := sort.Search(len(t.entries), func(i int) bool {
		return t.entries[i].base > base
	}) - 1
	if i < 0 {
		return nil, 0, syscall.EINVAL
	}
	reg := t.entries[i]
	dstBase := base - reg.base

	// Subtraction avoids overflowing a request end. Adjacent registrations
	// cannot be combined because the DMA needs one specific buffer_ref.
	if reg.closing || dstBase >= reg.size || size > reg.size-dstBase ||
		dstBase%uint64(v.ioUnit) != 0 {
		return nil, 0, syscall.EINVAL
	}
	if reg.inflight.Load() == 1<<31-1 {
		return nil, 0, syscall.EOVERFLOW
	}
	reg.inflight.Add(1)
	return reg, dstBase, nil
}

// releaseRegistration ends one request's ownership. The caller must hold the
// lifecycle read lock and have joined every worker that could still submit or
// await DMA using reg. Cancellation alone is insufficient. Sticky failures
// deliberately retain references; Close drains the engine without waiting for
// these counters to reach zero.
func (v *Volume) releaseRegistration(reg *registration) {
	if !v.e.broken.Load() && !v.regs.failed.Load() {
		reg.inflight.Add(-1)
	}
}
