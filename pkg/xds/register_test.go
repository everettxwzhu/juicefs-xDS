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
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// GPU addresses are opaque numbers in these tests. Keeping registration state
// separate from volumeClient.buffers avoids racing the engine's CPU-buffer map.
type registrationClient struct {
	*volumeClient
	registerCalls   atomic.Int32
	unregisterCalls atomic.Int32
	onRegister      func(uintptr, uint64) (uint64, error)
	onUnregister    func(uint64) error
}

func (c *registrationClient) registerGPU(ptr uintptr, size uint64) (uint64, error) {
	n := c.registerCalls.Add(1)
	if c.onRegister != nil {
		return c.onRegister(ptr, size)
	}
	return uint64(1000 + n), nil
}

func (c *registrationClient) unregister(ref uint64) error {
	c.unregisterCalls.Add(1)
	if c.onUnregister != nil {
		return c.onUnregister(ref)
	}
	return nil
}

func registrationVolume(t *testing.T) (*Volume, *registrationClient) {
	t.Helper()
	c := &registrationClient{volumeClient: newVolumeClient()}
	v, err := openVolume(c, &volumeTestMeta{}, &volumeTestStore{}, 4096, 8192, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = v.Close() })
	return v, c
}

// These synthetic pins submit no DMA. Individual tests may retain one beyond
// this lock to exercise registry bookkeeping or Close's quarantine behavior.
func registrationLookup(v *Volume, ptr uintptr, size uint64) (*registration, uint64, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.lookup(ptr, size)
}

func TestRegisterIntervals(t *testing.T) {
	for _, tc := range []struct {
		name string
		ptr  uintptr
		size uint64
		want error
	}{
		{"before", 0x1c000, 0x4000, nil},
		{"after", 0x24000, 0x4000, nil},
		{"duplicate", 0x20000, 0x4000, syscall.EEXIST},
		{"inside", 0x21000, 0x1000, syscall.EEXIST},
		{"contains", 0x1f000, 0x6000, syscall.EEXIST},
		{"left overlap", 0x1f000, 0x2000, syscall.EEXIST},
		{"right overlap", 0x23000, 0x2000, syscall.EEXIST},
		{"null", 0, 0x1000, syscall.EINVAL},
		{"empty", 0x10000, 0, syscall.EINVAL},
		{"overflow", ^uintptr(0) - 4095, 8192, syscall.EOVERFLOW},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, c := registrationVolume(t)
			if err := v.Register(0x20000, 0x4000); err != nil {
				t.Fatal(err)
			}
			if err := v.Register(tc.ptr, tc.size); err != tc.want {
				t.Fatalf("Register: %v; want %v", err, tc.want)
			}
			wantCalls := int32(1)
			if tc.want == nil {
				wantCalls++
			}
			if got := c.registerCalls.Load(); got != wantCalls {
				t.Fatalf("native registrations=%d; want %d", got, wantCalls)
			}
		})
	}
}

func TestRegisterPassesBackendArguments(t *testing.T) {
	v, c := registrationVolume(t)

	// Registration geometry belongs to the backend. Only offsets within a
	// successful registration must obey the service's I/O alignment unit.
	c.onRegister = func(ptr uintptr, size uint64) (uint64, error) {
		if ptr != 0x10003 || size != 8193 {
			t.Errorf("register arguments: ptr=%x size=%d", ptr, size)
		}
		return 1<<40 + 7, nil
	}
	c.onUnregister = func(ref uint64) error {
		if ref != 1<<40+7 {
			t.Errorf("unregister ref=%d", ref)
		}
		return nil
	}
	if err := v.Register(0x10003, 8193); err != nil {
		t.Fatal(err)
	}
	reg, off, err := registrationLookup(v, 0x11003, 4096)
	if err != nil || off != 4096 {
		t.Fatalf("relative alignment: offset=%d err=%v", off, err)
	}
	v.releaseRegistration(reg)
	if err := v.Unregister(0x10003); err != nil {
		t.Fatal(err)
	}
}

func TestRegistrationLookup(t *testing.T) {
	v, _ := registrationVolume(t)
	for _, ptr := range []uintptr{0x20000, 0x10000, 0x30000} {
		if err := v.Register(ptr, 8192); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name string
		ptr  uintptr
		size uint64
		off  uint64
		want error
	}{
		{"base", 0x20000, 8192, 0, nil},
		{"last unit", 0x21000, 4096, 4096, nil},
		{"size alignment belongs to ReadAt", 0x21000, 1, 4096, nil},
		{"missing", 0x40000, 4096, 0, syscall.EINVAL},
		{"at end", 0x22000, 1, 0, syscall.EINVAL},
		{"past end", 0x21000, 4097, 0, syscall.EINVAL},
		{"across registrations", 0x20000, 0x12000, 0, syscall.EINVAL},
		{"unaligned offset", 0x20001, 4096, 0, syscall.EINVAL},
		{"null", 0, 4096, 0, syscall.EINVAL},
		{"empty", 0x20000, 0, 0, syscall.EINVAL},
		{"overflow", ^uintptr(0) - 4095, 8192, 0, syscall.EOVERFLOW},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg, off, err := registrationLookup(v, tc.ptr, tc.size)
			if err != tc.want {
				t.Fatalf("lookup: %v; want %v", err, tc.want)
			}
			if err == nil {
				if off != tc.off || reg.inflight.Load() != 1 {
					t.Fatalf("offset=%d inflight=%d", off, reg.inflight.Load())
				}
				v.releaseRegistration(reg)
				if reg.inflight.Load() != 0 {
					t.Fatal("completed read retained its pin")
				}
			} else if reg != nil {
				t.Fatal("failed lookup returned a registration")
			}
		})
	}
}

func TestRegistrationPointerAndInflight(t *testing.T) {
	v, c := registrationVolume(t)
	if err := v.Register(0x10000, 4096); err != nil {
		t.Fatal(err)
	}
	reg, _, err := registrationLookup(v, 0x10000, 4096)
	if err != nil {
		t.Fatal(err)
	}

	// Insert before the pinned entry and force several slice reallocations.
	for i := 0; i < 40; i++ {
		ptr := uintptr(0x20000 + i*8192)
		if i == 0 {
			ptr = 0x8000
		}
		if err := v.Register(ptr, 4096); err != nil {
			t.Fatal(err)
		}
	}
	if err := v.Unregister(0x8000); err != nil {
		t.Fatal(err)
	}
	again, _, err := registrationLookup(v, 0x10000, 4096)
	if err != nil || again != reg || reg.inflight.Load() != 2 {
		t.Fatalf("registration moved: same=%v err=%v", again == reg, err)
	}
	if err := v.Unregister(0x10001); err != syscall.ENOENT {
		t.Fatalf("interior pointer: %v", err)
	}
	if err := v.Unregister(0x10000); err != syscall.EBUSY || c.unregisterCalls.Load() != 1 {
		t.Fatalf("pinned unregister: %v, calls=%d", err, c.unregisterCalls.Load())
	}
	v.releaseRegistration(again)
	v.releaseRegistration(reg)
	if err := v.Unregister(0x10000); err != nil {
		t.Fatal(err)
	}
	if err := v.Unregister(0x10000); err != syscall.ENOENT {
		t.Fatalf("already removed: %v", err)
	}
}

func TestRegisterConcurrentOverlap(t *testing.T) {
	v, c := registrationVolume(t)
	start, done := make(chan struct{}), make(chan error, 16)
	for range cap(done) {
		go func() {
			<-start
			done <- v.Register(0x10000, 4096)
		}()
	}
	close(start)

	successes := 0
	for range cap(done) {
		if err := receive(t, done); err == nil {
			successes++
		} else if err != syscall.EEXIST {
			t.Fatal(err)
		}
	}
	if successes != 1 || c.registerCalls.Load() != 1 {
		t.Fatalf("successes=%d native calls=%d", successes, c.registerCalls.Load())
	}
}

func TestUnregisterClosingAllowsOtherLookups(t *testing.T) {
	v, c := registrationVolume(t)
	for _, ptr := range []uintptr{0x10000, 0x20000} {
		if err := v.Register(ptr, 4096); err != nil {
			t.Fatal(err)
		}
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	c.onUnregister = func(ref uint64) error {
		if ref == 1001 {
			close(entered)
			<-release
		}
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- v.Unregister(0x10000) }()
	receive(t, entered)

	checked := make(chan error, 1)
	go func() {
		if _, _, err := registrationLookup(v, 0x10000, 4096); err != syscall.EINVAL {
			checked <- fmt.Errorf("closing lookup=%v; want EINVAL", err)
			return
		}
		reg, _, err := registrationLookup(v, 0x20000, 4096)
		if err == nil {
			v.releaseRegistration(reg)
		}
		checked <- err
	}()
	if err := receive(t, checked); err != nil {
		t.Fatalf("lookup while native unregister is blocked: %v", err)
	}
	unblock()
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestRegistrationNativeFailureQuarantines(t *testing.T) {
	for _, operation := range []string{
		"register error",
		"zero ref",
		"unregister error",
	} {
		t.Run(operation, func(t *testing.T) {
			v, c := registrationVolume(t)
			if err := v.Register(0x10000, 4096); err != nil {
				t.Fatal(err)
			}
			reg, _, err := registrationLookup(v, 0x10000, 4096)
			if err != nil {
				t.Fatal(err)
			}
			want := error(syscall.EACCES)
			if operation == "unregister error" {
				v.releaseRegistration(reg)
				c.onUnregister = func(uint64) error {
					return syscall.EACCES
				}
				err = v.Unregister(0x10000)
				v.regs.mu.Lock()
				closing := reg.closing
				v.regs.mu.Unlock()
				if !closing {
					t.Fatal("uncertain unregister restored the registration")
				}
			} else {
				c.onRegister = func(uintptr, uint64) (uint64, error) {
					return 0, syscall.EACCES
				}
				if operation == "zero ref" {
					want = syscall.EIO
					c.onRegister = func(uintptr, uint64) (uint64, error) {
						return 0, nil
					}
				}
				err = v.Register(0x20000, 4096)
				v.releaseRegistration(reg)
				if reg.inflight.Load() != 1 {
					t.Fatal("failed volume released a potentially active registration")
				}
			}
			if err != want || !v.regs.failed.Load() {
				t.Fatalf("native failure=%v failed=%v", err, v.regs.failed.Load())
			}
			calls := c.registerCalls.Load()
			if err := v.Register(0x30000, 4096); err != syscall.EIO ||
				c.registerCalls.Load() != calls {
				t.Fatalf("register after failure: %v", err)
			}
			if _, _, err := registrationLookup(v, 0x10000, 4096); err != syscall.EIO {
				t.Fatalf("lookup after failure: %v", err)
			}
			if err := v.Unregister(0x10000); err != syscall.EBUSY {
				t.Fatalf("unregister after failure: %v", err)
			}
			unregisterCalls := c.unregisterCalls.Load()
			if err := v.Close(); err != syscall.EIO {
				t.Fatalf("uncertain control state must prevent confirmed revocation: %v", err)
			}
			if c.unregisterCalls.Load() != unregisterCalls {
				t.Fatal("Close reused a failed control connection")
			}
		})
	}
}

func TestRegistrationEngineFailureKeepsPin(t *testing.T) {
	v, c := registrationVolume(t)
	if err := v.Register(0x10000, 4096); err != nil {
		t.Fatal(err)
	}
	reg, _, err := registrationLookup(v, 0x10000, 4096)
	if err != nil {
		t.Fatal(err)
	}

	// Initialization has finished. The next engine submission reads this hook
	// only after receiving the operation, which synchronizes the assignment.
	c.reject = func(int64) error { return syscall.ENODEV }
	if err := v.e.do(&ioOp{n: 4096}); err != syscall.ENODEV || !v.e.broken.Load() {
		t.Fatalf("engine error=%v broken=%v", err, v.e.broken.Load())
	}
	v.releaseRegistration(reg)
	if reg.inflight.Load() != 1 {
		t.Fatal("broken engine released a quarantined pin")
	}
	if err := v.Unregister(0x10000); err != syscall.EBUSY ||
		c.unregisterCalls.Load() != 0 {
		t.Fatalf("unregister with broken engine: %v", err)
	}
	if _, _, err := registrationLookup(v, 0x10000, 4096); err != syscall.EIO {
		t.Fatalf("lookup with broken engine: %v", err)
	}
	// The failing submission published no DMA. stop can still prove a clean
	// drain, so Close must revoke the device despite the retained read counter
	// and sticky engine state which make public Unregister reject this record.
	if err := v.Close(); err != nil || c.unregisterCalls.Load() != 1 {
		t.Fatalf("Close after confirmed broken-engine drain = %v, unregister calls=%d", err, c.unregisterCalls.Load())
	}
	if len(v.regs.entries) != 0 {
		t.Fatal("Close retained an acknowledged device registration")
	}
}

func TestRegistrationCloseSynchronouslyRevokes(t *testing.T) {
	v, c := registrationVolume(t)
	for _, ptr := range []uintptr{0x30000, 0x10000, 0x20000} {
		if err := v.Register(ptr, 4096); err != nil {
			t.Fatal(err)
		}
	}
	// An explicitly removed record must not be revoked a second time by Close.
	if err := v.Unregister(0x20000); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	events := make(chan string, 3)
	c.onUnregister = func(ref uint64) error {
		select {
		case <-v.e.exited:
		default:
			t.Error("device unregister preceded engine drain")
		}
		events <- fmt.Sprintf("unregister %d", ref)
		if ref == 1002 {
			close(entered)
			<-release
		}
		return nil
	}
	c.onClose = func() { events <- "close" }
	done := make(chan error, 1)
	go func() { done <- v.Close() }()
	receive(t, entered)
	select {
	case err := <-done:
		t.Fatalf("Close returned before synchronous revocation: %v", err)
	default:
	}
	if c.closes.Load() != 0 {
		t.Fatal("native client closed before device revocation acknowledgement")
	}
	unblock()
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"unregister 1002", "unregister 1001", "close"} {
		if got := receive(t, events); got != want {
			t.Fatalf("teardown event=%q; want %q", got, want)
		}
	}
	if len(v.regs.entries) != 0 || c.unregisterCalls.Load() != 3 || c.closes.Load() != 1 {
		t.Fatalf("remaining=%d unregisters=%d closes=%d", len(v.regs.entries), c.unregisterCalls.Load(), c.closes.Load())
	}
	if err := v.Close(); err != nil || c.unregisterCalls.Load() != 3 || c.closes.Load() != 1 {
		t.Fatalf("repeated Close=%v unregisters=%d closes=%d", err, c.unregisterCalls.Load(), c.closes.Load())
	}
}

func TestRegistrationCloseRevocationFailure(t *testing.T) {
	v, c := registrationVolume(t)
	for _, ptr := range []uintptr{0x10000, 0x20000, 0x30000} {
		if err := v.Register(ptr, 4096); err != nil {
			t.Fatal(err)
		}
	}
	var revoked []uint64
	c.onUnregister = func(ref uint64) error {
		revoked = append(revoked, ref)
		if ref == 1002 {
			return syscall.EACCES
		}
		return nil
	}
	if err := v.Close(); err != syscall.EIO {
		t.Fatalf("uncertain device revocation = %v", err)
	}
	if len(revoked) != 2 || revoked[0] != 1001 || revoked[1] != 1002 {
		t.Fatalf("RPCs continued after a failed revocation: %v", revoked)
	}
	if !v.regs.failed.Load() || len(v.regs.entries) != 2 || v.regs.entries[0].ref != 1002 {
		t.Fatal("acknowledged record was retained or uncertain records were discarded")
	}
	if err := v.Close(); err != syscall.EIO || c.unregisterCalls.Load() != 2 || c.closes.Load() != 1 {
		t.Fatalf("repeated Close=%v unregisters=%d closes=%d", err, c.unregisterCalls.Load(), c.closes.Load())
	}
}

func TestRegistrationCloseAfterControlFailure(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty registry", true: "existing registration"}[existing], func(t *testing.T) {
			v, c := registrationVolume(t)
			if existing {
				if err := v.Register(0x10000, 4096); err != nil {
					t.Fatal(err)
				}
			}
			// A lost native reply may hide a successful service registration.
			// Even an empty local table must not turn socket close into proof
			// that the allocation is no longer accessible by the service.
			c.onRegister = func(uintptr, uint64) (uint64, error) {
				return 0, syscall.ETIMEDOUT
			}
			if err := v.Register(0x20000, 4096); err != syscall.ETIMEDOUT {
				t.Fatal(err)
			}
			if err := v.Close(); err != syscall.EIO || c.unregisterCalls.Load() != 0 || c.closes.Load() != 1 {
				t.Fatalf("Close after failed control=%v unregisters=%d closes=%d", err, c.unregisterCalls.Load(), c.closes.Load())
			}
			if err := v.Close(); err != syscall.EIO || c.closes.Load() != 1 {
				t.Fatalf("repeated Close after failed control=%v closes=%d", err, c.closes.Load())
			}
		})
	}
}

func TestRegistrationCloseAfterUncertainDrain(t *testing.T) {
	v, c := registrationVolume(t)
	if err := v.Register(0x10000, 4096); err != nil {
		t.Fatal(err)
	}
	// Replace only the idle engine to shorten this fault-injection deadline.
	// No tier reads or prefetches are active in this registry-only fixture.
	if err := v.e.stop(); err != nil {
		t.Fatal(err)
	}
	for len(c.submitted) != 0 {
		<-c.submitted
	}
	c.complete = false
	v.e = newEngineWithTimeout(c, 30*time.Millisecond)
	done := asyncDo(v.e, &ioOp{blob: 101, ref: 201, n: 4096})
	_ = receive(t, c.submitted)
	if err := v.Close(); err != syscall.EIO {
		t.Fatalf("Close with unconfirmed DMA = %v", err)
	}
	if err := receive(t, done); err != syscall.EIO {
		t.Fatalf("incomplete DMA = %v", err)
	}
	if c.unregisterCalls.Load() != 0 || len(v.regs.entries) != 1 || c.closes.Load() != 1 {
		t.Fatalf("unregisters=%d remaining=%d closes=%d", c.unregisterCalls.Load(), len(v.regs.entries), c.closes.Load())
	}
}

func TestRegistrationCloseWaitsForNative(t *testing.T) {
	for _, unregister := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "register", true: "unregister"}[unregister],
			func(t *testing.T) {
				v, c := registrationVolume(t)
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				defer unblock()
				if unregister {
					if err := v.Register(0x10000, 4096); err != nil {
						t.Fatal(err)
					}
					c.onUnregister = func(uint64) error {
						close(entered)
						<-release
						return nil
					}
				} else {
					c.onRegister = func(uintptr, uint64) (uint64, error) {
						close(entered)
						<-release
						return 1001, nil
					}
				}
				op := make(chan error, 1)
				go func() {
					if unregister {
						op <- v.Unregister(0x10000)
					} else {
						op <- v.Register(0x10000, 4096)
					}
				}()
				receive(t, entered)

				closed := make(chan error, 1)
				go func() { closed <- v.Close() }()
				deadline := time.After(5 * time.Second)
				for v.mu.TryRLock() {
					v.mu.RUnlock()
					select {
					case <-closed:
						t.Fatal("Close returned while native control call was active")
					case <-deadline:
						t.Fatal("Close did not request the lifecycle write lock")
					default:
						runtime.Gosched()
					}
				}
				if c.closes.Load() != 0 {
					t.Fatal("client closed while native control call was active")
				}
				unblock()
				if err := receive(t, op); err != nil {
					t.Fatal(err)
				}
				if err := receive(t, closed); err != nil {
					t.Fatal(err)
				}
				if err := v.Register(0x30000, 4096); err != syscall.EBADF {
					t.Fatalf("Register after Close: %v", err)
				}
				if err := v.Unregister(0x10000); err != syscall.EBADF {
					t.Fatalf("Unregister after Close: %v", err)
				}
				if _, _, err := registrationLookup(v, 0x10000, 4096); err != syscall.EBADF {
					t.Fatalf("lookup after Close: %v", err)
				}
			},
		)
	}
}

func TestRegisterEngineFailureDuringNative(t *testing.T) {
	v, c := registrationVolume(t)
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	c.onRegister = func(uintptr, uint64) (uint64, error) {
		close(entered)
		<-release
		return 1001, nil
	}
	done := make(chan error, 1)
	go func() { done <- v.Register(0x10000, 4096) }()
	receive(t, entered)

	// Fail the real engine while registration is blocked. No GPU I/O or
	// direct mutation of engine-owned state is needed to exercise this race.
	c.reject = func(int64) error { return syscall.ENODEV }
	if err := v.e.do(&ioOp{n: 4096}); err != syscall.ENODEV {
		t.Fatal(err)
	}
	unblock()
	if err := receive(t, done); err != syscall.EIO {
		t.Fatalf("registration completed after engine failure: %v", err)
	}
	v.regs.mu.Lock()
	retained := len(v.regs.entries) == 1 && v.regs.entries[0].closing
	v.regs.mu.Unlock()
	if !retained || c.unregisterCalls.Load() != 0 {
		t.Fatal("successful native registration must be retained for Close")
	}
}

func TestRegistrationLookupLimits(t *testing.T) {
	v, _ := registrationVolume(t)
	for _, ptr := range []uintptr{0x10000, 0x11000} {
		if err := v.Register(ptr, 4096); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := registrationLookup(v, 0x10000, 8192); err != syscall.EINVAL {
		t.Fatalf("request spans adjacent registrations: %v", err)
	}
	v.regs.mu.Lock()
	reg := v.regs.entries[0]
	reg.inflight.Store(1<<31 - 1)
	v.regs.mu.Unlock()
	if _, _, err := registrationLookup(v, 0x10000, 4096); err != syscall.EOVERFLOW {
		t.Fatalf("reference counter overflow: %v", err)
	}

	// This synthetic count owns no DMA, so it can be reset for cleanup.
	reg.inflight.Store(0)
}

func BenchmarkRegistrationLookup(b *testing.B) {
	for _, count := range []int{1, 32, 1024} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			// Measure admission and release only; GPU allocation and native
			// control RPCs are outside the per-read path.
			v := &Volume{e: &engine{}, ioUnit: 4096}
			v.regs.entries = make([]*registration, count)
			for i := range v.regs.entries {
				v.regs.entries[i] = &registration{
					base: uint64(0x10000 + i*8192),
					size: 4096,
					ref:  uint64(i + 1),
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					v.mu.RLock()
					reg, _, err := v.lookup(uintptr(0x10000+i*8192), 4096)
					if err == nil {
						v.releaseRegistration(reg)
					}
					v.mu.RUnlock()
					if err != nil {
						b.Error(err)
						return
					}
					i = (i + 1) % count
				}
			})
		})
	}
}
