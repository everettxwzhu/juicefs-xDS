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
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type submission struct {
	result
	write                bool
	blob                 uintptr
	ref, bufOff, blobOff uint64
}

// Hooks are configured before starting the engine. Queue state is owner-only;
// tests communicate through channels/atomics instead of reading engine maps.
type fakeClient struct {
	ready     chan result
	submitted chan submission
	expired   chan struct{}
	auto      bool
	waitOnly  bool
	reject    func(int64) error
	pollErr   error
	waitErr   error
	next      uint64
	sincePoll int64
	active    atomic.Int32
	overlap   atomic.Bool
	submits   atomic.Int64
	polls     atomic.Int64
	waits     atomic.Int64
	maxBurst  atomic.Int64
}

var _ client = (*fakeClient)(nil)

func newFakeClient() *fakeClient {
	return &fakeClient{
		ready: make(chan result, 8192), submitted: make(chan submission, 8192),
		expired: make(chan struct{}, 1),
	}
}

func (c *fakeClient) enter() func() {
	if c.active.Add(1) != 1 {
		c.overlap.Store(true)
	}
	return func() { c.active.Add(-1) }
}

func (c *fakeClient) metadata() (clientMeta, error) { panic("unexpected metadata") }
func (c *fakeClient) getBlob() (uintptr, error)     { panic("unexpected getBlob") }
func (c *fakeClient) allocBuffer(uint64) ([]byte, uint64, error) {
	panic("unexpected allocBuffer")
}
func (c *fakeClient) registerGPU(uintptr, uint64) (uint64, error) {
	panic("unexpected registerGPU")
}
func (c *fakeClient) unregister(uint64) error { panic("unexpected unregister") }
func (c *fakeClient) close()                  { panic("engine must not close client") }

func (c *fakeClient) submit(op *ioOp) (uint64, error) {
	defer c.enter()()
	attempt := c.submits.Add(1)
	c.next += 7 // ENOSPC consumes an ID, just like the native implementation.
	if c.reject != nil {
		if err := c.reject(attempt); err != nil {
			return 0, err
		}
	}
	c.sincePoll++
	if c.sincePoll > c.maxBurst.Load() {
		c.maxBurst.Store(c.sincePoll)
	}
	r := result{id: c.next, completed: op.n}
	select {
	case c.submitted <- submission{r, op.write, op.blob, op.ref, op.bufOff, op.blobOff}:
	default: // Benchmarks need no permanent history of every submission.
	}
	if c.auto {
		c.ready <- r
	}
	return r.id, nil
}

func (c *fakeClient) poll(out []result) (int, error) {
	defer c.enter()()
	c.polls.Add(1)
	c.sincePoll = 0
	if c.pollErr != nil || c.waitOnly {
		return 0, c.pollErr
	}
	for i := range out {
		select {
		case out[i] = <-c.ready:
		default:
			return i, nil
		}
	}
	return len(out), nil
}

func (c *fakeClient) wait(us uint64) (result, error) {
	defer c.enter()()
	c.waits.Add(1)
	if c.waitErr != nil {
		return result{}, c.waitErr
	}
	timer := time.NewTimer(time.Duration(us) * time.Microsecond)
	defer timer.Stop()
	select {
	case r := <-c.ready:
		return r, nil
	case <-timer.C:
		select {
		case c.expired <- struct{}{}:
		default:
		}
		return result{}, syscall.ETIMEDOUT
	}
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for engine")
		var zero T
		return zero
	}
}

func testEngine(t *testing.T, c *fakeClient, timeout time.Duration) *engine {
	t.Helper()
	e := newEngineWithTimeout(c, timeout)
	t.Cleanup(func() { _ = e.stop() })
	return e
}

func asyncDo(e *engine, op *ioOp) <-chan error {
	done := make(chan error, 1)
	go func() { done <- e.do(op) }()
	return done
}

func TestEngineConcurrent(t *testing.T) {
	c := newFakeClient()
	c.auto = true
	e := testEngine(t, c, time.Second)
	done := make(chan error, 512)
	for i := 0; i < cap(done); i++ {
		go func() { done <- e.do(&ioOp{n: 4096}) }()
	}
	for i := 0; i < cap(done); i++ {
		if err := receive(t, done); err != nil {
			t.Fatal(err)
		}
	}
	if c.overlap.Load() || c.polls.Load() == 0 || c.maxBurst.Load() > pollBurst {
		t.Fatalf("overlap=%v, polls=%d, max submit burst=%d",
			c.overlap.Load(), c.polls.Load(), c.maxBurst.Load())
	}
}

func TestEngineOutOfOrder(t *testing.T) {
	c := newFakeClient()
	e := testEngine(t, c, time.Second)
	done := make([]<-chan error, 32)
	for i := range done {
		done[i] = asyncDo(e, &ioOp{write: i%2 == 0, blob: 123, ref: uint64(i),
			bufOff: 4096, blobOff: 8192, n: 4096})
	}
	requests := make([]submission, len(done))
	for i := range requests {
		requests[i] = receive(t, c.submitted)
		r := requests[i]
		if r.blob != 123 || r.bufOff != 4096 || r.blobOff != 8192 ||
			r.write != (r.ref%2 == 0) || r.completed != 4096 {
			t.Fatalf("transfer fields changed: %+v", r)
		}
	}
	for i := len(requests) - 1; i >= 0; i-- {
		r := requests[i]
		c.ready <- r.result
		if err := receive(t, done[r.ref]); err != nil {
			t.Fatal(err)
		}
	}
}

func TestEngineENOSPC(t *testing.T) {
	for _, permanent := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry", true: "timeout"}[permanent], func(t *testing.T) {
			c := newFakeClient()
			c.auto = true
			c.reject = func(attempt int64) error {
				if permanent || attempt == 1 {
					return syscall.ENOSPC
				}
				return nil
			}
			e := testEngine(t, c, 30*time.Millisecond)
			err := receive(t, asyncDo(e, &ioOp{n: 4096}))
			if permanent {
				if err != syscall.EIO || !e.broken.Load() || c.waits.Load() != 0 {
					t.Fatalf("do=%v, broken=%v, waits=%d",
						err, e.broken.Load(), c.waits.Load())
				}
				if c.submits.Load() > 1000 {
					t.Fatalf("ENOSPC retried without backoff: %d attempts",
						c.submits.Load())
				}
			} else if err != nil || receive(t, c.submitted).id != 14 {
				t.Fatalf("retry failed: %v", err)
			}
		})
	}
}

func TestEngineWaitTimeout(t *testing.T) {
	c := newFakeClient()
	c.waitOnly = true
	e := testEngine(t, c, time.Second)
	done := asyncDo(e, &ioOp{n: 4096})
	r := receive(t, c.submitted)
	receive(t, c.expired)
	if e.broken.Load() {
		t.Fatal("a short wait timeout broke the engine")
	}
	c.ready <- r.result
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestEngineCompletionErrors(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int32
		completed uint64
		want      error
	}{
		{"short", 0, 2048, syscall.EIO},
		{"negative", -int32(syscall.ENODEV), 0, syscall.ENODEV},
		{"positive", 1, 4096, syscall.EIO},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newFakeClient()
			e := testEngine(t, c, time.Second)
			done := asyncDo(e, &ioOp{n: 4096})
			r := receive(t, c.submitted).result
			r.status, r.completed = tc.status, tc.completed
			c.ready <- r
			if err := receive(t, done); err != tc.want || !e.broken.Load() {
				t.Fatalf("do=%v, want=%v, broken=%v",
					err, tc.want, e.broken.Load())
			}
			if err := e.do(&ioOp{n: 4096}); err != syscall.EIO {
				t.Fatalf("do after failure: %v", err)
			}
			if err := e.stop(); err != nil {
				t.Fatalf("known completion should allow a confirmed drain: %v", err)
			}
		})
	}
}

func TestEnginePollWaitErrors(t *testing.T) {
	for _, poll := range []bool{false, true} {
		t.Run(map[bool]string{false: "wait", true: "poll"}[poll], func(t *testing.T) {
			c := newFakeClient()
			if poll {
				c.pollErr = syscall.ENODEV
			} else {
				c.waitErr = syscall.ENODEV
			}
			e := testEngine(t, c, 30*time.Millisecond)
			if err := receive(t, asyncDo(e, &ioOp{n: 4096})); err != syscall.EIO {
				t.Fatal(err)
			}
			if err := e.stop(); err != syscall.EIO || !e.broken.Load() {
				t.Fatalf("stop=%v, broken=%v", err, e.broken.Load())
			}
		})
	}
}

func TestEngineSubmitAndProtocolErrors(t *testing.T) {
	t.Run("submit", func(t *testing.T) {
		c := newFakeClient()
		c.reject = func(int64) error { return syscall.ENODEV }
		e := testEngine(t, c, time.Second)
		if err := e.do(&ioOp{n: 4096}); err != syscall.ENODEV || !e.broken.Load() {
			t.Fatalf("submit error=%v, broken=%v", err, e.broken.Load())
		}
		if err := e.stop(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("unknown completion", func(t *testing.T) {
		c := newFakeClient()
		e := testEngine(t, c, time.Second)
		done := asyncDo(e, &ioOp{n: 4096})
		r := receive(t, c.submitted).result
		c.ready <- result{id: r.id + 1}
		c.ready <- r
		if err := receive(t, done); err != syscall.EIO {
			t.Fatal(err)
		}
		if err := e.stop(); err != syscall.EIO {
			t.Fatalf("protocol uncertainty must survive an empty map: %v", err)
		}
	})
}

func TestEngineAdmissionTimeout(t *testing.T) {
	c := newFakeClient()
	c.auto = true
	e := testEngine(t, c, 30*time.Millisecond)
	// Occupy admission without creating any DMA. Timeout must affect only this
	// unadmitted request, and releasing capacity must restore normal operation.
	for i := 0; i < cap(e.credits); i++ {
		e.credits <- struct{}{}
	}
	if err := e.do(&ioOp{n: 4096}); err != syscall.EIO || e.broken.Load() {
		t.Fatalf("admission error=%v, broken=%v", err, e.broken.Load())
	}
	if c.submits.Load() != 0 {
		t.Fatal("unadmitted request was submitted")
	}
	for i := 0; i < cap(e.credits); i++ {
		<-e.credits
	}
	if err := e.do(&ioOp{n: 4096}); err != nil {
		t.Fatal(err)
	}
}

// engineWaitContext exposes when do starts considering cancellation without
// sleeping to guess whether a goroutine has reached its admission wait.
type engineWaitContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (c *engineWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

func TestEngineCancellationBeforeAdmission(t *testing.T) {
	for _, congested := range []bool{false, true} {
		t.Run(map[bool]string{false: "already cancelled", true: "waiting for credit"}[congested], func(t *testing.T) {
			c := newFakeClient()
			c.auto = true
			e := testEngine(t, c, time.Second)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if congested {
				for i := 0; i < cap(e.credits); i++ {
					e.credits <- struct{}{}
				}
				waitCtx := &engineWaitContext{Context: ctx, observed: make(chan struct{})}
				done := asyncDo(e, &ioOp{n: 4096, ctx: waitCtx})
				receive(t, waitCtx.observed)
				cancel()
				if err := receive(t, done); err != context.Canceled {
					t.Fatalf("cancelled admission = %v", err)
				}
				for i := 0; i < cap(e.credits); i++ {
					<-e.credits
				}
			} else {
				cancel()
				if err := e.do(&ioOp{n: 4096, ctx: ctx}); err != context.Canceled {
					t.Fatalf("already cancelled admission = %v", err)
				}
			}
			if c.submits.Load() != 0 || e.broken.Load() {
				t.Fatalf("cancelled admission submitted=%d broken=%v", c.submits.Load(), e.broken.Load())
			}
			if err := e.do(&ioOp{n: 4096}); err != nil {
				t.Fatalf("independent operation after cancellation = %v", err)
			}
		})
	}
}

func TestEngineCancellationBehindENOSPC(t *testing.T) {
	c := newFakeClient()
	c.auto = true
	first, retry := make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	var allow atomic.Bool
	c.reject = func(attempt int64) error {
		if attempt == 1 {
			close(first)
			<-release
		}
		if attempt == 2 {
			close(retry)
		}
		if !allow.Load() {
			return syscall.ENOSPC
		}
		return nil
	}
	e := testEngine(t, c, 2*time.Second)
	t.Cleanup(releaseOnce)
	head := asyncDo(e, &ioOp{n: 4096, ref: 1})
	receive(t, first)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tail := asyncDo(e, &ioOp{n: 4096, ref: 2, ctx: ctx})
	// The engine is blocked in the first submit. Wait for the second caller's
	// actual ingress send; the next loop must consume it before retrying head.
	deadline := time.Now().Add(time.Second)
	for len(e.in) != 1 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if len(e.in) != 1 {
		t.Fatal("tail did not reach ingress")
	}
	releaseOnce()
	receive(t, retry)
	cancel()
	if err := receive(t, tail); err != context.Canceled {
		t.Fatalf("cancelled tail behind a live ENOSPC head = %v", err)
	}
	if e.broken.Load() {
		t.Fatal("cancelling queued work broke the engine")
	}
	allow.Store(true)
	if err := receive(t, head); err != nil {
		t.Fatalf("independent head = %v", err)
	}
	if s := receive(t, c.submitted); s.ref != 1 {
		t.Fatalf("cancelled operation reached native submit: %+v", s)
	}
	select {
	case s := <-c.submitted:
		t.Fatalf("unexpected extra submission: %+v", s)
	default:
	}
}

func TestEngineCancellationAfterSubmissionWaitsForCompletion(t *testing.T) {
	c := newFakeClient()
	e := testEngine(t, c, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := asyncDo(e, &ioOp{n: 4096, ctx: ctx})
	s := receive(t, c.submitted)
	cancel()
	receive(t, c.expired)
	select {
	case err := <-done:
		t.Fatalf("submitted DMA returned before completion: %v", err)
	default:
	}
	c.ready <- s.result
	// do reports native completion. ReadAt separately reports the caller's
	// cancellation after joining its workers; the engine must not break here.
	if err := receive(t, done); err != nil || e.broken.Load() {
		t.Fatalf("submitted completion = %v, broken=%v", err, e.broken.Load())
	}
}

func TestEngineCancelledQueueDoesNotExpire(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	e := &engine{failed: make(chan struct{}), inflight: make(map[uint64]*ioOp)}
	done := make(chan error, 1)
	e.push(&ioOp{n: 4096, ctx: ctx, done: done, expire: time.Now().Add(-time.Second)})
	live := &ioOp{n: 4096, expire: time.Now().Add(time.Second)}
	e.push(live)
	e.checkExpired(time.Now())
	if err := receive(t, done); err != context.Canceled || e.broken.Load() {
		t.Fatalf("cancelled expired queue = %v, broken=%v", err, e.broken.Load())
	}
	if e.queued != 1 || e.pop() != live {
		t.Fatal("cancellation changed the live FIFO")
	}
}

func TestEngineLateCompletionAfterReuse(t *testing.T) {
	c := newFakeClient()
	e := testEngine(t, c, 30*time.Millisecond)
	op := &ioOp{n: 4096}
	done := asyncDo(e, op)
	r := receive(t, c.submitted).result
	if err := receive(t, done); err != syscall.EIO {
		t.Fatal(err)
	}
	other := newFakeClient()
	other.auto = true
	e2 := testEngine(t, other, time.Second)
	// A late completion must touch only the tombstone, even while another
	// engine owns and modifies the same operation. The race detector checks it.
	c.ready <- r
	for i := 0; i < 128; i++ {
		if err := e2.do(op); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.stop(); err != nil {
		t.Fatalf("late completion did not drain: %v", err)
	}
}

func TestEngineStopDrain(t *testing.T) {
	for _, complete := range []bool{false, true} {
		t.Run(map[bool]string{false: "timeout", true: "complete"}[complete], func(t *testing.T) {
			c := newFakeClient()
			e := testEngine(t, c, 50*time.Millisecond)
			done := asyncDo(e, &ioOp{n: 4096})
			r := receive(t, c.submitted).result
			stopped := make(chan error, 1)
			go func() { stopped <- e.stop() }()
			receive(t, e.stopping)
			if complete {
				c.ready <- r
			}
			want := error(syscall.EIO)
			if complete {
				want = nil
			}
			if err := receive(t, stopped); err != want {
				t.Fatalf("stop=%v, want=%v", err, want)
			}
			if err := receive(t, done); err != want {
				t.Fatalf("do=%v, want=%v", err, want)
			}
			if err := e.stop(); err != want {
				t.Fatalf("repeated stop: %v", err)
			}
		})
	}
}

func TestEngineStopFullQueue(t *testing.T) {
	c := newFakeClient()
	c.reject = func(int64) error { return syscall.ENOSPC }
	e := testEngine(t, c, 4*time.Second)
	done := make(chan error, 3*engineQueueSize)
	for i := 0; i < cap(done); i++ {
		go func() { done <- e.do(&ioOp{n: 4096}) }()
	}
	deadline := time.Now().Add(time.Second)
	for len(e.credits) < cap(e.credits) && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if len(e.credits) != cap(e.credits) {
		t.Fatal("admission credits did not fill")
	}
	stopped := make(chan error, 16)
	for i := 0; i < cap(stopped); i++ {
		go func() { stopped <- e.stop() }()
	}
	for i := 0; i < cap(stopped); i++ {
		if err := receive(t, stopped); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < cap(done); i++ {
		if err := receive(t, done); err != syscall.EBADF {
			t.Fatalf("do: %v", err)
		}
	}
}

func TestEngineIdleAndInvalid(t *testing.T) {
	c := newFakeClient()
	e := testEngine(t, c, time.Second)
	if e.do(nil) != syscall.EINVAL || e.do(&ioOp{}) != syscall.EINVAL {
		t.Fatal("invalid operation was accepted")
	}
	time.Sleep(5 * time.Millisecond) // Observation interval: idle must perform no C calls.
	if c.polls.Load() != 0 || c.waits.Load() != 0 {
		t.Fatal("idle engine polled")
	}
	if err := e.stop(); err != nil {
		t.Fatal(err)
	}
	if err := e.do(&ioOp{n: 4096}); err != syscall.EBADF {
		t.Fatalf("stopped do: %v", err)
	}
}

func BenchmarkEngineDo(b *testing.B) {
	c := newFakeClient()
	c.auto = true
	e := newEngine(c)
	defer e.stop()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		op := &ioOp{n: 4096}
		for pb.Next() {
			if err := e.do(op); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
