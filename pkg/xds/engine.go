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
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	engineQueueSize = 1024
	expireInterval  = time.Millisecond
)

// engine exclusively owns the client's scalar I/O data plane. All getBlob
// calls must finish successfully before construction; the owner must stop the
// engine before closing the client. The engine never closes the client itself.
type engine struct {
	c       client
	timeout time.Duration
	in      chan *ioOp
	credits chan struct{} // Bounds all admitted calls, including ingress queueing.

	// Never close in: a concurrent do may still select its send case.
	stopping chan struct{}
	failed   chan struct{}
	exited   chan struct{}
	stopOnce sync.Once
	broken   atomic.Bool

	// Only run accesses the fields below. Backlog is a bounded FIFO. Credits
	// ensure an ingress request always has room to enter this owned state.
	backlog  [engineQueueSize]*ioOp
	head     int
	queued   int
	inflight map[uint64]*ioOp
	results  [pollBurst]result

	// A nil inflight value retains an ID whose caller already received EIO.
	// uncertain records protocol/poll errors that prevent proving a clean drain.
	uncertain bool
	stopErr   error // Published by closing exited; read only after receiving it.
}

func newEngine(c client) *engine {
	return newEngineWithTimeout(c, ioTimeout)
}

// A private timeout avoids global-variable changes in concurrent tests.
func newEngineWithTimeout(c client, timeout time.Duration) *engine {
	e := &engine{
		c:        c,
		timeout:  timeout,
		in:       make(chan *ioOp, engineQueueSize),
		credits:  make(chan struct{}, engineQueueSize),
		stopping: make(chan struct{}),
		failed:   make(chan struct{}),
		exited:   make(chan struct{}),
		inflight: make(map[uint64]*ioOp, engineQueueSize),
	}
	go e.run()
	return e
}

// do transfers ownership of op until its result is published. Do not modify or
// concurrently reuse op during this call. Cancellation belongs above this layer:
// submitted DMA cannot be cancelled by returning from a Go select.
//
// EIO with broken=true may precede physical completion. The owner must quarantine
// affected buffers/slots/registrations until drain or teardown; do does not own
// those resources. An admission timeout has submitted nothing and is safe alone.
func (e *engine) do(op *ioOp) error {
	if op == nil || op.n == 0 {
		return syscall.EINVAL
	}
	if e.broken.Load() {
		return syscall.EIO
	}
	select {
	case <-e.stopping:
		return syscall.EBADF
	default:
	}

	expire := time.Now().Add(e.timeout)
	select {
	case e.credits <- struct{}{}:
	default:
		// Only congested admission allocates a timer. No DMA has been submitted
		// yet, so expiration here does not require breaking other requests.
		timer := time.NewTimer(time.Until(expire))
		select {
		case e.credits <- struct{}{}:
		case <-e.failed:
			timer.Stop()
			return syscall.EIO
		case <-e.stopping:
			timer.Stop()
			return e.rejected()
		case <-timer.C:
			return syscall.EIO
		}
		timer.Stop()
	}
	defer func() { <-e.credits }()

	done := make(chan error, 1)
	op.done, op.expire = done, expire
	select {
	case e.in <- op:
	case <-e.failed:
		return syscall.EIO
	case <-e.stopping:
		return e.rejected()
	}

	select {
	case err := <-done:
		return err
	case <-e.exited:
		// A send can win concurrently with stop, even after run has exited.
		// Such an op was never submitted. Prefer a result already published
		// by run so a real completion is not overwritten by EBADF/EIO.
		select {
		case err := <-done:
			return err
		default:
			return e.rejected()
		}
	}
}

// stop is concurrent-safe and idempotent. Nil means every successful submission
// was observed complete. EIO means DMA may remain outstanding: neither stopping
// the goroutine nor client.close is a device-memory synchronization barrier.
func (e *engine) stop() error {
	e.stopOnce.Do(func() { close(e.stopping) })
	<-e.exited
	return e.stopErr
}

func (e *engine) rejected() error {
	if e.broken.Load() {
		return syscall.EIO
	}
	return syscall.EBADF
}

// finish is called only by run. Its send is the last access to op: receiving
// the result allows the caller to immediately reuse or discard the operation.
func finish(op *ioOp, err error) {
	if op == nil || op.done == nil {
		return
	}
	done := op.done
	op.done = nil
	done <- err
}

func (e *engine) push(op *ioOp) {
	e.backlog[(e.head+e.queued)%len(e.backlog)] = op
	e.queued++
}

func (e *engine) pop() *ioOp {
	op := e.backlog[e.head]
	e.backlog[e.head] = nil
	e.head = (e.head + 1) % len(e.backlog)
	e.queued--
	return op
}

// fail publishes the sticky failure before waking any caller. IDs remain as
// tombstones until their real completions arrive; no old ioOp pointer is kept.
func (e *engine) fail() {
	if e.broken.Swap(true) {
		return
	}
	close(e.failed)
	for e.queued != 0 {
		finish(e.pop(), syscall.EIO)
	}
	for id, op := range e.inflight {
		e.inflight[id] = nil
		finish(op, syscall.EIO)
	}
}

func (e *engine) complete(r result) {
	op, ok := e.inflight[r.id]
	if !ok {
		e.uncertain = true
		e.fail()
		return
	}
	delete(e.inflight, r.id)
	if op == nil {
		return // The caller was notified on failure; this is physical completion.
	}

	var err error
	if r.status < 0 {
		err = syscall.Errno(-int64(r.status))
	} else if r.status != 0 || r.completed != op.n {
		err = syscall.EIO
	}
	if err != nil {
		e.fail()
	}
	finish(op, err)
}

func (e *engine) checkExpired(now time.Time) {
	for i := 0; i < e.queued; i++ {
		if !now.Before(e.backlog[(e.head+i)%len(e.backlog)].expire) {
			e.fail()
			return
		}
	}
	for _, op := range e.inflight {
		if op != nil && !now.Before(op.expire) {
			e.fail()
			return
		}
	}
}

// pause is used only when progress is impossible without retrying: ENOSPC with
// no local inflight request, or a failing poll/wait. Unlike native wait, this
// sleeps and is interruptible by stop. A closed stop channel is passed as nil
// once draining starts, avoiding a tight loop on an always-ready select case.
func pause(stop <-chan struct{}) {
	timer := time.NewTimer(pollWait)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-stop:
	}
}

func (e *engine) run() {
	runtime.LockOSThread()
	defer close(e.exited)
	defer runtime.UnlockOSThread()

	stop := (<-chan struct{})(e.stopping)
	var drainDeadline time.Time
	nextCheck := time.Now().Add(expireInterval)
	for {
		select {
		case <-stop:
			stop = nil
			drainDeadline = time.Now().Add(e.timeout)
			for e.queued != 0 {
				finish(e.pop(), e.rejected())
			}
		default:
		}
		draining := !drainDeadline.IsZero()
		if draining {
			if len(e.inflight) == 0 {
				if e.uncertain {
					e.stopErr = syscall.EIO
				}
				return
			}
			if !time.Now().Before(drainDeadline) {
				e.fail()
				e.stopErr = syscall.EIO
				return
			}
		}

		// Process at most a burst before returning to completion handling.
		progress := false
	receive:
		for range pollBurst {
			if !draining && !e.broken.Load() &&
				e.queued+len(e.inflight) == engineQueueSize {
				break
			}
			select {
			case op := <-e.in:
				progress = true
				if draining || e.broken.Load() {
					finish(op, e.rejected())
				} else {
					e.push(op)
				}
			default:
				break receive
			}
		}

	submit:
		for i := 0; !draining && !e.broken.Load() && e.queued != 0 && i < pollBurst; i++ {
			select {
			case <-e.stopping:
				break submit
			default:
			}
			op := e.backlog[e.head]
			if !time.Now().Before(op.expire) {
				e.fail()
				break
			}
			id, err := e.c.submit(op)
			if errors.Is(err, syscall.ENOSPC) {
				break // No request was published; retry the same FIFO head.
			}
			op = e.pop()
			progress = true
			if err != nil {
				e.fail()
				finish(op, err)
				break
			}
			if _, exists := e.inflight[id]; id == 0 || exists {
				e.uncertain = true // A submitted DMA cannot be tracked reliably.
				e.fail()
				finish(op, syscall.EIO)
				break
			}
			e.inflight[id] = op
		}

		if now := time.Now(); !now.Before(nextCheck) {
			if !draining && !e.broken.Load() {
				e.checkExpired(now)
			}
			nextCheck = now.Add(expireInterval)
		}
		if len(e.inflight) != 0 {
			n, err := e.c.poll(e.results[:])
			if err != nil || n < 0 || n > len(e.results) {
				e.uncertain = true
				e.fail()
				pause(stop)
				continue
			}
			for i := range n {
				e.complete(e.results[i])
			}
			if n == 0 {
				r, err := e.c.wait(uint64(pollWait / time.Microsecond))
				switch {
				case err == nil:
					e.complete(r)
				case errors.Is(err, syscall.ETIMEDOUT):
					// This short wait interval expired; the DMA is not cancelled.
				default:
					e.uncertain = true
					e.fail()
					pause(stop)
				}
			}
			continue
		}
		if progress {
			continue
		}
		if e.queued != 0 {
			pause(stop)
			continue
		}

		// Truly idle: do not enter native wait, which polls and sched_yield()s.
		select {
		case <-stop:
		case op := <-e.in:
			if e.broken.Load() {
				finish(op, syscall.EIO)
			} else {
				e.push(op)
			}
		}
	}
}
