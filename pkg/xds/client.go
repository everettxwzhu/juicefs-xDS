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
	"time"
)

// client adapts the subset of xds/client.h used by this package.
//
// submit, poll and wait share mutable queue/completion state and must be
// called exclusively by the I/O engine. getBlob uses that same state and
// must run before the engine starts.
//
// allocBuffer, registerGPU and unregister use the C client's control mutex.
// metadata reads connection metadata without issuing a control RPC.
// close must not run concurrently with any other method.
type client interface {
	metadata() (clientMeta, error)

	// getBlob returns an opaque handle, not a dereferenceable memory address.
	// On ETIMEDOUT the owner must abort initialization and close the client:
	// the request may still acquire a blob after the caller stops waiting.
	getBlob() (uintptr, error)

	// allocBuffer returns a borrowed view of xDS-owned host memory.
	// The view becomes invalid when the client closes. It must not be reused
	// while submitted I/O still references ref.
	//
	// Do not hand this view to chunk readers and immediately recycle it:
	// timeout workers and asynchronous cache writes may retain their pages.
	allocBuffer(size uint64) (buf []byte, ref uint64, err error)

	// registerGPU runs on the caller's current OS thread. Go callers must
	// LockOSThread before activating the device context and keep it locked
	// through registration. This method does not take ownership of memory.
	// An error may follow a successful service operation with a lost reply;
	// the errno alone does not prove that service state is unchanged.
	registerGPU(ptr uintptr, size uint64) (ref uint64, err error)

	// unregister is for GPU registrations. The owner must ensure that no
	// scalar I/O still uses ref; xDS only explicitly tracks vector I/O here.
	// An error may occur after the service removed ref from its lookup table.
	// The owner must not make a failed unregister available to readers again.
	unregister(ref uint64) error

	// submit success means queued, not completed. ENOSPC is returned
	// unchanged so the engine can retry after polling completions.
	submit(op *ioOp) (id uint64, err error)

	// poll consumes at most len(out) completions. An empty poll is (0, nil).
	poll(out []result) (int, error)

	// wait returns ETIMEDOUT when this wait interval has no completion.
	// On success, result.status may still describe a failed I/O.
	wait(timeoutUs uint64) (result, error)

	// close releases the client and its host mappings. It is not a device
	// synchronization barrier or a strict upper bound on shutdown time.
	close()
}

type clientMeta struct {
	blobSize      uint64
	ioUnit        uint64
	capacity      uint64 // Capacity advertised for this rank, in bytes.
	gpuBufferSize uint64
}

// result describes an I/O completion, separately from poll/wait call errors.
type result struct {
	id        uint64
	status    int32
	completed uint64
}

// ioOp describes one scalar transfer. write=false means blob to buffer;
// write=true means buffer to blob. All offsets and n are in bytes.
// The caller must not modify or concurrently reuse this operation during do.
// After do returns the operation may be reused, but an EIO from a broken engine
// does not prove DMA completion: referenced blobs/buffers must remain protected
// until a successful drain or until teardown establishes that DMA has stopped.
type ioOp struct {
	write   bool
	blob    uintptr
	ref     uint64
	bufOff  uint64
	blobOff uint64
	n       uint64

	// ctx cancels admission and queued work only. nil preserves the original
	// non-cancellable behavior, including zero-slot initialization. Once native
	// submit starts, completion (or sticky engine failure) is the only ownership
	// barrier: cancellation must never let the caller recycle live DMA memory.
	ctx context.Context

	// do initializes these fields before enqueueing; run exclusively owns them
	// afterward. done is buffered with capacity one. run clears it before sending
	// the result and never accesses the operation after notification. do waits on
	// a local channel copy so immediate operation reuse is safe.
	done chan error

	// expire uses monotonic time and includes admission/queueing time. An
	// admission timeout has submitted nothing; expiry after enqueue breaks
	// the engine and quarantines any resources still referenced by DMA.
	expire time.Time
}
