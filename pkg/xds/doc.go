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

// Package xds provides in-process GPU reads for JuiceFS through an xDS service.
// A [Volume] owns one service rank, an I/O engine, device registrations, and a
// private volatile block cache. Aligned reads use direct DMA from cached blocks;
// misses fill the cache through the existing chunk store. Unaligned mappings
// are assembled in host staging buffers. This package exposes a Go API; it does
// not add FUSE GPU operations or C, Python, or Java SDK entry points.
//
// # Public API and lifecycle
//
// [Config] identifies the service and its exclusively owned Rank. WorldSize
// must be positive, identical among clients of that service, and no greater
// than the service's queue count; Rank must be less than WorldSize.
//
// [Open] constructs a Volume from Config and the same metadata engine, chunk
// store, and chunk configuration used to construct the application's
// fs.FileSystem. It leases the rank's blobs, allocates staging buffers, and
// initializes the zero slot before returning. The metadata engine and chunk
// store are borrowed: close the Volume before closing either dependency.
// Construct Volumes with Open and do not copy them after first use.
//
// [Volume.IOUnit] reports the service's byte alignment unit. This immutable
// value remains available after Close.
//
// [Volume.Register] registers a caller-owned device allocation for subsequent
// reads. [Volume.Unregister] removes a registration by its exact starting
// address. Register once and reuse the allocation across reads rather than
// registering on every request.
//
// [Volume.ReadAt] synchronously reads an open file into a registered device
// range. [Volume.Close] waits for admitted operations and background work,
// drains the engine, and releases the session's resources. Close is concurrent
// safe and idempotent; repeated calls return the original drain result.
//
// The normal sequence is Open, Register, ReadAt (repeated as needed),
// Unregister, then Close. Close also tears down remaining registrations, so
// individual Unregister calls are optional when closing the whole Volume.
// The application retains ownership of its device allocations throughout.
//
// # Examples
//
// The example attached to [Open] shows a complete single-read lifecycle:
// activate the device context on a locked thread, open the Volume, choose an
// aligned request size with IOUnit, register memory, open a readable file,
// synchronize, read, unregister, and close. It reports the operation error and
// Close error separately, so an ordinary read failure is not confused with a
// failure to drain DMA.
//
// The example attached to [Volume.ReadAt] reuses an existing registration for
// sequential batches. It waits for previous device work before overwriting the
// buffer, passes only valid bytes to an application callback, and stops after a
// positive EOF short read rather than continuing at an unaligned file offset.
//
// Both examples are in example_test.go and are compiled by go test. They have
// no Output directive and do not execute GPU operations during tests. The
// application supplies its existing file system, metadata engine, chunk store,
// device allocation, and context/synchronization callbacks. These callbacks
// belong to the application, not this package; no GPU allocation API is assumed.
// Neither example frees device memory. The caller must retain it if Close
// fails, and must also finish its own device work before releasing it.
//
// # Device memory and concurrency
//
// Register runs on the calling OS thread, which must already have the
// allocation's device context active. Go callers must use runtime.LockOSThread
// before activating that context and retain the thread lock through Register.
// Device pointers are opaque addresses; this package never dereferences them
// as Go memory. Registration ranges must not overlap, and their address/size
// alignment is checked by the native backend independently of IOUnit.
// Keep total registrations within the service's configured GPU buffer budget;
// the Go API does not enforce that budget itself.
//
// Before ReadAt, finish all device work touching the destination. Observe the
// backend's device visibility requirements; AMD requires caller-side HIP
// synchronization both before and after the read. Concurrent ReadAt calls must
// use disjoint destination ranges. Do not concurrently write, truncate, or
// close the same file handle. File length comes from the handle's cached Stat;
// reads do not establish a snapshot against other handles or clients.
//
// # Read contract
//
// ReadAt takes a meta.Context, an open readable regular fs.File from the same
// metadata instance, a device pointer, a positive byte count, and a nonnegative
// file offset. Open flags and the file's closed state are caller obligations;
// internal virtual files are unsupported. The offset, byte count, and device
// offset relative to the registration base must be multiples of IOUnit. The
// entire requested device range must fit one registration even if EOF shortens
// the actual read. The file's write buffer is flushed before planning.
//
// A successful read returns (n, nil), including a positive short read at EOF.
// Starting at or beyond EOF returns (0, io.EOF), following fs.File.Pread rather
// than the standard io.ReaderAt short-read convention. Holes read as zero. Bytes
// from n to the next IOUnit boundary are zeroed; subsequent device bytes remain
// untouched. Other failures return (0, errno), although some destination bytes
// may already have changed; discard that request's result.
//
// Cancellation skips operations whose native submission has not started and
// waits for already submitted DMA before returning EINTR. A concurrent native
// submission may already be committed. Sticky device failure is the exception
// to this completion guarantee, as described below.
//
// # Errors and failure recovery
//
// Errors are syscall.Errno values, except for ReadAt's io.EOF. Invalid arguments
// return EINVAL and unrepresentable ranges/layouts return EOVERFLOW. Register
// rejects overlapping ranges with EEXIST. Unregister returns ENOENT for an
// unknown base and EBUSY while reads own the registration or the device path
// has failed. Operations on a closed Volume return EBADF, except for IOUnit
// and repeated Close calls.
//
// ReadAt preserves metadata errno values and reports storage failures as EIO.
// Native Register and Unregister errors are returned unchanged and fail the
// device path, because an error does not prove service state was unchanged.
// An engine or registration-control failure makes subsequent reads return EIO;
// close and reopen the Volume to establish a new session. Failed reads retain
// registration ownership when physical DMA completion cannot be assumed.
//
// Keep device allocations alive until successful Unregister or successful
// Close. If Close returns EIO, outstanding DMA could not be ruled out: retain
// the allocation until the service has exited or another mechanism proves DMA
// has stopped. Native client teardown alone is not a device synchronization
// barrier. The engine drain timeout does not bound the entire Close call.
//
// # Build support
//
// The native client requires Linux, cgo, and the xds build tag. Other builds
// use a stub and return ENOTSUP from Open for otherwise valid arguments,
// without requiring the xDS submodule. The service must support GPU direct I/O
// and CPU buffer allocation.
//
// Build with make juicefs.xds to include external native sources and headers in
// cgo's cache key. Client changes are then rebuilt automatically without clearing
// the Go build cache or forcing all dependencies to rebuild.
package xds
