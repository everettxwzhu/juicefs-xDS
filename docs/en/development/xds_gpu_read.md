---
title: xDS GPU reads
slug: /development/xds-gpu-read
---

`pkg/xds` provides an in-process Go API for reading JuiceFS files into registered device memory. It requires Linux, cgo, the `xds` build tag, and an xDS service with both GPU direct I/O and CPU buffer allocation capabilities. Build with `make juicefs.xds`. The default build returns `ENOTSUP` from `xds.Open`.

This API does not add FUSE GPU operations or C, Python, or Java SDK entry points. Writes continue to use the existing host-memory path.

## Ownership and setup

Pass the same metadata engine, chunk store, and chunk configuration used by `fs.NewFileSystem` to `xds.Open`:

```go
v, err := xds.Open(xds.Config{
	Service: "xds-svc", Rank: rank, WorldSize: worldSize,
}, m, store, conf.Chunk)
```

Check every returned error. Each Volume exclusively owns a service rank and a private volatile block cache. All clients of one service must use the same world size. The metadata engine and chunk store remain owned by the application; close the Volume before closing either dependency.

The service blob size must be divisible by the volume block size, which must be divisible by the service I/O unit. Open reserves one zero slot and `S` scratch slots, allocates `S` host staging buffers, and requires at least `S` cache slots, where `S = min(32, 128 MiB / BlockSize)`. Provision hugepages and rank capacity accordingly.

Register device memory on an OS thread that has the allocation's device context active. Go callers must lock that thread with `runtime.LockOSThread` before selecting the context and keep it locked through `v.Register(ptr, capacity)`. Keep the allocation alive until successful `Unregister` or successful `Close`.

## Read contract

```go
n, err := v.ReadAt(ctx, file, devicePointer, size, offset)
```

- `file` must be an open, readable regular file belonging to the same metadata instance. Open flags and closed state are caller obligations; internal virtual files are unsupported.
- `size` must be positive. File offset, size, and device offset relative to the registration base must be multiples of `v.IOUnit()`.
- The full requested device range must fit within one registration, including when EOF shortens the read. Concurrent device destination ranges must not overlap.
- The call flushes the file's write buffer and uses its cached `Stat` length, following `fs.File.Pread`. Do not concurrently write, truncate, or close the same file handle. Reads do not establish a snapshot against changes through other handles or clients.
- A positive EOF short read returns `(n, nil)`; starting at or beyond EOF returns `(0, io.EOF)`. Holes read as zero. The bytes between `n` and its next I/O-unit boundary are zeroed; subsequent device bytes are untouched.
- Other errors return `(0, errno)`. Some destination windows may already have changed; discard the result of that request. Metadata errno values are preserved, storage failures return `EIO`, and cancellation returns `EINTR`.

Complete all device work touching the destination before calling ReadAt. Follow the xDS backend's visibility requirements; AMD callers must synchronize HIP both before and after the call.

## Planning and execution

Planning reads each covered chunk's resolved metadata mappings and splits them at both source block boundaries and request-relative block-sized windows. Aligned pieces read directly from cached xDS slots into device memory. Cache misses fetch a full immutable block through the existing chunk reader before filling the slot. A window containing unaligned mappings is assembled in a host stager, written to its scratch slot, then read into the device.

EOF padding can use a cached block's zero tail only when the file range ends at that block's effective source-data end. If truncation hides bytes inside a longer object, the window must instead be assembled and its tail explicitly zeroed.

Foreground scheduling uses at most 32 workers per request, independently of staging-buffer count. Cache fill is coalesced per block, cache slots stay pinned through DMA, and successful requests opportunistically prefetch subsequent blocks of each touched slice.

## Cancellation and failures

Cancellation skips operations that have not started native submission. A concurrent submission may already be committed; submitted DMA must complete before a normal cancellation return. Cancellation is not a device-memory synchronization barrier by itself.

An engine or registration-control failure makes the device path unusable. ReadAt returns `EIO`, and affected registrations and DMA resources remain protected until teardown. Unregister returns `EBUSY` while a read owns the registration or the device path has failed.

Close waits for foreground and background work, drains the engine, and closes the client. A nil result permits freeing device memory. If Close returns `EIO`, outstanding DMA could not be ruled out: retain device memory until the service has exited or another mechanism proves DMA has stopped. The engine drain timeout bounds draining, not the entire Close call. Reopen a Volume to establish a new session after failure.
