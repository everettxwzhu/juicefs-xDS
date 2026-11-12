//go:build xds && linux && cgo

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

/*
#cgo CFLAGS: -std=gnu11 -D_GNU_SOURCE -DXDS_ENABLE_HOTPATH_PROFILING=0
#cgo CFLAGS: -I${SRCDIR}/../../xDS/include
#cgo CFLAGS: -I${SRCDIR}/../../xDS/src/client
#cgo CFLAGS: -I${SRCDIR}/../../xDS/kernels/user/include
#cgo CFLAGS: -I${SRCDIR}/../../xDS/kernels/include
#cgo LDFLAGS: -ldl -lpthread

#include <stdlib.h>
#include "xds/client.h"
#include "xds/control_protocol.h"

// A device address is external memory, not a Go host-memory pointer.
// Keep the integer-to-pointer conversion on the C side.
static inline int
jfs_xds_register_gpu(xds_client_t *client, uintptr_t addr,
                     uint64_t size, uint64_t *ref)
{
	return xds_client_register_gpu_buffer(client, (void *)addr, size, ref);
}
*/
import "C"

import (
	"strings"
	"syscall"
	"time"
	"unsafe"
)

type cclient struct {
	ptr *C.xds_client_t

	// The I/O engine exclusively owns these output fields. Reusing them
	// avoids allocating output storage on each submit/poll/wait cgo call.
	submitID    C.uint64_t
	waitResult  C.struct_xds_client_result
	pollResults [C.XDS_CLIENT_MAX_POLL_BURST]C.struct_xds_client_result
}

var _ client = (*cclient)(nil)

func newClient(conf Config) (client, error) {
	if conf.Service == "" || strings.IndexByte(conf.Service, 0) >= 0 ||
		conf.WorldSize == 0 || conf.Rank >= conf.WorldSize {
		return nil, syscall.EINVAL
	}

	name := C.CString(conf.Service)
	defer C.free(unsafe.Pointer(name))

	options := C.struct_xds_client_options{
		service_name: name,
		rank_id:      C.uint32_t(conf.Rank),
		world_size:   C.uint32_t(conf.WorldSize),
		required_capabilities: C.uint64_t(
			C.XDS_CTRL_CAP_GPU_DIRECT | C.XDS_CTRL_CAP_CPU_BUFFER_ALLOC),
	}
	c := &cclient{}
	// connect_ex uses options synchronously and rolls back its own partial
	// resources on failure. It does not retain name or the options pointer.
	if err := clientError(C.xds_client_connect_ex(&options, &c.ptr)); err != nil {
		return nil, err
	}
	return c, nil
}

// clientError converts xDS's negative return codes, not thread-local errno.
// Returning nil explicitly avoids a non-nil error containing Errno(0).
func clientError(rc C.int) error {
	if rc < 0 {
		return syscall.Errno(-int64(rc))
	}
	return nil
}

func (c *cclient) metadata() (clientMeta, error) {
	var m C.struct_xds_client_metadata
	if err := clientError(C.xds_client_get_metadata(c.ptr, &m)); err != nil {
		return clientMeta{}, err
	}
	return clientMeta{
		blobSize:      uint64(m.blob_size_bytes),
		ioUnit:        uint64(m.io_unit_size),
		capacity:      uint64(m.capacity_bytes),
		gpuBufferSize: uint64(m.gpu_buffer_size_bytes),
	}, nil
}

func (c *cclient) getBlob() (uintptr, error) {
	var handle C.uintptr_t
	rc := C.xds_client_get_blob(
		c.ptr,
		C.uint64_t(blobAcquireTimeout/time.Microsecond),
		&handle,
	)
	if err := clientError(rc); err != nil {
		// ETIMEDOUT does not cancel the submitted acquisition. The owner
		// must close this client rather than accept partial initialization.
		return 0, err
	}
	return uintptr(handle), nil
}

func (c *cclient) allocBuffer(size uint64) ([]byte, uint64, error) {
	if size == 0 {
		return nil, 0, syscall.EINVAL
	}
	// Validate before allocation: unsafe.Slice takes an int length.
	if size > uint64(^uint(0)>>1) {
		return nil, 0, syscall.EOVERFLOW
	}

	var addr unsafe.Pointer
	var ref C.uint64_t
	rc := C.xds_client_alloc_buffer(c.ptr, C.uint64_t(size), &addr, &ref)
	if err := clientError(rc); err != nil {
		return nil, 0, err
	}

	// This is only a view. xDS owns the mapping and destroys it on close.
	// Go must neither free it nor access it after the client has closed.
	return unsafe.Slice((*byte)(addr), int(size)), uint64(ref), nil
}

func (c *cclient) registerGPU(ptr uintptr, size uint64) (uint64, error) {
	var ref C.uint64_t
	// Do not move registration to a goroutine: the caller must keep the
	// appropriate CUDA/HIP context active on this OS thread.
	rc := C.jfs_xds_register_gpu(
		c.ptr, C.uintptr_t(ptr), C.uint64_t(size), &ref,
	)
	if err := clientError(rc); err != nil {
		return 0, err
	}
	return uint64(ref), nil
}

func (c *cclient) unregister(ref uint64) error {
	return clientError(C.xds_client_unregister_buffer(c.ptr, C.uint64_t(ref)))
}

func (c *cclient) submit(op *ioOp) (uint64, error) {
	if op == nil {
		return 0, syscall.EINVAL
	}

	var rc C.int
	// Pass only scalars. ioOp also contains Go-managed fields and must not
	// be passed to C or retained there.
	if op.write {
		rc = C.xds_client_submit_write(
			c.ptr, C.uintptr_t(op.blob), C.uint64_t(op.ref),
			C.uint64_t(op.bufOff), C.uint64_t(op.blobOff),
			C.uint64_t(op.n), &c.submitID,
		)
	} else {
		rc = C.xds_client_submit_read(
			c.ptr, C.uintptr_t(op.blob), C.uint64_t(op.ref),
			C.uint64_t(op.bufOff), C.uint64_t(op.blobOff),
			C.uint64_t(op.n), &c.submitID,
		)
	}
	if err := clientError(rc); err != nil {
		// submitID may contain an earlier request ID after failure.
		return 0, err
	}
	return uint64(c.submitID), nil
}

func (c *cclient) poll(out []result) (int, error) {
	limit := min(len(out), len(c.pollResults))
	if limit == 0 {
		return 0, nil
	}

	rc := C.xds_client_poll_burst(
		c.ptr, &c.pollResults[0], C.uint32_t(limit),
	)
	if rc == -C.int(syscall.EAGAIN) {
		return 0, nil
	}
	if err := clientError(rc); err != nil {
		return 0, err
	}

	// poll consumes completions, so never ask for more than out can hold.
	// The C layout has extra fields and cannot be cast to the Go layout.
	n := int(rc)
	for i := range n {
		r := &c.pollResults[i]
		out[i] = result{
			id:        uint64(r.request_id),
			status:    int32(r.status),
			completed: uint64(r.completed_bytes),
		}
	}
	return n, nil
}

func (c *cclient) wait(timeoutUs uint64) (result, error) {
	rc := C.xds_client_wait_any_result(
		c.ptr, C.uint64_t(timeoutUs), &c.waitResult,
	)
	if err := clientError(rc); err != nil {
		// Preserve ETIMEDOUT: the engine treats it as an empty wait interval.
		// Never expose the reused output field on a failed call.
		return result{}, err
	}

	r := &c.waitResult
	return result{
		id:        uint64(r.request_id),
		status:    int32(r.status),
		completed: uint64(r.completed_bytes),
	}, nil
}

func (c *cclient) close() {
	if c.ptr != nil {
		C.xds_client_close(c.ptr)
		c.ptr = nil
	}
}
