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

import (
	"os"
	"strconv"
	"syscall"
	"testing"
)

func TestClientInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		conf Config
	}{
		{"empty service", Config{WorldSize: 1}},
		{"embedded NUL", Config{Service: "xds\x00other", WorldSize: 1}},
		// This name passes Go validation and exercises connect_ex in C.
		{"invalid service path", Config{Service: "xds/test", WorldSize: 1}},
		{"zero world", Config{Service: "xds-test"}},
		{"rank outside world", Config{Service: "xds-test", Rank: 1, WorldSize: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := newClient(tt.conf)
			if c != nil {
				c.close()
				t.Fatal("invalid configuration returned a client")
			}
			if err != syscall.EINVAL {
				t.Fatalf("newClient: %v; want EINVAL", err)
			}
		})
	}
}

// The C entry points reject a null client before touching service or device
// resources. Exercise the actual linked C functions without requiring hardware.
func TestClientWithoutConnection(t *testing.T) {
	c := &cclient{}
	if len(c.pollResults) != pollBurst {
		t.Fatalf("xDS poll limit=%d, engine limit=%d",
			len(c.pollResults), pollBurst)
	}
	if m, err := c.metadata(); err != syscall.EINVAL || m != (clientMeta{}) {
		t.Fatalf("metadata: %+v, %v; want zero, EINVAL", m, err)
	}
	if h, err := c.getBlob(); err != syscall.EINVAL || h != 0 {
		t.Fatalf("getBlob: %d, %v; want zero, EINVAL", h, err)
	}
	if buf, ref, err := c.allocBuffer(4096); err != syscall.EINVAL ||
		buf != nil || ref != 0 {
		t.Fatalf("allocBuffer: len=%d, ref=%d, err=%v", len(buf), ref, err)
	}
	if ref, err := c.registerGPU(0, 4096); err != syscall.EINVAL || ref != 0 {
		t.Fatalf("registerGPU: %d, %v; want zero, EINVAL", ref, err)
	}
	if err := c.unregister(1); err != syscall.EINVAL {
		t.Fatalf("unregister: %v; want EINVAL", err)
	}

	// A failed C call must not return an output value from an earlier call.
	c.submitID = 99
	for _, write := range []bool{false, true} {
		id, err := c.submit(&ioOp{write: write, n: 4096})
		if err != syscall.EINVAL || id != 0 {
			t.Fatalf("submit(write=%t): %d, %v; want zero, EINVAL",
				write, id, err)
		}
	}
	if id, err := c.submit(nil); err != syscall.EINVAL || id != 0 {
		t.Fatalf("submit(nil): %d, %v; want zero, EINVAL", id, err)
	}

	out := []result{{id: 77}}
	if n, err := c.poll(out); err != syscall.EINVAL || n != 0 {
		t.Fatalf("poll: %d, %v; want zero, EINVAL", n, err)
	}
	if out[0].id != 77 {
		t.Fatal("failed poll modified the caller's output")
	}
	if n, err := c.poll(nil); err != nil || n != 0 {
		t.Fatalf("poll(nil): %d, %v; want zero, nil", n, err)
	}

	c.waitResult.request_id = 88
	if r, err := c.wait(1); err != syscall.EINVAL || r != (result{}) {
		t.Fatalf("wait: %+v, %v; want zero, EINVAL", r, err)
	}
	c.close()
	c.close()
}

func TestClientBufferSize(t *testing.T) {
	c := &cclient{}
	for _, tt := range []struct {
		size uint64
		want syscall.Errno
	}{
		{0, syscall.EINVAL},
		{^uint64(0), syscall.EOVERFLOW},
	} {
		buf, ref, err := c.allocBuffer(tt.size)
		if buf != nil || ref != 0 || err != tt.want {
			t.Fatalf("allocBuffer(%d): len=%d, ref=%d, err=%v; want %v",
				tt.size, len(buf), ref, err, tt.want)
		}
	}
}

// TestClientService is opt-in because connecting exclusively occupies a rank.
// Use an idle queue, and set rank/world to match the service's other clients.
func TestClientService(t *testing.T) {
	service := os.Getenv("JFS_XDS_TEST_SERVICE")
	if service == "" {
		t.Skip("set JFS_XDS_TEST_SERVICE to test a real xDS service")
	}
	conf := Config{
		Service:   service,
		Rank:      testUint32Env(t, "JFS_XDS_TEST_RANK", 0),
		WorldSize: testUint32Env(t, "JFS_XDS_TEST_WORLD_SIZE", 1),
	}
	c, err := newClient(conf)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	defer c.close()

	m, err := c.metadata()
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	if m.blobSize == 0 || m.ioUnit == 0 ||
		m.blobSize%m.ioUnit != 0 || m.capacity < m.blobSize {
		t.Fatalf("unexpected metadata: %+v", m)
	}

	// Do not retry a timed-out acquisition on this client. Fatal runs the
	// deferred close, which discards the entire initialization session.
	if _, err := c.getBlob(); err != nil {
		t.Fatalf("getBlob: %v", err)
	}
	buf, ref, err := c.allocBuffer(m.ioUnit)
	if err != nil {
		t.Fatalf("allocBuffer: %v", err)
	}
	if uint64(len(buf)) != m.ioUnit || ref == 0 {
		t.Fatalf("allocBuffer: len=%d, ref=%d", len(buf), ref)
	}

	// Only validate the host mapping here. DMA and GPU synchronization
	// require the I/O engine and device-registration lifecycle.
	buf[0] = 0x5a
	buf[len(buf)-1] = 0xa5
	if buf[len(buf)-1] != 0xa5 {
		t.Fatal("host buffer is not writable")
	}
}

func testUint32Env(t *testing.T, key string, fallback uint32) uint32 {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	n, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		t.Fatalf("%s: %v", key, err)
	}
	return uint32(n)
}
