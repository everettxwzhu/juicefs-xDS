//go:build linux && xds

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
	"bytes"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestCgoRuntimeCQConcurrentAndStop exercises the real cgoRuntime wait-free
// MPSC CQ, eventfd Go netpoller wakeup, inline/overflow iovec staging, and
// Stop() drain against a linked mock libjfs_xds_runtime.so when
// JFS_XDS_TEST_MOCK_RUNTIME=1 is set.
func TestCgoRuntimeCQConcurrentAndStop(t *testing.T) {
	if os.Getenv("JFS_XDS_TEST_MOCK_RUNTIME") != "1" {
		t.Skip("set JFS_XDS_TEST_MOCK_RUNTIME=1 with mock libjfs_xds_runtime.so to run")
	}

	rt := NewDefaultRuntime()
	cfg := &Config{
		BdevName:      "MockNvme0n1",
		ReactorMask:   "0x1",
		PeerBDFs:      []string{"0000:3b:00.0"},
		DeviceBackend: BackendNvidia,
		BlockSize:     8192,
		BlobSize:      16384,
		BlobCount:     2,
		EnableFastMap: true,
	}
	require.NoError(t, cfg.Normalize())

	info, err := rt.Start(cfg)
	require.NoError(t, err)
	require.Equal(t, uint64(4096), info.IOUnitSize)
	require.True(t, info.DirectReady)

	blobs, err := rt.LeaseBlobs(2)
	require.NoError(t, err)
	require.Len(t, blobs, 2)

	// Write & read back CPU blocks concurrently to stress the CQ across C pthreads.
	payload := bytes.Repeat([]byte{0x42}, 8192)
	require.NoError(t, rt.WriteCPU(blobs[0], 0, payload))

	evFD, err := unix.Eventfd(0, unix.EFD_CLOEXEC)
	require.NoError(t, err)
	reg, ioPtr, err := rt.ImportBuffer(evFD, 0x7000000000, 1<<20, []PeerExport{
		{PeerBDF: "0000:3b:00.0", Handle: 7, PageSize: 4096, MappedLen: 1 << 20},
	})
	require.NoError(t, err)
	require.NotZero(t, ioPtr)

	var wg sync.WaitGroup
	// 1. High-concurrency WriteCPU + scalar/vector/overflow-IOV ReadDirectBatch.
	for g := range 16 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range 32 {
				if i%8 == 0 {
					require.NoError(t, rt.WriteCPU(blobs[1], uint64((id%2)*8192), payload))
				}

				if i%4 == 0 {
					// Exercise dyn_iovs heap fallback (> 64 iovs).
					iovs := make([]IOVec, 70)
					for k := range iovs {
						iovs[k] = IOVec{Base: ioPtr + uintptr(k*4096), Len: 4096}
					}
					require.NoError(t, rt.ReadDirectBatch([]DirectReadDesc{
						{Blob: blobs[0], BlobOffset: 0, Size: 70 * 4096, Iovs: iovs},
					}))
				} else {
					require.NoError(t, rt.ReadDirectBatch([]DirectReadDesc{
						{Blob: blobs[0], BlobOffset: 0, Size: 4096, Payload: ioPtr + uintptr(id*4096)},
					}))
				}
			}
		}(g)
	}
	wg.Wait()

	// 2. Concurrent Stop() while new I/Os are actively racing must drain cleanly
	//    with zero hangs and zero SIGSEGV/race.
	var raceWG sync.WaitGroup
	for range 8 {
		raceWG.Add(1)
		go func() {
			defer raceWG.Done()
			for range 64 {
				if err := rt.WriteCPU(blobs[1], 0, payload[:4096]); err != nil {
					return
				}
			}
		}()
	}
	require.NoError(t, rt.UnregisterBuffer(reg))
	require.NoError(t, rt.ReleaseBlobs(blobs))
	require.NoError(t, rt.Stop())
	raceWG.Wait()

	// Post-stop I/O must return ErrClosed immediately.
	require.ErrorIs(t, rt.WriteCPU(blobs[0], 0, payload[:4096]), ErrClosed)
}
