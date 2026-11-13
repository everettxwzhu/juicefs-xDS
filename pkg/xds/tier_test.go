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
	"syscall"
	"testing"
	"unsafe"
)

func TestMakeLayout(t *testing.T) {
	tests := []struct {
		name string
		meta clientMeta
		bs   int
		want slotLayout
	}{
		{
			name: "four MiB blocks",
			meta: clientMeta{
				blobSize: 64 << 20, ioUnit: 4096, capacity: 320 << 20,
			},
			bs: 4 << 20,
			want: slotLayout{
				blobCount: 5, perBlob: 16, slotCount: 80,
				stagerCount: 32, cacheStart: 33,
			},
		},
		{
			name: "stagers limited by byte budget",
			meta: clientMeta{
				blobSize: 64 << 20, ioUnit: 4096, capacity: 320 << 20,
			},
			bs: 8 << 20,
			want: slotLayout{
				blobCount: 5, perBlob: 8, slotCount: 40,
				stagerCount: 16, cacheStart: 17,
			},
		},
		{
			name: "one stager",
			meta: clientMeta{
				blobSize: 128 << 20, ioUnit: 4096, capacity: 384 << 20,
			},
			bs: 128 << 20,
			want: slotLayout{
				blobCount: 3, perBlob: 1, slotCount: 3,
				stagerCount: 1, cacheStart: 2,
			},
		},
		{
			name: "exact minimum slot count",
			meta: clientMeta{
				blobSize: 4096, ioUnit: 4096, capacity: 65 * 4096,
			},
			bs: 4096,
			want: slotLayout{
				blobCount: 65, perBlob: 1, slotCount: 65,
				stagerCount: 32, cacheStart: 33,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := makeLayout(tt.meta, tt.bs)
			if err != nil || got != tt.want {
				t.Fatalf("makeLayout: %+v, %v; want %+v, nil", got, err, tt.want)
			}
		})
	}
}

func TestMakeLayoutInvalid(t *testing.T) {
	valid := clientMeta{
		blobSize: 64 << 20, ioUnit: 4096, capacity: 320 << 20,
	}
	tests := []struct {
		name string
		meta clientMeta
		bs   int
	}{
		{"zero block size", valid, 0},
		{"negative block size", valid, -1},
		{
			"zero blob size",
			clientMeta{ioUnit: 4096, capacity: 320 << 20},
			4 << 20,
		},
		{
			"zero I/O unit",
			clientMeta{blobSize: 64 << 20, capacity: 320 << 20},
			4 << 20,
		},
		{
			"zero capacity",
			clientMeta{blobSize: 64 << 20, ioUnit: 4096},
			4 << 20,
		},
		{
			"capacity contains a partial blob",
			clientMeta{
				blobSize: 64 << 20, ioUnit: 4096,
				capacity: (320 << 20) + 1,
			},
			4 << 20,
		},
		{
			"blob contains a partial block",
			clientMeta{
				blobSize: (64 << 20) + 4096, ioUnit: 4096,
				capacity: 5 * ((64 << 20) + 4096),
			},
			4 << 20,
		},
		{
			"block is not I/O aligned",
			clientMeta{
				blobSize: 16 * ((4 << 20) + 1), ioUnit: 4096,
				capacity: 80 * ((4 << 20) + 1),
			},
			(4 << 20) + 1,
		},
		{
			"block exceeds stager byte budget",
			clientMeta{
				blobSize: 129 << 20, ioUnit: 4096, capacity: 387 << 20,
			},
			129 << 20,
		},
		{
			"one slot short",
			clientMeta{
				blobSize: 64 << 20, ioUnit: 4096, capacity: 256 << 20,
			},
			4 << 20,
		},
		{
			"capacity smaller than one blob",
			clientMeta{
				blobSize: 64 << 20, ioUnit: 4096, capacity: 4 << 20,
			},
			4 << 20,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := makeLayout(tt.meta, tt.bs); err != syscall.EINVAL {
				t.Fatalf("makeLayout: %+v, %v; want EINVAL", got, err)
			}
		})
	}
}

func TestMakeLayoutOverflow(t *testing.T) {
	// These inputs are geometrically valid. Only their representation is too
	// large, so a failed divisibility check must not mask the overflow case.
	for _, capacity := range []uint64{1 << 31, 1 << 63, ^uint64(0)} {
		cm := clientMeta{blobSize: 1, ioUnit: 1, capacity: capacity}
		if got, err := makeLayout(cm, 1); err != syscall.EOVERFLOW {
			t.Fatalf("capacity=%d: %+v, %v; want EOVERFLOW", capacity, got, err)
		}
	}

	// A slice length may fit int and int32 while its backing allocation does
	// not fit int on a 32-bit target. This calculation never allocates memory.
	maxInt := uint64(^uint(0) >> 1)
	firstOversizedSlice := maxInt/uint64(unsafe.Sizeof(slotRef{})) + 1
	if firstOversizedSlice <= 1<<31-1 {
		cm := clientMeta{
			blobSize: 1, ioUnit: 1, capacity: firstOversizedSlice,
		}
		if got, err := makeLayout(cm, 1); err != syscall.EOVERFLOW {
			t.Fatalf("oversized slot slice: %+v, %v; want EOVERFLOW", got, err)
		}
	}
}
