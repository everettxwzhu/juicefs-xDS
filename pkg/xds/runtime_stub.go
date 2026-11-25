//go:build !linux || !xds

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

type stubRuntime struct{}

// NewDefaultRuntime returns a compile-time stub when built without -tags xds
// or on non-Linux platforms, keeping standard JuiceFS builds dependency-free.
func NewDefaultRuntime() Runtime {
	return &stubRuntime{}
}

func (*stubRuntime) Start(*Config) (RuntimeInfo, error)      { return RuntimeInfo{}, ErrUnsupported }
func (*stubRuntime) Stop() error                             { return nil }
func (*stubRuntime) LeaseBlobs(uint32) ([]BlobHandle, error) { return nil, ErrUnsupported }
func (*stubRuntime) ReleaseBlobs([]BlobHandle) error         { return nil }
func (*stubRuntime) ImportBuffer(int, uint64, uint64, []PeerExport) (RegHandle, uintptr, error) {
	return 0, 0, ErrUnsupported
}
func (*stubRuntime) UnregisterBuffer(RegHandle) error          { return ErrUnsupported }
func (*stubRuntime) WriteCPU(BlobHandle, uint64, []byte) error { return ErrUnsupported }
func (*stubRuntime) ReadDirectBatch([]DirectReadDesc) error    { return ErrUnsupported }
