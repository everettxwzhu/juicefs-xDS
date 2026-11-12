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

// Package xds provides the in-process client adapter for xDS GPU reads.
//
// The native client is enabled only on Linux with cgo and the xds build tag.
// Other builds use a stub and do not require the xDS submodule.
//
// Build with make juicefs.xds to include the external native sources and
// headers in cgo's cache key. Client changes are then rebuilt automatically,
// without clearing the Go build cache or forcing all dependencies to rebuild.
package xds

import "time"

// Config identifies the xDS queue exclusively owned by one Volume.
type Config struct {
	// Service is the service name passed to xds_service.
	Service string
	// Rank selects a queue. It must be unique among clients of this service.
	Rank uint32
	// WorldSize must be positive, identical for all connected clients, and
	// no greater than the service's configured queue count.
	WorldSize uint32
}

const (
	ioTimeout = 60 * time.Second
	pollWait  = 100 * time.Microsecond
	pollBurst = 64

	// get_blob uses the same timeout for service-side acquisition and for
	// waiting for its completion. Zero may lose a successfully acquired
	// handle when the completion has not arrived yet.
	blobAcquireTimeout = 5 * time.Second
)
