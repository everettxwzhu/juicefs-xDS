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

// Package xds implements a device-agnostic local NVMe cache engine and
// cross-process direct-read server backed by the embedded xDS SPDK runtime.
//
// The package intentionally has no dependency on pkg/chunk, pkg/vfs, pkg/meta,
// or any GPU/NPU vendor SDK. Upper layers bridge file resolution through the
// FileResolver interface and wrap Engine.Cache() into chunk.CacheManager.
package xds

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// Common errors returned by the xds package.
var (
	ErrUnsupported = errors.New("xds: support not compiled into this binary")
	ErrClosed      = errors.New("xds: engine or server is closed")
	ErrNotCached   = errors.New("xds: block not cached")
	ErrBusy        = errors.New("xds: resource busy")
	ErrInvalidArg  = errors.New("xds: invalid argument")
	ErrPerm        = errors.New("xds: permission denied")
	ErrNoSpace     = errors.New("xds: no free cache slot available")
	ErrCorrupt     = errors.New("xds: cached block checksum mismatch")
)

// DeviceBackend identifies the opaque xDS P2P kernel backend passed through to
// xDS runtime initialization and announced to clients during handshake.
// Values strictly match enum xds_device_backend in xDS/include/xds/dma_registry.h.
type DeviceBackend uint32

const (
	BackendNvidia DeviceBackend = iota
	BackendAscend
	BackendIluvatar
	BackendAMD
)

// String returns the canonical lowercase backend name expected by xDS configs.
func (b DeviceBackend) String() string {
	switch b {
	case BackendNvidia:
		return "nvidia"
	case BackendAscend:
		return "ascend"
	case BackendIluvatar:
		return "iluvatar"
	case BackendAMD:
		return "amd"
	default:
		return fmt.Sprintf("unknown(%d)", uint32(b))
	}
}

// MarshalJSON encodes DeviceBackend as its canonical lowercase name.
func (b DeviceBackend) MarshalJSON() ([]byte, error) {
	return json.Marshal(b.String())
}

// UnmarshalJSON decodes a backend name string into DeviceBackend.
func (b *DeviceBackend) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("%w: xds_device_backend must be a string", ErrInvalidArg)
	}
	parsed, err := ParseDeviceBackend(s)
	if err != nil {
		return err
	}
	*b = parsed
	return nil
}

// ParseDeviceBackend parses a backend name from configuration. Empty string
// defaults to BackendNvidia, matching xDS service behavior.
func ParseDeviceBackend(s string) (DeviceBackend, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "nvidia":
		return BackendNvidia, nil
	case "ascend":
		return BackendAscend, nil
	case "iluvatar":
		return BackendIluvatar, nil
	case "amd":
		return BackendAMD, nil
	default:
		return 0, fmt.Errorf("%w: unsupported xds device backend %q", ErrInvalidArg, s)
	}
}

// Config configures the embedded xDS runtime, slab block cache, and IPC server.
// JSON tags align with xDS setup JSON so an existing xDS config file can be
// loaded directly via LoadConfig.
type Config struct {
	// SPDK / xDS runtime configuration.
	BdevName          string        `json:"bdev_name"`
	JSONConfigFile    string        `json:"json_config_file"`
	ReactorMask       string        `json:"reactor_mask"`
	MainCore          uint32        `json:"main_core"`
	RPCAddr           string        `json:"rpc_addr"`
	PeerBDFs          []string      `json:"xds_peer_bdfs"`
	DeviceBackend     DeviceBackend `json:"xds_device_backend"`
	MetadataPageCount uint32        `json:"xds_metadata_page_count"`

	// Cache geometry. BlockSize should match JuiceFS chunk.Config.BlockSize
	// (default 4 MiB); ClusterSize defaults to BlockSize so every block stays
	// inside a single SPDK cluster for 100% FastMap direct-LBA hits.
	BlockSize      int           `json:"block_size"`
	BlobSize       uint64        `json:"xds_blob_size_bytes"`
	BlobCount      uint32        `json:"xds_blob_count"`
	EnableFastMap  bool          `json:"xds_enable_blob_fast_map"`
	VerifyChecksum bool          `json:"verify_checksum"`
	CacheExpire    time.Duration `json:"cache_expire"`

	// Cross-process Unix SOCK_SEQPACKET server settings.
	SocketPath  string      `json:"socket_path"`
	SocketMode  os.FileMode `json:"socket_mode"`
	AllowOther  bool        `json:"allow_other"`
	MaxInflight uint32      `json:"max_inflight"` // concurrent direct reads per session
}

// EndpointInfo is embedded into JuiceFS's .config virtual file so external
// clients automatically discover the mount's xDS direct-read endpoint.
type EndpointInfo struct {
	SocketPath      string   `json:"socketPath,omitempty"`
	DeviceBackend   string   `json:"deviceBackend,omitempty"`
	PeerBDFs        []string `json:"peerBdfs,omitempty"`
	IOUnitSize      uint64   `json:"ioUnitSize,omitempty"`
	BlockSize       uint64   `json:"blockSize,omitempty"`
	MountGeneration uint64   `json:"mountGeneration,omitempty"`
}

// LoadConfig reads a JSON configuration file and applies defaults.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("xds: read config %q: %w", path, err)
	}
	cfg := &Config{EnableFastMap: true}
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("xds: parse config %q: %w", path, err)
	}
	if err := cfg.Normalize(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Normalize validates cfg and populates safe defaults.
func (c *Config) Normalize() error {
	if c == nil {
		return fmt.Errorf("%w: nil config", ErrInvalidArg)
	}
	if c.DeviceBackend > BackendAMD {
		return fmt.Errorf("%w: unsupported xds device backend %d", ErrInvalidArg, uint32(c.DeviceBackend))
	}
	if c.BlockSize <= 0 {
		c.BlockSize = 4 << 20 // 4 MiB, JuiceFS default
	}
	if c.BlockSize&(4096-1) != 0 {
		return fmt.Errorf("%w: block_size %d must be 4 KiB aligned", ErrInvalidArg, c.BlockSize)
	}
	if c.BlobSize == 0 {
		c.BlobSize = uint64(c.BlockSize) * 16 // 64 MiB slab blob by default
	}
	if c.BlobSize < uint64(c.BlockSize) || c.BlobSize%uint64(c.BlockSize) != 0 {
		return fmt.Errorf("%w: blob_size %d must be a positive multiple of block_size %d",
			ErrInvalidArg, c.BlobSize, c.BlockSize)
	}
	if c.BlobCount == 0 {
		return fmt.Errorf("%w: xds_blob_count must be > 0", ErrInvalidArg)
	}
	if c.MaxInflight == 0 {
		c.MaxInflight = 64
	}
	if c.SocketMode == 0 {
		if c.AllowOther {
			c.SocketMode = 0o666
		} else {
			c.SocketMode = 0o600
		}
	}
	return nil
}

// TotalSlots returns the total number of BlockSize cache slots across all blobs.
func (c *Config) TotalSlots() uint32 {
	slotsPerBlob := uint32(c.BlobSize / uint64(c.BlockSize))
	return c.BlobCount * slotsPerBlob
}

func alignUp64(v, align uint64) uint64 {
	return (v + align - 1) &^ (align - 1)
}
