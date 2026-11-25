//go:build linux

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

package cmd

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/xds"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

func TestLoadXDSConfig(t *testing.T) {
	newCtx := func(cfgPath string) *cli.Context {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		fs.String("xds-config", cfgPath, "")
		return cli.NewContext(cli.NewApp(), fs, nil)
	}

	chunkConf := &chunk.Config{
		BlockSize:     4 << 20,
		CacheChecksum: chunk.CsFull,
		CacheExpire:   5 * time.Minute,
		Writeback:     true,
	}

	// Empty flag disables xDS cleanly.
	cfg, err := loadXDSConfig(newCtx(""), chunkConf, false)
	require.NoError(t, err)
	require.Nil(t, cfg)

	// Valid config aligns BlockSize, AllowOther socket mode, checksum, and expiry
	// without touching POSIX chunkConf settings such as Writeback.
	cfgPath := filepath.Join(t.TempDir(), "xds.json")
	raw := `{
		"bdev_name": "Nvme0n1",
		"xds_device_backend": "nvidia",
		"xds_blob_count": 4,
		"xds_peer_bdfs": ["0000:3b:00.0"],
		"socket_path": "/tmp/jfs-xds-test.sock"
	}`
	require.NoError(t, os.WriteFile(cfgPath, []byte(raw), 0o600))

	cfg, err = loadXDSConfig(newCtx(cfgPath), chunkConf, true)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Equal(t, 4<<20, cfg.BlockSize)
	require.True(t, cfg.AllowOther)
	require.Equal(t, os.FileMode(0o666), cfg.SocketMode)
	require.Equal(t, 5*time.Minute, cfg.CacheExpire)
	require.Equal(t, xds.BackendNvidia, cfg.DeviceBackend)
	require.True(t, chunkConf.Writeback)

	// Non-4MiB volume (e.g. 1 MiB block size with 2 MiB blobs) and stage-1
	// zero BlockSize must both normalize without falsely failing.
	smallPath := filepath.Join(t.TempDir(), "xds-1m.json")
	require.NoError(t, os.WriteFile(smallPath, []byte(`{
		"bdev_name": "Nvme0n1",
		"block_size": 1048576,
		"xds_blob_size_bytes": 2097152,
		"xds_blob_count": 4,
		"socket_path": "/tmp/jfs-xds-1m.sock"
	}`), 0o600))
	cfg1M, err := loadXDSConfig(newCtx(smallPath), &chunk.Config{BlockSize: 1 << 20}, false)
	require.NoError(t, err)
	require.Equal(t, 1<<20, cfg1M.BlockSize)
	cfgStage1, err := loadXDSConfig(newCtx(smallPath), &chunk.Config{BlockSize: 0}, false)
	require.NoError(t, err)
	require.Equal(t, 1<<20, cfgStage1.BlockSize)

	// Sidecar mode requires socket_path.
	noSockPath := filepath.Join(t.TempDir(), "no-sock.json")
	require.NoError(t, os.WriteFile(noSockPath, []byte(`{"bdev_name":"Nvme0n1","xds_blob_count":4}`), 0o600))
	_, err = loadXDSConfig(newCtx(noSockPath), chunkConf, false)
	require.ErrorIs(t, err, xds.ErrInvalidArg)

	// Missing config file fails immediately.
	_, err = loadXDSConfig(newCtx(filepath.Join(t.TempDir(), "missing.json")), chunkConf, false)
	require.Error(t, err)
}
