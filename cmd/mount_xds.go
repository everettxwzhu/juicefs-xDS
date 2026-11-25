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
	"fmt"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/juicedata/juicefs/pkg/xds"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/urfave/cli/v2"
)

type xdsMountRuntime struct {
	engine *xds.Engine
	server *xds.Server
}

// loadXDSConfig parses and validates --xds-config during early mount stages
// without starting the SPDK runtime (which cannot survive daemon fork).
// Normal POSIX cache configuration in chunkConf is never modified.
func loadXDSConfig(c *cli.Context, chunkConf *chunk.Config, allowOther bool) (*xds.Config, error) {
	cfgPath := c.String("xds-config")
	if cfgPath == "" {
		return nil, nil
	}
	cfg, err := xds.LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}
	if cfg.SocketPath == "" {
		return nil, fmt.Errorf("%w: socket_path is required for xDS direct device I/O", xds.ErrInvalidArg)
	}
	if chunkConf.BlockSize > 0 {
		cfg.BlockSize = chunkConf.BlockSize
	}
	if allowOther {
		cfg.AllowOther = true
		if cfg.SocketMode == 0o600 {
			cfg.SocketMode = 0o666
		}
	}
	if cfg.CacheExpire == 0 && chunkConf.CacheExpire > 0 {
		cfg.CacheExpire = chunkConf.CacheExpire
	}
	if err := cfg.Normalize(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// startXDS launches the sidecar embedded xDS SPDK engine and cross-process
// direct-read server in stage 3 without modifying JuiceFS's POSIX data path.
func startXDS(cfg *xds.Config, v *vfs.VFS, reg prometheus.Registerer) (*xdsMountRuntime, error) {
	if cfg == nil {
		return nil, nil
	}
	eng, err := xds.NewEngine(cfg, nil, reg)
	if err != nil {
		return nil, fmt.Errorf("init xds engine: %w", err)
	}
	srv, err := xds.NewServer(eng, v.NewXDSResolver(eng.Cache(), 0))
	if err != nil {
		_ = eng.Close()
		return nil, fmt.Errorf("start xds server: %w", err)
	}

	ep := srv.EndpointInfo()
	v.SetXDSEndpoint(&ep)

	go func() {
		if err := srv.Serve(); err != nil {
			logger.Errorf("xds server serve: %v", err)
		}
	}()
	logger.Infof("xDS sidecar direct I/O ready: socket=%q backend=%s peers=%v slots=%d blockSize=%d ioUnit=%d directReady=%v",
		ep.SocketPath, cfg.DeviceBackend, cfg.PeerBDFs, cfg.TotalSlots(), cfg.BlockSize, eng.Info().IOUnitSize, eng.Info().DirectReady)
	return &xdsMountRuntime{engine: eng, server: srv}, nil
}

func (r *xdsMountRuntime) stopServer() {
	if r == nil || r.server == nil {
		return
	}
	if err := r.server.Close(); err != nil {
		logger.Errorf("close xds server: %v", err)
	}
	r.server = nil
}

func (r *xdsMountRuntime) closeEngine() {
	if r == nil || r.engine == nil {
		return
	}
	if err := r.engine.Close(); err != nil {
		logger.Errorf("close xds engine: %v", err)
	}
	r.engine = nil
}
