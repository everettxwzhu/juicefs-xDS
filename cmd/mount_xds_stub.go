//go:build !linux

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

type xdsMountRuntime struct{}

func loadXDSConfig(c *cli.Context, _ *chunk.Config, _ bool) (*xds.Config, error) {
	if c.String("xds-config") != "" {
		return nil, fmt.Errorf("--xds-config is only supported on Linux: %w", xds.ErrUnsupported)
	}
	return nil, nil
}

func startXDS(*xds.Config, *vfs.VFS, prometheus.Registerer) (*xdsMountRuntime, error) {
	return nil, nil
}

func (*xdsMountRuntime) stopServer()  {}
func (*xdsMountRuntime) closeEngine() {}
