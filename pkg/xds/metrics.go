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

import "github.com/prometheus/client_golang/prometheus"

// Metrics exposes Prometheus counters, gauges, and histograms for the xDS cache
// engine and direct-read server.
type Metrics struct {
	CacheHits       prometheus.Counter
	CacheMisses     prometheus.Counter
	CacheEvictions  prometheus.Counter
	CacheSlotsUsed  prometheus.Gauge
	CacheBytesUsed  prometheus.Gauge
	DirectReadReqs  prometheus.Counter
	DirectReadBytes prometheus.Counter
	DirectReadErrs  prometheus.Counter
	DirectReadDur   prometheus.Histogram
	ActiveSessions  prometheus.Gauge
	ImportedBuffers prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		CacheHits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "xds_cache_hits_total",
			Help: "Total number of xDS NVMe block cache hits.",
		}),
		CacheMisses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "xds_cache_misses_total",
			Help: "Total number of xDS NVMe block cache misses.",
		}),
		CacheEvictions: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "xds_cache_evictions_total",
			Help: "Total number of xDS NVMe cache slots evicted.",
		}),
		CacheSlotsUsed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "xds_cache_slots_used",
			Help: "Current number of occupied xDS NVMe cache slots.",
		}),
		CacheBytesUsed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "xds_cache_bytes_used",
			Help: "Current number of cached bytes stored in xDS NVMe slots.",
		}),
		DirectReadReqs: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "xds_direct_read_requests_total",
			Help: "Total number of cross-process direct-read requests served.",
		}),
		DirectReadBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "xds_direct_read_bytes_total",
			Help: "Total bytes transferred over the xDS direct-read path.",
		}),
		DirectReadErrs: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "xds_direct_read_errors_total",
			Help: "Total number of failed xDS direct-read requests.",
		}),
		DirectReadDur: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "xds_direct_read_duration_seconds",
			Help:    "Direct-read dispatch latency distribution in seconds.",
			Buckets: prometheus.ExponentialBuckets(0.00001, 2, 18),
		}),
		ActiveSessions: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "xds_active_sessions",
			Help: "Current number of connected external xDS client sessions.",
		}),
		ImportedBuffers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "xds_imported_buffers",
			Help: "Current number of imported cross-process P2P device buffers.",
		}),
	}
	if reg != nil {
		reg.MustRegister(
			m.CacheHits, m.CacheMisses, m.CacheEvictions,
			m.CacheSlotsUsed, m.CacheBytesUsed,
			m.DirectReadReqs, m.DirectReadBytes, m.DirectReadErrs, m.DirectReadDur,
			m.ActiveSessions, m.ImportedBuffers,
		)
	}
	return m
}
