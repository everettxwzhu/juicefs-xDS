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

package chunk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type flakyGetStorage struct {
	object.ObjectStorage
	failsLeft atomic.Int32
}

func (f *flakyGetStorage) Get(ctx context.Context, key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	if f.failsLeft.Add(-1) >= 0 {
		return nil, errors.New("transient 503")
	}
	return f.ObjectStorage.Get(ctx, key, off, limit, getters...)
}

func TestLoadBlockNoPOSIXCachePollutionAndLocalHit(t *testing.T) {
	const blockSize = 8192

	conf := Config{
		BlockSize:       blockSize,
		Compress:        "lz4",
		HashPrefix:      true,
		CacheDir:        "memory",
		CacheSize:       1 << 20,
		CacheFullBlock:  true,
		CacheLargeWrite: true,
		MaxUpload:       2,
		MaxDownload:     4,
		BufferSize:      64 << 20,
	}
	blob, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	store := NewCachedStore(blob, conf, prometheus.NewRegistry()).(*cachedStore)

	payload := make([]byte, blockSize+1234)
	for i := range payload {
		payload[i] = byte((i*31 + 7) & 0xff)
	}
	const sliceID uint64 = 1024
	w := store.NewWriter(sliceID, 0)
	n, err := w.WriteAt(payload, 0)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)
	require.NoError(t, w.Finish(len(payload)))

	k0 := BlockKey(true, sliceID, 0, blockSize)
	k1 := BlockKey(true, sliceID, 1, 1234)
	require.Equal(t, sliceForRead(sliceID, len(payload), store).key(0), k0)
	require.Equal(t, sliceForRead(sliceID, len(payload), store).key(1), k1)

	// Evict from POSIX cache; LoadBlock must fetch plaintext from object storage
	// WITHOUT inserting back into store.bcache.
	require.NoError(t, store.EvictCache(sliceID, uint32(len(payload)), nil))
	_, cached := store.bcache.exist(k0)
	require.False(t, cached)

	p0, err := store.LoadBlock(context.Background(), k0, blockSize)
	require.NoError(t, err)
	require.True(t, bytes.Equal(payload[:blockSize], p0.Data))
	p0.Release()

	p1, err := store.LoadBlock(context.Background(), k1, 1234)
	require.NoError(t, err)
	require.True(t, bytes.Equal(payload[blockSize:], p1.Data))
	p1.Release()

	// POSIX cache remains untouched.
	_, cached = store.bcache.exist(k0)
	require.False(t, cached)
	_, cached = store.bcache.exist(k1)
	require.False(t, cached)

	// If a block IS in local POSIX cache (e.g., writeback staging or warm read),
	// LoadBlock serves it locally even when deleted from object storage.
	require.NoError(t, store.FillCache(sliceID, uint32(len(payload)), []Range{{Off: 0, Len: blockSize}}))
	_, cached = store.bcache.exist(k0)
	require.True(t, cached)
	require.NoError(t, blob.Delete(context.Background(), k0))

	pLocal, err := store.LoadBlock(context.Background(), k0, blockSize)
	require.NoError(t, err)
	require.True(t, bytes.Equal(payload[:blockSize], pLocal.Data))
	pLocal.Release()

	// Invalid size fails cleanly.
	_, err = store.LoadBlock(context.Background(), k0, 0)
	require.Error(t, err)
	_, err = store.LoadBlock(context.Background(), k0, blockSize+1)
	require.Error(t, err)

	// Transient object-storage failures are retried cleanly without polluting POSIX cache.
	flaky := &flakyGetStorage{ObjectStorage: blob}
	flaky.failsLeft.Store(2)
	retryConf := conf
	retryConf.MaxRetries = 3
	retryStore := NewCachedStore(flaky, retryConf, prometheus.NewRegistry()).(*cachedStore)
	pRetry, err := retryStore.LoadBlock(context.Background(), k1, 1234)
	require.NoError(t, err)
	require.True(t, bytes.Equal(payload[blockSize:], pRetry.Data))
	pRetry.Release()
	_, cached = retryStore.bcache.exist(k1)
	require.False(t, cached)
}
