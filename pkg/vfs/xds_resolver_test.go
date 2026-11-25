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

package vfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/xds"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

type vfsTestXDSBuf struct {
	ioBase uintptr
	data   []byte
}

type vfsTestXDSRuntime struct {
	mu       sync.Mutex
	ioUnit   uint64
	blobSize uint64
	blobs    map[xds.BlobHandle][]byte
	imported map[xds.RegHandle]*vfsTestXDSBuf
	nextReg  xds.RegHandle
}

func newVFSTestXDSRuntime(ioUnit uint64) *vfsTestXDSRuntime {
	return &vfsTestXDSRuntime{
		ioUnit:   ioUnit,
		blobs:    make(map[xds.BlobHandle][]byte),
		imported: make(map[xds.RegHandle]*vfsTestXDSBuf),
		nextReg:  1,
	}
}

func (f *vfsTestXDSRuntime) Start(cfg *xds.Config) (xds.RuntimeInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blobSize = cfg.BlobSize
	return xds.RuntimeInfo{
		IOUnitSize:  f.ioUnit,
		ClusterSize: uint64(cfg.BlockSize),
		WorkerCount: 2,
		DirectReady: true,
	}, nil
}

func (f *vfsTestXDSRuntime) Stop() error { return nil }

func (f *vfsTestXDSRuntime) LeaseBlobs(count uint32) ([]xds.BlobHandle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]xds.BlobHandle, count)
	for i := range count {
		h := xds.BlobHandle(i + 1)
		f.blobs[h] = make([]byte, f.blobSize)
		out[i] = h
	}
	return out, nil
}

func (f *vfsTestXDSRuntime) ReleaseBlobs([]xds.BlobHandle) error { return nil }

func (f *vfsTestXDSRuntime) ImportBuffer(p2pFD int, _, size uint64, _ []xds.PeerExport) (xds.RegHandle, uintptr, error) {
	if p2pFD >= 0 {
		_ = unix.Close(p2pFD)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	reg := f.nextReg
	f.nextReg++
	ioBase := uintptr(reg) << 32
	f.imported[reg] = &vfsTestXDSBuf{
		ioBase: ioBase,
		data:   make([]byte, size),
	}
	return reg, ioBase, nil
}

func (f *vfsTestXDSRuntime) UnregisterBuffer(reg xds.RegHandle) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.imported, reg)
	return nil
}

func (f *vfsTestXDSRuntime) WriteCPU(blob xds.BlobHandle, blobOff uint64, src []byte) error {
	f.mu.Lock()
	data := f.blobs[blob]
	f.mu.Unlock()
	copy(data[blobOff:blobOff+uint64(len(src))], src)
	return nil
}

func (f *vfsTestXDSRuntime) copyToAliasLocked(addr uintptr, src []byte) {
	for _, b := range f.imported {
		if addr >= b.ioBase && uint64(addr-b.ioBase)+uint64(len(src)) <= uint64(len(b.data)) {
			off := uint64(addr - b.ioBase)
			copy(b.data[off:off+uint64(len(src))], src)
			return
		}
	}
	panic(fmt.Sprintf("vfsTestXDSRuntime: invalid alias 0x%x len=%d", addr, len(src)))
}

func (f *vfsTestXDSRuntime) ReadDirectBatch(descs []xds.DirectReadDesc) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, d := range descs {
		src := f.blobs[d.Blob][d.BlobOffset : d.BlobOffset+d.Size]
		if len(d.Iovs) == 0 {
			f.copyToAliasLocked(d.Payload, src)
			continue
		}
		var pos uint64
		for _, v := range d.Iovs {
			f.copyToAliasLocked(v.Base, src[pos:pos+v.Len])
			pos += v.Len
		}
	}
	return nil
}

func createXDSTestVFS(t *testing.T, sockPath string) (*VFS, *xds.Engine, *vfsTestXDSRuntime) {
	t.Helper()
	const blockSize = 8192

	rt := newVFSTestXDSRuntime(4096)
	eng, err := xds.NewEngine(&xds.Config{
		BlockSize:     blockSize,
		BlobSize:      blockSize * 16,
		BlobCount:     2,
		DeviceBackend: xds.BackendNvidia,
		PeerBDFs:      []string{"0000:3b:00.0"},
		SocketPath:    sockPath,
	}, rt, prometheus.NewRegistry())
	require.NoError(t, err)
	t.Cleanup(func() { _ = eng.Close() })

	metaConf := meta.DefaultConf()
	metaConf.MountPoint = "/jfs-xds-test"
	m := meta.NewClient("memkv://", metaConf)
	format := &meta.Format{
		Name:        "xds-test",
		UUID:        uuid.New().String(),
		Storage:     "mem",
		BlockSize:   blockSize / 1024,
		Compression: "lz4",
		HashPrefix:  true,
	}
	require.NoError(t, m.Init(format, true))

	chunkConf := &chunk.Config{
		BlockSize:      blockSize,
		Compress:       format.Compression,
		HashPrefix:     true,
		CacheDir:       "memory",
		CacheSize:      10 << 20,
		MaxUpload:      2,
		MaxDownload:    16,
		BufferSize:     64 << 20,
		CacheFullBlock: true,
	}
	blob, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	registry := prometheus.NewRegistry()
	store := chunk.NewCachedStore(blob, *chunkConf, registry)
	conf := &Config{
		Meta:       metaConf,
		Format:     *format,
		Version:    "JuiceFS",
		Chunk:      chunkConf,
		FuseOpts:   &FuseOptions{},
		Mountpoint: metaConf.MountPoint,
	}
	return NewVFS(conf, m, store, registry, registry), eng, rt
}

func TestVFSXDSVirtualFilesAndBackwardCompat(t *testing.T) {
	v, _, _ := createXDSTestVFS(t, "")
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))

	// When xDS endpoint is not published, .xds is not reserved: users can
	// create, read, and unlink a normal file named ".xds" in the root directory.
	_, errno := v.Lookup(ctx, rootID, ".xds")
	require.Equal(t, syscall.ENOENT, errno)
	_, errno = v.GetAttr(ctx, XDSInode, 0)
	require.Equal(t, syscall.ENOENT, errno)
	_, _, errno = v.Open(ctx, XDSInode, syscall.O_RDONLY)
	require.Equal(t, syscall.ENOENT, errno)

	userEnt, userFH, errno := v.Create(ctx, rootID, ".xds", 0644, 0, syscall.O_RDWR)
	require.Equal(t, syscall.Errno(0), errno)
	require.NotEqual(t, Ino(XDSInode), userEnt.Inode)
	require.Equal(t, syscall.Errno(0), v.Write(ctx, userEnt.Inode, []byte("user-data"), 0, userFH))
	v.Release(ctx, userEnt.Inode, userFH)
	gotUser, errno := v.Lookup(ctx, rootID, ".xds")
	require.Equal(t, syscall.Errno(0), errno)
	require.Equal(t, userEnt.Inode, gotUser.Inode)
	require.Equal(t, syscall.Errno(0), v.Unlink(ctx, rootID, ".xds"))

	fh, errno := v.Opendir(ctx, rootID, 0)
	require.Equal(t, syscall.Errno(0), errno)
	entries, _, errno := v.Readdir(ctx, rootID, 64<<10, 0, fh, false)
	require.Equal(t, syscall.Errno(0), errno)
	for _, e := range entries {
		require.NotEqual(t, ".xds", string(e.Name))
	}
	v.Releasedir(ctx, rootID, fh)

	// Publish endpoint metadata and verify .xds (0444) and .config expose it.
	ep := xds.EndpointInfo{
		SocketPath:      "/run/juicefs/xds-test.sock",
		MountGeneration: 42,
		DeviceBackend:   "nvidia",
		PeerBDFs:        []string{"0000:3b:00.0"},
		IOUnitSize:      4096,
		BlockSize:       8192,
	}
	v.SetXDSEndpoint(&ep)

	ent, errno := v.Lookup(ctx, rootID, ".xds")
	require.Equal(t, syscall.Errno(0), errno)
	require.Equal(t, Ino(XDSInode), ent.Inode)
	require.Equal(t, uint16(0444), ent.Attr.Mode)
	require.Greater(t, ent.Attr.Length, uint64(0))

	_, _, errno = v.Open(ctx, XDSInode, syscall.O_WRONLY)
	require.Equal(t, syscall.EACCES, errno)

	openEnt, openFH, errno := v.Open(ctx, XDSInode, syscall.O_RDONLY)
	require.Equal(t, syscall.Errno(0), errno)
	raw := make([]byte, openEnt.Attr.Length)
	n, errno := v.Read(ctx, XDSInode, raw, 0, openFH)
	require.Equal(t, syscall.Errno(0), errno)
	v.Release(ctx, XDSInode, openFH)

	var decoded xds.EndpointInfo
	require.NoError(t, json.Unmarshal(raw[:n], &decoded))
	require.Equal(t, ep, decoded)

	// .config must include the live xds field and matching GetAttr length.
	cfgAttr, errno := v.GetAttr(ctx, ConfigInode, 0)
	require.Equal(t, syscall.Errno(0), errno)
	_, cfgFH, errno := v.Open(ctx, ConfigInode, syscall.O_RDONLY)
	require.Equal(t, syscall.Errno(0), errno)
	cfgBuf := make([]byte, cfgAttr.Attr.Length)
	cn, errno := v.Read(ctx, ConfigInode, cfgBuf, 0, cfgFH)
	require.Equal(t, syscall.Errno(0), errno)
	v.Release(ctx, ConfigInode, cfgFH)

	var cfgDecoded Config
	require.NoError(t, json.Unmarshal(cfgBuf[:cn], &cfgDecoded))
	require.NotNil(t, cfgDecoded.XDS)
	require.Equal(t, ep.SocketPath, cfgDecoded.XDS.SocketPath)

	// Readdir now includes .xds.
	fh, errno = v.Opendir(ctx, rootID, 0)
	require.Equal(t, syscall.Errno(0), errno)
	entries, _, errno = v.Readdir(ctx, rootID, 64<<10, 0, fh, false)
	require.Equal(t, syscall.Errno(0), errno)
	foundXDS := false
	for _, e := range entries {
		if string(e.Name) == ".xds" {
			foundXDS = true
		}
	}
	require.True(t, foundXDS)
	v.Releasedir(ctx, rootID, fh)

	// While active, .xds is protected as an internal virtual file.
	_, _, errno = v.Create(ctx, rootID, ".xds", 0644, 0, syscall.O_RDWR)
	require.Equal(t, syscall.EEXIST, errno)
	require.Equal(t, syscall.EPERM, v.Unlink(ctx, rootID, ".xds"))

	// Clearing endpoint hides .xds again while preserving .config GetAttr length.
	v.SetXDSEndpoint(nil)
	_, errno = v.Lookup(ctx, rootID, ".xds")
	require.Equal(t, syscall.ENOENT, errno)
	cfgAfterClear, errno := v.GetAttr(ctx, ConfigInode, 0)
	require.Equal(t, syscall.Errno(0), errno)
	require.Greater(t, cfgAfterClear.Attr.Length, uint64(0))
}

func TestVFSXDSResolverFlushFillSparseAndEndToEndServer(t *testing.T) {
	sockPath := filepath.Join(t.TempDir(), "xds.sock")
	v, eng, rt := createXDSTestVFS(t, sockPath)
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))

	// Create a file with two data regions separated by a sparse hole, plus a
	// trailing sparse hole extended via SetAttr — without calling v.Flush first.
	ent, fh, errno := v.Create(ctx, rootID, "tensor.bin", 0644, 0, syscall.O_RDWR)
	require.Equal(t, syscall.Errno(0), errno)

	partA := bytes.Repeat([]byte{0x3a}, 12288) // [0, 12288): spans 8192 + 4096
	partB := bytes.Repeat([]byte{0x7c}, 8192)  // [24576, 32768): aligned 8192 block
	require.Equal(t, syscall.Errno(0), v.Write(ctx, ent.Inode, partA, 0, fh))
	require.Equal(t, syscall.Errno(0), v.Write(ctx, ent.Inode, partB, 24576, fh))
	_, errno = v.SetAttr(ctx, ent.Inode, meta.SetAttrSize, 0, 0, 0, 0, 0, 0, 0, 0, 40960)
	require.Equal(t, syscall.Errno(0), errno)
	v.Release(ctx, ent.Inode, fh)

	// First ResolveRead flushes buffered dirty writes and warms the cache.
	resolver := v.NewXDSResolver(eng.Cache(), 1)
	exts, fsize, err := resolver.ResolveRead(context.Background(), uint64(ent.Inode), 0, 40960)
	require.NoError(t, err)
	require.Equal(t, uint64(40960), fsize)
	require.Len(t, exts, 5) // 8192 data + 4096 data + 12288 hole + 8192 data + 8192 hole
	require.False(t, exts[0].IsHole)
	require.Equal(t, uint32(8192), exts[0].Len)
	require.False(t, exts[1].IsHole)
	require.Equal(t, uint32(4096), exts[1].Len)
	require.True(t, exts[2].IsHole)
	require.Equal(t, uint64(12288), exts[2].DstOff)
	require.Equal(t, uint32(12288), exts[2].Len)
	require.False(t, exts[3].IsHole)
	require.Equal(t, uint64(24576), exts[3].DstOff)
	require.Equal(t, uint32(8192), exts[3].Len)
	require.True(t, exts[4].IsHole)
	require.Equal(t, uint64(32768), exts[4].DstOff)
	require.Equal(t, uint32(8192), exts[4].Len)

	// Evict all cached blocks from both xDS and JuiceFS POSIX cache, then verify
	// ResolveRead refills ONLY xds.BlockCache without polluting the POSIX cache.
	var chunkSlices []meta.Slice
	require.Equal(t, syscall.Errno(0), v.Meta.Read(meta.Background(), ent.Inode, 0, &chunkSlices))
	for _, sl := range chunkSlices {
		if sl.Id != 0 {
			require.NoError(t, v.Store.EvictCache(sl.Id, sl.Size, nil))
		}
	}
	for _, ex := range exts {
		if !ex.IsHole {
			eng.Cache().Remove(ex.Key)
			require.False(t, eng.Cache().Exist(ex.Key))
		}
	}
	exts2, _, err := resolver.ResolveRead(context.Background(), uint64(ent.Inode), 0, 40960)
	require.NoError(t, err)
	for _, ex := range exts2 {
		if !ex.IsHole {
			require.True(t, eng.Cache().Exist(ex.Key))
		}
	}
	for _, sl := range chunkSlices {
		if sl.Id != 0 {
			require.NoError(t, v.Store.CheckCache(sl.Id, sl.Size, nil, func(exists bool, _ string, _ int) {
				require.False(t, exists, "sidecar xDS fill must not pollute POSIX cache")
			}))
		}
	}

	// EOF, cancelled context, and special-inode rejection checks.
	eofExts, eofSize, err := resolver.ResolveRead(context.Background(), uint64(ent.Inode), 40960, 4096)
	require.NoError(t, err)
	require.Empty(t, eofExts)
	require.Equal(t, uint64(40960), eofSize)

	_, _, err = resolver.ResolveRead(context.Background(), uint64(ConfigInode), 0, 4096)
	require.ErrorIs(t, err, syscall.EINVAL)
	_, _, err = resolver.ResolveRead(context.Background(), uint64(XDSInode), 0, 4096)
	require.ErrorIs(t, err, syscall.EINVAL)

	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = resolver.ResolveRead(cctx, uint64(ent.Inode), 0, 4096)
	require.ErrorIs(t, err, context.Canceled)

	// End-to-end test over a real Unix SOCK_SEQPACKET xds.Server wired to XDSResolver.
	dummyPath := filepath.Join(t.TempDir(), "bind-target")
	require.NoError(t, os.WriteFile(dummyPath, []byte("x"), 0o600))
	dummyFile, err := os.Open(dummyPath)
	require.NoError(t, err)
	defer func() { _ = dummyFile.Close() }()

	var stx unix.Statx_t
	require.NoError(t, unix.Statx(int(dummyFile.Fd()), "", unix.AT_EMPTY_PATH, unix.STATX_INO, &stx))
	dummyDev := unix.Mkdev(stx.Dev_major, stx.Dev_minor)

	remapped := &inoRemapResolver{
		inner:    v.NewXDSResolver(eng.Cache(), dummyDev),
		fromIno:  stx.Ino,
		targetIn: uint64(ent.Inode),
	}
	srv, err := xds.NewServer(eng, remapped)
	require.NoError(t, err)
	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve() }()
	defer func() {
		require.NoError(t, srv.Close())
		require.NoError(t, <-serveDone)
	}()

	ep := srv.EndpointInfo()
	v.SetXDSEndpoint(&ep)

	cliFD, err := unix.Socket(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	defer func() { _ = unix.Close(cliFD) }()
	require.NoError(t, unix.Connect(cliFD, &unix.SockaddrUnix{Name: sockPath}))

	// 1. HELLO
	helloReq := make([]byte, 32)
	binary.LittleEndian.PutUint32(helloReq[0:4], 1) // abi_version
	binary.LittleEndian.PutUint32(helloReq[4:8], 1) // MSG_HELLO
	binary.LittleEndian.PutUint32(helloReq[8:12], 32)
	binary.LittleEndian.PutUint64(helloReq[16:24], 1)
	require.NoError(t, seqSend(cliFD, helloReq, -1))
	rep := seqRecv(t, cliFD)
	require.Equal(t, int32(0), int32(binary.LittleEndian.Uint32(rep[12:16])))

	// 2. BIND_FILE with SCM_RIGHTS
	bindReq := make([]byte, 24)
	binary.LittleEndian.PutUint32(bindReq[0:4], 1)
	binary.LittleEndian.PutUint32(bindReq[4:8], 3) // MSG_BIND_FILE
	binary.LittleEndian.PutUint32(bindReq[8:12], 24)
	binary.LittleEndian.PutUint64(bindReq[16:24], 2)
	require.NoError(t, seqSend(cliFD, bindReq, int(dummyFile.Fd())))
	rep = seqRecv(t, cliFD)
	require.Equal(t, int32(0), int32(binary.LittleEndian.Uint32(rep[12:16])))
	fileToken := binary.LittleEndian.Uint64(rep[24:32])
	require.NotZero(t, fileToken)

	// 3. REGISTER_BUF with SCM_RIGHTS (pipe fd as dummy p2p fd for fake runtime)
	p2pR, p2pW, err := os.Pipe()
	require.NoError(t, err)
	defer func() { _ = p2pR.Close(); _ = p2pW.Close() }()

	regReq := make([]byte, 368)
	binary.LittleEndian.PutUint32(regReq[0:4], 1)
	binary.LittleEndian.PutUint32(regReq[4:8], 6) // MSG_REGISTER_BUF
	binary.LittleEndian.PutUint32(regReq[8:12], 368)
	binary.LittleEndian.PutUint64(regReq[16:24], 3)
	binary.LittleEndian.PutUint64(regReq[24:32], 0x7f0000000000) // device_va
	binary.LittleEndian.PutUint64(regReq[32:40], 40960)          // size_bytes
	binary.LittleEndian.PutUint32(regReq[40:44], 1)              // peer_count
	binary.LittleEndian.PutUint32(regReq[48:52], 7)              // peers[0].handle
	binary.LittleEndian.PutUint64(regReq[64:72], 40960)          // peers[0].mapped_len
	require.NoError(t, seqSend(cliFD, regReq, int(p2pR.Fd())))
	rep = seqRecv(t, cliFD)
	require.Equal(t, int32(0), int32(binary.LittleEndian.Uint32(rep[12:16])))
	bufToken := binary.LittleEndian.Uint64(rep[24:32])
	require.NotZero(t, bufToken)

	// 4. READ_DIRECT [0, 40960) -> single destination slice
	readReq := make([]byte, 56+24)
	binary.LittleEndian.PutUint32(readReq[0:4], 1)
	binary.LittleEndian.PutUint32(readReq[4:8], 9) // MSG_READ_DIRECT
	binary.LittleEndian.PutUint32(readReq[8:12], uint32(len(readReq)))
	binary.LittleEndian.PutUint64(readReq[16:24], 4)
	binary.LittleEndian.PutUint64(readReq[24:32], fileToken)
	binary.LittleEndian.PutUint64(readReq[32:40], 0)
	binary.LittleEndian.PutUint64(readReq[40:48], 40960)
	binary.LittleEndian.PutUint32(readReq[48:52], 1) // slice_count
	binary.LittleEndian.PutUint64(readReq[56:64], bufToken)
	binary.LittleEndian.PutUint64(readReq[64:72], 0)
	binary.LittleEndian.PutUint64(readReq[72:80], 40960)
	require.NoError(t, seqSend(cliFD, readReq, -1))
	rep = seqRecv(t, cliFD)
	require.Equal(t, int32(0), int32(binary.LittleEndian.Uint32(rep[12:16])))
	require.Equal(t, uint64(40960), binary.LittleEndian.Uint64(rep[24:32])) // bytes_read
	holeCount := binary.LittleEndian.Uint32(rep[32:36])
	require.Equal(t, uint32(2), holeCount)
	// Hole 0: [12288, 24576), Hole 1: [32768, 40960)
	require.Equal(t, uint64(12288), binary.LittleEndian.Uint64(rep[40:48]))
	require.Equal(t, uint64(12288), binary.LittleEndian.Uint64(rep[48:56]))
	require.Equal(t, uint64(32768), binary.LittleEndian.Uint64(rep[64:72]))
	require.Equal(t, uint64(8192), binary.LittleEndian.Uint64(rep[72:80]))

	// Inspect simulated GPU VRAM to confirm DMA payload landed accurately.
	rt.mu.Lock()
	var gpuMem []byte
	for _, b := range rt.imported {
		gpuMem = append([]byte(nil), b.data...)
	}
	rt.mu.Unlock()
	require.True(t, bytes.Equal(partA, gpuMem[0:12288]))
	require.True(t, bytes.Equal(partB, gpuMem[24576:32768]))
}

type inoRemapResolver struct {
	inner    *XDSResolver
	fromIno  uint64
	targetIn uint64
}

func (r *inoRemapResolver) MountDev() uint64 { return r.inner.MountDev() }

func (r *inoRemapResolver) ResolveRead(ctx context.Context, ino, offset, length uint64) ([]xds.BlockExtent, uint64, error) {
	if ino == r.fromIno {
		ino = r.targetIn
	}
	return r.inner.ResolveRead(ctx, ino, offset, length)
}

func seqSend(fd int, payload []byte, attachFD int) error {
	var oob []byte
	if attachFD >= 0 {
		oob = unix.UnixRights(attachFD)
	}
	return unix.Sendmsg(fd, payload, oob, nil, unix.MSG_NOSIGNAL)
}

func seqRecv(t *testing.T, fd int) []byte {
	t.Helper()
	buf := make([]byte, 4096)
	n, _, _, _, err := unix.Recvmsg(fd, buf, nil, 0)
	require.NoError(t, err)
	return buf[:n]
}
