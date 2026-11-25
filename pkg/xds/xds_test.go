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

package xds

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

type fakeImportedBuf struct {
	ioBase uintptr
	data   []byte
}

// fakeRuntime simulates SPDK blobs, imported DMA aliases, and P2P batch reads
// completely in memory.
type fakeRuntime struct {
	mu             sync.Mutex
	ioUnit         uint64
	blobSize       uint64
	blobs          map[BlobHandle][]byte
	imported       map[RegHandle]*fakeImportedBuf
	nextReg        RegHandle
	activeImport   atomic.Int32
	lastReadCPULen atomic.Int64
	blockGate      chan struct{} // optional gate to hold DMA in flight
	submitNotify   chan struct{} // optional signal when ReadDirectBatch enters
}

func newFakeRuntime(ioUnit uint64) *fakeRuntime {
	return &fakeRuntime{
		ioUnit:   ioUnit,
		blobs:    make(map[BlobHandle][]byte),
		imported: make(map[RegHandle]*fakeImportedBuf),
		nextReg:  1,
	}
}

func (f *fakeRuntime) Start(cfg *Config) (RuntimeInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blobSize = cfg.BlobSize
	return RuntimeInfo{
		IOUnitSize:  f.ioUnit,
		ClusterSize: uint64(cfg.BlockSize),
		WorkerCount: 2,
		DirectReady: true,
	}, nil
}

func (f *fakeRuntime) Stop() error { return nil }

func (f *fakeRuntime) LeaseBlobs(count uint32) ([]BlobHandle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]BlobHandle, count)
	for i := range count {
		h := BlobHandle(i + 1)
		f.blobs[h] = make([]byte, f.blobSize)
		out[i] = h
	}
	return out, nil
}

func (f *fakeRuntime) ReleaseBlobs([]BlobHandle) error { return nil }

func (f *fakeRuntime) ImportBuffer(p2pFD int, deviceVA, size uint64, _ []PeerExport) (RegHandle, uintptr, error) {
	if p2pFD >= 0 {
		_ = unix.Close(p2pFD)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	reg := f.nextReg
	f.nextReg++
	ioBase := uintptr(reg) << 32
	f.imported[reg] = &fakeImportedBuf{
		ioBase: ioBase,
		data:   make([]byte, size),
	}
	f.activeImport.Add(1)
	return reg, ioBase, nil
}

func (f *fakeRuntime) UnregisterBuffer(reg RegHandle) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.imported[reg]; ok {
		delete(f.imported, reg)
		f.activeImport.Add(-1)
	}
	return nil
}

func (f *fakeRuntime) copyToAliasLocked(addr uintptr, src []byte) {
	for _, b := range f.imported {
		if addr >= b.ioBase && uint64(addr-b.ioBase)+uint64(len(src)) <= uint64(len(b.data)) {
			off := uint64(addr - b.ioBase)
			copy(b.data[off:off+uint64(len(src))], src)
			return
		}
	}
	panic(fmt.Sprintf("fakeRuntime: invalid alias address 0x%x len=%d", addr, len(src)))
}

func (f *fakeRuntime) ReadCPU(blob BlobHandle, blobOff uint64, dst []byte) error {
	f.lastReadCPULen.Store(int64(len(dst)))
	f.mu.Lock()
	data := f.blobs[blob]
	f.mu.Unlock()
	copy(dst, data[blobOff:blobOff+uint64(len(dst))])
	return nil
}

func (f *fakeRuntime) WriteCPU(blob BlobHandle, blobOff uint64, src []byte) error {
	f.mu.Lock()
	data := f.blobs[blob]
	f.mu.Unlock()
	copy(data[blobOff:blobOff+uint64(len(src))], src)
	return nil
}

func (f *fakeRuntime) setGate(gate, notify chan struct{}) {
	f.mu.Lock()
	f.blockGate = gate
	f.submitNotify = notify
	f.mu.Unlock()
}

func (f *fakeRuntime) ReadDirectBatch(descs []DirectReadDesc) error {
	f.mu.Lock()
	gate := f.blockGate
	notify := f.submitNotify
	f.mu.Unlock()

	if notify != nil {
		select {
		case notify <- struct{}{}:
		default:
		}
	}
	if gate != nil {
		<-gate
	}

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

// TestProtocolLayout asserts that every Go wire struct matches the exact sizes
// verified by _Static_assert in proto.h.
func TestProtocolLayout(t *testing.T) {
	require.Equal(t, uintptr(24), unsafe.Sizeof(msgHdr{}))
	require.Equal(t, uintptr(32), unsafe.Sizeof(helloMsg{}))
	require.Equal(t, uintptr(192), unsafe.Sizeof(helloReplyMsg{}))
	require.Equal(t, uintptr(48), unsafe.Sizeof(bindFileReplyMsg{}))
	require.Equal(t, uintptr(32), unsafe.Sizeof(tokenMsg{}))
	require.Equal(t, uintptr(40), unsafe.Sizeof(peerExportWire{}))
	require.Equal(t, uintptr(368), unsafe.Sizeof(registerBufMsg{}))
	require.Equal(t, uintptr(24), unsafe.Sizeof(wireSlice{}))
	require.Equal(t, uintptr(24), unsafe.Sizeof(HoleSpan{}))
	require.Equal(t, uintptr(0), unsafe.Offsetof(HoleSpan{}.DstOffset))
	require.Equal(t, uintptr(8), unsafe.Offsetof(HoleSpan{}.Length))
	require.Equal(t, uintptr(16), unsafe.Offsetof(HoleSpan{}.SliceIndex))
	require.Equal(t, uintptr(56), unsafe.Sizeof(readDirectReqMsg{}))
	require.Equal(t, uintptr(40), unsafe.Sizeof(readDirectReplyMsg{}))
}

func TestConfigJSONAndNormalize(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "xds.json")
	raw := `{
		"bdev_name": "Nvme0n1",
		"xds_device_backend": "ascend",
		"xds_blob_count": 4,
		"xds_peer_bdfs": ["0000:3b:00.0"]
	}`
	require.NoError(t, os.WriteFile(cfgPath, []byte(raw), 0o600))

	cfg, err := LoadConfig(cfgPath)
	require.NoError(t, err)
	require.Equal(t, BackendAscend, cfg.DeviceBackend)
	require.Equal(t, 4<<20, cfg.BlockSize)
	require.Equal(t, uint32(64), cfg.MaxInflight)

	encoded, err := json.Marshal(cfg)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"xds_device_backend":"ascend"`)

	var bad Config
	require.ErrorIs(t, json.Unmarshal([]byte(`{"xds_device_backend":"unknown"}`), &bad), ErrInvalidArg)
	bad = Config{BlobCount: 1, DeviceBackend: DeviceBackend(99)}
	require.ErrorIs(t, bad.Normalize(), ErrInvalidArg)

	// PeerBDFs count > maxPeers, empty/overlong BDFs, and TotalSlots overflow must fail.
	tooManyPeers := make([]string, maxPeers+1)
	for i := range tooManyPeers {
		tooManyPeers[i] = fmt.Sprintf("0000:3b:%02x.0", i)
	}
	bad = Config{BlobCount: 1, PeerBDFs: tooManyPeers}
	require.ErrorIs(t, bad.Normalize(), ErrInvalidArg)

	bad = Config{BlobCount: 1, PeerBDFs: []string{""}}
	require.ErrorIs(t, bad.Normalize(), ErrInvalidArg)
	bad = Config{BlobCount: 1, PeerBDFs: []string{"0000:3b:00.01234"}} // 16 bytes -> no NUL room
	require.ErrorIs(t, bad.Normalize(), ErrInvalidArg)

	bad = Config{BlockSize: 4096, BlobSize: 4096 * 1024, BlobCount: ^uint32(0)}
	require.ErrorIs(t, bad.Normalize(), ErrInvalidArg)
}

func TestBlockCachePinSafeEvictionCRCExpireAndWindowRead(t *testing.T) {
	cfg := &Config{
		BlockSize:      8192,
		BlobSize:       16384, // 2 slots per blob
		BlobCount:      2,     // 4 total slots
		VerifyChecksum: true,
	}
	rt := newFakeRuntime(4096)
	eng, err := NewEngine(cfg, rt, prometheus.NewRegistry())
	require.NoError(t, err)
	defer func() { require.NoError(t, eng.Close()) }()

	cache := eng.Cache()
	for i := range 4 {
		payload := bytes.Repeat([]byte{byte('a' + i)}, 8192)
		require.NoError(t, cache.Put(fmt.Sprintf("k%d", i), payload))
	}

	// Pin k0, k2, k3 so only k1 is eligible for eviction.
	p0, ok := cache.Pin("k0")
	require.True(t, ok)
	p2, ok := cache.Pin("k2")
	require.True(t, ok)
	p3, ok := cache.Pin("k3")
	require.True(t, ok)

	require.NoError(t, cache.Put("k4", bytes.Repeat([]byte{'e'}, 8192)))
	require.True(t, cache.Exist("k0"))
	require.False(t, cache.Exist("k1"))
	require.True(t, cache.Exist("k2"))
	require.True(t, cache.Exist("k3"))
	require.True(t, cache.Exist("k4"))

	// All 4 slots pinned -> ErrNoSpace; removing a pinned key defers reclamation.
	p4, ok := cache.Pin("k4")
	require.True(t, ok)
	require.ErrorIs(t, cache.Put("k5", bytes.Repeat([]byte{'f'}, 8192)), ErrNoSpace)

	cache.Remove("k0")
	require.False(t, cache.Exist("k0"))
	require.ErrorIs(t, cache.Put("k5", bytes.Repeat([]byte{'f'}, 8192)), ErrNoSpace)

	cache.Unpin(p0.SlotIndex)
	require.NoError(t, cache.Put("k5", bytes.Repeat([]byte{'f'}, 8192)))
	require.True(t, cache.Exist("k5"))

	cache.Unpin(p2.SlotIndex)
	cache.Unpin(p3.SlotIndex)
	cache.Unpin(p4.SlotIndex)

	// Verify ReadAt + CRC32C check and corruption eviction.
	got := make([]byte, 8192)
	n, err := cache.ReadAt("k4", got, 0)
	require.NoError(t, err)
	require.Equal(t, 8192, n)
	require.Equal(t, bytes.Repeat([]byte{'e'}, 8192), got)

	cnt, usedBytes := cache.Stats()
	require.Equal(t, int64(4), cnt)
	require.Equal(t, int64(4*8192), usedBytes)

	rt.mu.Lock()
	rt.blobs[p4.Blob][p4.BlobOffset] ^= 0xff
	rt.mu.Unlock()
	_, err = cache.ReadAt("k4", got, 0)
	require.ErrorIs(t, err, ErrCorrupt)
	require.False(t, cache.Exist("k4"))

	// Unaligned non-CRC ReadAt reads only the minimal IOUnit window.
	winRT := newFakeRuntime(4096)
	winEng, err := NewEngine(&Config{
		BlockSize:   65536,
		BlobSize:    65536,
		BlobCount:   1,
		CacheExpire: 40 * time.Millisecond,
	}, winRT, prometheus.NewRegistry())
	require.NoError(t, err)
	defer func() { require.NoError(t, winEng.Close()) }()

	block := make([]byte, 65536)
	for i := range block {
		block[i] = byte(i % 251)
	}
	require.NoError(t, winEng.Cache().Put("big", block))

	sub := make([]byte, 100)
	n, err = winEng.Cache().ReadAt("big", sub, 4100)
	require.NoError(t, err)
	require.Equal(t, 100, n)
	require.Equal(t, block[4100:4200], sub)
	require.Equal(t, int64(4096), winRT.lastReadCPULen.Load())

	require.Eventually(t, func() bool {
		return !winEng.Cache().Exist("big")
	}, time.Second, 10*time.Millisecond)

	// Put must overwrite an expired entry directly without needing Pin first,
	// and recycle the expired slot so slot count stays 1.
	fresh := bytes.Repeat([]byte{0x7a}, 65536)
	require.NoError(t, winEng.Cache().Put("big", fresh))
	require.True(t, winEng.Cache().Exist("big"))
	cnt, usedBytes = winEng.Cache().Stats()
	require.Equal(t, int64(1), cnt)
	require.Equal(t, int64(65536), usedBytes)
	n, err = winEng.Cache().ReadAt("big", sub, 4100)
	require.NoError(t, err)
	require.Equal(t, 100, n)
	require.Equal(t, fresh[4100:4200], sub)

	// Wait for fresh entry to expire and verify Pin lazily deletes it.
	require.Eventually(t, func() bool {
		return !winEng.Cache().Exist("big")
	}, time.Second, 10*time.Millisecond)
	_, ok = winEng.Cache().Pin("big")
	require.False(t, ok)

	// Verify Engine.ImportBuffer consumes p2pFD even when Engine is closed.
	require.NoError(t, winEng.Close())
	evFD, err := unix.Eventfd(0, unix.EFD_CLOEXEC)
	require.NoError(t, err)
	_, _, err = winEng.ImportBuffer(evFD, 0x1000, 4096, nil)
	require.ErrorIs(t, err, ErrClosed)
	_, err = unix.FcntlInt(uintptr(evFD), unix.F_GETFD, 0)
	require.ErrorIs(t, err, syscall.EBADF)
}

func TestBlockCacheConcurrentPinEvict(t *testing.T) {
	cfg := &Config{
		BlockSize:      4096,
		BlobSize:       16384,
		BlobCount:      4,
		VerifyChecksum: true,
	}
	eng, err := NewEngine(cfg, newFakeRuntime(4096), prometheus.NewRegistry())
	require.NoError(t, err)
	defer func() { require.NoError(t, eng.Close()) }()

	cache := eng.Cache()
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			buf := make([]byte, 4096)
			for i := range 64 {
				key := fmt.Sprintf("blk-%d", (id*7+i)%32)
				payload := bytes.Repeat([]byte{byte((id*7 + i) % 32)}, 4096)
				_ = cache.Put(key, payload)
				if _, err := cache.ReadAt(key, buf, 0); err == nil {
					require.Equal(t, payload[0], buf[0])
				}
			}
		}(g)
	}
	wg.Wait()
}

type fakeResolver struct {
	mu       sync.RWMutex
	dev      uint64
	fileSize uint64
	extents  []BlockExtent
}

func (r *fakeResolver) MountDev() uint64 { return r.dev }

func (r *fakeResolver) setExtents(fileSize uint64, extents []BlockExtent) {
	r.mu.Lock()
	r.fileSize = fileSize
	r.extents = extents
	r.mu.Unlock()
}

func (r *fakeResolver) ResolveRead(_ context.Context, _ uint64, _, _ uint64) ([]BlockExtent, uint64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.extents, r.fileSize, nil
}

// testClient talks directly to Server over Unix SOCK_SEQPACKET.
type testClient struct {
	t      *testing.T
	conn   *net.UnixConn
	nextID uint64
}

func dialTestClient(t *testing.T, sockPath string) (*testClient, helloReplyMsg) {
	t.Helper()
	conn, err := net.DialUnix("unixpacket", nil, &net.UnixAddr{Name: sockPath, Net: "unixpacket"})
	require.NoError(t, err)

	hello := helloMsg{
		Hdr: msgHdr{
			ABIVersion: protoABIVersion,
			Type:       msgHello,
			MsgSize:    uint32(unsafe.Sizeof(helloMsg{})),
			ReqID:      1,
		},
		ClientPID: uint32(os.Getpid()),
	}
	_, _, err = conn.WriteMsgUnix(structBytes(&hello), nil, nil)
	require.NoError(t, err)

	buf := make([]byte, 4096)
	n, _, _, _, err := conn.ReadMsgUnix(buf, nil)
	require.NoError(t, err)
	require.Equal(t, int(unsafe.Sizeof(helloReplyMsg{})), n)
	rep := *(*helloReplyMsg)(unsafe.Pointer(&buf[0]))
	require.Equal(t, int32(0), rep.Hdr.Status)
	return &testClient{t: t, conn: conn, nextID: 2}, rep
}

func (c *testClient) close() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

func (c *testClient) bindFile(fd int) (bindFileReplyMsg, int32) {
	c.t.Helper()
	id := c.nextID
	c.nextID++
	req := msgHdr{
		ABIVersion: protoABIVersion,
		Type:       msgBindFile,
		MsgSize:    uint32(unsafe.Sizeof(msgHdr{})),
		ReqID:      id,
	}
	_, _, err := c.conn.WriteMsgUnix(structBytes(&req), unix.UnixRights(fd), nil)
	require.NoError(c.t, err)

	buf := make([]byte, 4096)
	n, _, _, _, err := c.conn.ReadMsgUnix(buf, nil)
	require.NoError(c.t, err)
	hdr := *(*msgHdr)(unsafe.Pointer(&buf[0]))
	if hdr.Type == msgStatus {
		return bindFileReplyMsg{Hdr: hdr}, hdr.Status
	}
	require.Equal(c.t, int(unsafe.Sizeof(bindFileReplyMsg{})), n)
	rep := *(*bindFileReplyMsg)(unsafe.Pointer(&buf[0]))
	return rep, rep.Hdr.Status
}

func (c *testClient) registerBuf(size uint64) uint64 {
	c.t.Helper()
	id := c.nextID
	c.nextID++
	dummyFD, err := unix.Eventfd(0, unix.EFD_CLOEXEC)
	require.NoError(c.t, err)
	req := registerBufMsg{
		Hdr: msgHdr{
			ABIVersion: protoABIVersion,
			Type:       msgRegisterBuf,
			MsgSize:    uint32(unsafe.Sizeof(registerBufMsg{})),
			ReqID:      id,
		},
		DeviceVA:  0x7000000000,
		SizeBytes: size,
		PeerCount: 1,
	}
	_, _, err = c.conn.WriteMsgUnix(structBytes(&req), unix.UnixRights(dummyFD), nil)
	_ = unix.Close(dummyFD)
	require.NoError(c.t, err)

	buf := make([]byte, 4096)
	_, _, _, _, err = c.conn.ReadMsgUnix(buf, nil)
	require.NoError(c.t, err)
	rep := *(*tokenMsg)(unsafe.Pointer(&buf[0]))
	require.Equal(c.t, int32(0), rep.Hdr.Status)
	return rep.Token
}

func (c *testClient) sendUnregisterBuf(token uint64) uint64 {
	c.t.Helper()
	id := c.nextID
	c.nextID++
	msg := tokenMsg{
		Hdr: msgHdr{
			ABIVersion: protoABIVersion,
			Type:       msgUnregisterBuf,
			MsgSize:    uint32(unsafe.Sizeof(tokenMsg{})),
			ReqID:      id,
		},
		Token: token,
	}
	_, _, err := c.conn.WriteMsgUnix(structBytes(&msg), nil, nil)
	require.NoError(c.t, err)
	return id
}

func (c *testClient) sendReadDirect(fileTok, fileOff, length uint64, slices []wireSlice) uint64 {
	c.t.Helper()
	id := c.nextID
	c.nextID++
	hdrSize := int(unsafe.Sizeof(readDirectReqMsg{}))
	sliceBytes := len(slices) * int(unsafe.Sizeof(wireSlice{}))
	req := readDirectReqMsg{
		Hdr: msgHdr{
			ABIVersion: protoABIVersion,
			Type:       msgReadDirect,
			MsgSize:    uint32(hdrSize + sliceBytes),
			ReqID:      id,
		},
		FileToken:  fileTok,
		FileOffset: fileOff,
		Length:     length,
		SliceCount: uint32(len(slices)),
	}
	pkt := make([]byte, hdrSize+sliceBytes)
	copy(pkt[:hdrSize], structBytes(&req))
	if sliceBytes > 0 {
		copy(pkt[hdrSize:], unsafe.Slice((*byte)(unsafe.Pointer(&slices[0])), sliceBytes))
	}
	_, _, err := c.conn.WriteMsgUnix(pkt, nil, nil)
	require.NoError(c.t, err)
	return id
}

func (c *testClient) recvReply() (msgHdr, readDirectReplyMsg, []HoleSpan) {
	c.t.Helper()
	buf := make([]byte, 4096)
	n, _, _, _, err := c.conn.ReadMsgUnix(buf, nil)
	require.NoError(c.t, err)
	hdr := *(*msgHdr)(unsafe.Pointer(&buf[0]))
	if hdr.Type != msgReadDirectReply {
		return hdr, readDirectReplyMsg{Hdr: hdr}, nil
	}
	repSize := int(unsafe.Sizeof(readDirectReplyMsg{}))
	require.GreaterOrEqual(c.t, n, repSize)
	rep := *(*readDirectReplyMsg)(unsafe.Pointer(&buf[0]))
	var holes []HoleSpan
	if rep.HoleCount > 0 {
		holes = make([]HoleSpan, rep.HoleCount)
		copy(holes, unsafe.Slice((*HoleSpan)(unsafe.Pointer(&buf[repSize])), rep.HoleCount))
	}
	return hdr, rep, holes
}

func TestServerDirectReadOpsPipeliningAndValidation(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "xds.sock")
	samplePath := filepath.Join(dir, "weights.bin")
	require.NoError(t, os.WriteFile(samplePath, make([]byte, 16384), 0o600))

	f, err := os.Open(samplePath)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	var stx unix.Statx_t
	require.NoError(t, unix.Statx(int(f.Fd()), "", unix.AT_EMPTY_PATH, unix.STATX_INO, &stx))
	mountDev := unix.Mkdev(stx.Dev_major, stx.Dev_minor)

	cfg := &Config{
		BlockSize:  8192,
		BlobSize:   16384,
		BlobCount:  2,
		PeerBDFs:   []string{"0000:3b:00.0"},
		SocketPath: sockPath,
	}
	rt := newFakeRuntime(4096)
	eng, err := NewEngine(cfg, rt, prometheus.NewRegistry())
	require.NoError(t, err)
	defer func() { require.NoError(t, eng.Close()) }()

	blk0 := bytes.Repeat([]byte{'A'}, 8192)
	require.NoError(t, eng.Cache().Put("blk0", blk0))

	resolver := &fakeResolver{
		dev:      mountDev,
		fileSize: 16384,
		extents: []BlockExtent{
			{Key: "blk0", BlockOff: 0, Len: 8192, DstOff: 0},
			{Len: 8192, DstOff: 8192, IsHole: true},
		},
	}

	srv, err := NewServer(eng, resolver)
	require.NoError(t, err)
	go func() { _ = srv.Serve() }()
	defer func() { require.NoError(t, srv.Close()) }()

	c, helloRep := dialTestClient(t, sockPath)
	defer c.close()
	require.Equal(t, uint32(64), helloRep.MaxInflight)
	require.Equal(t, uint64(4096), helloRep.IOUnitSize)

	// BIND_FILE rejects write-only fd (-EACCES) and cross-device fd (-EXDEV).
	woFile, err := os.OpenFile(samplePath, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, st := c.bindFile(int(woFile.Fd()))
	_ = woFile.Close()
	require.Equal(t, -int32(syscall.EACCES), st)

	devNull, err := os.Open("/dev/null")
	require.NoError(t, err)
	_, st = c.bindFile(int(devNull.Fd()))
	_ = devNull.Close()
	require.Equal(t, -int32(syscall.EXDEV), st)

	bindRep, st := c.bindFile(int(f.Fd()))
	require.Equal(t, int32(0), st)
	require.Equal(t, stx.Ino, bindRep.Inode)

	bufTok := c.registerBuf(32768)
	require.Equal(t, int32(1), rt.activeImport.Load())

	// 1. Scalar 1-slice READ_DIRECT + sparse hole.
	id := c.sendReadDirect(bindRep.FileToken, 0, 16384, []wireSlice{
		{BufToken: bufTok, BufOffset: 0, Length: 16384},
	})
	hdr, rep, holes := c.recvReply()
	require.Equal(t, id, hdr.ReqID)
	require.Equal(t, int32(0), hdr.Status)
	require.Equal(t, uint64(16384), rep.BytesRead)
	require.Equal(t, []HoleSpan{{DstOffset: 8192, Length: 8192, SliceIndex: 0}}, holes)

	rt.mu.Lock()
	gpuMem := append([]byte(nil), rt.imported[1].data...)
	rt.mu.Unlock()
	require.Equal(t, blk0, gpuMem[:8192])

	// 2. Scatter multi-slice READ_DIRECT across two non-contiguous VRAM ranges.
	id = c.sendReadDirect(bindRep.FileToken, 0, 16384, []wireSlice{
		{BufToken: bufTok, BufOffset: 4096, Length: 4096},
		{BufToken: bufTok, BufOffset: 16384, Length: 12288},
	})
	hdr, rep, holes = c.recvReply()
	require.Equal(t, id, hdr.ReqID)
	require.Equal(t, int32(0), hdr.Status)
	require.Equal(t, []HoleSpan{{DstOffset: 16384 + 4096, Length: 8192, SliceIndex: 1}}, holes)

	rt.mu.Lock()
	gpuMem = append([]byte(nil), rt.imported[1].data...)
	rt.mu.Unlock()
	require.Equal(t, bytes.Repeat([]byte{'A'}, 4096), gpuMem[4096:8192])
	require.Equal(t, bytes.Repeat([]byte{'A'}, 4096), gpuMem[16384:16384+4096])

	// 3. Pipelined concurrent READ_DIRECT requests matched by req_id.
	wantIDs := make(map[uint64]bool, 4)
	for range 4 {
		rid := c.sendReadDirect(bindRep.FileToken, 0, 16384, []wireSlice{
			{BufToken: bufTok, BufOffset: 0, Length: 16384},
		})
		wantIDs[rid] = true
	}
	for range 4 {
		hdr, rep, _ = c.recvReply()
		require.True(t, wantIDs[hdr.ReqID])
		delete(wantIDs, hdr.ReqID)
		require.Equal(t, int32(0), hdr.Status)
		require.Equal(t, uint64(16384), rep.BytesRead)
	}

	// 4. SliceCount > maxSlices returns -EINVAL.
	badReq := readDirectReqMsg{
		Hdr: msgHdr{
			ABIVersion: protoABIVersion,
			Type:       msgReadDirect,
			MsgSize:    uint32(unsafe.Sizeof(readDirectReqMsg{})),
			ReqID:      900,
		},
		FileToken:  bindRep.FileToken,
		Length:     4096,
		SliceCount: maxSlices + 1,
	}
	_, _, err = c.conn.WriteMsgUnix(structBytes(&badReq), nil, nil)
	require.NoError(t, err)
	hdr, _, _ = c.recvReply()
	require.Equal(t, -int32(syscall.EINVAL), hdr.Status)

	// 5. Misaligned extent returns -EINVAL without leaking slot pins.
	resolver.setExtents(500, []BlockExtent{{Key: "blk0", BlockOff: 0, Len: 500, DstOff: 0}})
	c.sendReadDirect(bindRep.FileToken, 0, 500, []wireSlice{{BufToken: bufTok, BufOffset: 0, Length: 500}})
	hdr, _, _ = c.recvReply()
	require.Equal(t, -int32(syscall.EINVAL), hdr.Status)

	// 6. More than maxHoles disjoint sparse holes aborts with -E2BIG and leaks 0 pins.
	manyHoles := make([]BlockExtent, 0, (maxHoles+1)*2)
	var pos uint64
	for i := range maxHoles + 1 {
		if i > 0 {
			manyHoles = append(manyHoles, BlockExtent{Key: "blk0", BlockOff: 0, Len: 4096, DstOff: pos})
			pos += 4096
		}
		manyHoles = append(manyHoles, BlockExtent{Len: 4096, DstOff: pos, IsHole: true})
		pos += 4096
	}
	bigBufTok := c.registerBuf(pos)
	resolver.setExtents(pos, manyHoles)
	c.sendReadDirect(bindRep.FileToken, 0, pos, []wireSlice{{BufToken: bigBufTok, BufOffset: 0, Length: pos}})
	hdr, _, _ = c.recvReply()
	require.Equal(t, -int32(syscall.E2BIG), hdr.Status)
	for i := range eng.Cache().slotPins {
		require.Equal(t, uint32(0), eng.Cache().slotPins[i].Load())
	}

	// 7. Resolver extent gap, short coverage, and OrigLen overrun must fail with -EIO (no silent mis-offset).
	resolver.setExtents(8192, []BlockExtent{{Key: "blk0", BlockOff: 0, Len: 4096, DstOff: 4096}})
	c.sendReadDirect(bindRep.FileToken, 0, 8192, []wireSlice{{BufToken: bufTok, BufOffset: 0, Length: 8192}})
	hdr, _, _ = c.recvReply()
	require.Equal(t, -int32(syscall.EIO), hdr.Status)

	resolver.setExtents(8192, []BlockExtent{{Key: "blk0", BlockOff: 0, Len: 4096, DstOff: 0}})
	c.sendReadDirect(bindRep.FileToken, 0, 8192, []wireSlice{{BufToken: bufTok, BufOffset: 0, Length: 8192}})
	hdr, _, _ = c.recvReply()
	require.Equal(t, -int32(syscall.EIO), hdr.Status)

	require.NoError(t, eng.Cache().Put("shortBlk", bytes.Repeat([]byte{'S'}, 4096)))
	resolver.setExtents(8192, []BlockExtent{{Key: "shortBlk", BlockOff: 0, Len: 8192, DstOff: 0}})
	c.sendReadDirect(bindRep.FileToken, 0, 8192, []wireSlice{{BufToken: bufTok, BufOffset: 0, Length: 8192}})
	hdr, _, _ = c.recvReply()
	require.Equal(t, -int32(syscall.EIO), hdr.Status)

	for i := range eng.Cache().slotPins {
		require.Equal(t, uint32(0), eng.Cache().slotPins[i].Load())
	}

	// 8. BIND_FILE with unexpected payload size returns -EMSGSIZE.
	badBind := tokenMsg{
		Hdr: msgHdr{
			ABIVersion: protoABIVersion,
			Type:       msgBindFile,
			MsgSize:    uint32(unsafe.Sizeof(tokenMsg{})),
			ReqID:      901,
		},
	}
	_, _, err = c.conn.WriteMsgUnix(structBytes(&badBind), unix.UnixRights(int(f.Fd())), nil)
	require.NoError(t, err)
	hdr, _, _ = c.recvReply()
	require.Equal(t, -int32(syscall.EMSGSIZE), hdr.Status)
}

func TestServerUnregisterDrainBackpressureAndCrashCleanup(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "xds.sock")
	samplePath := filepath.Join(dir, "data.bin")
	require.NoError(t, os.WriteFile(samplePath, make([]byte, 8192), 0o600))

	f, err := os.Open(samplePath)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	var stx unix.Statx_t
	require.NoError(t, unix.Statx(int(f.Fd()), "", unix.AT_EMPTY_PATH, unix.STATX_INO, &stx))
	mountDev := unix.Mkdev(stx.Dev_major, stx.Dev_minor)

	cfg := &Config{
		BlockSize:   8192,
		BlobSize:    16384,
		BlobCount:   2,
		PeerBDFs:    []string{"0000:3b:00.0"},
		SocketPath:  sockPath,
		MaxInflight: 2,
	}
	rt := newFakeRuntime(4096)
	eng, err := NewEngine(cfg, rt, prometheus.NewRegistry())
	require.NoError(t, err)
	defer func() { require.NoError(t, eng.Close()) }()

	require.NoError(t, eng.Cache().Put("blk0", bytes.Repeat([]byte{'Z'}, 8192)))
	resolver := &fakeResolver{
		dev:      mountDev,
		fileSize: 8192,
		extents:  []BlockExtent{{Key: "blk0", BlockOff: 0, Len: 8192, DstOff: 0}},
	}

	srv, err := NewServer(eng, resolver)
	require.NoError(t, err)
	go func() { _ = srv.Serve() }()
	defer func() { require.NoError(t, srv.Close()) }()

	// Part A: Pipelined UNREGISTER_BUF must wait for inflight READ_DIRECT DMA to finish.
	c1, _ := dialTestClient(t, sockPath)
	bind1, st := c1.bindFile(int(f.Fd()))
	require.Equal(t, int32(0), st)
	bufTok1 := c1.registerBuf(8192)
	require.Equal(t, int32(1), rt.activeImport.Load())

	gate := make(chan struct{})
	submitted := make(chan struct{}, 4)
	rt.setGate(gate, submitted)

	readID := c1.sendReadDirect(bind1.FileToken, 0, 8192, []wireSlice{
		{BufToken: bufTok1, BufOffset: 0, Length: 8192},
	})
	<-submitted
	unregID := c1.sendUnregisterBuf(bufTok1)

	// Buffer must stay imported while DMA is held behind gate.
	time.Sleep(30 * time.Millisecond)
	require.Equal(t, int32(1), rt.activeImport.Load())

	close(gate)
	got := map[uint64]int32{}
	for range 2 {
		hdr, _, _ := c1.recvReply()
		got[hdr.ReqID] = hdr.Status
	}
	require.Equal(t, int32(0), got[readID])
	require.Equal(t, int32(0), got[unregID])
	require.Equal(t, int32(0), rt.activeImport.Load())
	c1.close()

	// Part B: Client saturates MaxInflight=2, pipelines a 3rd read, then crashes abruptly.
	gate2 := make(chan struct{})
	submitted2 := make(chan struct{}, 4)
	rt.setGate(gate2, submitted2)

	c2, _ := dialTestClient(t, sockPath)
	bind2, st := c2.bindFile(int(f.Fd()))
	require.Equal(t, int32(0), st)
	bufTok2 := c2.registerBuf(8192)
	require.Equal(t, int32(1), rt.activeImport.Load())

	for range 3 {
		c2.sendReadDirect(bind2.FileToken, 0, 8192, []wireSlice{
			{BufToken: bufTok2, BufOffset: 0, Length: 8192},
		})
	}
	// First 2 reads enter DMA immediately; 3rd waits on MaxInflight=2 backpressure.
	<-submitted2
	<-submitted2

	// Crash client without reading replies or calling UNREGISTER_BUF, then unblock DMA.
	c2.close()
	close(gate2)
	require.Eventually(t, func() bool {
		return rt.activeImport.Load() == 0
	}, 2*time.Second, 5*time.Millisecond)
}

func TestServerCloseUnblocksIdleClient(t *testing.T) {
	sockPath := filepath.Join(t.TempDir(), "xds.sock")
	cfg := &Config{
		BlockSize:  8192,
		BlobSize:   16384,
		BlobCount:  1,
		SocketPath: sockPath,
	}
	eng, err := NewEngine(cfg, newFakeRuntime(4096), prometheus.NewRegistry())
	require.NoError(t, err)
	defer func() { require.NoError(t, eng.Close()) }()

	srv, err := NewServer(eng, &fakeResolver{dev: 1})
	require.NoError(t, err)
	go func() { _ = srv.Serve() }()

	conn, err := net.DialUnix("unixpacket", nil, &net.UnixAddr{Name: sockPath, Net: "unixpacket"})
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	done := make(chan error, 1)
	go func() { done <- srv.Close() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Server.Close blocked on idle client connection")
	}
}
