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
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Wire protocol constants and binary structures mirroring proto.h.
const (
	protoABIVersion    = 1
	maxSlices          = 64
	maxHoles           = 32
	maxFilesPerSession = 4096
	maxBufsPerSession  = 1024
)

const (
	msgHello = iota + 1
	msgHelloReply
	msgBindFile
	msgBindFileReply
	msgUnbindFile
	msgRegisterBuf
	msgRegisterBufReply
	msgUnregisterBuf
	msgReadDirect
	msgReadDirectReply
	msgStatus
)

type msgHdr struct {
	ABIVersion uint32
	Type       uint32
	MsgSize    uint32
	Status     int32
	ReqID      uint64
}

type helloMsg struct {
	Hdr       msgHdr
	ClientPID uint32
	Reserved  uint32
}

type helloReplyMsg struct {
	Hdr             msgHdr
	MountGeneration uint64
	IOUnitSize      uint64
	BlockSize       uint64
	MaxInflight     uint32
	DeviceBackend   uint32
	PeerCount       uint32
	Reserved        uint32
	PeerBDFs        [maxPeers][bdfLen]byte
}

type bindFileReplyMsg struct {
	Hdr       msgHdr
	FileToken uint64
	Inode     uint64
	FileSize  uint64
}

type tokenMsg struct {
	Hdr   msgHdr
	Token uint64
}

type peerExportWire struct {
	Handle       int32
	Backend      uint32
	PageSize     uint64
	MappedLen    uint64
	SegmentCount uint32
	Reserved     uint32
	FirstDMAAddr uint64
}

type registerBufMsg struct {
	Hdr       msgHdr
	DeviceVA  uint64
	SizeBytes uint64
	PeerCount uint32
	Reserved  uint32
	Peers     [maxPeers]peerExportWire
}

type wireSlice struct {
	BufToken  uint64
	BufOffset uint64
	Length    uint64
}

type readDirectReqMsg struct {
	Hdr        msgHdr
	FileToken  uint64
	FileOffset uint64
	Length     uint64
	SliceCount uint32
	Flags      uint32
}

type readDirectReplyMsg struct {
	Hdr       msgHdr
	BytesRead uint64
	HoleCount uint32
	Reserved  uint32
}

// BlockExtent describes one resolved JuiceFS block segment (or sparse hole)
// covering a portion of a client read request.
type BlockExtent struct {
	// Key is the JuiceFS object/cache block key (empty when IsHole is true).
	Key string
	// BlockOff is the byte offset inside the JuiceFS block.
	BlockOff uint32
	// Len is the byte length contributed by this extent.
	Len uint32
	// DstOff is the logical byte offset relative to the start of the read.
	DstOff uint64
	// IsHole indicates a logical zero-filled region (JuiceFS slice ID == 0).
	IsHole bool
}

// FileResolver bridges JuiceFS VFS/ChunkStore metadata resolution into pkg/xds
// without introducing a circular package import.
type FileResolver interface {
	// MountDev returns the Linux device number (st_dev) of the JuiceFS mount.
	MountDev() uint64

	// ResolveRead flushes any overlapping dirty writes for ino, resolves the
	// file range [offset, offset+length) into ordered BlockExtents, and ensures
	// every non-hole block is populated in the shared xds.Engine cache.
	ResolveRead(ctx context.Context, ino uint64, offset, length uint64) (extents []BlockExtent, fileSize uint64, err error)
}

type importedBuf struct {
	reg      RegHandle
	ioPtr    uintptr
	size     uint64
	inflight sync.WaitGroup
}

type clientSession struct {
	server      *Server
	conn        *net.UnixConn
	ctx         context.Context
	cancel      context.CancelFunc
	writeMu     sync.Mutex
	inflightSem chan struct{}
	ioWG        sync.WaitGroup

	// Owned exclusively by the single handleConn reader goroutine.
	nextToken uint64
	files     map[uint64]uint64 // fileToken -> inode
	bufs      map[uint64]*importedBuf
}

// Server serves cross-process direct-read requests over a Unix SOCK_SEQPACKET
// socket for one JuiceFS mount point. Clients may pipeline up to MaxInflight
// concurrent READ_DIRECT requests on a single connection using req_id matching.
type Server struct {
	engine     *Engine
	cache      *BlockCache
	resolver   FileResolver
	cfg        Config
	generation uint64

	ln     *net.UnixListener
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed atomic.Bool
}

// NewServer creates and binds the Unix SOCK_SEQPACKET direct-read server.
func NewServer(engine *Engine, resolver FileResolver) (*Server, error) {
	if engine == nil || resolver == nil {
		return nil, ErrInvalidArg
	}
	cfg := engine.Config()
	if cfg.SocketPath == "" {
		return nil, fmt.Errorf("%w: empty socket_path", ErrInvalidArg)
	}
	_ = os.Remove(cfg.SocketPath)

	addr := &net.UnixAddr{Name: cfg.SocketPath, Net: "unixpacket"}
	ln, err := net.ListenUnix("unixpacket", addr)
	if err != nil {
		return nil, fmt.Errorf("xds: listen %q: %w", cfg.SocketPath, err)
	}
	if err := os.Chmod(cfg.SocketPath, cfg.SocketMode); err != nil {
		_ = ln.Close()
		_ = os.Remove(cfg.SocketPath)
		return nil, fmt.Errorf("xds: chmod socket: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		engine:     engine,
		cache:      engine.Cache(),
		resolver:   resolver,
		cfg:        cfg,
		generation: uint64(time.Now().UnixNano()),
		ln:         ln,
		ctx:        ctx,
		cancel:     cancel,
	}, nil
}

// EndpointInfo returns the discovery metadata to embed into JuiceFS's .config.
func (s *Server) EndpointInfo() EndpointInfo {
	info := s.engine.Info()
	bdfs := append([]string(nil), s.cfg.PeerBDFs...)
	return EndpointInfo{
		SocketPath:      s.cfg.SocketPath,
		DeviceBackend:   s.cfg.DeviceBackend.String(),
		PeerBDFs:        bdfs,
		IOUnitSize:      info.IOUnitSize,
		BlockSize:       uint64(s.cfg.BlockSize),
		MountGeneration: s.generation,
	}
}

// Serve accepts incoming client connections until Close is called.
func (s *Server) Serve() error {
	for {
		conn, err := s.ln.AcceptUnix()
		if err != nil {
			if s.closed.Load() || errors.Is(err, net.ErrClosed) {
				return nil
			}
			continue
		}
		s.wg.Add(1)
		go func(c *net.UnixConn) {
			defer s.wg.Done()
			s.handleConn(c)
		}(conn)
	}
}

// Close stops accepting new sessions, drains all inflight SPDK I/Os across
// active clients, unregisters all imported buffers, and removes the socket.
func (s *Server) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.cancel()
	err := s.ln.Close()
	s.wg.Wait()
	_ = os.Remove(s.cfg.SocketPath)
	return err
}

func (s *Server) handleConn(conn *net.UnixConn) {
	defer func() { _ = conn.Close() }()

	ucred, err := peerCred(conn)
	if err != nil {
		return
	}
	if !s.cfg.AllowOther && ucred.Uid != uint32(os.Geteuid()) && ucred.Uid != 0 {
		return
	}

	sessCtx, sessCancel := context.WithCancel(s.ctx)
	defer sessCancel()
	// Unblock conn.ReadMsgUnix promptly on Server.Close or fatal write errors.
	stopClose := context.AfterFunc(sessCtx, func() { _ = conn.Close() })
	defer stopClose()

	sess := &clientSession{
		server:      s,
		conn:        conn,
		ctx:         sessCtx,
		cancel:      sessCancel,
		inflightSem: make(chan struct{}, s.cfg.MaxInflight),
		nextToken:   1,
		files:       make(map[uint64]uint64),
		bufs:        make(map[uint64]*importedBuf),
	}

	s.engine.Metrics().ActiveSessions.Inc()
	defer func() {
		sess.teardown()
		s.engine.Metrics().ActiveSessions.Dec()
	}()

	buf := make([]byte, 4096)
	oob := make([]byte, unix.CmsgSpace(4*4))

	for {
		n, oobn, flags, _, err := conn.ReadMsgUnix(buf, oob)
		if err != nil || n < int(unsafe.Sizeof(msgHdr{})) {
			return
		}
		fds := extractFDs(oob[:oobn])
		hdr := *(*msgHdr)(unsafe.Pointer(&buf[0]))
		if hdr.ABIVersion != protoABIVersion || int(hdr.MsgSize) != n || flags&unix.MSG_TRUNC != 0 {
			closeFDs(fds)
			sess.sendStatus(hdr.ReqID, -int32(syscall.EPROTO))
			continue
		}

		switch hdr.Type {
		case msgHello:
			closeFDs(fds)
			if n != int(unsafe.Sizeof(helloMsg{})) {
				sess.sendStatus(hdr.ReqID, -int32(syscall.EMSGSIZE))
				continue
			}
			sess.handleHello(hdr.ReqID)

		case msgBindFile:
			if n != int(unsafe.Sizeof(msgHdr{})) {
				closeFDs(fds)
				sess.sendStatus(hdr.ReqID, -int32(syscall.EMSGSIZE))
				continue
			}
			sess.handleBindFile(hdr.ReqID, fds)

		case msgUnbindFile:
			closeFDs(fds)
			if n != int(unsafe.Sizeof(tokenMsg{})) {
				sess.sendStatus(hdr.ReqID, -int32(syscall.EMSGSIZE))
				continue
			}
			msg := *(*tokenMsg)(unsafe.Pointer(&buf[0]))
			delete(sess.files, msg.Token)
			sess.sendStatus(hdr.ReqID, 0)

		case msgRegisterBuf:
			if n != int(unsafe.Sizeof(registerBufMsg{})) {
				closeFDs(fds)
				sess.sendStatus(hdr.ReqID, -int32(syscall.EMSGSIZE))
				continue
			}
			msg := *(*registerBufMsg)(unsafe.Pointer(&buf[0]))
			sess.handleRegisterBuf(&msg, fds)

		case msgUnregisterBuf:
			closeFDs(fds)
			if n != int(unsafe.Sizeof(tokenMsg{})) {
				sess.sendStatus(hdr.ReqID, -int32(syscall.EMSGSIZE))
				continue
			}
			msg := *(*tokenMsg)(unsafe.Pointer(&buf[0]))
			rc := sess.unregisterBuf(msg.Token)
			sess.sendStatus(hdr.ReqID, rc)

		case msgReadDirect:
			closeFDs(fds)
			sess.handleReadDirect(buf[:n])

		default:
			closeFDs(fds)
			sess.sendStatus(hdr.ReqID, -int32(syscall.ENOTSUP))
		}
	}
}

func (sess *clientSession) handleHello(reqID uint64) {
	info := sess.server.engine.Info()
	rep := helloReplyMsg{
		Hdr: msgHdr{
			ABIVersion: protoABIVersion,
			Type:       msgHelloReply,
			MsgSize:    uint32(unsafe.Sizeof(helloReplyMsg{})),
			ReqID:      reqID,
		},
		MountGeneration: sess.server.generation,
		IOUnitSize:      info.IOUnitSize,
		BlockSize:       uint64(sess.server.cfg.BlockSize),
		MaxInflight:     sess.server.cfg.MaxInflight,
		DeviceBackend:   uint32(sess.server.cfg.DeviceBackend),
		PeerCount:       uint32(len(sess.server.cfg.PeerBDFs)),
	}
	for i, bdf := range sess.server.cfg.PeerBDFs {
		if i >= maxPeers {
			break
		}
		copy(rep.PeerBDFs[i][:], bdf)
	}
	sess.sendRaw(structBytes(&rep))
}

// handleBindFile validates a client-supplied open fd using pure in-kernel
// statx(AT_STATX_DONT_SYNC) + fcntl(F_GETFL) so no FUSE upcall is triggered.
func (sess *clientSession) handleBindFile(reqID uint64, fds []int) {
	if len(fds) != 1 {
		closeFDs(fds)
		sess.sendStatus(reqID, -int32(syscall.EPROTO))
		return
	}
	fd := fds[0]
	defer func() { _ = unix.Close(fd) }()

	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil || (flags&unix.O_ACCMODE) == unix.O_WRONLY {
		sess.sendStatus(reqID, -int32(syscall.EACCES))
		return
	}

	var stx unix.Statx_t
	err = unix.Statx(fd, "", unix.AT_EMPTY_PATH|unix.AT_STATX_DONT_SYNC,
		unix.STATX_INO|unix.STATX_TYPE|unix.STATX_SIZE, &stx)
	if err != nil {
		sess.sendStatus(reqID, -int32(errnoOf(err)))
		return
	}
	dev := unix.Mkdev(stx.Dev_major, stx.Dev_minor)
	if dev != sess.server.resolver.MountDev() {
		sess.sendStatus(reqID, -int32(syscall.EXDEV))
		return
	}
	if (stx.Mode & unix.S_IFMT) != unix.S_IFREG {
		sess.sendStatus(reqID, -int32(syscall.EINVAL))
		return
	}
	if len(sess.files) >= maxFilesPerSession {
		sess.sendStatus(reqID, -int32(syscall.ENOSPC))
		return
	}

	tok := sess.nextToken
	sess.nextToken++
	sess.files[tok] = stx.Ino

	rep := bindFileReplyMsg{
		Hdr: msgHdr{
			ABIVersion: protoABIVersion,
			Type:       msgBindFileReply,
			MsgSize:    uint32(unsafe.Sizeof(bindFileReplyMsg{})),
			ReqID:      reqID,
		},
		FileToken: tok,
		Inode:     stx.Ino,
		FileSize:  stx.Size,
	}
	sess.sendRaw(structBytes(&rep))
}

func (sess *clientSession) handleRegisterBuf(msg *registerBufMsg, fds []int) {
	if len(fds) != 1 {
		closeFDs(fds)
		sess.sendStatus(msg.Hdr.ReqID, -int32(syscall.EPROTO))
		return
	}
	p2pFD := fds[0]
	cfgPeers := sess.server.cfg.PeerBDFs
	if msg.PeerCount == 0 || msg.PeerCount > maxPeers || int(msg.PeerCount) != len(cfgPeers) || msg.SizeBytes == 0 {
		_ = unix.Close(p2pFD)
		sess.sendStatus(msg.Hdr.ReqID, -int32(syscall.EINVAL))
		return
	}
	if len(sess.bufs) >= maxBufsPerSession {
		_ = unix.Close(p2pFD)
		sess.sendStatus(msg.Hdr.ReqID, -int32(syscall.ENOSPC))
		return
	}

	peers := make([]PeerExport, msg.PeerCount)
	for i := uint32(0); i < msg.PeerCount; i++ {
		w := msg.Peers[i]
		peers[i] = PeerExport{
			PeerBDF:      cfgPeers[i],
			Handle:       w.Handle,
			Backend:      w.Backend,
			PageSize:     w.PageSize,
			MappedLen:    w.MappedLen,
			SegmentCount: w.SegmentCount,
			FirstDMAAddr: w.FirstDMAAddr,
		}
	}

	reg, ioPtr, err := sess.server.engine.ImportBuffer(p2pFD, msg.DeviceVA, msg.SizeBytes, peers)
	if err != nil {
		sess.sendStatus(msg.Hdr.ReqID, -int32(errnoOf(err)))
		return
	}

	tok := sess.nextToken
	sess.nextToken++
	sess.bufs[tok] = &importedBuf{
		reg:   reg,
		ioPtr: ioPtr,
		size:  msg.SizeBytes,
	}

	rep := tokenMsg{
		Hdr: msgHdr{
			ABIVersion: protoABIVersion,
			Type:       msgRegisterBufReply,
			MsgSize:    uint32(unsafe.Sizeof(tokenMsg{})),
			ReqID:      msg.Hdr.ReqID,
		},
		Token: tok,
	}
	sess.sendRaw(structBytes(&rep))
}

func (sess *clientSession) unregisterBuf(token uint64) int32 {
	b, ok := sess.bufs[token]
	if !ok {
		return -int32(syscall.ENOENT)
	}
	delete(sess.bufs, token)
	b.inflight.Wait()
	if err := sess.server.engine.UnregisterBuffer(b.reg); err != nil {
		return -int32(errnoOf(err))
	}
	return 0
}

// teardown cancels session context, waits for all inflight direct reads to
// finish DMA and unpin cache slots, then unregisters all remaining P2P buffers.
func (sess *clientSession) teardown() {
	sess.cancel()
	sess.ioWG.Wait()
	for _, b := range sess.bufs {
		_ = sess.server.engine.UnregisterBuffer(b.reg)
	}
	clear(sess.bufs)
	clear(sess.files)
}

func (sess *clientSession) handleReadDirect(raw []byte) {
	start := time.Now()
	metrics := sess.server.engine.Metrics()
	metrics.DirectReadReqs.Inc()

	hdrSize := int(unsafe.Sizeof(readDirectReqMsg{}))
	if len(raw) < hdrSize {
		metrics.DirectReadErrs.Inc()
		hdr := *(*msgHdr)(unsafe.Pointer(&raw[0]))
		sess.sendStatus(hdr.ReqID, -int32(syscall.EMSGSIZE))
		return
	}
	req := *(*readDirectReqMsg)(unsafe.Pointer(&raw[0]))
	fail := func(code syscall.Errno) {
		metrics.DirectReadErrs.Inc()
		sess.sendStatus(req.Hdr.ReqID, -int32(code))
	}

	if req.Length == 0 || req.SliceCount == 0 || req.SliceCount > maxSlices {
		fail(syscall.EINVAL)
		return
	}
	sliceBytes := int(req.SliceCount) * int(unsafe.Sizeof(wireSlice{}))
	if len(raw) != hdrSize+sliceBytes {
		fail(syscall.EMSGSIZE)
		return
	}

	ino, ok := sess.files[req.FileToken]
	if !ok {
		fail(syscall.EBADF)
		return
	}

	wireSlices := unsafe.Slice((*wireSlice)(unsafe.Pointer(&raw[hdrSize])), req.SliceCount)
	dstSlices := slices.Clone(wireSlices)

	// Validate slices and collect referenced buffers synchronously before
	// advancing the socket reader, preserving ordering with UNREGISTER_BUF.
	var sum uint64
	bufMap := make([]*importedBuf, len(dstSlices))
	pinnedBufs := make([]*importedBuf, 0, 4)
	for i, sl := range dstSlices {
		if sl.Length == 0 || sum+sl.Length < sum {
			fail(syscall.EINVAL)
			return
		}
		sum += sl.Length
		b, ok := sess.bufs[sl.BufToken]
		if !ok {
			fail(syscall.EPERM)
			return
		}
		if sl.BufOffset+sl.Length < sl.BufOffset || sl.BufOffset+sl.Length > b.size {
			fail(syscall.EINVAL)
			return
		}
		bufMap[i] = b
		if !slices.Contains(pinnedBufs, b) {
			pinnedBufs = append(pinnedBufs, b)
		}
	}
	if sum != req.Length {
		fail(syscall.EINVAL)
		return
	}

	select {
	case sess.inflightSem <- struct{}{}:
	case <-sess.ctx.Done():
		return
	}

	for _, b := range pinnedBufs {
		b.inflight.Add(1)
	}
	sess.ioWG.Add(1)

	go func() {
		defer func() {
			for _, b := range pinnedBufs {
				b.inflight.Done()
			}
			<-sess.inflightSem
			sess.ioWG.Done()
		}()
		sess.execReadDirect(req.Hdr.ReqID, ino, req.FileOffset, req.Length, dstSlices, bufMap, start)
	}()
}

type directPlan struct {
	descs       []DirectReadDesc
	pinnedSlots []uint32
	holes       []HoleSpan
}

func (sess *clientSession) planDirectExtents(
	extents []BlockExtent, totalBytes uint64,
	dstSlices []wireSlice, bufMap []*importedBuf,
) (directPlan, syscall.Errno) {
	unit := sess.server.engine.Info().IOUnitSize
	blockSize := uint64(sess.server.cfg.BlockSize)
	plan := directPlan{
		descs:       make([]DirectReadDesc, 0, len(extents)),
		pinnedSlots: make([]uint32, 0, len(extents)),
		holes:       make([]HoleSpan, 0, 4),
	}
	unpinAll := func() {
		for _, idx := range plan.pinnedSlots {
			sess.server.cache.Unpin(idx)
		}
	}

	si := 0
	var sConsumed uint64
	var cursor uint64

	for _, ext := range extents {
		if ext.DstOff >= totalBytes {
			break
		}
		if ext.DstOff != cursor {
			unpinAll()
			return directPlan{}, syscall.EIO
		}
		extLen := uint64(ext.Len)
		if extLen == 0 {
			continue
		}
		if ext.DstOff+extLen > totalBytes {
			extLen = totalBytes - ext.DstOff
		}

		if ext.IsHole {
			rem := extLen
			for rem > 0 && si < len(dstSlices) {
				sAvail := dstSlices[si].Length - sConsumed
				take := min(rem, sAvail)
				dstOff := dstSlices[si].BufOffset + sConsumed
				if n := len(plan.holes); n > 0 &&
					plan.holes[n-1].SliceIndex == uint32(si) &&
					plan.holes[n-1].DstOffset+plan.holes[n-1].Length == dstOff {
					plan.holes[n-1].Length += take
				} else {
					if len(plan.holes) >= maxHoles {
						unpinAll()
						return directPlan{}, syscall.E2BIG
					}
					plan.holes = append(plan.holes, HoleSpan{
						DstOffset:  dstOff,
						Length:     take,
						SliceIndex: uint32(si),
					})
				}
				rem -= take
				sConsumed += take
				if sConsumed == dstSlices[si].Length {
					si++
					sConsumed = 0
				}
			}
			if rem != 0 {
				unpinAll()
				return directPlan{}, syscall.EINVAL
			}
			cursor += extLen
			continue
		}

		if uint64(ext.BlockOff)%unit != 0 || extLen%unit != 0 || uint64(ext.BlockOff)+extLen > blockSize {
			unpinAll()
			return directPlan{}, syscall.EINVAL
		}

		slot, ok := sess.server.cache.Pin(ext.Key)
		if !ok {
			unpinAll()
			return directPlan{}, syscall.EAGAIN
		}
		plan.pinnedSlots = append(plan.pinnedSlots, slot.SlotIndex)
		if uint64(ext.BlockOff)+extLen > uint64(slot.OrigLen) {
			unpinAll()
			return directPlan{}, syscall.EIO
		}

		var iovs []IOVec
		rem := extLen
		for rem > 0 && si < len(dstSlices) {
			sAvail := dstSlices[si].Length - sConsumed
			take := min(rem, sAvail)
			addr := bufMap[si].ioPtr + uintptr(dstSlices[si].BufOffset+sConsumed)
			if n := len(iovs); n > 0 && iovs[n-1].Base+uintptr(iovs[n-1].Len) == addr {
				iovs[n-1].Len += take
			} else {
				iovs = append(iovs, IOVec{Base: addr, Len: take})
			}
			rem -= take
			sConsumed += take
			if sConsumed == dstSlices[si].Length {
				si++
				sConsumed = 0
			}
		}
		if rem != 0 || len(iovs) == 0 {
			unpinAll()
			return directPlan{}, syscall.EINVAL
		}

		d := DirectReadDesc{
			Blob:       slot.Blob,
			BlobOffset: slot.BlobOffset + uint64(ext.BlockOff),
			Size:       extLen,
		}
		if len(iovs) == 1 {
			d.Payload = iovs[0].Base
		} else {
			d.Iovs = iovs
		}
		plan.descs = append(plan.descs, d)
		cursor += extLen
	}

	if cursor != totalBytes {
		unpinAll()
		return directPlan{}, syscall.EIO
	}
	return plan, 0
}

func (sess *clientSession) execReadDirect(
	reqID, ino, fileOff, reqLen uint64,
	dstSlices []wireSlice, bufMap []*importedBuf, start time.Time,
) {
	metrics := sess.server.engine.Metrics()
	fail := func(code syscall.Errno) {
		metrics.DirectReadErrs.Inc()
		sess.sendStatus(reqID, -int32(code))
	}

	extents, fileSize, err := sess.server.resolver.ResolveRead(sess.ctx, ino, fileOff, reqLen)
	if err != nil {
		fail(errnoOf(err))
		return
	}

	var totalBytes uint64
	if fileOff < fileSize {
		totalBytes = min(reqLen, fileSize-fileOff)
	}
	if totalBytes == 0 {
		sess.sendReadDirectReply(reqID, 0, nil)
		return
	}

	plan, code := sess.planDirectExtents(extents, totalBytes, dstSlices, bufMap)
	if code != 0 {
		fail(code)
		return
	}
	defer func() {
		for _, idx := range plan.pinnedSlots {
			sess.server.cache.Unpin(idx)
		}
	}()

	if len(plan.descs) > 0 {
		if err := sess.server.engine.ReadDirectBatch(plan.descs); err != nil {
			fail(errnoOf(err))
			return
		}
	}

	sess.sendReadDirectReply(reqID, totalBytes, plan.holes)
	metrics.DirectReadBytes.Add(float64(totalBytes))
	metrics.DirectReadDur.Observe(time.Since(start).Seconds())
}

// Low-level Unix socket & wire helpers.

func (sess *clientSession) sendRaw(payload []byte) {
	sess.writeMu.Lock()
	_, _, err := sess.conn.WriteMsgUnix(payload, nil, nil)
	sess.writeMu.Unlock()
	if err != nil {
		sess.cancel()
	}
}

func (sess *clientSession) sendStatus(reqID uint64, status int32) {
	hdr := msgHdr{
		ABIVersion: protoABIVersion,
		Type:       msgStatus,
		MsgSize:    uint32(unsafe.Sizeof(msgHdr{})),
		Status:     status,
		ReqID:      reqID,
	}
	sess.sendRaw(structBytes(&hdr))
}

func (sess *clientSession) sendReadDirectReply(reqID, bytesRead uint64, holes []HoleSpan) {
	hdrSize := int(unsafe.Sizeof(readDirectReplyMsg{}))
	holeBytes := len(holes) * int(unsafe.Sizeof(HoleSpan{}))
	rep := readDirectReplyMsg{
		Hdr: msgHdr{
			ABIVersion: protoABIVersion,
			Type:       msgReadDirectReply,
			MsgSize:    uint32(hdrSize + holeBytes),
			ReqID:      reqID,
		},
		BytesRead: bytesRead,
		HoleCount: uint32(len(holes)),
	}
	if len(holes) == 0 {
		sess.sendRaw(structBytes(&rep))
		return
	}
	var pkt [40 + maxHoles*24]byte
	copy(pkt[:hdrSize], structBytes(&rep))
	copy(pkt[hdrSize:hdrSize+holeBytes], unsafe.Slice((*byte)(unsafe.Pointer(&holes[0])), holeBytes))
	sess.sendRaw(pkt[:hdrSize+holeBytes])
}

func peerCred(conn *net.UnixConn) (*unix.Ucred, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return nil, err
	}
	var cred *unix.Ucred
	var cErr error
	if err := raw.Control(func(fd uintptr) {
		cred, cErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return nil, err
	}
	return cred, cErr
}

func extractFDs(oob []byte) []int {
	if len(oob) == 0 {
		return nil
	}
	msgs, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return nil
	}
	var out []int
	for _, m := range msgs {
		fds, err := unix.ParseUnixRights(&m)
		if err == nil {
			out = append(out, fds...)
		}
	}
	return out
}

func closeFDs(fds []int) {
	for _, fd := range fds {
		if fd >= 0 {
			_ = unix.Close(fd)
		}
	}
}

func structBytes[T any](v *T) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), unsafe.Sizeof(*v))
}

func errnoOf(err error) syscall.Errno {
	var e syscall.Errno
	if errors.As(err, &e) {
		return e
	}
	switch {
	case errors.Is(err, ErrInvalidArg):
		return syscall.EINVAL
	case errors.Is(err, ErrPerm):
		return syscall.EPERM
	case errors.Is(err, ErrNoSpace):
		return syscall.ENOSPC
	case errors.Is(err, ErrNotCached):
		return syscall.ENOENT
	case errors.Is(err, ErrBusy):
		return syscall.EBUSY
	case errors.Is(err, ErrUnsupported):
		return syscall.ENOTSUP
	default:
		return syscall.EIO
	}
}
