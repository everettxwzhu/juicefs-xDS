---
title: xDS GPU 读取
slug: /development/xds-gpu-read
---

`pkg/xds` 提供进程内 Go API，将 JuiceFS 文件读入已注册的设备内存。它要求 Linux、cgo、`xds` 构建标签，以及同时支持 GPU 直读和 CPU 缓冲分配的 xDS 服务。使用 `make juicefs.xds` 构建。默认构建调用 `xds.Open` 返回 `ENOTSUP`。

该 API 不增加 FUSE GPU 操作，也不增加 C、Python 或 Java SDK 入口。写入继续使用现有主机内存路径。

## 资源归属与初始化

将构造 `fs.NewFileSystem` 时使用的同一组元数据引擎、chunk store 和 chunk 配置传给 `xds.Open`：

```go
v, err := xds.Open(xds.Config{
	Service: "xds-svc", Rank: rank, WorldSize: worldSize,
}, m, store, conf.Chunk)
```

调用方必须检查所有错误。每个 Volume 独占服务的一个 rank 和一份易失 block 缓存。同一服务的所有客户端必须使用相同的 world size。元数据引擎和 chunk store 仍由应用管理，必须先关闭 Volume，再关闭这两个依赖。

服务的 blob 大小必须能被卷的 BlockSize 整除，BlockSize 必须能被服务的 I/O 单位整除。Open 预留一个零 slot 和 `S` 个 scratch slot，分配 `S` 个主机缓冲，并要求至少还有 `S` 个缓存 slot，其中 `S = min(32, 128 MiB / BlockSize)`。部署时据此分配 hugepage 和每个 rank 的容量。

注册设备内存时，调用线程必须已激活该内存所属的设备 context。Go 调用方应先执行 `runtime.LockOSThread`，再设置 context，并保持线程锁定直至 `v.Register(ptr, capacity)` 返回。在 Unregister 成功或 Close 成功之前，必须保持设备内存有效。

## 读取契约

```go
n, err := v.ReadAt(ctx, file, devicePointer, size, offset)
```

- `file` 必须是仍然打开、以可读方式打开的普通文件，并属于同一个元数据实例。打开标志和关闭状态由调用方保证；不支持内部虚拟文件。
- `size` 必须大于零。文件偏移、长度，以及设备地址相对注册基址的偏移，都必须是 `v.IOUnit()` 的整数倍。
- 完整的请求设备范围必须位于同一块注册内，即使 EOF 会缩短实际读取长度。并发请求的目标设备范围不能重叠。
- 调用先 flush 本文件写缓冲，再使用句柄缓存的 `Stat` 长度，与 `fs.File.Pread` 一致。读取期间不能对同一句柄并发写入、truncate 或 Close。跨句柄、跨客户端的修改不提供快照保证。
- EOF 前读出正数字节时返回 `(n, nil)`；起点已到 EOF 时返回 `(0, io.EOF)`。空洞返回零；`n` 到下一个 I/O 单位边界之间补零，再往后的设备内存不变。
- 其他错误返回 `(0, errno)`。部分目标窗口可能已经改变，调用方应丢弃本次请求的结果。元数据 errno 原样透传，存储失败返回 `EIO`，取消返回 `EINTR`。

调用前应完成所有涉及目标范围的设备工作，并遵守 xDS 后端的可见性要求；AMD 调用方必须在调用前后分别进行 HIP 同步。

## 请求规划与执行

规划器读取请求覆盖的各个 chunk 的已解析映射，同时按源 block 边界和请求相对的 BlockSize 窗口切分。对齐的数据段从 xDS 缓存 slot 直接读入设备；缓存未命中时先通过现有 chunk reader 读取完整的不可变 block，再填充 slot。包含未对齐映射的窗口在主机 stager 中拼装，写入配对的 scratch slot，然后读入设备。

只有文件范围同时结束于源 block 的有效数据尾部时，才能利用缓存填充时写入的尾零完成 EOF 补齐。如果 truncate 将更长对象中的字节隐藏，必须拼装该窗口并显式清零尾部。

每个请求最多使用 32 个前台 worker，与 stager 数量独立。相同 block 的填充会合并，缓存 slot 在 DMA 完成前保持引用，成功请求会尽力预读所涉及 slice 的后续 block。

## 取消与故障

取消会跳过尚未开始原生提交的操作。与取消并发的提交可能已经生效；正常取消返回之前，已提交 DMA 必须完成。取消本身不能证明设备内存已经停止被 DMA 访问。

引擎故障或注册控制面故障会使设备路径不可继续使用。ReadAt 返回 `EIO`，相关注册和 DMA 资源保持保护直到关闭。在途读取占用注册或设备路径已失败时，Unregister 返回 `EBUSY`。

Close 等待前台和后台工作，排空引擎并关闭客户端。返回 nil 后可以释放设备内存。如果 Close 返回 `EIO`，表示无法排除仍有 DMA 在途；必须保持设备内存有效，直到服务退出或通过其他机制证明 DMA 已停止。引擎排空超时只约束排空步骤，不约束整个 Close 调用。故障后应重新 Open Volume 建立新会话。
