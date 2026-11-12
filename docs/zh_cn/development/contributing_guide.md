---
title: 贡献指南
sidebar_position: 1
description: JuiceFS 是开源软件，代码由全球开发者共同贡献和维护，您可以参考本文了解参与开发的流程和注意事项。
---

## 基本准则 {#guidelines}

- 在开始修复功能或错误之前，请先通过 GitHub、Discord 等渠道与我们沟通。此步骤的目的是确保没有其他人已经在处理它，如有必要，我们将要求您创建一个 GitHub issue。
- 在开始贡献前，使用 GitHub issue 来讨论功能实现并与核心开发者达成一致。
- 如果这是一个重大的特性更新，写一份设计文档来帮助社区理解你的动机和解决方案。
- 对于首次贡献者来说，找到合适 issue 的好方法是使用标签 ["kind/good-first-issue"](https://github.com/juicedata/juicefs/labels/kind%2Fgood-first-issue) 或 ["kind/help-wanted"](https://github.com/juicedata/juicefs/labels/kind%2Fhelp-wanted) 搜索未解决的问题。

## 构建 xDS 版本 {#building-with-xds}

在 Linux 上，首次构建需初始化 xDS 的嵌套子模块，并参考
[xDS 构建说明](https://github.com/Codesheep11/xDS#构建) 安装 SPDK 编译依赖。
服务还需要 LZ4 开发库（Ubuntu 对应 `liblz4-dev`）。之后在 JuiceFS 仓库根目录执行：

```sh
git submodule update --init --recursive xDS
make juicefs.xds
```

此目标会构建 xDS 用户态 P2P 库、SPDK 和 `xDS/src/service/xds_service`，
运行 xDS 客户端单元测试及 `go vet`，最后生成 `./juicefs.xds`。
客户端 C 源码由 cgo 编译。构建目标将外部客户端源码和头文件的内容摘要加入
Go 测试、vet 和构建共用的 `CGO_CFLAGS`，同时保留已有编译选项。
修改这些文件后会自动重编客户端，无需手动清理缓存或执行 `-a`。
可使用 `make juicefs.xds JOBS=8` 限制 SPDK 编译并发，
或使用 `make juicefs.xds BUILD=debug` 启用 Go 程序和 xDS 服务的调试编译参数。
已有 SPDK 配置会保留。

此目标要求启用 cgo，暂不支持 `STATIC=1`；不会安装 Python SDK、编译或加载厂商内核模块、
配置设备或启动服务。需要运行中 xDS 服务的测试会跳过。
当前开发阶段的根命令尚未提供 GPU 读取接口，客户端的编译和链接由包测试验证。
全包回归仍通过独立的 `make test.pkg` 运行，并需要其对应的服务和 GlusterFS 依赖。

构建缓存的回归测试不依赖 xDS 服务或 SPDK，可单独运行：

```sh
python3 test/xds_build_cache_test.py
```

## 代码风格 {#coding-style}

- 我们遵循 ["Effective Go"](https://go.dev/doc/effective_go) 和 ["Go Code Review Comments"](https://github.com/golang/go/wiki/CodeReviewComments)。
- 在提交前使用 `go fmt` 格式化你的代码。你可以在 [Go 的编辑器和 IDE](https://github.com/golang/go/wiki/IDEsAndTextEditorPlugins) 中找到支持 Go 的相关工具的信息。
- 每个新的源文件都必须以许可证头开始。
- 安装 [pre-commit](https://pre-commit.com) 并使用它来设置一个预提交钩子来进行静态分析。只需在仓库根目录下运行 `pre-commit install` 即可。

## 签署 CLA {#sign-the-cla}

在您为 JuiceFS 进行贡献之前，您需要签署[贡献者许可协议](https://cla-assistant.io/juicedata/juicefs)。当你第一次提交 PR 的时候，将有一个 CLA 助手指导你。

## 什么是好的 PR {#what-is-a-good-pr}

- 足够的单元测试
- 遵循编码风格
- 足够的行内注释
- 简要解释的提交内容

## 贡献流程 {#contribution-flow}

1. 基于主分支创建一个要贡献的主题分支。这个主分支通常是 `main` 分支；
1. 提交代码；
1. 确保提交消息的格式正确；
1. 将主题分支中的更改推到个人 fork 的仓库；
1. 提交一个 PR 到 [`juicedata/juicefs`](https://github.com/juicedata/juicefs/compare) 仓库。这个 PR 应该链接到你或其他人创建的一个 issue；
1. PR 在合并之前必须得到至少一个维护者的批准。
