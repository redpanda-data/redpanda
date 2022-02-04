# Redpanda
[![Documentation](https://img.shields.io/badge/documentation-black)](https://vectorized.io/documentation)
[![Slack](https://img.shields.io/badge/slack-purple)](https://vectorized.io/slack)
[![Twitter](https://img.shields.io/twitter/follow/redpandadata.svg?style=social&label=Follow)](https://twitter.com/intent/follow?screen_name=redpandadata)
![Go](https://github.com/redpanda-data/redpanda/workflows/Go/badge.svg)
![C++](https://github.com/redpanda-data/redpanda/workflows/build-test/badge.svg)

[<p align="center"><img src="docs/PANDA_sitting.jpg" alt="redpanda sitting" width="400"/></p>](https://vectorized.io/redpanda)
<img src="https://static.scarf.sh/a.png?x-pxid=3c187215-e862-4b67-8057-45aa9a779055" />

Redpanda 是一个用于关键任务型工作负载的流计算平台。兼容 Kafka® 协议，
无需 Zookeeper®，无需 JVM，也无需更改代码。使用您喜欢的所有开源工具 - 速度提高 10 倍。

我们正在为现代应用程序构建实时流处理引擎 - 从企业到在笔记本电脑上制作 React 应用程序原型的个人开发者。 
我们超越了 Kafka 协议，进入了具有内联 WASM 转换 (inline WASM transforms) 和地理位置分布式分层存储 (geo-replicated hierarchical storage) 的流计算的未来。一个可扩展的新平台，
从最小的项目到分布在全球的 PB 级数据。

# 社区

[Slack](https://vectorized.io/slack) 是社区实时互动的主要方式 :)

[Github 讨论](https://github.com/redpanda-data/redpanda/discussions) 是更长时间、异步、深思熟虑的讨论的首选

[GitHub 问题](https://github.com/redpanda-data/redpanda/issues) 仅用于实际 issues。请使用邮件列表进行讨论。

[行为准则](./CODE_OF_CONDUCT.md) 社区行为准则

[贡献文档](./CONTRIBUTING.md)  

# 入门

## 预建包 (Prebuilt Packages)

我们建议使用下面的免费和预建的稳定版本。

### MacOS

下载我们的 `rpk` [二进制文件](https://github.com/redpanda-data/redpanda/releases)。我们需要在 MacOS 上使用 Docker

```shell
brew install redpanda-data/tap/redpanda && rpk container start
```

### Debian/Ubuntu

```shell
curl -1sLf \
  'https://packages.vectorized.io/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' \
  | sudo -E bash
  
sudo apt-get install redpanda
```

### Fedora/RedHat/Amazon Linux

```shell
curl -1sLf \
  'https://packages.vectorized.io/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.rpm.sh' \
  | sudo -E bash
  
sudo yum install redpanda
```

## 手动 Build

我们提供了一个非常简单的构建系统 (build system)，使用您的系统库。我们推荐
用户利用我们预先构建的稳定版本，这些版本经过严格审查、测试，是可复制
整个传递依赖图 (transitive dependency graph) 的版本，包括编译器，
全部从源代码构建。我们还没有 build 的唯一东西是 Linux 内核，但很快！

对于 hackers 来说，这是最简单方便的：

```shell
sudo ./install-dependencies.sh && ./build.sh
```

# Beta Builds

给我们这些喜欢 live on the edge 的人！

如果你想测试一个特定的功能，我们可以在任何时候从 `/dev` 分支中创建一个可以 release 的版本。
只需让我们知道你想测试 dev 的功能，我们很高兴为你创建一个 beta 版本。


## Debian/Ubuntu 上的 Beta 版本

```shell
curl -1sLf \
  'https://packages.vectorized.io/HxYRCzL4xbbaEtPi/redpanda-beta/setup.deb.sh' \
  | sudo -E bash
  
sudo apt-get install redpanda
```

## Fedora/RedHat/Amazon Linux 上的 Beta 版本

```shell
curl -1sLf \
  'https://packages.vectorized.io/HxYRCzL4xbbaEtPi/redpanda-beta/setup.rpm.sh' \
  | sudo -E bash
  
sudo yum install redpanda
```

## Docker 上的 Beta 版本

这是 21.3.5 版本之前的 `v21.3.5-beta3` 版本的示例。

```shell
docker.vectorized.io/vectorized/redpanda:v21.3.5-beta3
```
