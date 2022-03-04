
# Redpanda
[![Documentation](https://img.shields.io/badge/documentation-black)](https://redpanda.com/documentation)
[![Slack](https://img.shields.io/badge/slack-purple)](https://redpanda.com/slack)
[![Twitter](https://img.shields.io/twitter/follow/redpandadata.svg?style=social&label=Follow)](https://twitter.com/intent/follow?screen_name=redpandadata)
![Go](https://github.com/redpanda-data/redpanda/workflows/Go/badge.svg)
![C++](https://github.com/redpanda-data/redpanda/workflows/build-test/badge.svg)

[<p align="center"><img src="docs/PANDA_sitting.jpg" alt="redpanda sitting" width="400"/></p>](https://redpanda.com/redpanda)
<img src="https://static.scarf.sh/a.png?x-pxid=3c187215-e862-4b67-8057-45aa9a779055" />

Redpanda is a streaming platform for mission critical workloads. Kafka® compatible, 
No Zookeeper®, no JVM, and no code changes required. Use all your favorite open source tooling - 10x faster.

We are building a real-time streaming engine for modern applications - from the 
enterprise to the solo dev prototyping a react application on her laptop. 
We go beyond the Kafka protocol, into the future of streaming with inline WASM 
transforms and geo-replicated hierarchical storage. A new platform that scales with 
you from the smallest projects to petabytes of data distributed across the globe.

# Community

[Slack](https://redpanda.com/slack) is the main way the community interacts with one another in real time :) 

[Github Discussion](https://github.com/redpanda-data/redpanda/discussions) is preferred for longer, async, thoughtful discussions

[GitHub Issues](https://github.com/redpanda-data/redpanda/issues) is reserved only for actual issues. Please use the mailing list for discussions.

[Code of conduct](./CODE_OF_CONDUCT.md) code of conduct for the community

[Contributing docs](./CONTRIBUTING.md)  

# Getting Started

## Prebuilt Packages

We recommend using our free & prebuilt stable releases below.  

### On MacOS

Simply download our `rpk` [binary here](https://github.com/redpanda-data/redpanda/releases). We require Docker on MacOS

```
brew install redpanda-data/tap/redpanda && rpk container start
```

### On Debian/Ubuntu

```
curl -1sLf \
  'https://packages.vectorized.io/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' \
  | sudo -E bash
  
sudo apt-get install redpanda
```

### On Fedora/RedHat/Amazon Linux

```
curl -1sLf \
  'https://packages.vectorized.io/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.rpm.sh' \
  | sudo -E bash
  
sudo yum install redpanda
```

## GitHub Actions


```yaml
    - name: start redpanda
      uses: redpanda-data/github-action@v0.1.3
      with:
        version: "latest"
```

Now you should be able to connect to `redpanda` (kafka-api) running at `localhost:9092` 


## Build Manually

We provide a very simple build system that uses your system libraries. We recommend
users leverage our pre-built stable releases which are vetted, tested, and reproducible with exact
versions of the entire transitive dependency graph, including exact compilers
all built from source. The only thing we do not build yet is the Linux Kernel, but soon!

For hackers, here is the short and sweet:

```
sudo ./install-dependencies.sh && CC=clang CXX=clang++ ./build.sh
```

For quicker dev setup, we provide a [docker image](tools/docker/README.md) with the toolchain installed.

# Beta builds

For those of us who like to live on the edge!

We can cut a release at any point from the `/dev` branch if you want to test a particular feature.
Simply let us know you would like to test a feature from dev and we're happy to cut a beta release.


## Beta releases on Debian/Ubuntu

```
curl -1sLf \
  'https://packages.vectorized.io/HxYRCzL4xbbaEtPi/redpanda-beta/setup.deb.sh' \
  | sudo -E bash
  
sudo apt-get install redpanda
```

## Beta releases on Fedora/RedHat/Amazon Linux

```
curl -1sLf \
  'https://packages.vectorized.io/HxYRCzL4xbbaEtPi/redpanda-beta/setup.rpm.sh' \
  | sudo -E bash
  
sudo yum install redpanda
```

## Beta releases on Docker

This is an example with the `v21.3.5-beta3` version prior to the 21.3.5 release.

```
docker.vectorized.io/vectorized/redpanda:v21.3.5-beta3
```
