# Redpanda
[![Documentation](https://img.shields.io/badge/documentation-black)](https://vectorized.io/documentation)
[![Slack](https://img.shields.io/badge/slack-purple)](https://vectorized.io/slack)
[![Twitter](https://img.shields.io/twitter/follow/vectorizedio.svg?style=social&label=Follow)](https://twitter.com/intent/follow?screen_name=vectorizedio)

[<img src="docs/PANDA_sitting.jpg" alt="redpanda sitting" width="400"/>](https://vectorized.io/redpanda)


Redpanda is a modern streaming platform for mission critical workloads. Kafka® compatible, 
No Zookeeper®, no JVM, and no code changes required. Use all your favorite open source tooling - 10x faster.

Our goal is to empower every developer to supercharge their applications with real-time streaming. 
From solo JS devs to enterprise data engineers- we've got your back.
We're building the future of streaming. Whether it's providing inline WASM transforms for one-shot 
transformations or helping with tiered hierarchical storage to unify real-time and historical data, 
we're pushing the boundaries of what's possible with streaming.


# Community

[Slack](vectorized.io/slack) is the main way the community interacts with one another in real time :) 

[User mailing list](https://groups.google.com/g/redpanda-users) is preferred for longer, async, thoughtful discussions

[GitHub Issues](https://github.com/vectorizedio/redpanda/issues) is reserved only for actual issues. Please use the mailing list for discussions.

[Code of conduct](./CODE_OF_CONDUCT.md)

# Getting Started

## Prebuilt Packages

We recommend using our free & prebuilt stable releases below.  

### On MacOS

Simply download our `rpk` binary [here]. We require docker on MacOS

```
rpk container start
```

### On Debian/Ubuntu

```
curl -1sLf \
  'https://packages.vectorized.io/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' \
  | sudo -E bash
  
apt-get install redpanda
```

### On Fedora/RedHat/Amazon Linux

```
curl -1sLf \
  'https://packages.vectorized.io/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.rpm.sh' \
  | sudo -E bash
  
yum install redpanda
```

## Build Manually

We provide a very simple build system that uses your system libraries. We recommend
users leverage our pre-built stable releases which are vetted, tested, and reproducible with exact
versions of the entire transitive dependency graph, including exact compilers
all built from source. The only thing we do not build yet is the Linux Kernel, but soon!

For hackers, here is the short and sweet:

```
sudo ./install-dependencies.sh && ./build.sh
```

See the [contributing docs](./CONTRIBUTING.md)
