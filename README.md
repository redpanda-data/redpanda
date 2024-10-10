# Redpanda

[![Documentation](https://img.shields.io/badge/documentation-black)](https://redpanda.com/documentation)
[![Slack](https://img.shields.io/badge/slack-purple)](https://redpanda.com/slack)
[![Twitter](https://img.shields.io/twitter/follow/redpandadata.svg?style=social&label=Follow)](https://twitter.com/intent/follow?screen_name=redpandadata)
![Go](https://github.com/redpanda-data/redpanda/workflows/Go/badge.svg)
![C++](https://github.com/redpanda-data/redpanda/workflows/build-test/badge.svg)
[![Redpanda University](https://img.shields.io/badge/Redpanda%20University-black)](https://university.redpanda.com/)
<p align="center">
<a href="https://redpanda.com/redpanda"><img src="docs/PANDA_sitting.png" alt="redpanda sitting" width="400"></a>
</p>

Redpanda is a streaming data platform for developers. Kafka® API-compatible. ZooKeeper® free. JVM free. We built it from the ground up to eliminate complexity common to Apache Kafka, improve performance by up to 10x, and make the storage architecture safer, and more resilient. The simpler devex lets you focus on your code (instead of fighting Kafka) and develop new use cases that were never before possible. The business benefits from a significantly lower total cost and faster time to market. A new platform that scales with you from the smallest projects to petabytes of data distributed across the globe!

# Community

[Slack](https://redpanda.com/slack) is the main way the community interacts with one another in real-time :)

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
  'https://dl.redpanda.com/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' \
  | sudo -E bash

sudo apt-get install redpanda
```

### On Fedora/RedHat/Amazon Linux

```
curl -1sLf \
  'https://dl.redpanda.com/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.rpm.sh' \
  | sudo -E bash

sudo yum install redpanda
```

### On Other Linux

To install from a `.tar.gz` archive, download the file and extract it into `/opt/redpanda`.

For amd64:

```
curl -LO \
  https://dl.redpanda.com/nzc4ZYQK3WRGd9sy/redpanda/raw/names/redpanda-amd64/versions/23.3.6/redpanda-23.3.6-amd64.tar.gz
```

For arm64:

```
curl -LO \
  https://dl.redpanda.com/nzc4ZYQK3WRGd9sy/redpanda/raw/names/redpanda-arm64/versions/23.3.6/redpanda-23.3.6-arm64.tar.gz
```

Replace `23.3.6` with the appropriate version you are trying to download.

## GitHub Actions


```yaml
    - name: start redpanda
      uses: redpanda-data/github-action@v0.1.3
      with:
        version: "latest"
```

Now you should be able to connect to `redpanda` (kafka-api) running at `localhost:9092`


## Build Manually

We use [Bazel](https://bazel.build/) as our build system. Bazel automatically manages most of the toolchains and third-party dependencies,
but there are a few system libraries and preinstalled tools our build assumes are available locally. You can bootstrap and build Redpanda
along with all its tests using:

```bash
sudo ./bazel/install-deps.sh
bazel build --config=release //...
```

For more build configurations check out `.bazelrc`.

# Release candidate builds

We create a release candidate (RC) build when we get close to a new release and publish these to make new features available for testing.
RC builds are not recommended for production use.

## RC releases on Debian/Ubuntu

```bash
curl -1sLf \
  'https://dl.redpanda.com/E4xN1tVe3Xy60GTx/redpanda-unstable/setup.deb.sh' \
  | sudo -E bash

sudo apt-get install redpanda
```

## RC releases on Fedora/RedHat/Amazon Linux

```bash
curl -1sLf \
  'https://dl.redpanda.com/E4xN1tVe3Xy60GTx/redpanda-unstable/setup.rpm.sh' \
  | sudo -E bash

sudo yum install redpanda
```

## RC releases on Docker

This is an example with the `v23.1.1-rc1` version prior to the 23.1.1 release.

```bash
docker pull docker.redpanda.com/redpandadata/redpanda-unstable:v23.1.1-rc1
```
