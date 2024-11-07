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

Redpanda is the most complete, Apache Kafka®-compatible streaming data platform, designed from the ground up to be lighter, faster, and simpler to operate. Free from ZooKeeper™ and JVMs, it prioritizes an end-to-end developer experience with a huge ecosystem of connectors, configurable tiered storage, and more.

# Table of Contents
- [Get started](#get-started)
  - [Prebuilt packages](#prebuilt-packages)
    - [Debian/Ubuntu](#debianubuntu)
    - [Fedora/RedHat/Amazon Linux](#fedoraredhatamazon-linux)
    - [macOS](#macos)
    - [Other Linux environments](#other-linux-environments)
  - [Build manually](#build-manually)
  - [Release candidate builds](#release-candidate-builds)
    - [RC releases on Debian/Ubuntu](#rc-releases-on-debianubuntu)
    - [RC releases on Fedora/RedHat/Amazon Linux](#rc-releases-on-fedoraredhatamazon-linux)
    - [RC releases on Docker](#rc-releases-on-docker)
- [Community](#community)
- [Resources](#resources)

# Get started

## Prebuilt packages

Redpanda Data recommends using the following free, prebuilt stable releases.

### Debian/Ubuntu

```
curl -1sLf \
  'https://dl.redpanda.com/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' \
  | sudo -E bash

sudo apt-get install redpanda
```

### Fedora/RedHat/Amazon Linux

```
curl -1sLf \
  'https://dl.redpanda.com/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.rpm.sh' \
  | sudo -E bash

sudo yum install redpanda
```

### macOS

Download the `rpk` [binary here](https://github.com/redpanda-data/redpanda/releases). Docker is required on MacOS.

```
brew install redpanda-data/tap/redpanda && rpk container start
```

### Other Linux environments

To install from a `.tar.gz` archive, download the file and extract it into `/opt/redpanda`.

For amd64:

```
curl -LO \
  https://dl.redpanda.com/nzc4ZYQK3WRGd9sy/redpanda/raw/names/redpanda-amd64/versions/23.3.6/redpanda-24.2.7-amd64.tar.gz
```

For arm64:

```
curl -LO \
  https://dl.redpanda.com/nzc4ZYQK3WRGd9sy/redpanda/raw/names/redpanda-arm64/versions/23.3.6/redpanda-24.2.7-arm64.tar.gz
```

Replace `24.2.7` with the version you want to download. See [Release Notes](https://github.com/redpanda-data/redpanda/releases).

## Build Manually

Redpanda Data uses [Bazel](https://bazel.build/) as the build system. Bazel automatically manages most of the toolchains and third-party dependencies.

We rely on [bazelisk](https://github.com/bazelbuild/bazelisk) to get the right
version of bazel needed for the build. You can for example install it as follows
and add it to your $PATH (or use one of the other suggested ways from their
repo).

```
wget -O ~/bin/bazel https://github.com/bazelbuild/bazelisk/releases/latest/download/bazelisk-linux-amd64 && chmod +x ~/bin/bazel
```

There are a few system libraries and preinstalled tools our build assumes are
available locally. To bootstrap and build redpanda along with all its tests.

```bash
sudo ./bazel/install-deps.sh
bazel build --config=release //...
```

For more build configurations, see `.bazelrc`.

## Release candidate builds

Redpanda Data creates a release candidate (RC) build when we get close to a new release, and we publish it to make new features available for testing.
RC builds are not recommended for production use.

### RC releases on Debian/Ubuntu

```bash
curl -1sLf \
  'https://dl.redpanda.com/E4xN1tVe3Xy60GTx/redpanda-unstable/setup.deb.sh' \
  | sudo -E bash

sudo apt-get install redpanda
```

### RC releases on Fedora/RedHat/Amazon Linux

```bash
curl -1sLf \
  'https://dl.redpanda.com/E4xN1tVe3Xy60GTx/redpanda-unstable/setup.rpm.sh' \
  | sudo -E bash

sudo yum install redpanda
```

### RC releases on Docker

Example with `v23.1.1-rc1`:

```bash
docker pull docker.redpanda.com/redpandadata/redpanda-unstable:v23.1.1-rc1
```

# Community

[Slack](https://redpanda.com/slack): This is the primary way the community interacts in real time. :)

[Github Discussions](https://github.com/redpanda-data/redpanda/discussions): This is for longer, async, thoughtful discussions.

[GitHub Issues](https://github.com/redpanda-data/redpanda/issues): This is reserved only for actual issues. Please use the mailing list for discussions.

[Code of Conduct](./CODE_OF_CONDUCT.md)

[Contribute to the Code](./CONTRIBUTING.md)

# Resources

[Redpanda Documentation](https://docs.redpanda.com/home/)

[Redpanda Blog](https://www.redpanda.com/blog)

[Upcoming Redpanda Events](https://www.redpanda.com/events)

[Redpanda Support](https://support.redpanda.com/)

[Redpanda University](https://university.redpanda.com/)