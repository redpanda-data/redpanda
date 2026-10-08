# Redpanda

[![Documentation](https://img.shields.io/badge/documentation-black)](https://redpanda.com/documentation)
[![Slack](https://img.shields.io/badge/slack-purple)](https://redpanda.com/slack)
[![Twitter](https://img.shields.io/twitter/follow/redpandadata.svg?style=social&label=Follow)](https://twitter.com/intent/follow?screen_name=redpandadata)
[![Redpanda University](https://img.shields.io/badge/Redpanda%20University-black)](https://university.redpanda.com/)
<p align="center">
<a href="https://www.redpanda.com/"><img src="docs/icon-redpanda.svg" alt="redpanda icon" width="400"></a>
</p>

Redpanda is the most complete, Apache Kafka®-compatible streaming data platform, designed from the ground up to be lighter, faster, and simpler to operate. Free from ZooKeeper™ and JVMs, it prioritizes an end-to-end developer experience with a huge ecosystem of connectors, configurable tiered storage, and more.

# Table of Contents

- [Get started](#get-started)
  - [Prebuilt packages](#prebuilt-packages)
    - [Debian/Ubuntu](#debianubuntu)
    - [Fedora/RedHat/Amazon Linux](#fedoraredhatamazon-linux)
    - [macOS](#macos)
    - [rpk binaries](#rpk-binaries)
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
  'https://linux.pkg.redpanda.com/setup-redpanda.deb.sh' \
  | sudo -E bash

sudo apt-get install redpanda
```

### Fedora/RedHat/Amazon Linux

```
curl -1sLf \
  'https://linux.pkg.redpanda.com/setup-redpanda.rpm.sh' \
  | sudo -E bash

sudo yum install redpanda
```

### macOS

Docker is required on MacOS.

```
brew install redpanda-data/tap/redpanda && rpk container start
```

### rpk binaries

Prebuilt `rpk` binaries for Linux, macOS and Windows (amd64 and arm64) are
published at `https://rpk.redpanda.com/<release>/rpk-<os>-<arch>.zip`, where
`<release>` is a version tag such as `v26.2.4`, or `latest` for the newest
stable release.

```
# latest stable release
curl -LO https://rpk.redpanda.com/latest/rpk-linux-amd64.zip
curl -LO https://rpk.redpanda.com/latest/rpk-darwin-arm64.zip
curl -LO https://rpk.redpanda.com/latest/rpk-windows-amd64.zip

# a specific release
curl -LO https://rpk.redpanda.com/v26.2.4/rpk-linux-arm64.zip

# SHA256 checksums (note: no `v` in the file name)
curl -LO https://rpk.redpanda.com/v26.2.4/rpk_26.2.4_checksums.txt

# SHA256 checksums for latest, without the version in the file name
curl -LO https://rpk.redpanda.com/latest/rpk_checksums.txt
```

Available files per release: `rpk-linux-amd64.zip`, `rpk-linux-arm64.zip`,
`rpk-darwin-amd64.zip`, `rpk-darwin-arm64.zip`, `rpk-windows-amd64.zip`,
`rpk-windows-arm64.zip` and `rpk_<version>_checksums.txt`. `latest/` also
carries the checksums as `rpk_checksums.txt`.

### Other Linux environments

To install from a `.tar.gz` archive, download the file and extract it into `/opt/redpanda`.

For amd64:

```
curl -LO https://vectorized-public.s3.us-west-2.amazonaws.com/releases/redpanda/25.2.7/redpanda-25.2.7-amd64.tar.gz
```

For arm64:

```
curl -LO https://vectorized-public.s3.us-west-2.amazonaws.com/releases/redpanda/25.2.7/redpanda-25.2.7-arm64.tar.gz
```

Replace `25.2.7` with the version you want to download. See [Release Notes](https://docs.redpanda.com/streaming/current/reference/releases/redpanda/) for available releases.

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
  'https://linux.pkg.redpanda.com/setup-redpanda-unstable.deb.sh' \
  | sudo -E bash

sudo apt-get install redpanda
```

### RC releases on Fedora/RedHat/Amazon Linux

```bash
curl -1sLf \
  'https://linux.pkg.redpanda.com/setup-redpanda-unstable.rpm.sh' \
  | sudo -E bash

sudo yum install redpanda
```

### RC releases on Docker

Example with `v25.1.1-rc1`:

```bash
docker pull docker.redpanda.com/redpandadata/redpanda-unstable:v25.1.1-rc1
```

# Community

- [Slack](https://redpanda.com/slack): This is the primary way the community interacts in real time. :)
- [Github Discussions](https://github.com/redpanda-data/redpanda/discussions): This is for longer, async, thoughtful discussions.
- [GitHub Issues](https://github.com/redpanda-data/redpanda/issues): This is reserved only for actual issues. Please use the mailing list for discussions.
- [Code of Conduct](./CODE_OF_CONDUCT.md)
- [Contribute to the Code](./CONTRIBUTING.md)

# Resources

- [Redpanda Documentation](https://docs.redpanda.com/home/)
- [Redpanda Blog](https://www.redpanda.com/blog)
- [Upcoming Redpanda Events](https://www.redpanda.com/events)
- [Redpanda Support](https://support.redpanda.com/)
- [Redpanda University](https://university.redpanda.com/)
