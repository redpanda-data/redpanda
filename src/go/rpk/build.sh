#!/bin/bash
# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

version=$1
rev=${2:-$(git rev-parse --short HEAD)}
buildTime="$(date -Iseconds)"
hostOs="$(go env GOHOSTOS)"
hostArch="$(go env GOHOSTARCH)"
# auth0 clientId for the vectorized cloud, optional
clientId=$3
img_tag=${version:-latest}

out_dir="$(go env GOOS)-$(go env GOARCH)"

mkdir -p "${out_dir}"

ver_pkg='github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/version'
cont_pkg='github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/container/common'

go build \
  -ldflags "-X ${ver_pkg}.version=${version} -X ${ver_pkg}.rev=${rev} -X ${cont_pkg}.tag=${img_tag} -X ${ver_pkg}.buildTime=${buildTime} -X ${ver_pkg}.hostOs=${hostOs} -X ${ver_pkg}.hostArch=${hostArch}" \
  -o "${out_dir}" ./...
