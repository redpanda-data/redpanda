#!/bin/bash
# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

version=$1
rev=${2:-$(git rev-parse --short HEAD)}
# auth0 clientId for the vectorized cloud, optional
clientId=$3
img_tag=${version:-latest}

out_dir="$(go env GOOS)-$(go env GOARCH)"

mkdir -p "${out_dir}"

ver_pkg='github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/version'
cont_pkg='github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/container/common'
auth0_pkg='github.com/redpanda-data/redpanda/src/go/rpk/pkg/vcloud'

go build \
  -ldflags "-X ${ver_pkg}.version=${version} -X ${ver_pkg}.rev=${rev} -X ${cont_pkg}.tag=${img_tag} -X ${auth0_pkg}.clientId=${clientId}" \
  -o "${out_dir}" ./...
