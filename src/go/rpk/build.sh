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

out_dir="$(go env GOOS)-$(go env GOARCH)"

mkdir -p ${out_dir}

pkg='vectorized/pkg/cli/cmd/version'

go build -ldflags "-X ${pkg}.version=${version} -X ${pkg}.rev=${rev}" -o ${out_dir} ./...
