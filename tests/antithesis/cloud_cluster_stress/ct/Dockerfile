# syntax=docker/dockerfile:1
#
# ==================================================================
# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
# ==================================================================
#
# Workload image for the ct test template. Builds the franz-go workload with
# Antithesis assertion cataloging, then ships it on a glibc base so the SDK
# can dlopen /usr/lib/libvoidstar.so (injected by Antithesis) at runtime.
# CGO must stay enabled for that path; without libvoidstar the SDK falls
# back to local behavior, so the same image also runs locally.

FROM golang:1.26-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Generate the assertion catalog (antithesis_catalog.go) so every assertion
# is pre-registered with Antithesis, then build with cgo for libvoidstar.
RUN go run github.com/antithesishq/antithesis-sdk-go/tools/antithesis-go-instrumentor@v0.7.2 \
    -assert_only -skip_test_files . \
    && CGO_ENABLED=1 go build -o /out/workload .

FROM debian:bookworm-slim
ENV NO_COLOR=1

# The binary lives inside the test template as a helper_ file (Antithesis
# ignores helper_-prefixed entries) and each command is a RELATIVE symlink to
# it. Relative, in-directory links stay valid when the template is copied out
# of the container (e.g. by `snouty validate` or Antithesis discovery); an
# absolute symlink to a path outside the template would dangle. The binary
# reports its own commands, so the list lives only in the Go source.
COPY --from=build /out/workload /opt/antithesis/test/v1/ct/helper_workload
RUN cd /opt/antithesis/test/v1/ct \
    && for c in $(./helper_workload list-commands); do \
         ln -s helper_workload "$c"; \
       done

# The container idles here (setup waits for health, emits setup_complete,
# then blocks) so Antithesis can execute the test commands inside it.
ENTRYPOINT ["/opt/antithesis/test/v1/ct/helper_workload", "setup"]
