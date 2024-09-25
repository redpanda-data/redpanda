# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

GOCMD=go
GOLANGCILINTCMD=golangci-lint
GOFUMPTCMD=gofumpt
BAZELCMD=bazel

GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
OUTDIR := $(GOOS)-$(GOARCH)

REV := $(shell git rev-parse --short HEAD)
VER := $(or $(VERSION),local-dev)
IMG_TAG:= $(or $(VERSION),latest)
BUILD_TIME=$(shell date -Iseconds)
VER_PKG='github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/version'
CONT_PKG='github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/container/common'

LDFLAGS=-X $(VER_PKG).version=$(VER) -X $(VER_PKG).rev=$(REV) -X $(CONT_PKG).tag=$(IMG_TAG) -X ${VER_PKG}.hostOs=${GOOS} -X ${VER_PKG}.hostArch=${GOARCH} -X ${VER_PKG}.buildTime=${BUILD_TIME}

.PHONY: ready
ready: build test fmt lint bazel check_diff

.PHONY: build
build:
	$(shell mkdir -p $(OUTDIR))
	$(GOCMD) build -ldflags '$(LDFLAGS)' -o $(OUTDIR) ./...

.PHONY: test
test:
	$(GOCMD) test ./... -count=1

.PHONY: tidy
tidy:
	$(GOCMD) mod tidy

.PHONY: lint
lint: install_golangci_lint run_linter

.PHONY: fmt
fmt: install_gofumpt run_gofumpt

.PHONY: bazel
bazel: install_bazelisk bazel_generate_build bazel_tidy

.PHONY: install_gofumpt
install_gofumpt:
	@echo "installing gofumpt..."
	@$(GOCMD) install mvdan.cc/gofumpt@latest

.PHONY: run_gofumpt
run_gofumpt:
	@echo "running gofumpt"
	$(shell find -type f -name '*.go' | xargs -n1 $(GOFUMPTCMD) -w)

.PHONY: install_golangcilint
install_golangci_lint:
	@echo "installing golangci-lint"
	@$(GOCMD) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

.PHONY: run_linter
run_linter:
	$(GOLANGCILINTCMD) run

.PHONY: install_bazelisk
install_bazelisk:
	@echo "installing bazelisk"
	@$(GOCMD) install github.com/bazelbuild/bazelisk@latest

.PHONY: bazel_generate_build
bazel_generate_build: install_bazelisk
	@$(BAZELCMD) run //:gazelle

.PHONY: bazel_tidy
bazel_tidy: install_bazelisk
	@$(BAZELCMD) mod tidy

.PHONY: check_diff
check_diff:
	$(shell git diff --exit-code)