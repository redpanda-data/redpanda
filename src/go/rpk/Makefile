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

ifeq ($(shell uname),Darwin)
    BAZELCMD := bazelisk
    BAZEL_GAZELLE_CMD := run --config system-clang //:gazelle
    GOFUMPT_OS_CMD := $(shell find . -type f -name '*.go' -print0 | xargs -0 -n 1 $(GOFUMPTCMD) -w)
else
	BAZELCMD := bazel
    BAZEL_GAZELLE_CMD := run //:gazelle
    GOFUMPT_OS_CMD := 	$(shell find -type f -name '*.go' | xargs -n1 $(GOFUMPTCMD) -w)
endif

GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
GOPATH ?= $(shell go env GOPATH)
OUTDIR := $(GOOS)-$(GOARCH)

REV := $(shell git rev-parse --short HEAD)
VER := $(or $(VERSION),local-dev)
IMG_TAG:= $(or $(VERSION),latest)
BUILD_TIME=$(shell date -Iseconds)
VER_PKG='github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/version'
CONT_PKG='github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/container/common'

LDFLAGS=-X $(VER_PKG).version=$(VER) -X $(VER_PKG).rev=$(REV) -X $(CONT_PKG).tag=$(IMG_TAG) -X ${VER_PKG}.hostOs=${GOOS} -X ${VER_PKG}.hostArch=${GOARCH} -X ${VER_PKG}.buildTime=${BUILD_TIME}

all: help

ready: build test fmt lint bazel check_diff ## Runs all. Ensures commit is ready.

.PHONY: build
build: ## Build rpk.
	$(shell mkdir -p $(OUTDIR))
	$(GOCMD) build -ldflags '$(LDFLAGS)' -o $(OUTDIR) ./...

test: ## Run rpk unit tests.
	$(GOCMD) test ./... -count=1

tidy: ## Run go mod tidy.
	$(GOCMD) mod tidy

lint: install_golangci_lint run_linter ## Run golangci-lint linter.

fmt: install_gofumpt run_gofumpt ## Run Gofumpt formatter.

bazel: install_bazelisk bazel_generate_build bazel_tidy ## Autogenerates BUILD files and resolve bazel dependencies.

install_gofumpt:
	@echo "installing gofumpt..."
	@$(GOCMD) install mvdan.cc/gofumpt@latest

run_gofumpt:
	@echo "running gofumpt"
	$(GOFUMPT_OS_CMD)

install_golangci_lint:
	@which $(GOLANGCILINTCMD) || (if [ $$? -eq 1 ]; then  echo "golangci-lint not found, installing..."; curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin v1.62.0; fi)

run_linter:
	$(GOLANGCILINTCMD) run

install_bazelisk:
	@echo "installing bazelisk"
	@$(GOCMD) install github.com/bazelbuild/bazelisk@latest

bazel_generate_build: install_bazelisk
	@$(BAZELCMD) $(BAZEL_GAZELLE_CMD)

bazel_tidy: install_bazelisk
	@$(BAZELCMD) mod tidy

check_diff:
	git diff --exit-code

GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
CYAN   := $(shell tput -Txterm setaf 6)
RESET  := $(shell tput -Txterm sgr0)

help: ## Show this help.
	@echo ''
	@echo 'Usage:'
	@echo '  ${YELLOW}make${RESET} ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} { \
		if (/^[a-zA-Z_-]+:.*?##.*$$/) {printf "    ${YELLOW}%-20s${GREEN}%s${RESET}\n", $$1, $$2} \
		else if (/^## .*$$/) {printf "  ${CYAN}%s${RESET}\n", substr($$1,4)} \
		}' $(MAKEFILE_LIST)