# Author 2021 Per Buer
# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

GOCMD=go
GOTEST=$(GOCMD) test
GOBUILD=$(GOCMD) build
GOOS := $(shell go env GOOS)
GOARCH := $(shell go env GOARCH)
OUTDIR := $(GOOS)-$(GOARCH)

VER_PKG=github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/version
CONT_PKG=github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/container/common

REV := $(shell git rev-parse --short HEAD)
VERSION := $(shell git describe --tags --abbrev=0)
IMG_TAG=$(REV)
LDFLAGS=-X $(VER_PKG).version=$(VERSION) -X $(VER_PKG).rev=$(REV) -X $(CONT_PKG).tag=$(IMG_TAG)

# Silly color to make the help pretty.
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
RESET  := $(shell tput -Txterm sgr0)

all: help

build: ## Build rpk
	$(shell mkdir -p $(OUTDIR))
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o $(OUTDIR) ./...

test: ## Run the tests
	$(GOTEST) ./...

coverage: ## Run the tests with coverage
	$(GOTEST) -coverprofile=coverage.out ./...

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