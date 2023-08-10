
IMAGE ?= "localhost/cluster-to-redpanda-migration:latest"
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

GOLANGCI_LINT ?= $(LOCALBIN)/golangci-lint

ifeq (aarch64,$(uname -m))
TARGETARCH = arm64
else
TARGETARCH = amd64
endif

GOOS ?= linux

VERSION ?= v0.0.1
VER_PKG=github.com/redpanda-data/redpanda/src/go/cluster-to-redpanda-migration/cmd/version
LDFLAGS=-X $(VER_PKG).version=$(VERSION)

GOBUILD_CLI=go build -ldflags '$(LDFLAGS)' -o $(LOCALBIN)/cluster-to-redpanda-migration/cluster-to-redpanda-migration


clean: ## clean build output
	rm -rf $(LOCALBIN)/cluster-to-redpanda-migration/*

compile: clean ## build and place in local bin directory
	mkdir -p $(LOCALBIN)/cluster-to-redpanda-migration/
	GOOS=$(GOOS) GOARCH=$(TARGETARCH) ${GOBUILD_CLI} ./main.go
	chmod +x $(LOCALBIN)/cluster-to-redpanda-migration/cluster-to-redpanda-migration

compile-linux: GOOS=linux
compile-linux: TARGETARCH=amd64
compile-linux: compile


.PHONY: gofumpt-install
gofumpt-install:
	go install mvdan.cc/gofumpt@latest

.PHONY: goimports-install
goimports-install:
	go install golang.org/x/tools/cmd/goimports@latest

.PHONY: gofumpt-lint
gofumpt-lint: gofumpt-install
	find . -type f -name '*.go' | xargs -n1 gofumpt -w -lang=1.20

.PHONY: goimports
goimports: goimports-install
	goimports -w .

.PHONY: golangci-lint-install
golangci-lint-install:
	mkdir -p $(GOLANGCI_LINT)
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOLANGCI_LINT) v1.52.2

.PHONY: golangci-lint
golangci-lint:
	$(GOLANGCI_LINT)/golangci-lint run --go=1.20

.PHONY: lint
lint: gofumpt-lint golangci-lint
