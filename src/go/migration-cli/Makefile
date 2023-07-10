
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

GOLANGCI_LINT ?= $(LOCALBIN)/golangci-lint

GOBUILD_CLI=go build -o $(LOCALBIN)/migration-cli/migration-cli

clean: ## clean build output
	rm -rf $(LOCALBIN)/migration-cli/*

compile: clean ## build and place in local bin directory
	mkdir -p $(LOCALBIN)/migration-cli/
	${GOBUILD_CLI} ./main.go
	chmod +x $(LOCALBIN)/migration-cli/migration-cli

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
