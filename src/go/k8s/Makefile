
# Image URL to use all building/pushing image targets
OPERATOR_IMG_LATEST ?= "localhost/redpanda-operator:dev"
CONFIGURATOR_IMG_LATEST ?= "localhost/configurator:dev"
# Produce CRDs that work back to Kubernetes 1.11 (no version conversion)
CRD_OPTIONS ?= "crd:trivialVersions=true,preserveUnknownFields=false"

# default redpanda image to load
REDPANDA_IMG ?= "localhost/redpanda:dev"

ifeq (aarch64,$(uname -m))
TARGETARCH = arm64
else
TARGETARCH = amd64
endif

SHELL := /bin/bash

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

ifneq ($(TEST_NAME), )
TEST_ONLY_FLAG = --test $(TEST_NAME)
endif

all: manager

# Run tests
test: generate vet manifests envtest
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" go test --race -v ./... -coverprofile cover.out

# Build manager binary
manager: generate vet
	go build -o bin/manager main.go

# Run against the configured Kubernetes cluster in ~/.kube/config
run: generate vet manifests
	go run ./main.go

# Install CRDs into a cluster
install: manifests kustomize
	$(KUSTOMIZE) build config/crd | kubectl apply -f -

# Uninstall CRDs from a cluster
uninstall: manifests kustomize
	$(KUSTOMIZE) build config/crd | kubectl delete -f -

# Deploy controller in the configured Kubernetes cluster in ~/.kube/config
deploy: manifests kustomize
	cd config/manager && $(KUSTOMIZE) edit set image vectorized/redpanda-operator=${OPERATOR_IMG_LATEST}
	$(KUSTOMIZE) build config/default | kubectl apply -f -
	kind load docker-image ${REDPANDA_IMG}

# Deploy pre loaded controller in the configured Kind Kubernetes cluster
deploy-to-kind: manifests kustomize push-to-kind deploy

# UnDeploy controller from the configured Kubernetes cluster in ~/.kube/config
undeploy:
	$(KUSTOMIZE) build config/default | kubectl delete -f -

# Generate manifests e.g. CRD, RBAC etc.
manifests: controller-gen
	$(CONTROLLER_GEN) $(CRD_OPTIONS) rbac:roleName=manager-role webhook paths="./..." output:crd:artifacts:config=config/crd/bases

# Run go vet against code
vet:
	go vet ./...

# Generate code
generate: controller-gen
	$(CONTROLLER_GEN) object:headerFile="../../../licenses/boilerplate.go.txt" paths="./..."

# Creates kind cluster
kind-create:
	(kind get clusters | grep kind && echo "kind cluster already exists") || (kind create cluster --config kind.yaml && echo "kind cluster created")

# Install cert-manager
certmanager-install: kind-create
	./hack/install-cert-manager.sh

prepare-dockerfile:
	echo "ARG BUILDPLATFORM" > Dockerfile.out
	cat Dockerfile >> Dockerfile.out

# Build the docker image
docker-build: prepare-dockerfile
	echo "~~~ Building operator image :docker:"
	docker buildx build --build-arg BUILDPLATFORM='linux/${TARGETARCH}' --build-arg TARGETARCH=${TARGETARCH} --target=manager --load -f Dockerfile.out -t ${OPERATOR_IMG_LATEST} ../

# Build the docker image
docker-build-configurator: prepare-dockerfile
	echo "~~~ Building configurator image :docker:"
	docker buildx build --build-arg BUILDPLATFORM='linux/${TARGETARCH}' --build-arg TARGETARCH=${TARGETARCH} --target=configurator --load -f Dockerfile.out -t ${CONFIGURATOR_IMG_LATEST} ../

# Preload controller image to kind cluster
push-to-kind: kind-create certmanager-install
	kind load docker-image ${OPERATOR_IMG_LATEST}
	kind load docker-image ${CONFIGURATOR_IMG_LATEST}

# Execute end to end tests
e2e-tests: kuttl test docker-build docker-build-configurator
	echo "~~~ Running kuttl tests :k8s:"
	$(KUTTL) test $(TEST_ONLY_FLAG) $(KUTTL_TEST_FLAGS)

# Execute end to end unstable tests
e2e-unstable-tests: kuttl test docker-build docker-build-configurator
	echo "~~~ Running kuttl unstable tests :k8s:"
	$(KUTTL) test --config kuttl-unstable-test.yaml --kind-context=${PR_NR:-kind} $(TEST_ONLY_FLAG) $(KUTTL_TEST_FLAGS)

# Execute end to end tests using helm as an installation
helm-e2e-tests: kuttl test docker-build docker-build-configurator
	echo "~~~ Running kuttl tests :k8s:"
	$(KUTTL) test --config kuttl-helm-test.yaml $(TEST_ONLY_FLAG) $(KUTTL_TEST_FLAGS)

##@ Build Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
KUTTL ?= $(LOCALBIN)/kubectl-kuttl

## Tool Versions
KUSTOMIZE_VERSION ?= v4.5.7
CONTROLLER_TOOLS_VERSION ?= v0.4.1
KUTTL_VERSION ?= v0.15.0

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	test -s $(LOCALBIN)/controller-gen || GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

KUSTOMIZE_INSTALL_SCRIPT ?= "https://raw.githubusercontent.com/kubernetes-sigs/kustomize/master/hack/install_kustomize.sh"
.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	test -s $(LOCALBIN)/kustomize || { curl -Ss $(KUSTOMIZE_INSTALL_SCRIPT) | bash -s -- $(subst v,,$(KUSTOMIZE_VERSION)) $(LOCALBIN); }

.PHONY: envtest
envtest: $(ENVTEST) ## Download envtest-setup locally if necessary.
$(ENVTEST): $(LOCALBIN)
	test -s $(LOCALBIN)/setup-envtest || GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest

.PHONY: kuttl
kuttl: $(KUTTL)
$(KUTTL): $(LOCALBIN)
	test -s $(LOCALBIN)/kubectl-kuttl || GOBIN=$(LOCALBIN) go install github.com/kudobuilder/kuttl/cmd/kubectl-kuttl@$(KUTTL_VERSION)
# go-get-tool will 'go get' any package $2 and install it to $1.
PROJECT_DIR := $(shell dirname $(abspath $(lastword $(MAKEFILE_LIST))))
define go-get-tool
@[ -f $(1) ] || { \
set -e ;\
TMP_DIR=$$(mktemp -d) ;\
cd $$TMP_DIR ;\
go mod init tmp ;\
echo "Downloading $(2)" ;\
GOBIN=$(PROJECT_DIR)/bin go install $(2) ;\
rm -rf $$TMP_DIR ;\
}
endef
