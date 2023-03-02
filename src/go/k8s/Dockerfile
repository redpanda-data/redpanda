FROM --platform=$BUILDPLATFORM public.ecr.aws/docker/library/golang:1.20.1 as builder

ARG TARGETARCH

# Copy the rpk as a close depedency
WORKDIR /workspace
COPY rpk/ rpk/

WORKDIR /workspace/k8s
# Copy the Go Modules manifests
COPY k8s/go.mod go.mod
COPY k8s/go.sum go.sum

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source
COPY k8s/ .

# build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} GO111MODULE=on go build -a -o manager main.go && \
    CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} GO111MODULE=on go build -a -o configurator cmd/configurator/main.go

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/static:nonroot as manager
WORKDIR /
COPY --from=builder /workspace/k8s/manager .
USER 65532:65532
ENTRYPOINT ["/manager"]

FROM gcr.io/distroless/static:nonroot as configurator
WORKDIR /
COPY --from=builder /workspace/k8s/configurator .
USER 65532:65532
ENTRYPOINT ["/configurator"]
