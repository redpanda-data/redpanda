module kafka-request-generator

// TODO: ubuntu:mantic and fedora:38 only have go version v1.21, so we are
// stuck with 1.21. This is until we upgrade the OS used for the open
// source CI build. (hopefully soon, once we can also build redpanda with
// clang-18)
go 1.21

require github.com/twmb/franz-go/pkg/kmsg v1.8.0
