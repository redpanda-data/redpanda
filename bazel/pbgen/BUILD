load("@rules_go//go:def.bzl", "go_binary", "go_library", "go_test")

go_library(
    name = "pbgen_lib",
    srcs = [
        "main.go",
        "utils.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/bazel/pbgen",
    visibility = ["//visibility:private"],
    deps = [
        "//proto/redpanda/core/pbgen:options_go_proto",
        "//proto/redpanda/core/pbgen:rpc_go_proto",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//reflect/protodesc",
        "@org_golang_google_protobuf//reflect/protoreflect",
        "@org_golang_google_protobuf//reflect/protoregistry",
        "@org_golang_google_protobuf//types/descriptorpb",
        "@org_golang_google_protobuf//types/pluginpb",
    ],
)

go_binary(
    name = "pbgen",
    embed = [":pbgen_lib"],
    visibility = ["//visibility:public"],
)

go_test(
    name = "pbgen_test",
    size = "small",
    srcs = ["utils_test.go"],
    embed = [":pbgen_lib"],
)
