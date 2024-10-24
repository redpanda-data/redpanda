load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "sr",
    srcs = [
        "abi.go",  # keep
        "client.go",
        "doc.go",
        "encoding.go",
        "serde.go",
        "stub_abi.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/transform-sdk/go/transform/sr",
    visibility = ["//visibility:public"],
    deps = [
        "//src/transform-sdk/go/transform/internal/cache",
        "//src/transform-sdk/go/transform/internal/rwbuf",
    ],
)

go_test(
    name = "sr_test",
    size = "small",
    srcs = [
        "encoding_test.go",
        "serde_test.go",
    ],
    embed = [":sr"],
    deps = ["//src/transform-sdk/go/transform/internal/rwbuf"],
)
