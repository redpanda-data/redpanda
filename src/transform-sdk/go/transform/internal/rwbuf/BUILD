load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "rwbuf",
    srcs = [
        "rwbuf.go",
        "unsafe.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/transform-sdk/go/transform/internal/rwbuf",
    visibility = ["//src/transform-sdk/go/transform:__subpackages__"],
)

go_test(
    name = "rwbuf_test",
    size = "small",
    srcs = ["rwbuf_test.go"],
    embed = [":rwbuf"],
)
