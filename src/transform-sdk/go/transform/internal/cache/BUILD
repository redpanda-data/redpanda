load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "cache",
    srcs = ["cache.go"],
    importpath = "github.com/redpanda-data/redpanda/src/transform-sdk/go/transform/internal/cache",
    visibility = ["//src/transform-sdk/go/transform:__subpackages__"],
)

go_test(
    name = "cache_test",
    size = "small",
    srcs = ["cache_test.go"],
    deps = [":cache"],
)
