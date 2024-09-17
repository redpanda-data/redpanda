load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "iotune",
    srcs = [
        "data.go",
        "io_tune.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/iotune",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/cloud/provider",
        "//src/go/rpk/pkg/os",
        "@org_uber_go_zap//:zap",
    ],
)

go_test(
    name = "iotune_test",
    size = "small",
    srcs = ["data_test.go"],
    deps = [
        ":iotune",
        "@com_github_stretchr_testify//require",
    ],
)
