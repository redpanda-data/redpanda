load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "cloud",
    srcs = ["providers.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/cloud/aws",
        "//src/go/rpk/pkg/cloud/gcp",
        "//src/go/rpk/pkg/cloud/provider",
        "@org_uber_go_zap//:zap",
    ],
)

go_test(
    name = "cloud_test",
    size = "small",
    srcs = ["providers_test.go"],
    embed = [":cloud"],
    deps = [
        "//src/go/rpk/pkg/cloud/provider",
        "@com_github_stretchr_testify//require",
    ],
)
