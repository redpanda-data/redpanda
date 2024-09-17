load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "cobraext",
    srcs = ["cobraext.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cobraext",
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_spf13_cobra//:cobra",
        "@com_github_spf13_pflag//:pflag",
    ],
)

go_test(
    name = "cobraext_test",
    size = "small",
    srcs = ["cobraext_test.go"],
    embed = [":cobraext"],
    deps = [
        "@com_github_spf13_cobra//:cobra",
        "@com_github_spf13_pflag//:pflag",
    ],
)
