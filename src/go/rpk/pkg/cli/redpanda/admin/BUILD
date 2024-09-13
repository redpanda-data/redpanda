load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "admin",
    srcs = ["admin.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/redpanda/admin",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/cli/redpanda/admin/brokers",
        "//src/go/rpk/pkg/cli/redpanda/admin/config",
        "//src/go/rpk/pkg/cli/redpanda/admin/partitions",
        "//src/go/rpk/pkg/config",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
    ],
)
