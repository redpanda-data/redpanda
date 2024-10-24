load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "user",
    srcs = [
        "create.go",
        "delete.go",
        "list.go",
        "update.go",
        "user.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/security/user",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/adminapi",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/out",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
    ],
)
