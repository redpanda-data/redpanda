load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "auth",
    srcs = [
        "auth.go",
        "create.go",
        "delete.go",
        "edit.go",
        "list.go",
        "rename.go",
        "use.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cloud/auth",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/out",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
    ],
)
