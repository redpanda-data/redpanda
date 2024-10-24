load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "registry",
    srcs = [
        "compatibility_level.go",
        "registry.go",
        "subject.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/registry",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/cli/registry/mode",
        "//src/go/rpk/pkg/cli/registry/schema",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/out",
        "//src/go/rpk/pkg/schemaregistry",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@com_github_twmb_franz_go_pkg_sr//:sr",
    ],
)
