load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "context",
    srcs = [
        "context.go",
        "delete.go",
        "list.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/registry/context",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/adminapi",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/out",
        "//src/go/rpk/pkg/schemaregistry",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@com_github_twmb_franz_go_pkg_sr//:sr",
    ],
)

go_test(
    name = "context_test",
    srcs = ["context_test.go"],
    deps = [
        ":context",
        "//src/go/rpk/pkg/cli/registry",
        "//src/go/rpk/pkg/config",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@com_github_stretchr_testify//require",
        "@com_github_twmb_franz_go_pkg_sr//:sr",
        "@com_github_twmb_franz_go_pkg_sr//srfake",
    ],
)
