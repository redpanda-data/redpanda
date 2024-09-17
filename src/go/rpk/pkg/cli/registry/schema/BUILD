load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "schema",
    srcs = [
        "check_compatibility.go",
        "create.go",
        "delete.go",
        "get.go",
        "list.go",
        "references.go",
        "schema.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/registry/schema",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/out",
        "//src/go/rpk/pkg/schemaregistry",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@com_github_twmb_franz_go_pkg_sr//:sr",
        "@com_github_twmb_types//:types",
    ],
)

go_test(
    name = "schema_test",
    size = "small",
    srcs = ["schema_test.go"],
    embed = [":schema"],
    deps = [
        "@com_github_spf13_afero//:afero",
        "@com_github_stretchr_testify//require",
        "@com_github_twmb_franz_go_pkg_sr//:sr",
    ],
)
