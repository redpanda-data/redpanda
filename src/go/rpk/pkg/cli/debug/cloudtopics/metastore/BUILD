load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "metastore",
    srcs = [
        "flags.go",
        "metastore.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug/cloudtopics/metastore",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/config",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
    ],
)

go_test(
    name = "metastore_test",
    srcs = ["flags_test.go"],
    embed = [":metastore"],
    deps = ["@com_github_stretchr_testify//require"],
)
