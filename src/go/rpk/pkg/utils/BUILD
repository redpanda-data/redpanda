load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "utils",
    srcs = [
        "chained_error.go",
        "collect.go",
        "files.go",
        "os.go",
        "regex_filter.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils",
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_spf13_afero//:afero",
        "@org_uber_go_zap//:zap",
    ],
)

go_test(
    name = "utils_test",
    size = "small",
    srcs = [
        "files_test.go",
        "regex_filter_test.go",
    ],
    embed = [":utils"],
    deps = [
        "@com_github_spf13_afero//:afero",
        "@com_github_stretchr_testify//require",
    ],
)
