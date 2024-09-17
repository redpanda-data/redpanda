load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "group",
    srcs = [
        "describe.go",
        "group.go",
        "offset_delete.go",
        "seek.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/group",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/kafka",
        "//src/go/rpk/pkg/out",
        "//src/go/rpk/pkg/utils",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@com_github_twmb_franz_go//pkg/kerr",
        "@com_github_twmb_franz_go_pkg_kadm//:kadm",
    ],
)

go_test(
    name = "group_test",
    size = "small",
    srcs = ["seek_test.go"],
    embed = [":group"],
    deps = [
        "//src/go/rpk/pkg/testfs",
        "@com_github_stretchr_testify//require",
        "@com_github_twmb_franz_go_pkg_kadm//:kadm",
    ],
)
