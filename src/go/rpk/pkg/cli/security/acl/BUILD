load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "acl",
    srcs = [
        "acl.go",
        "common.go",
        "create.go",
        "delete.go",
        "list.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/security/acl",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/kafka",
        "//src/go/rpk/pkg/out",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@com_github_twmb_franz_go_pkg_kadm//:kadm",
        "@com_github_twmb_franz_go_pkg_kmsg//:kmsg",
        "@com_github_twmb_types//:types",
    ],
)

go_test(
    name = "acl_test",
    size = "small",
    srcs = ["common_test.go"],
    embed = [":acl"],
    deps = [
        "@com_github_stretchr_testify//require",
        "@com_github_twmb_franz_go_pkg_kadm//:kadm",
        "@com_github_twmb_franz_go_pkg_kmsg//:kmsg",
    ],
)
