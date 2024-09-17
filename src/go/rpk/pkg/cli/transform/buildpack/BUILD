load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "buildpack",
    srcs = ["buildpack.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/transform/buildpack",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/httpapi",
        "//src/go/rpk/pkg/os",
        "//src/go/rpk/pkg/utils",
        "@com_github_schollz_progressbar_v3//:progressbar",
        "@com_github_spf13_afero//:afero",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "buildpack_test",
    size = "small",
    srcs = ["download_test.go"],
    embed = [":buildpack"],
    deps = [
        "//src/go/rpk/pkg/testfs",
        "@com_github_spf13_afero//:afero",
        "@com_github_stretchr_testify//require",
    ],
)
