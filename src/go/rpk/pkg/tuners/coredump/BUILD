load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "coredump",
    srcs = ["tuner.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/coredump",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/tuners",
        "//src/go/rpk/pkg/tuners/executors",
        "//src/go/rpk/pkg/tuners/executors/commands",
        "@com_github_spf13_afero//:afero",
    ],
)

go_test(
    name = "coredump_test",
    size = "small",
    srcs = ["tuner_test.go"],
    embed = [":coredump"],
    deps = [
        "//src/go/rpk/pkg/tuners/executors",
        "@com_github_spf13_afero//:afero",
        "@com_github_stretchr_testify//require",
    ],
)
