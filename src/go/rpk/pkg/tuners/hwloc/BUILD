load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "hwloc",
    srcs = [
        "cpuset.go",
        "hwloc.go",
        "hwloc_cmd.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/hwloc",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/os",
        "@org_uber_go_zap//:zap",
    ],
)

go_test(
    name = "hwloc_test",
    size = "small",
    srcs = ["cpuset_test.go"],
    embed = [":hwloc"],
    deps = ["@com_github_stretchr_testify//require"],
)
