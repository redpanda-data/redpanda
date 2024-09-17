load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "graf",
    srcs = [
        "dashboard.go",
        "graph.go",
        "row.go",
        "singlestat.go",
        "text.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/generate/graf",
    visibility = ["//visibility:public"],
)

go_test(
    name = "graf_test",
    size = "small",
    srcs = [
        "dashboard_test.go",
        "graph_test.go",
        "row_test.go",
        "singlestat_test.go",
    ],
    deps = [
        ":graf",
        "@com_github_stretchr_testify//require",
    ],
)
