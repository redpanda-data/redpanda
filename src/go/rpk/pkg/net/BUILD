load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "net",
    srcs = [
        "hostport.go",
        "interfaces.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/net",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/utils",
        "@org_uber_go_zap//:zap",
    ],
)

go_test(
    name = "net_test",
    size = "small",
    srcs = ["hostport_test.go"],
    embed = [":net"],
    deps = ["@com_github_stretchr_testify//require"],
)
