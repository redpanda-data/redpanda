load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "httpapi",
    srcs = ["httpapi.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi",
    visibility = ["//visibility:public"],
    deps = [
        "@in_gopkg_yaml_v3//:yaml_v3",
        "@org_uber_go_zap//:zap",
    ],
)

go_test(
    name = "httpapi_test",
    size = "small",
    srcs = ["httpapi_test.go"],
    embed = [":httpapi"],
    deps = ["@com_github_stretchr_testify//require"],
)
