load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "auth0",
    srcs = [
        "api.go",
        "auth0.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/cloudapi",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/httpapi",
        "//src/go/rpk/pkg/oauth",
        "@com_github_pkg_browser//:browser",
    ],
)
