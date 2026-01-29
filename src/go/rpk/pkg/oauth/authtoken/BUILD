load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "authtoken",
    srcs = [
        "authtoken.go",
        "errors.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/authtoken",
    visibility = ["//visibility:public"],
    deps = ["@com_github_lestrrat_go_jwx_v2//jwt"],
)

go_test(
    name = "authtoken_test",
    srcs = ["authtoken_test.go"],
    embed = [":authtoken"],
    deps = [
        "@com_github_lestrrat_go_jwx_v2//jwa",
        "@com_github_lestrrat_go_jwx_v2//jwt",
        "@com_github_stretchr_testify//require",
    ],
)
