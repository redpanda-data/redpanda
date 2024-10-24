load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "oauth",
    srcs = [
        "errors.go",
        "load.go",
        "oauth.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/cloudapi",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/httpapi",
        "//src/go/rpk/pkg/os",
        "@com_github_lestrrat_go_jwx//jwt",
        "@com_github_spf13_afero//:afero",
        "@org_uber_go_zap//:zap",
    ],
)

go_test(
    name = "oauth_test",
    size = "small",
    srcs = [
        "load_test.go",
        "oauth_test.go",
    ],
    embed = [":oauth"],
    deps = [
        "//src/go/rpk/pkg/config",
        "@com_github_lestrrat_go_jwx//jwa",
        "@com_github_lestrrat_go_jwx//jwt",
        "@com_github_spf13_afero//:afero",
        "@com_github_stretchr_testify//require",
    ],
)
