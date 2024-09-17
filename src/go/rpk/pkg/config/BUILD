load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "config",
    srcs = [
        "config.go",
        "format.go",
        "params.go",
        "redpanda_yaml.go",
        "rpk_yaml.go",
        "weak.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/config",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/net",
        "//src/go/rpk/pkg/os",
        "//src/go/rpk/pkg/out",
        "//src/go/rpk/pkg/publicapi",
        "@com_github_mattn_go_isatty//:go-isatty",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@com_github_twmb_tlscfg//:tlscfg",
        "@in_gopkg_yaml_v3//:yaml_v3",
        "@org_golang_x_exp//maps",
        "@org_golang_x_term//:term",
        "@org_uber_go_zap//:zap",
        "@org_uber_go_zap//zapcore",
    ],
)

go_test(
    name = "config_test",
    size = "small",
    srcs = [
        "config_test.go",
        "params_test.go",
        "rpk_yaml_test.go",
        "weak_test.go",
    ],
    data = ["//conf:redpanda.yaml"],
    embed = [":config"],
    env = {
        # NOTE: this is a bit of a hack and rules_go should do this by default
        "HOME": "$TEST_TMPDIR",
    },
    deps = [
        "//src/go/rpk/pkg/testfs",
        "//src/go/rpk/pkg/utils",
        "@com_github_spf13_afero//:afero",
        "@com_github_stretchr_testify//require",
        "@in_gopkg_yaml_v3//:yaml_v3",
        "@org_golang_x_exp//maps",
    ],
)
