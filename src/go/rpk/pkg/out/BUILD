load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "out",
    srcs = [
        "color.go",
        "in.go",
        "out.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/out",
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_alecaivazis_survey_v2//:survey",
        "@com_github_fatih_color//:color",
        "@com_github_spf13_afero//:afero",
        "@com_github_twmb_franz_go_pkg_kadm//:kadm",
        "@in_gopkg_yaml_v3//:yaml_v3",
    ],
)

go_test(
    name = "out_test",
    size = "small",
    srcs = [
        "in_test.go",
        "out_test.go",
    ],
    embed = [":out"],
    deps = [
        "//src/go/rpk/pkg/testfs",
        "@com_github_stretchr_testify//require",
    ],
)
