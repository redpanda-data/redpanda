load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "tests",
    srcs = [
        "admin_client.go",
        "test_util.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/transform-sdk/tests",
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_stretchr_testify//require",
        "@com_github_testcontainers_testcontainers_go//:testcontainers-go",
        "@com_github_testcontainers_testcontainers_go_modules_redpanda//:redpanda",
        "@com_github_twmb_franz_go//pkg/kgo",
    ],
)

# TODO(bazel): Bazelify all the transform tests and run integration tests this way.
go_test(
    name = "tests_test",
    srcs = ["integration_test.go"],
    embed = [":tests"],
    tags = ["manual"],
    deps = [
        "@com_github_hamba_avro//:avro",
        "@com_github_stretchr_testify//require",
        "@com_github_testcontainers_testcontainers_go_modules_redpanda//:redpanda",
        "@com_github_twmb_franz_go//pkg/kgo",
        "@com_github_twmb_franz_go_pkg_kadm//:kadm",
        "@com_github_twmb_franz_go_pkg_sr//:sr",
    ],
)
