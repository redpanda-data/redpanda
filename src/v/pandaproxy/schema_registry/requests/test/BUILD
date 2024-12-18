load("//bazel:test.bzl", "redpanda_cc_btest")

redpanda_cc_btest(
    name = "get_subject_versions_version",
    timeout = "short",
    srcs = [
        "get_subject_versions_version.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/pandaproxy",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "post_subject_versions",
    timeout = "short",
    srcs = [
        "post_subject_versions.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/pandaproxy",
        "//src/v/test_utils:seastar_boost",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)
