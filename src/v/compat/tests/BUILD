load("//bazel:test.bzl", "redpanda_cc_btest")

redpanda_cc_btest(
    name = "compat_test",
    timeout = "short",
    srcs = [
        "compat_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/compat",
        "//src/v/compat:run",
        "//src/v/json",
        "//src/v/model",
        "//src/v/model:compression",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:directory_walker",
        "//src/v/utils:file_io",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)
