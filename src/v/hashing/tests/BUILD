load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_cc_btest_no_seastar")

redpanda_cc_btest(
    name = "secure_hash_test",
    timeout = "short",
    srcs = [
        "secure_tests.cc",
    ],
    deps = [
        "//src/v/hashing:secure",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest_no_seastar(
    name = "xx_hash_test",
    timeout = "short",
    srcs = [
        "xx_tests.cc",
    ],
    deps = [
        "//src/v/hashing:xx",
        "//src/v/utils:named_type",
    ],
)
