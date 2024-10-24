load("//bazel:test.bzl", "redpanda_cc_btest")

redpanda_cc_btest(
    name = "atomic_token_bucket_test",
    timeout = "short",
    srcs = [
        "atomic_token_bucket_test.cc",
    ],
    deps = [
        "//src/v/kafka/server",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:to_string",
        "@boost//:test",
    ],
)

redpanda_cc_btest(
    name = "error_mapping_test",
    timeout = "short",
    srcs = [
        "error_mapping_test.cc",
    ],
    deps = [
        "//src/v/kafka/server",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
    ],
)

# TODO does this really not depend on kafka server?
redpanda_cc_btest(
    name = "timeouts_conversion_test",
    timeout = "short",
    srcs = [
        "timeouts_conversion_test.cc",
    ],
    deps = [
        "//src/v/kafka/protocol",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
    ],
)
