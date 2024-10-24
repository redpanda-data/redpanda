load("//bazel:test.bzl", "redpanda_cc_btest")

redpanda_cc_btest(
    name = "errors_test",
    timeout = "short",
    srcs = [
        "errors.cc",
    ],
    deps = [
        "//src/v/kafka/protocol",
        "//src/v/pandaproxy",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
    ],
)

redpanda_cc_btest(
    name = "one_shot_test",
    timeout = "short",
    srcs = [
        "one_shot.cc",
    ],
    deps = [
        "//src/v/pandaproxy",
        "//src/v/test_utils:seastar_boost",
        "@boost//:range",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "kafka_client_cache_test",
    timeout = "short",
    srcs = [
        "kafka_client_cache.cc",
    ],
    deps = [
        "//src/v/config",
        "//src/v/kafka/client",
        "//src/v/pandaproxy",
        "//src/v/security",
        "//src/v/ssx:future_util",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)
