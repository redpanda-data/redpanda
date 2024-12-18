load("//bazel:test.bzl", "redpanda_cc_btest")

redpanda_cc_btest(
    name = "fetch",
    timeout = "short",
    srcs = [
        "fetch.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/container:fragmented_vector",
        "//src/v/json",
        "//src/v/kafka/client/test:utils",
        "//src/v/kafka/protocol",
        "//src/v/model",
        "//src/v/pandaproxy",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "produce",
    timeout = "short",
    srcs = [
        "produce.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/kafka/protocol:produce",
        "//src/v/kafka/protocol/schemata:produce_response",
        "//src/v/model",
        "//src/v/pandaproxy",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:to_string",
        "@seastar",
        "@seastar//:testing",
    ],
)
