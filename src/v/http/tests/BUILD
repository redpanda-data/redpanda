load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "utils",
    srcs = [
        "http_imposter.cc",
        "registered_urls.cc",
    ],
    hdrs = [
        "http_imposter.h",
        "registered_urls.h",
    ],
    include_prefix = "http/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/utils:unresolved_address",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@boost//:beast",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "http_client_test",
    timeout = "short",
    srcs = [
        "http_client_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/bytes:iostream",
        "//src/v/http",
        "//src/v/json",
        "//src/v/net",
        "//src/v/test_utils:seastar_boost",
        "@boost//:algorithm",
        "@boost//:beast",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "framing_test",
    timeout = "short",
    srcs = [
        "framing_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/http",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "http_imposter_test",
    timeout = "short",
    srcs = [
        "http_imposter_test.cc",
    ],
    deps = [
        ":utils",
        "//src/v/bytes:iobuf_parser",
        "//src/v/http",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
    ],
)
