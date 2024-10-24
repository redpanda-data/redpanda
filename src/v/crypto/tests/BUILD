load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "crypto_test_utils",
    hdrs = [
        "crypto_test_utils.h",
        "test_values.h",
    ],
    visibility = ["//visibility:private"],
    deps = [
        "//src/v/bytes",
        "@boost//:algorithm",
    ],
)

redpanda_cc_gtest(
    name = "digest_test",
    timeout = "short",
    srcs = [
        "digest_tests.cc",
    ],
    deps = [
        ":crypto_test_utils",
        "//src/v/crypto",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "hmac_test",
    timeout = "short",
    srcs = [
        "hmac_tests.cc",
    ],
    deps = [
        ":crypto_test_utils",
        "//src/v/crypto",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "random_test",
    timeout = "short",
    srcs = [
        "random_tests.cc",
    ],
    deps = [
        "//src/v/crypto",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "signature_test",
    timeout = "short",
    srcs = [
        "signature_tests.cc",
    ],
    deps = [
        ":crypto_test_utils",
        "//src/v/bytes",
        "//src/v/crypto",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
