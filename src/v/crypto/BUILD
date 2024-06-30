load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_library(
    name = "crypto",
    srcs = [
        "crypto.cc",
        "digest.cc",
        "hmac.cc",
        "internal.h",
        "key.cc",
        "key.h",
        "ossl_context_service.cc",
        "random.cc",
        "signature.cc",
        "ssl_utils.cc",
        "ssl_utils.h",
    ],
    hdrs = [
        "include/crypto/crypto.h",
        "include/crypto/exceptions.h",
        "include/crypto/ossl_context_service.h",
        "include/crypto/types.h",
    ],
    exclude_layering_check = True,
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/ssx:thread_worker",
        "@abseil-cpp//absl/container:node_hash_map",
        "@openssl",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "crypto_test_utils",
    hdrs = [
        "tests/crypto_test_utils.h",
        "tests/test_values.h",
    ],
    deps = [
        "//src/v/bytes",
        "@boost//:algorithm",
    ],
)

redpanda_cc_gtest(
    name = "digest_test",
    timeout = "short",
    srcs = [
        "tests/digest_tests.cc",
    ],
    deps = [
        ":crypto",
        ":crypto_test_utils",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "hmac_test",
    timeout = "short",
    srcs = [
        "tests/hmac_tests.cc",
    ],
    deps = [
        ":crypto",
        ":crypto_test_utils",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "random_test",
    timeout = "short",
    srcs = [
        "tests/random_tests.cc",
    ],
    deps = [
        ":crypto",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "signature_test",
    timeout = "short",
    srcs = [
        "tests/signature_tests.cc",
    ],
    deps = [
        ":crypto",
        ":crypto_test_utils",
        "//src/v/bytes",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
