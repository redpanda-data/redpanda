load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_cc_btest_no_seastar")

redpanda_cc_library(
    name = "crc32c",
    hdrs = [
        "include/hashing/crc32c.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:iobuf",
        "@crc32c",
    ],
)

redpanda_cc_library(
    name = "secure",
    hdrs = [
        "include/hashing/secure.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes",
        "//src/v/crypto",
    ],
)

redpanda_cc_library(
    name = "xx",
    hdrs = [
        "include/hashing/xx.h",
    ],
    defines = [
        "XXH_PRIVATE_API",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@xxhash",
    ],
)

redpanda_cc_btest(
    name = "secure_hash_test",
    timeout = "short",
    srcs = [
        "tests/secure_tests.cc",
    ],
    deps = [
        ":secure",
    ],
)

redpanda_cc_btest_no_seastar(
    name = "xx_hash_test",
    timeout = "short",
    srcs = [
        "tests/xx_tests.cc",
    ],
    deps = [
        ":xx",
        "//src/v/utils:named_type",
    ],
)
