load("@bazel_skylib//rules:copy_file.bzl", "copy_file")
load("@bazel_skylib//rules:expand_template.bzl", "expand_template")
load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_gtest", "redpanda_test_cc_library")

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

redpanda_cc_gtest(
    name = "key_test",
    timeout = "short",
    srcs = [
        "key_tests.cc",
    ],
    deps = [
        ":crypto_test_utils",
        "//src/v/crypto",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

expand_template(
    name = "openssl_conf",
    out = "openssl_conf.cnf",
    substitutions = {
        "@REDPANDA_DEPS_INSTALL_DIR@/etc/ssl": "src/v/crypto/tests",
    },
    template = "test_openssl_conf.cnf.in",
)

copy_file(
    name = "fipsmodule_cnf",
    src = "@openssl-fips//:fipsmodule_cnf",
    out = "fipsmodule.cnf",
    allow_symlink = True,
)

copy_file(
    name = "fipsmodule_so",
    src = "@openssl-fips//:fipsmodule_so",
    out = "fips.so",
    allow_symlink = True,
)

filegroup(
    name = "fips_test_assets",
    srcs = [
        ":fipsmodule_cnf",
        ":fipsmodule_so",
        ":openssl_conf",
    ],
)

redpanda_cc_bench(
    name = "crypto_bench_rpbench",
    srcs = ["crypto_bench.cc"],
    data = [":fips_test_assets"],
    deps = [
        "//src/v/base",
        "//src/v/crypto",
        "//src/v/random:generators",
        "//src/v/ssx:thread_worker",
        "//src/v/test_utils:runfiles",
        "@openssl-fips",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_bench(
    name = "crypto_bench_fips_rpbench",
    srcs = ["crypto_bench.cc"],
    data = [":fips_test_assets"],
    defines = ["PERF_FIPS_MODE"],
    deps = [
        "//src/v/base",
        "//src/v/crypto",
        "//src/v/random:generators",
        "//src/v/ssx:thread_worker",
        "//src/v/test_utils:runfiles",
        "@openssl",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_test_cc_library(
    name = "ossl_context_service_test_base",
    hdrs = [
        "ossl_context_service_test_base.h",
    ],
    visibility = ["//visibility:private"],
    deps = [
        "//src/v/ssx:thread_worker",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:runfiles",
        "@googletest//:gtest",
        "@openssl",
    ],
)

redpanda_cc_gtest(
    name = "ossl_context_service_test",
    timeout = "short",
    srcs = [
        "ossl_context_service_test.cc",
    ],
    cpu = 2,
    data = [":fips_test_assets"],
    defines = ["FIPS_MODULE_REQUIRED"],
    env = {"RP_FIXTURE_ENV": ""},
    deps = [
        ":crypto_test_utils",
        ":ossl_context_service_test_base",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/crypto",
        "//src/v/ssx:thread_worker",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@openssl",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "ossl_context_service_failure_tests",
    timeout = "short",
    srcs = [
        "ossl_context_service_failure_tests.cc",
    ],
    cpu = 2,
    data = [":fips_test_assets"],
    defines = ["FIPS_MODULE_REQUIRED"],
    env = {"RP_FIXTURE_ENV": ""},
    deps = [
        ":crypto_test_utils",
        ":ossl_context_service_test_base",
        "//src/v/crypto",
        "//src/v/ssx:thread_worker",
        "@seastar",
    ],
)
