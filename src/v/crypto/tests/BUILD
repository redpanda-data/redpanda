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
        "@REDPANDA_DEPS_INSTALL_DIR@": "src/v/crypto/tests",
    },
    template = "test_openssl_conf.cnf.in",
)

genrule(
    name = "fipsmodule_cnf",
    srcs = ["@openssl-fips//:gen_dir"],
    outs = ["etc/ssl/fipsmodule.cnf"],
    cmd = "cp -L $(SRCS)/ssl/fipsmodule.cnf $@",
)

common_data_deps = [
    ":fipsmodule_cnf",
    "@openssl-fips//:fipsmodule_so",
    ":openssl_conf",
]

common_env = {
    "OPENSSL_CONF": "$(rootpath :openssl_conf)",
    "__FIPS_MODULE_PATH": "$(rootpath @openssl-fips//:fipsmodule_so)",
}

redpanda_cc_bench(
    name = "crypto_bench",
    timeout = "short",
    srcs = ["crypto_bench.cc"],
    data = common_data_deps,
    env = common_env,
    deps = [
        "//src/v/base",
        "//src/v/crypto",
        "//src/v/random:generators",
        "//src/v/ssx:thread_worker",
        "@openssl-fips",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_bench(
    name = "crypto_bench_fips",
    timeout = "short",
    srcs = ["crypto_bench.cc"],
    data = common_data_deps,
    defines = ["PERF_FIPS_MODE"],
    env = common_env,
    deps = [
        "//src/v/base",
        "//src/v/crypto",
        "//src/v/random:generators",
        "//src/v/ssx:thread_worker",
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
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "ossl_context_service_test",
    timeout = "short",
    srcs = [
        "ossl_context_service_test.cc",
    ],
    cpu = 2,
    data = common_data_deps,
    defines = ["FIPS_MODULE_REQUIRED"],
    env = common_env,
    deps = [
        ":crypto_test_utils",
        ":ossl_context_service_test_base",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/crypto",
        "//src/v/ssx:thread_worker",
        "//src/v/test_utils:gtest",
        "//src/v/thirdparty/openssl",
        "@googletest//:gtest",
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
    data = common_data_deps,
    defines = ["FIPS_MODULE_REQUIRED"],
    env = common_env,
    deps = [
        ":crypto_test_utils",
        ":ossl_context_service_test_base",
        "//src/v/crypto",
        "//src/v/ssx:thread_worker",
        "@seastar",
    ],
)
