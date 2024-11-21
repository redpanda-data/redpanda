load("@bazel_skylib//rules:expand_template.bzl", "expand_template")
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

