load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "utils",
    hdrs = [
        "randoms.h",
    ],
    include_prefix = "security/tests",
    deps = [
        "//src/v/bytes:random",
        "//src/v/security",
        "//src/v/test_utils:random",
    ],
)

redpanda_cc_btest(
    name = "scram_algorithm_test",
    timeout = "short",
    srcs = [
        "scram_algorithm_test.cc",
    ],
    deps = [
        "//src/v/random:generators",
        "//src/v/security",
        "//src/v/utils:base64",
        "@boost//:algorithm",
        "@boost//:test",
        "@fmt",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "credential_store_test",
    timeout = "short",
    srcs = [
        "credential_store_test.cc",
    ],
    deps = [
        "//src/v/random:generators",
        "//src/v/security",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:base64",
        "@boost//:algorithm",
        "@boost//:test",
        "@fmt",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "authorizer_test",
    timeout = "short",
    srcs = [
        "authorizer_test.cc",
    ],
    deps = [
        "//src/v/config",
        "//src/v/random:generators",
        "//src/v/security",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:base64",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@boost//:algorithm",
        "@boost//:test",
        "@fmt",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "mtls_test",
    timeout = "short",
    srcs = [
        "mtls_test.cc",
    ],
    deps = [
        "//src/v/config",
        "//src/v/random:generators",
        "//src/v/security",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:base64",
        "@boost//:algorithm",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "license_test",
    timeout = "short",
    srcs = [
        "license_test.cc",
    ],
    deps = [
        "//src/v/security:license",
        "//src/v/test_utils:seastar_boost",
        "@boost//:date_time",
        "@boost//:filesystem",
        "@boost//:test",
    ],
)

redpanda_cc_btest(
    name = "gssapi_principal_mapper_test",
    timeout = "short",
    srcs = [
        "gssapi_principal_mapper_test.cc",
    ],
    deps = [
        "//src/v/config",
        "//src/v/security",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
    ],
)

redpanda_cc_btest(
    name = "jwt_test",
    timeout = "short",
    srcs = [
        "jwt_test.cc",
    ],
    deps = [
        "//src/v/security",
        "//src/v/security:config",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "url_test",
    timeout = "short",
    srcs = [
        "url_test.cc",
    ],
    deps = [
        "//src/v/security:config",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@fmt",
    ],
)

redpanda_cc_btest(
    name = "oidc_principal_mapping_test",
    timeout = "short",
    srcs = [
        "oidc_principal_mapping_test.cc",
    ],
    deps = [
        "//src/v/config",
        "//src/v/json",
        "//src/v/security",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "role_store_test",
    timeout = "short",
    srcs = [
        "role_store_test.cc",
    ],
    deps = [
        "//src/v/random:generators",
        "//src/v/security",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:node_hash_set",
        "@boost//:algorithm",
        "@boost//:range",
        "@boost//:test",
        "@fmt",
    ],
)

redpanda_cc_btest(
    name = "ephemeral_credential_store_test",
    timeout = "short",
    srcs = [
        "ephemeral_credential_store_test.cc",
    ],
    deps = [
        ":utils",
        "//src/v/random:generators",
        "//src/v/security",
        "//src/v/test_utils:random",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)
