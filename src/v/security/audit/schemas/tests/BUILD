load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_cc_gtest")

redpanda_cc_btest(
    name = "ocsf_schemas_test",
    timeout = "short",
    srcs = [
        "ocsf_schemas_test.cc",
    ],
    deps = [
        "//src/v/json",
        "//src/v/security",
        "//src/v/security:request_auth",
        "//src/v/security/audit",
        "//src/v/security/audit:types",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@fmt",
        "@rapidjson",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "utils_test",
    timeout = "short",
    srcs = [
        "utils_test.cc",
    ],
    deps = [
        "//src/v/kafka/protocol:messages",
        "//src/v/security/audit",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
