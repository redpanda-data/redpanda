load("//bazel:test.bzl", "redpanda_cc_btest")

redpanda_cc_btest(
    name = "signature_test",
    timeout = "short",
    srcs = [
        "signature_test.cc",
    ],
    deps = [
        "//src/v/cloud_roles:signature",
        "//src/v/cloud_roles:types",
        "//src/v/http",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/strings",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "schema_parsing_test",
    timeout = "short",
    srcs = [
        "schema_parsing_tests.cc",
    ],
    deps = [
        "//src/v/bytes:streambuf",
        "//src/v/cloud_roles:request_response_helpers",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
    ],
)

redpanda_cc_btest(
    name = "role_client_tests",
    timeout = "short",
    srcs = [
        "role_client_tests.cc",
        "test_definitions.h",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_roles",
        "//src/v/http/tests:utils",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:file_io",
        "@boost//:algorithm",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "categorization_tests",
    timeout = "short",
    srcs = [
        "categorization_tests.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_roles",
        "//src/v/cloud_roles:logger",
        "//src/v/http/tests:utils",
        "//src/v/test_utils:seastar_boost",
        "@boost//:algorithm",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "credential_tests",
    timeout = "short",
    srcs = [
        "credential_tests.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_roles",
        "//src/v/cloud_roles:logger",
        "//src/v/http/tests:utils",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "env_override_tests",
    timeout = "short",
    srcs = [
        "env_override_tests.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_roles",
        "//src/v/config",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "fetch_credentials_tests",
    timeout = "short",
    srcs = [
        "fetch_credentials_tests.cc",
        "test_definitions.h",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_roles",
        "//src/v/cloud_roles:logger",
        "//src/v/config",
        "//src/v/http/tests:utils",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:seastar_boost",
        "//src/v/test_utils:tee_log",
        "//src/v/utils:file_io",
        "@boost//:algorithm",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)
