load("//bazel:test.bzl", "redpanda_cc_btest")

redpanda_cc_btest(
    name = "signature_test",
    timeout = "short",
    srcs = [
        "signature_test.cc",
    ],
    deps = [
        "//src/v/cloud_roles:signature",
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
