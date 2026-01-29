load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_cc_gtest(
    name = "backend_detection_test",
    timeout = "short",
    srcs = [
        "backend_detection_test.cc",
    ],
    deps = [
        "//src/v/cloud_storage_clients",
        "//src/v/config",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_btest(
    name = "client_pool_test",
    timeout = "short",
    srcs = [
        "client_pool_test.cc",
    ],
    cpu = 1,
    deps = [
        ":client_pool_builder",
        "//src/v/base",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_storage_clients",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "s3_client_test",
    timeout = "short",
    srcs = [
        "s3_client_test.cc",
    ],
    deps = [
        ":client_pool_builder",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage_clients",
        "//src/v/hashing:secure",
        "//src/v/http/tests:utils",
        "//src/v/net",
        "//src/v/net:types",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:base64",
        "//src/v/utils:unresolved_address",
        "@boost//:algorithm",
        "@boost//:beast",
        "@boost//:property_tree",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "xml_sax_parser_test",
    timeout = "short",
    srcs = [
        "xml_sax_parser_test.cc",
    ],
    deps = [
        "//src/v/cloud_storage_clients",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
    ],
)

redpanda_cc_btest(
    name = "exception_test",
    timeout = "short",
    srcs = [
        "exception_test.cc",
    ],
    deps = [
        "//src/v/cloud_storage_clients",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "util_test",
    timeout = "short",
    srcs = [
        "util_test.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/cloud_storage_clients",
        "@googletest//:gtest",
        "@googletest//:gtest_main",
    ],
)

redpanda_cc_gtest(
    name = "types_test",
    timeout = "short",
    srcs = [
        "types_test.cc",
    ],
    deps = [
        "//src/v/cloud_storage_clients",
        "//src/v/config",
        "//src/v/test_utils:gtest",
    ],
)

redpanda_cc_gtest(
    name = "bucket_name_parts_test",
    timeout = "short",
    srcs = [
        "bucket_name_parts_test.cc",
    ],
    deps = [
        "//src/v/cloud_storage_clients",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_test_cc_library(
    name = "client_pool_builder",
    hdrs = [
        "client_pool_builder.h",
    ],
    deps = [
        "//src/v/cloud_storage_clients",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "client_pool_mt_test",
    timeout = "short",
    srcs = [
        "client_pool_mt_test.cc",
    ],
    cpu = 2,
    deps = [
        ":client_pool_builder",
        "//src/v/base",
        "//src/v/cloud_storage_clients",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
        "@boost//:range",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "abs_client_test",
    timeout = "short",
    srcs = [
        "abs_client_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_storage_clients",
        "//src/v/http/tests:utils",
        "//src/v/net",
        "//src/v/net:types",
        "//src/v/test_utils:gtest",
        "//src/v/utils:unresolved_address",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "gcs_client_test",
    timeout = "short",
    srcs = [
        "gcs_client_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_storage_clients",
        "//src/v/http/tests:utils",
        "//src/v/net",
        "//src/v/test_utils:gtest",
        "//src/v/utils:unresolved_address",
        "@googletest//:gtest",
        "@seastar",
    ],
)
