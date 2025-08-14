load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_cc_gtest(
    name = "parser_test",
    timeout = "short",
    srcs =
        [
            "parser_test.cc",
        ],
    cpu = 1,
    data = [
        "testdata/json-test-suite/sample.json",
    ],
    deps = [
        ":data",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/serde/json:parser",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:runfiles",
        "//src/v/utils:file_io",
        "@googletest//:gtest",
        "@googletest//:gtest_main",
        "@seastar",
    ],
)

redpanda_cc_bench(
    name = "parser_rpbench",
    timeout = "short",
    srcs = [
        "parser_bench.cc",
    ],
    cpu = 1,
    data = [
        "testdata/json-test-suite/sample.json",
    ],
    deps = [
        ":data",
        "//src/v/bytes:streambuf",
        "//src/v/json",
        "//src/v/serde/json:parser",
        "@boost//:test",
        "@rapidjson",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_gtest(
    name = "writer_test",
    timeout = "short",
    srcs = ["writer_test.cc"],
    cpu = 1,
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/random:generators",
        "//src/v/serde/json:writer",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/cleanup",
        "@abseil-cpp//absl/strings",
        "@boost//:locale",
        "@googletest//:gtest",
        "@googletest//:gtest_main",
        "@rapidjson",
    ],
)

redpanda_cc_gtest(
    name = "json_checker_test",
    timeout = "short",
    srcs =
        [
            "json_checker_test.cc",
        ],
    cpu = 1,
    data = glob([
        "testdata/jsonchecker/*",
    ]),
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/serde/json:parser",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:runfiles",
        "//src/v/utils:file_io",
        "@googletest//:gtest",
        "@googletest//:gtest_main",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "data",
    srcs = [
        "data.cc",
    ],
    hdrs = [
        "data.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/test_utils:runfiles",
        "//src/v/utils:file_io",
    ],
    visibility = ["//visibility:private"],
    deps = [
        "//src/v/bytes:iobuf",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "dom",
    srcs = [
        "dom.cc",
    ],
    hdrs = [
        "dom.h",
    ],
    implementation_deps = [
        "//src/v/bytes:iobuf_parser",
        "@abseil-cpp//absl/strings",
        "@seastar",
    ],
    visibility = ["//visibility:private"],
    deps = [
        "//src/v/bytes:hash",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
    ],
)

redpanda_test_cc_library(
    name = "dom_serde",
    srcs = [
        "dom_serde.cc",
    ],
    hdrs = [
        "dom_serde.h",
    ],
    implementation_deps = [
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/serde/json:parser",
    ],
    visibility = ["//visibility:private"],
    deps = [
        ":dom",
        "//src/v/bytes:iobuf",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "dom_serde_test",
    timeout = "short",
    srcs =
        [
            "dom_serde_test.cc",
        ],
    cpu = 1,
    data = ["testdata/jsonchecker/pass1.json"],
    deps = [
        ":dom",
        ":dom_serde",
        "//src/v/bytes:iobuf",
        "//src/v/serde/json:parser",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:runfiles",
        "//src/v/utils:file_io",
        "@googletest//:gtest",
        "@googletest//:gtest_main",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "dom_rapidjson",
    srcs = [
        "dom_rapidjson.cc",
    ],
    hdrs = [
        "dom_rapidjson.h",
    ],
    implementation_deps = [
        "//src/v/bytes:streambuf",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/json",
        "//src/v/serde/json:parser",
        "@rapidjson",
    ],
    visibility = ["//visibility:private"],
    deps = [
        ":dom",
        "//src/v/bytes:iobuf",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "dom_rapidjson_test",
    timeout = "short",
    srcs =
        [
            "dom_rapidjson_test.cc",
        ],
    cpu = 1,
    data = ["testdata/jsonchecker/pass1.json"],
    deps = [
        ":dom",
        ":dom_rapidjson",
        "//src/v/bytes:iobuf",
        "//src/v/serde/json:parser",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:runfiles",
        "//src/v/utils:file_io",
        "@googletest//:gtest",
        "@googletest//:gtest_main",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "conformance_rapidjson_test",
    timeout = "short",
    srcs =
        [
            "conformance_rapidjson_test.cc",
        ],
    cpu = 1,
    data = [
        "testdata/json-test-suite/sample.json",
    ],
    deps = [
        ":data",
        ":dom",
        ":dom_rapidjson",
        ":dom_serde",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "depth_limit_test",
    timeout = "short",
    srcs = [
        "depth_limit_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/serde/json:parser",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@googletest//:gtest_main",
        "@seastar",
    ],
)
