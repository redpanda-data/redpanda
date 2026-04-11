load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "avro_payloads",
    hdrs = [
        "avro_payloads.h",
    ],
    deps = [
        "//src/v/pandaproxy/schema_registry:core",
    ],
)

redpanda_test_cc_library(
    name = "store_fixture",
    srcs = [
        "store_fixture.cc",
    ],
    hdrs = [
        "store_fixture.h",
    ],
    deps = [
        "//src/v/pandaproxy/schema_registry:core",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "client_utils",
    hdrs = [
        "client_utils.h",
    ],
    deps = [
        "//src/v/http",
        "//src/v/json",
        "//src/v/pandaproxy/schema_registry:core",
        "//src/v/pandaproxy/test:utils",
        "@boost//:beast",
    ],
)

redpanda_test_cc_library(
    name = "compatibility_avro",
    hdrs = [
        "compatibility_avro.h",
    ],
    deps = [
        "//src/v/pandaproxy/schema_registry:core",
    ],
)

redpanda_test_cc_library(
    name = "compatibility_protobuf",
    hdrs = [
        "compatibility_protobuf.h",
    ],
    deps = [
        ":store_fixture",
        "//src/v/pandaproxy/schema_registry:core",
    ],
)

redpanda_test_cc_library(
    name = "compatibility_common",
    hdrs = [
        "compatibility_common.h",
    ],
    deps = [
        "//src/v/pandaproxy/schema_registry:core",
    ],
)

redpanda_test_cc_library(
    name = "protobuf_utils",
    srcs = [
        "protobuf_utils.cc",
    ],
    hdrs = [
        "protobuf_utils.h",
    ],
    deps = [
        ":store_fixture",
        "//src/v/pandaproxy/schema_registry:core",
    ],
)

redpanda_cc_btest(
    name = "sanitize_avro",
    timeout = "short",
    srcs = [
        "sanitize_avro.cc",
    ],
    deps = [
        ":compatibility_avro",
        "//src/v/pandaproxy:json",
        "//src/v/pandaproxy/schema_registry:core",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "util",
    timeout = "short",
    srcs = [
        "util.cc",
    ],
    deps = [
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "storage_test",
    timeout = "short",
    srcs = [
        "storage.cc",
    ],
    deps = [
        "//src/v/pandaproxy:json",
        "//src/v/pandaproxy/schema_registry:core",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
    ],
)

redpanda_cc_btest(
    name = "store",
    timeout = "short",
    srcs = [
        "store.cc",
    ],
    deps = [
        ":compatibility_avro",
        "//src/v/pandaproxy/schema_registry:core",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "schema_id_cache",
    timeout = "short",
    srcs = [
        "schema_id_cache.cc",
    ],
    deps = [
        "//src/v/config",
        "//src/v/pandaproxy/schema_registry:core",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "mt_sharded_store",
    timeout = "short",
    srcs = [
        "mt_sharded_store.cc",
    ],
    deps = [
        ":compatibility_protobuf",
        "//src/v/pandaproxy/schema_registry:core",
        "//src/v/test_utils:seastar_boost",
        "@boost//:range",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "sharded_store",
    timeout = "short",
    srcs = [
        "sharded_store.cc",
    ],
    cpu = 1,
    deps = [
        ":compatibility_protobuf",
        "//src/v/pandaproxy/schema_registry:core",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_test_cc_library(
    name = "utils",
    hdrs = [
        "utils.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/pandaproxy/schema_registry:core",
    ],
)

redpanda_cc_btest(
    name = "consume_to_store",
    timeout = "short",
    srcs = [
        "consume_to_store.cc",
    ],
    cpu = 1,
    deps = [
        ":utils",
        "//src/v/model",
        "//src/v/pandaproxy/schema_registry:core",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "compatibility_store",
    timeout = "short",
    srcs = [
        "compatibility_store.cc",
    ],
    cpu = 1,
    deps = [
        ":compatibility_avro",
        "//src/v/pandaproxy/schema_registry:core",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "compatibility_3rdparty",
    timeout = "short",
    srcs = [
        "compatibility_3rdparty.cc",
    ],
    cpu = 1,
    deps = [
        ":store_fixture",
        ":utils",
        "//src/v/model",
        "//src/v/pandaproxy/schema_registry:core",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "compatibility_protobuf_test",
    timeout = "short",
    srcs = [
        "compatibility_protobuf.cc",
    ],
    cpu = 1,
    data = [
    ],
    deps = [
        ":compatibility_common",
        ":compatibility_protobuf",
        ":protobuf_utils",
        ":store_fixture",
        "//src/v/bytes:iobuf_parser",
        "//src/v/pandaproxy/schema_registry:core",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "protobuf_schema_references_test",
    timeout = "short",
    srcs = [
        "protobuf_schema_references.cc",
    ],
    deps = [
        ":store_fixture",
        "//src/v/pandaproxy/schema_registry:core",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "compatibility_avro_test",
    timeout = "short",
    srcs = [
        "compatibility_avro.cc",
    ],
    cpu = 1,
    deps = [
        ":compatibility_avro",
        ":compatibility_common",
        ":store_fixture",
        "//src/v/pandaproxy/schema_registry:core",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@avro",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "avro_schema_references_test",
    timeout = "short",
    srcs = [
        "avro_schema_references.cc",
    ],
    cpu = 1,
    deps = [
        ":store_fixture",
        "//src/v/pandaproxy/schema_registry:core",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "context_subject_test",
    timeout = "short",
    srcs = [
        "context_subject.cc",
    ],
    deps = [
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_btest(
    name = "test_json_schema",
    timeout = "short",
    srcs = [
        "test_json_schema.cc",
    ],
    cpu = 1,
    deps = [
        ":compatibility_common",
        ":store_fixture",
        "//src/v/pandaproxy/schema_registry:core",
        "//src/v/test_utils:seastar_boost",
        "//src/v/test_utils:test_env",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@boost//:test",
        "@fmt",
        "@jsoncons",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "get_schema_types",
    timeout = "short",
    srcs = [
        "get_schema_types.cc",
    ],
    deps = [
        "//src/v/http",
        "//src/v/pandaproxy/test:fixture",
        "//src/v/pandaproxy/test:utils",
        "//src/v/test_utils:seastar_boost",
        "@boost//:beast",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "post_subjects_subject_version",
    timeout = "short",
    srcs = [
        "post_subjects_subject_version.cc",
    ],
    deps = [
        ":avro_payloads",
        ":client_utils",
        "//src/v/pandaproxy:core",
        "//src/v/pandaproxy:error",
        "//src/v/pandaproxy:json",
        "//src/v/pandaproxy/schema_registry:core",
        "//src/v/pandaproxy/schema_registry:rjson",
        "//src/v/pandaproxy/test:fixture",
        "//src/v/pandaproxy/test:utils",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "delete_subject_endpoints",
    timeout = "short",
    srcs = [
        "delete_subject_endpoints.cc",
    ],
    deps = [
        ":avro_payloads",
        ":client_utils",
        "//src/v/pandaproxy/schema_registry:core",
        "//src/v/pandaproxy/test:fixture",
        "//src/v/pandaproxy/test:utils",
        "//src/v/test_utils:seastar_boost",
        "@boost//:beast",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

filegroup(
    name = "test_protos",
    srcs = glob([
        "testdata/protobuf/*.proto",
    ]),
)

redpanda_cc_gtest(
    name = "protobuf_rendering_test",
    timeout = "short",
    srcs = [
        "protobuf_rendering.cc",
    ],
    cpu = 1,
    data = [
        ":test_protos",
    ],
    deps = [
        ":compatibility_common",
        ":compatibility_protobuf",
        ":protobuf_utils",
        "//src/v/bytes:iobuf_parser",
        "//src/v/pandaproxy/schema_registry:core",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:runfiles",
        "//src/v/utils:file_io",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "protobuf_large_alloc_test",
    timeout = "short",
    srcs = [
        "protobuf_large_alloc.cc",
    ],
    cpu = 1,
    data = [
        ":test_protos",
    ],
    memory = "256MiB",
    deps = [
        ":protobuf_utils",
        ":store_fixture",
        "//src/v/base",
        "//src/v/pandaproxy/schema_registry:core",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_bench(
    name = "api_rpbench",
    timeout = "moderate",
    srcs = [
        "api_bench.cc",
    ],
    # this bench is very slow, because posting many versions & subjects
    # in setup_bench is slow, so in test mode, run only the 1, 1 test
    test_regex = ".*(lookup|post)_x1_1",
    deps = [
        ":client_utils",
        ":protobuf_utils",
        "//src/v/base",
        "//src/v/http",
        "//src/v/json",
        "//src/v/pandaproxy/schema_registry:core",
        "//src/v/pandaproxy/test:fixture",
        "@abseil-cpp//absl/strings",
        "@seastar//:benchmark",
    ],
)

redpanda_test_cc_library(
    name = "random",
    hdrs = [
        "random.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/pandaproxy/schema_registry:subject_name_strategy",
        "//src/v/random:generators",
    ],
)
