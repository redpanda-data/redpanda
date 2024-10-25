load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "compatibility_avro",
    hdrs = [
        "compatibility_avro.h",
    ],
    include_prefix = "pandaproxy/schema_registry/test",
    deps = [
        "//src/v/pandaproxy",
    ],
)

redpanda_test_cc_library(
    name = "compatibility_protobuf",
    hdrs = [
        "compatibility_protobuf.h",
    ],
    include_prefix = "pandaproxy/schema_registry/test",
    deps = [
        "//src/v/pandaproxy",
    ],
)

redpanda_test_cc_library(
    name = "compatibility_common",
    hdrs = [
        "compatibility_common.h",
    ],
    include_prefix = "pandaproxy/schema_registry/test",
    deps = [
        "//src/v/pandaproxy",
    ],
)

redpanda_cc_btest(
    name = "sanitize_avro",
    timeout = "short",
    srcs = [
        "sanitize_avro.cc",
    ],
    deps = [
        "//src/v/pandaproxy",
        "//src/v/pandaproxy/schema_registry/test:compatibility_avro",
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
        "//src/v/pandaproxy",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "storage",
    timeout = "short",
    srcs = [
        "storage.cc",
    ],
    deps = [
        "//src/v/pandaproxy",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@fmt",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "store",
    timeout = "short",
    srcs = [
        "store.cc",
    ],
    deps = [
        "//src/v/pandaproxy",
        "//src/v/pandaproxy/schema_registry/test:compatibility_avro",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/algorithm:container",
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
        "//src/v/pandaproxy",
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
        "//src/v/pandaproxy",
        "//src/v/pandaproxy/schema_registry/test:compatibility_protobuf",
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
    deps = [
        "//src/v/pandaproxy",
        "//src/v/pandaproxy/schema_registry/test:compatibility_protobuf",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "consume_to_store",
    timeout = "short",
    srcs = [
        "consume_to_store.cc",
    ],
    deps = [
        "//src/v/model",
        "//src/v/pandaproxy",
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
    deps = [
        "//src/v/pandaproxy",
        "//src/v/pandaproxy/schema_registry/test:compatibility_avro",
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
    deps = [
        "//src/v/model",
        "//src/v/pandaproxy",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "compatibility_avro_test",
    timeout = "short",
    srcs = [
        "compatibility_avro.cc",
    ],
    deps = [
        "//src/v/pandaproxy",
        "//src/v/pandaproxy/schema_registry/test:compatibility_avro",
        "//src/v/pandaproxy/schema_registry/test:compatibility_common",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@avro",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "test_json_schema",
    timeout = "short",
    srcs = [
        "test_json_schema.cc",
    ],
    deps = [
        "//src/v/pandaproxy",
        "//src/v/pandaproxy/schema_registry/test:compatibility_common",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@boost//:test",
        "@fmt",
        "@jsoncons",
        "@seastar",
        "@seastar//:testing",
    ],
)
