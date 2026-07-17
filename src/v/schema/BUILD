load("//bazel:build.bzl", "redpanda_cc_library")

package(
    default_visibility = [
        "//src/v/schema:__pkg__",
    ],
)

redpanda_cc_library(
    name = "fwd",
    hdrs = [
        "fwd.h",
    ],
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "registry",
    srcs = [
        "registry.cc",
    ],
    hdrs = [
        "registry.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cluster:fwd",
        "//src/v/cluster:plugin_table",
        "//src/v/pandaproxy/schema_registry:api",
        "//src/v/pandaproxy/schema_registry:avro",
        "//src/v/pandaproxy/schema_registry:fwd",
        "//src/v/pandaproxy/schema_registry:json_schema",
        "//src/v/pandaproxy/schema_registry:protobuf_schema",
        "//src/v/pandaproxy/schema_registry:seq_writer",
        "//src/v/pandaproxy/schema_registry:server",
        "//src/v/pandaproxy/schema_registry:store",
        "//src/v/pandaproxy/schema_registry:types",
        "@seastar",
    ],
)
