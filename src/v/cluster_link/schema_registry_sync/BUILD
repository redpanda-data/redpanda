load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "source_reader",
    srcs = [
        "unavailable_source_reader.cc",
    ],
    hdrs = [
        "source_reader.h",
        "unavailable_source_reader.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/container:chunked_vector",
        "//src/v/pandaproxy/schema_registry:types",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "mirroring_task",
    srcs = [
        "mirroring_task.cc",
    ],
    hdrs = [
        "mirroring_task.h",
    ],
    implementation_deps = [
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/pandaproxy/schema_registry:types",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":source_reader",
        "//src/v/cluster_link:impl",
        "//src/v/cluster_link/model",
        "//src/v/model",
        "//src/v/schema:registry",
        "@seastar",
    ],
)
