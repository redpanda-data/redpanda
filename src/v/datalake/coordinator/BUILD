load("//bazel:build.bzl", "redpanda_cc_library")
load("//src/v/rpc:compiler.bzl", "redpanda_cc_rpc_library")

package(default_visibility = ["//src/v/datalake/coordinator:__subpackages__"])

redpanda_cc_rpc_library(
    name = "generated_datalake_coordinator_rpc",
    src = "rpc.json",
    include_prefix = "datalake/coordinator",
)

redpanda_cc_library(
    name = "data_file",
    hdrs = [
        "data_file.h",
    ],
    include_prefix = "datalake/coordinator",
    deps = [
        "//src/v/base",
        "//src/v/serde",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "model",
    hdrs = [
        "types.h",
    ],
    include_prefix = "datalake/coordinator",
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/serde",
        "//src/v/serde:enum",
        "//src/v/datalake:types",
        "//src/v/model",
        # todo: split writer further once it evolves
        "//src/v/datalake:writer",
    ],
)

redpanda_cc_library(
    name = "state",
    srcs = [
        "state.cc",
    ],
    hdrs = [
        "state.h",
    ],
    include_prefix = "datalake/coordinator",
    deps = [
        ":translated_offset_range",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "stm",
    srcs = [
        "state_machine.cc",
    ],
    hdrs = [
        "state_machine.h",
    ],
    include_prefix = "datalake/coordinator",
    visibility = ["//visibility:public"],
    deps = [
        ":model",
        "//src/v/cluster",
        "//src/v/datalake:logger",
        "//src/v/raft",
        "//src/v/serde",
        "//src/v/serde:enum",
    ],
)

redpanda_cc_library(
    name = "state_update",
    srcs = [
        "state_update.cc",
    ],
    hdrs = [
        "state_update.h",
    ],
    include_prefix = "datalake/coordinator",
    deps = [
        ":state",
        ":translated_offset_range",
        "//src/v/base",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "translated_offset_range",
    hdrs = [
        "translated_offset_range.h",
    ],
    include_prefix = "datalake/coordinator",
    deps = [
        ":data_file",
        "//src/v/base",
        "//src/v/container:fragmented_vector",
        "//src/v/serde",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "frontend",
    srcs = [
        "frontend.cc",
        "service.cc",
    ],
    hdrs = [
        "frontend.h",
        "service.h",
    ],
    include_prefix = "datalake/coordinator",
    visibility = ["//visibility:public"],
    deps = [
        ":generated_datalake_coordinator_rpc",
        ":model",
        ":stm",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/container:fragmented_vector",
        "//src/v/datalake:logger",
        "//src/v/datalake:types",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/rpc",
        "@fmt",
        "@seastar",
    ],
)
