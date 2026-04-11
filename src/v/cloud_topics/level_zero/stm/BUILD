load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    ":__subpackages__",
])

redpanda_cc_library(
    name = "placeholder",
    srcs = [
        "placeholder.cc",
    ],
    hdrs = [
        "placeholder.h",
    ],
    visibility = [
        ":__subpackages__",
        "//src/v/cloud_topics/batch_cache:__pkg__",
        "//src/v/cloud_topics/frontend:__pkg__",
        "//src/v/cloud_topics/level_zero/frontend_reader:__pkg__",
        "//src/v/cloud_topics/level_zero/reader:__pkg__",
        "//src/v/cloud_topics/level_zero/reader/tests:__pkg__",
    ],
    deps = [
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:named_type",
        "//src/v/serde:uuid",
        "//src/v/storage:record_batch_builder",
        "//src/v/utils:named_type",
        "//src/v/utils:uuid",
    ],
)

redpanda_cc_library(
    name = "dl_version",
    hdrs = ["dl_version.h"],
    deps = ["//src/v/utils:named_type"],
)

redpanda_cc_library(
    name = "dl_snapshot",
    srcs = [
        "dl_snapshot.cc",
    ],
    hdrs = [
        "dl_snapshot.h",
    ],
    implementation_deps = [
        "@fmt",
    ],
    deps = [
        ":dl_version",
        "//src/v/container:chunked_vector",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "types",
    srcs = ["types.cc"],
    hdrs = ["types.h"],
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/cloud_topics:types",
        "//src/v/container:intrusive",
        "//src/v/model",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "size_estimator",
    srcs = ["size_estimator.cc"],
    hdrs = ["size_estimator.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/model",
        "//src/v/serde",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "ctp_stm_commands",
    hdrs = ["ctp_stm_commands.h"],
    deps = [
        ":ctp_stm_state",
        ":types",
        "//src/v/model",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "ctp_stm_state",
    srcs = ["ctp_stm_state.cc"],
    hdrs = ["ctp_stm_state.h"],
    implementation_deps = [
        "//src/v/utils:to_string",
    ],
    deps = [
        ":size_estimator",
        "//src/v/base",
        "//src/v/cloud_topics:types",
        "//src/v/model",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "ctp_stm",
    srcs = ["ctp_stm.cc"],
    hdrs = ["ctp_stm.h"],
    implementation_deps = [
        ":ctp_stm_commands",
        ":placeholder",
        ":types",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/cloud_topics:types",
        "//src/v/serde:map",
        "//src/v/serde:uuid",
        "//src/v/serde:vector",
        "//src/v/ssx:future_util",
        "//src/v/ssx:watchdog",
        "//src/v/storage",
        "//src/v/utils:to_string",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":ctp_stm_state",
        ":types",
        "//src/v/cloud_topics/level_zero/common:producer_queue",
        "//src/v/raft",
        "//src/v/ssx:mutex",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "ctp_stm_api",
    srcs = ["ctp_stm_api.cc"],
    hdrs = ["ctp_stm_api.h"],
    implementation_deps = [
        ":ctp_stm",
        ":ctp_stm_commands",
        ":types",
        "//src/v/cloud_topics:types",
        "//src/v/raft",
        "//src/v/serde:uuid",
        "//src/v/storage:record_batch_builder",
        "//src/v/utils:prefix_logger",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":dl_snapshot",
        ":dl_version",
        "//src/v/base",
        "//src/v/cloud_topics/level_zero/common:producer_queue",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "ctp_stm_factory",
    srcs =
        [
            "ctp_stm_factory.cc",
        ],
    hdrs =
        [
            "ctp_stm_factory.h",
        ],
    implementation_deps = [
        ":ctp_stm",
        "//src/v/cloud_topics:logger",
    ],
    visibility = ["//visibility:public"],
    deps = ["//src/v/cluster:state_machine_registry"],
)
