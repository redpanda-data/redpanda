load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/cloud_topics/L0_read_path:__pkg__",
    "//src/v/cloud_topics/L0_read_path/tests:__pkg__",
    "//src/v/cloud_topics/batcher:__pkg__",
    "//src/v/cloud_topics/batcher/tests:__pkg__",
    "//src/v/cloud_topics/core:__pkg__",
    "//src/v/cloud_topics/core/tests:__pkg__",
    "//src/v/cloud_topics/dl_stm:__pkg__",
    "//src/v/cloud_topics/dl_stm/tests:__pkg__",
    "//src/v/cloud_topics/reconciler:__pkg__",
    "//src/v/cloud_topics/reconciler/tests:__pkg__",
    "//src/v/cloud_topics/tests:__pkg__",
    "//src/v/cloud_topics/throttler:__pkg__",
    "//src/v/cloud_topics/throttler/tests:__pkg__",
])

redpanda_cc_library(
    name = "logger",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    include_prefix = "cloud_topics",
    deps = [
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "types",
    srcs = [
        "types.cc",
    ],
    hdrs = [
        "errc.h",
        "types.h",
    ],
    include_prefix = "cloud_topics",
    deps = [
        "//src/v/utils:named_type",
        "//src/v/utils:uuid",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "dl_overlay",
    srcs = [
        "dl_overlay.cc",
    ],
    hdrs = [
        "dl_overlay.h",
    ],
    include_prefix = "cloud_topics",
    deps = [
        ":types",
        "//src/v/model",
        "//src/v/serde",
        "@abseil-cpp//absl/container:btree",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "placeholder",
    hdrs = [
        "dl_placeholder.h",
    ],
    include_prefix = "cloud_topics",
    deps = [
        ":types",
        "//src/v/serde",
        "//src/v/serde:named_type",
        "//src/v/serde:uuid",
        "//src/v/utils:named_type",
        "//src/v/utils:uuid",
    ],
)

redpanda_cc_library(
    name = "dl_version",
    hdrs = ["dl_version.h"],
    include_prefix = "cloud_topics",
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
    include_prefix = "cloud_topics",
    deps = [
        ":dl_overlay",
        ":dl_version",
        "//src/v/container:fragmented_vector",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "app",
    srcs = [
        "app.cc",
    ],
    hdrs = [
        "api.h",
        "app.h",
    ],
    implementation_deps = [
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage",
        "//src/v/cloud_topics/L0_read_path:L0_fetch_handler",
        "//src/v/cloud_topics/batcher",
        "//src/v/cloud_topics/core:read_pipeline",
        "//src/v/cloud_topics/core:write_pipeline",
        "//src/v/cloud_topics/interfaces:cluster_partition_manager",
        "//src/v/cloud_topics/reconciler",
        "//src/v/cloud_topics/throttler",
        "//src/v/model",
    ],
    include_prefix = "cloud_topics",
    visibility = ["//src/v/redpanda:__pkg__"],
    deps = [
        "@seastar",
    ],
)
