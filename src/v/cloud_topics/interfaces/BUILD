load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/cloud_topics/L0_read_path:__pkg__",
    "//src/v/cloud_topics/interfaces/tests:__pkg__",
])

redpanda_cc_library(
    name = "cluster_partition_manager",
    srcs = [
        "cluster_partition_manager.cc",
    ],
    hdrs = [
        "cluster_partition_manager.h",
    ],
    include_prefix = "cloud_topics/interfaces",
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/storage",
        "@seastar",
    ],
)
