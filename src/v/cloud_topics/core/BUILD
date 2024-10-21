load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/cloud_topics/batcher:__pkg__",
    "//src/v/cloud_topics/batcher/tests:__pkg__",
    "//src/v/cloud_topics/core/tests:__pkg__",
])

redpanda_cc_library(
    name = "serializer",
    srcs = [
        "serializer.cc",
    ],
    hdrs = [
        "serializer.h",
    ],
    implementation_deps = [
        "//src/v/storage:record_batch_utils",
    ],
    include_prefix = "cloud_topics/core",
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
    ],
)

redpanda_cc_library(
    name = "write_request",
    srcs = [
        "write_request.cc",
    ],
    hdrs = [
        "write_request.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics:logger",
    ],
    include_prefix = "cloud_topics/core",
    deps = [
        ":serializer",
        "//src/v/base",
        "//src/v/cloud_topics:types",
        "//src/v/model",
        "@seastar",
    ],
)
