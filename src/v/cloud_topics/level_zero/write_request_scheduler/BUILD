load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    ":__subpackages__",
    "//src/v/cloud_topics:__pkg__",
    "//src/v/cloud_topics/level_zero/tests:__pkg__",
    "//src/v/cloud_topics/level_zero/write_request_scheduler/tests:__pkg__",
])

redpanda_cc_library(
    name = "write_request_scheduler",
    srcs = [
        "write_request_scheduler.cc",
    ],
    hdrs = [
        "write_request_scheduler.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/cloud_topics/level_zero/common:level_zero_probe",
        "//src/v/cloud_topics/level_zero/pipeline:base_pipeline",
        "//src/v/cloud_topics/level_zero/pipeline:pipeline_actor",
        "//src/v/cloud_topics/level_zero/pipeline:write_pipeline",
        "//src/v/cloud_topics/level_zero/pipeline:write_request",
        "//src/v/config",
        "//src/v/random:time_jitter",
        "//src/v/ssx:future_util",
        "//src/v/ssx:semaphore",
        "//src/v/utils:named_type",
        "@abseil-cpp//absl/container:fixed_array",
        "@seastar",
    ],
)
