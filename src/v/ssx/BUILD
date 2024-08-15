load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "thread_worker",
    hdrs = [
        "thread_worker.h",
    ],
    include_prefix = "ssx",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@boost//:lockfree",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "sformat",
    hdrs = [
        "sformat.h",
    ],
    include_prefix = "ssx",
    visibility = ["//visibility:public"],
    deps = [
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "future_util",
    hdrs = [
        "future-util.h",
    ],
    include_prefix = "ssx",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/utils:functional",
    ],
)

redpanda_cc_library(
    name = "semaphore",
    hdrs = [
        "semaphore.h",
    ],
    include_prefix = "ssx",
    visibility = ["//visibility:public"],
    deps = [
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "abort_source",
    hdrs = [
        "abort_source.h",
    ],
    include_prefix = "ssx",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "async_algorithm",
    hdrs = [
        "async_algorithm.h",
    ],
    include_prefix = "ssx",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "async_clear",
    hdrs = [
        "async-clear.h",
    ],
    include_prefix = "ssx",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "watchdog",
    hdrs = [
        "watchdog.h",
    ],
    include_prefix = "ssx",
    visibility = ["//visibility:public"],
    deps = [
        ":future_util",
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "task_local",
    hdrs = [
        "task_local_ptr.h",
    ],
    include_prefix = "ssx",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "single_sharded",
    hdrs = [
        "single_sharded.h",
    ],
    include_prefix = "ssx",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "event",
    hdrs = [
        "event.h",
    ],
    include_prefix = "ssx",
    visibility = ["//visibility:public"],
    deps = [
        ":semaphore",
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "sleep_abortable",
    hdrs = [
        "sleep_abortable.h",
    ],
    include_prefix = "ssx",
    visibility = ["//visibility:public"],
    deps = [
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "sharded_ptr",
    hdrs = [
        "sharded_ptr.h",
    ],
    include_prefix = "ssx",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/utils:mutex",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "aligned",
    hdrs = [
        "aligned.h",
    ],
    include_prefix = "ssx",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "sharded_value",
    hdrs = [
        "sharded_value.h",
    ],
    include_prefix = "ssx",
    visibility = ["//visibility:public"],
    deps = [
        ":aligned",
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "work_queue",
    srcs = [
        "work_queue.cc",
    ],
    hdrs = [
        "work_queue.h",
    ],
    include_prefix = "ssx",
    visibility = ["//visibility:public"],
    deps = [
        ":future_util",
        "//src/v/base",
        "@seastar",
    ],
)
