load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_io:__pkg__"])

redpanda_cc_library(
    name = "io_resources",
    srcs = [
        "io_resources.cc",
    ],
    hdrs = [
        "io_resources.h",
    ],
    implementation_deps = [
        "//src/v/cloud_io:logger",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/config",
        "//src/v/ssx:future_util",
        "//src/v/ssx:mutex",
        "//src/v/ssx:semaphore",
        "//src/v/utils:adjustable_semaphore",
        "//src/v/utils:token_bucket",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "io_result",
    srcs = [
        "io_result.cc",
    ],
    hdrs = [
        "io_result.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "logger",
    hdrs = [
        "logger.h",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "remote_api",
    hdrs = [
        "remote_api.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":admission_control_types",
        "//src/v/cloud_io:io_result",
        "//src/v/cloud_io:transfer_details",
        "//src/v/cloud_storage_clients",
        "//src/v/utils:lazy_abort_source",
        "//src/v/utils:stream_provider",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "remote",
    srcs = [
        "remote.cc",
    ],
    hdrs = [
        "remote.h",
    ],
    implementation_deps = [
        ":transfer_details",
        "//src/v/bytes:iostream",
        "//src/v/cloud_io:logger",
        "//src/v/ssx:future_util",
        "//src/v/utils:retry_chain_node",
        "@boost//:beast",
        "@boost//:lexical_cast",
        "@boost//:range",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":admission_control_types",
        ":io_resources",
        ":io_result",
        ":provider",
        ":remote_api",
        "//src/v/base",
        "//src/v/cloud_roles:types",
        "//src/v/cloud_storage_clients",
        "//src/v/config",
        "//src/v/metrics",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/utils:lazy_abort_source",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "transfer_details",
    srcs = [
        "transfer_details.cc",
    ],
    hdrs = [
        "transfer_details.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_storage_clients",
        "//src/v/model",
        "//src/v/utils:log_hist",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "basic_cache_service_api",
    srcs = [
        "basic_cache_service_api.cc",
    ],
    hdrs = [
        "basic_cache_service_api.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "admission_control_types",
    srcs = [
        "admission_control_types.cc",
    ],
    hdrs = [
        "admission_control_types.h",
    ],
    implementation_deps = [
        "//src/v/strings:string_switch",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "reservation_policy_types",
    hdrs = [
        "reservation_policy_types.h",
    ],
    include_prefix = "cloud_io",
    visibility = ["//visibility:public"],
    deps = [
        ":admission_control_types",
        "//src/v/base",
        "//src/v/container:intrusive",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "admission_control_policy",
    hdrs = [
        "admission_control_policy.h",
    ],
    include_prefix = "cloud_io",
    visibility = ["//visibility:public"],
    deps = [
        ":admission_control_types",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "provider",
    hdrs = [
        "provider.h",
    ],
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "admission_control",
    srcs = [
        "admission_control.cc",
    ],
    hdrs = [
        "admission_control.h",
    ],
    implementation_deps = [
        ":reservation_policy",
        "//src/v/base",
    ],
    include_prefix = "cloud_io",
    visibility = ["//visibility:public"],
    deps = [
        ":admission_control_policy",
        ":admission_control_types",
        "//src/v/base",
        "//src/v/ssx:semaphore",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "reservation_policy",
    srcs = [
        "reservation_policy.cc",
    ],
    hdrs = [
        "reservation_policy.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/cloud_io:logger",
        "//src/v/config",
        "//src/v/ssx:sformat",
        "@fmt",
    ],
    include_prefix = "cloud_io",
    visibility = ["//visibility:public"],
    deps = [
        ":admission_control_policy",
        ":admission_control_types",
        ":reservation_policy_types",
        "//src/v/metrics",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "cache",
    srcs = [
        "access_time_tracker.cc",
        "cache_probe.cc",
        "cache_service.cc",
        "recursive_directory_walker.cc",
        "staging_file.cc",
    ],
    hdrs = [
        "access_time_tracker.h",
        "cache_probe.h",
        "cache_service.h",
        "recursive_directory_walker.h",
        "staging_file.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/container:chunked_vector",
        "//src/v/serde",
        "//src/v/serde:enum",
        "//src/v/serde:sstring",
        "//src/v/ssx:future_util",
        "//src/v/ssx:watchdog",
        "//src/v/utils:human",
        "@abseil-cpp//absl/container:btree",
        "@re2",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iostream",
        "//src/v/cloud_io:basic_cache_service_api",
        "//src/v/config",
        "//src/v/metrics",
        "//src/v/ssx:semaphore",
        "//src/v/storage:disk",
        "@seastar",
    ],
)
