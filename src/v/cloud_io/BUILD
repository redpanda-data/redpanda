load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_io:__pkg__"])

redpanda_cc_library(
    name = "auth_refresh_bg_op",
    srcs = [
        "auth_refresh_bg_op.cc",
    ],
    hdrs = [
        "auth_refresh_bg_op.h",
    ],
    implementation_deps = [
        "//src/v/cloud_io:logger",
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cloud_roles",
        "//src/v/cloud_storage_clients",
        "//src/v/model",
        "@seastar",
    ],
)

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
    name = "remote",
    srcs = [
        "remote.cc",
    ],
    hdrs = [
        "remote.h",
    ],
    implementation_deps = [
        "//src/v/bytes:iostream",
        "//src/v/cloud_io:logger",
        "//src/v/ssx:future_util",
        "//src/v/ssx:semaphore",
        "//src/v/ssx:watchdog",
        "@boost//:beast",
        "@boost//:lexical_cast",
        "@boost//:range",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":auth_refresh_bg_op",
        ":io_resources",
        ":io_result",
        ":provider",
        ":transfer_details",
        "//src/v/base",
        "//src/v/cloud_roles",
        "//src/v/cloud_storage_clients",
        "//src/v/config",
        "//src/v/model",
        "//src/v/utils:lazy_abort_source",
        "//src/v/utils:retry_chain_node",
        "//src/v/utils:stream_provider",
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
    name = "provider",
    hdrs = [
        "provider.h",
    ],
    visibility = ["//visibility:public"],
)
