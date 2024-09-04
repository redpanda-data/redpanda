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
    include_prefix = "cloud_io",
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
        "//src/v/resource_mgmt:io_priority",
    ],
    include_prefix = "cloud_io",
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
    name = "logger",
    hdrs = [
        "logger.h",
    ],
    include_prefix = "cloud_io",
)
