load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics/level_one:__subpackages__"])

redpanda_cc_library(
    name = "domain_supervisor",
    srcs = [
        "domain_supervisor.cc",
    ],
    hdrs = [
        "domain_supervisor.h",
    ],
    implementation_deps = [
        ":domain_manager",
        "//src/v/cloud_topics:logger",
        "//src/v/cluster",
        "//src/v/cluster/utils:partition_change_notifier_api",
        "//src/v/cluster/utils:partition_change_notifier_impl",
        "//src/v/model",
        "//src/v/ssx:when_all",
        "//src/v/ssx:work_queue",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "domain_manager",
    srcs = [
        "domain_manager.cc",
    ],
    hdrs = [
        "domain_manager.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics:logger",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_topics/level_one/metastore:rpc_types",
        "//src/v/cloud_topics/level_one/metastore:simple",
        "//src/v/cloud_topics/level_one/metastore:simple_stm",
        "@seastar",
    ],
)
