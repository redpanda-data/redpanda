load("//bazel:test.bzl", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "scoped_remote",
    srcs = [
        "scoped_remote.cc",
    ],
    hdrs = [
        "scoped_remote.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage_clients",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "s3_imposter",
    srcs = [
        "s3_imposter.cc",
    ],
    hdrs = [
        "s3_imposter.h",
    ],
    implementation_deps = [
        "@boost//:algorithm",
        "@boost//:core",
        "@boost//:property_tree",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cloud_storage_clients",
        "//src/v/config",
        "//src/v/http/tests:utils",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@seastar",
    ],
)
