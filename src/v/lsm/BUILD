load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "lsm",
    srcs = [
        "lsm.cc",
    ],
    hdrs = [
        "lsm.h",
    ],
    implementation_deps = [
        "//src/v/lsm/core/internal:files",
        "//src/v/lsm/core/internal:iterator",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/core/internal:options",
        "//src/v/lsm/db:impl",
        "//src/v/lsm/db:memtable",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/lsm/io:persistence",
        "//src/v/ssx:time",
        "//src/v/utils:named_type",
        "@abseil-cpp//absl/time",
        "@seastar",
    ],
)
