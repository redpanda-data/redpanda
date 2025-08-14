load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/cloud_topics:__pkg__",
    "//src/v/cloud_topics/level_zero/frontend_reader:__pkg__",
])

redpanda_cc_library(
    name = "frontend_reader",
    srcs = [
        "reader.cc",
    ],
    hdrs = [
        "reader.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics:data_plane_api",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_zero/stm:placeholder",
        "//src/v/cluster",
        "//src/v/config",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/model",
        "//src/v/storage",
        "@boost//:outcome",
        "@seastar",
    ],
)
