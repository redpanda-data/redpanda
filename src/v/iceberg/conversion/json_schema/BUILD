load("//bazel:build.bzl", "redpanda_cc_library")

package(
    default_visibility = [
        "//src/v/datalake:__subpackages__",
        "//src/v/iceberg/conversion:__subpackages__",
    ],
)

redpanda_cc_library(
    name = "ir",
    srcs = [
        "ir.cc",
    ],
    hdrs = [
        "ir.h",
    ],
    implementation_deps = [
        "//src/v/strings:string_switch",
    ],
    deps = [
        "//src/v/base",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "frontend",
    srcs = [
        "frontend.cc",
    ],
    hdrs = [
        "frontend.h",
    ],
    implementation_deps = [
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/http:utils",
        "//src/v/iceberg/conversion/json_schema/details:string_switch_table",
        "@abseil-cpp//absl/container:btree",
    ],
    deps = [
        ":ir",
        "//src/v/base",
        "//src/v/json",
        "@seastar",
    ],
)
