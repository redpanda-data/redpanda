load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "arity",
    hdrs = [
        "arity.h",
    ],
    include_prefix = "reflection",
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "to_tuple",
    hdrs = [
        "to_tuple.h",
    ],
    include_prefix = "reflection",
    visibility = ["//visibility:public"],
    deps = [
        ":arity",
    ],
)

redpanda_cc_library(
    name = "type_traits",
    hdrs = [
        "type_traits.h",
    ],
    include_prefix = "reflection",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/container:fragmented_vector",
        "//src/v/utils:named_type",
        "//src/v/utils:tristate",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "adl",
    hdrs = [
        "adl.h",
        "async_adl.h",
        "for_each_field.h",
        "seastar/circular_buffer.h",
    ],
    include_prefix = "reflection",
    visibility = ["//visibility:public"],
    deps = [
        ":arity",
        ":to_tuple",
        ":type_traits",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/container:fragmented_vector",
        "//src/v/ssx:future_util",
        "//src/v/utils:named_type",
        "//src/v/utils:tristate",
        "@abseil-cpp//absl/container:btree",
    ],
)
