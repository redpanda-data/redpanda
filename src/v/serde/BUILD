load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "serde",
    hdrs = glob([
        "read_header.h",
        "serde_exception.h",
        "type_str.h",
        "serde_size_t.h",
        "checksum_t.h",
        "envelope.h",
        "serde_is_enum.h",
        "rw/tags.h",
        "rw/reservable.h",
        "rw/rw.h",
        "rw/scalar.h",
        "rw/vector.h",
    ]),
    include_prefix = "serde",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/ssx:sformat",
    ],
)
