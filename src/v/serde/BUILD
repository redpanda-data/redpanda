load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "serde",
    hdrs = [
        "checksum_t.h",
        "envelope.h",
        "read_header.h",
        "rw/reservable.h",
        "rw/rw.h",
        "rw/scalar.h",
        "rw/tags.h",
        "rw/vector.h",
        "serde_exception.h",
        "serde_is_enum.h",
        "serde_size_t.h",
        "type_str.h",
    ],
    include_prefix = "serde",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/ssx:sformat",
    ],
)
