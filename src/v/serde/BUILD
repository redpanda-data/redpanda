load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "serde",
    hdrs = [
        "async.h",
        "checksum_t.h",
        "envelope.h",
        "envelope_for_each_field.h",
        "logger.h",
        "peek.h",
        "read_header.h",
        "rw/envelope.h",
        "rw/reservable.h",
        "rw/rw.h",
        "rw/scalar.h",
        "rw/tags.h",
        "serde_exception.h",
        "serde_is_enum.h",
        "serde_size_t.h",
        "type_str.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/hashing:crc32c",
        "//src/v/ssx:sformat",
    ],
)

redpanda_cc_library(
    name = "array",
    hdrs = [
        "rw/array.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":serde",
    ],
)

redpanda_cc_library(
    name = "bool_class",
    hdrs = [
        "rw/bool_class.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":serde",
    ],
)

redpanda_cc_library(
    name = "uuid",
    hdrs = [
        "rw/uuid.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":serde",
        "//src/v/utils:uuid",
    ],
)

redpanda_cc_library(
    name = "vector",
    hdrs = [
        "rw/vector.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":serde",
    ],
)

redpanda_cc_library(
    name = "optional",
    hdrs = [
        "rw/optional.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":serde",
    ],
)

redpanda_cc_library(
    name = "map",
    hdrs = [
        "rw/map.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":serde",
    ],
)

redpanda_cc_library(
    name = "pair",
    hdrs = [
        "rw/pair.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":serde",
    ],
)

redpanda_cc_library(
    name = "set",
    hdrs = [
        "rw/set.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":map",
        ":serde",
    ],
)

redpanda_cc_library(
    name = "named_type",
    hdrs = [
        "rw/named_type.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":serde",
        "//src/v/utils:named_type",
    ],
)

redpanda_cc_library(
    name = "sstring",
    hdrs = [
        "rw/sstring.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":serde",
    ],
)

redpanda_cc_library(
    name = "variant",
    hdrs = [
        "rw/variant.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":serde",
    ],
)

redpanda_cc_library(
    name = "iobuf",
    hdrs = [
        "rw/iobuf.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":serde",
    ],
)

redpanda_cc_library(
    name = "enum",
    hdrs = [
        "rw/enum.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":serde",
    ],
)

redpanda_cc_library(
    name = "chrono",
    hdrs = [
        "rw/chrono.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":serde",
    ],
)

redpanda_cc_library(
    name = "tristate",
    hdrs = [
        "rw/tristate_rw.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":serde",
        "//src/v/utils:tristate",
    ],
)

redpanda_cc_library(
    name = "inet_address",
    hdrs = [
        "rw/inet_address.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":iobuf",
        ":serde",
        "//src/v/ssx:sformat",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "bytes",
    hdrs = [
        "rw/bytes.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":serde",
        "//src/v/bytes",
    ],
)
