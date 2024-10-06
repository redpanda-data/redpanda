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
    include_prefix = "serde",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/hashing:crc32c",
        "//src/v/reflection:arity",
        "//src/v/reflection:to_tuple",
        "//src/v/ssx:future_util",
        "//src/v/ssx:sformat",
    ],
)

redpanda_cc_library(
    name = "array",
    hdrs = [
        "rw/array.h",
    ],
    include_prefix = "serde",
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
    include_prefix = "serde",
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
    include_prefix = "serde",
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
    include_prefix = "serde",
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
    include_prefix = "serde",
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
    include_prefix = "serde",
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
    include_prefix = "serde",
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
    include_prefix = "serde",
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
    include_prefix = "serde",
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
    include_prefix = "serde",
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
    include_prefix = "serde",
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
    include_prefix = "serde",
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
    include_prefix = "serde",
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
    include_prefix = "serde",
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
    include_prefix = "serde",
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
    include_prefix = "serde",
    visibility = ["//visibility:public"],
    deps = [
        ":serde",
        "//src/v/bytes",
    ],
)
