load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/iceberg:__subpackages__"])

redpanda_cc_library(
    name = "catalog_config",
    hdrs = [
        "catalog_config.h",
    ],
    include_prefix = "iceberg/rest_client",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/container:chunked_hash_map",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "client_probe",
    srcs = [
        "client_probe.cc",
    ],
    hdrs = [
        "client_probe.h",
    ],
    include_prefix = "iceberg/rest_client",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/http",
        "//src/v/metrics",
        "//src/v/net",
    ],
)

redpanda_cc_library(
    name = "error",
    srcs = [
        "error.cc",
    ],
    hdrs = [
        "error.h",
    ],
    include_prefix = "iceberg/rest_client",
    deps = [
        "//src/v/base",
        "//src/v/http:request_builder",
        "//src/v/thirdparty/ada",
        "//src/v/utils:named_type",
        "@boost//:beast",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "json",
    srcs = [
        "json.cc",
    ],
    hdrs = [
        "json.h",
    ],
    include_prefix = "iceberg/rest_client",
    deps = [
        ":catalog_config",
        ":oauth_token",
        "//src/v/iceberg:json_utils",
        "//src/v/json",
        "//src/v/utils:named_type",
        "@abseil-cpp//absl/strings",
    ],
)

redpanda_cc_library(
    name = "retry_policy",
    srcs = [
        "retry_policy.cc",
    ],
    hdrs = [
        "retry_policy.h",
    ],
    include_prefix = "iceberg/rest_client",
    deps = [
        ":error",
        "//src/v/bytes:streambuf",
        "//src/v/http",
        "//src/v/net",
    ],
)

redpanda_cc_library(
    name = "credentials",
    hdrs = [
        "credentials.h",
    ],
    include_prefix = "iceberg/rest_client",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "oauth_token",
    hdrs = [
        "oauth_token.h",
    ],
    include_prefix = "iceberg/rest_client",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "entities",
    srcs = [
        "entities.cc",
    ],
    hdrs = [
        "entities.h",
    ],
    include_prefix = "iceberg/rest_client",
    deps = [
        "//src/v/container:fragmented_vector",
        "//src/v/http:rest_entity",
        "//src/v/ssx:sformat",
    ],
)

redpanda_cc_library(
    name = "client",
    srcs = [
        "catalog_client.cc",
    ],
    hdrs = [
        "catalog_client.h",
    ],
    include_prefix = "iceberg/rest_client",
    visibility = ["//visibility:public"],
    deps = [
        ":client_probe",
        ":credentials",
        ":entities",
        ":error",
        ":json",
        ":oauth_token",
        ":retry_policy",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:streambuf",
        "//src/v/cloud_roles",
        "//src/v/config",
        "//src/v/datalake:credential_manager",
        "//src/v/hashing:secure",
        "//src/v/http",
        "//src/v/http:request_builder",
        "//src/v/http:rest_entity",
        "//src/v/http:utils",
        "//src/v/iceberg:json_writer",
        "//src/v/iceberg:logger",
        "//src/v/iceberg:table_identifier",
        "//src/v/iceberg:table_metadata",
        "//src/v/iceberg:table_metadata_json",
        "//src/v/iceberg:table_requests",
        "//src/v/iceberg:table_requests_json",
        "//src/v/json",
        "//src/v/ssx:future_util",
        "//src/v/utils:named_type",
        "//src/v/utils:retry_chain_node",
        "//src/v/utils:to_string",
        "@abseil-cpp//absl/strings",
        "@rapidjson",
        "@seastar",
    ],
)
