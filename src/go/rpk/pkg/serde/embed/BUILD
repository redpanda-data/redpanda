load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "embed",
    srcs = ["embed.go"],
    embedsrcs = [
        "protobuf/confluent/meta.proto",
        "protobuf/confluent/types/decimal.proto",
        "protobuf/google/type/calendar_period.proto",
        "protobuf/google/type/color.proto",
        "protobuf/google/type/date.proto",
        "protobuf/google/type/datetime.proto",
        "protobuf/google/type/dayofweek.proto",
        "protobuf/google/type/decimal.proto",
        "protobuf/google/type/expr.proto",
        "protobuf/google/type/fraction.proto",
        "protobuf/google/type/interval.proto",
        "protobuf/google/type/latlng.proto",
        "protobuf/google/type/localized_text.proto",
        "protobuf/google/type/money.proto",
        "protobuf/google/type/month.proto",
        "protobuf/google/type/phone_number.proto",
        "protobuf/google/type/postal_address.proto",
        "protobuf/google/type/quaternion.proto",
        "protobuf/google/type/timeofday.proto",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/serde/embed",
    visibility = ["//visibility:public"],
)

go_test(
    name = "embed_test",
    size = "small",
    srcs = ["embed_test.go"],
    data = ["//src/v/pandaproxy:schema_registry_protos"],
    embed = [":embed"],
    deps = ["@com_github_stretchr_testify//require"],
)
