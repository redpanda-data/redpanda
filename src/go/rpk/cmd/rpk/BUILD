load("@rules_go//go:def.bzl", "go_binary", "go_library")
load("@rules_shell//shell:sh_binary.bzl", "sh_binary")

go_library(
    name = "rpk_lib",
    srcs = ["main.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/cmd/rpk",
    visibility = ["//visibility:private"],
    deps = ["//src/go/rpk/pkg/cli"],
)

go_binary(
    name = "rpk",
    embed = [":rpk_lib"],
    gotags = select({
        "//bazel:gofips_enabled": ["fips"],
        "//conditions:default": [],
    }),
    visibility = ["//visibility:public"],
    x_defs = {
        "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/version.version": "{STABLE_GIT_LATEST_TAG}",
        "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/version.rev": "{STABLE_GIT_COMMIT}",
        "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/version.buildTime": "{FORMATTED_DATE}",
        "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/container/common.tag": "{STABLE_GIT_LATEST_TAG}",
    },
)

# This wrapper is only used when invoking rpk from bazel (e.g. `bazel run //:rpk`).
# The wrapper is used to conditionally inject arguments so that rpk operates in
# the context of the current Bazel build. For example, when running `rpk iotune ...`
# the wrapper will inject the iotune-specific option `iotune-path` so that rpk
# uses the version of iotune built by Bazel.
sh_binary(
    name = "rpk_wrapper",
    srcs = ["rpk_wrapper.sh"],
    args = [
        "$(rootpath :rpk)",
        "$(rootpath @seastar//:iotune)",
    ],
    data = [
        ":rpk",
        "@seastar//:iotune",
    ],
    visibility = ["//visibility:public"],
)
