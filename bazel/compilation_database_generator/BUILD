load("@rules_go//go:def.bzl", "go_binary", "go_library")
load("@rules_python//python:defs.bzl", "py_binary")

go_library(
    name = "compilation_database_generator_lib",
    srcs = ["main.go"],
    importpath = "github.com/redpanda-data/redpanda/bazel/compilation_database_generator",
    visibility = ["//visibility:private"],
)

go_binary(
    name = "compilation_database_generator",
    embed = [":compilation_database_generator_lib"],
    visibility = ["//visibility:public"],
)

py_binary(
    name = "linking_database_generator",
    srcs = ["cc_linker_gen.py"],
    main = "cc_linker_gen.py",
    visibility = ["//visibility:public"],
)
