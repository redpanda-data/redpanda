load("@rules_python//python:defs.bzl", "py_binary")
load("//src/v/kafka/protocol/schemata:generator.bzl", "generate_kafka_messages")

py_binary(
    name = "generator",
    srcs = ["generator.py"],
    main = "generator.py",
    visibility = ["//visibility:public"],
    deps = [
        "@python_deps//jinja2",
        "@python_deps//jsonschema",
    ],
)

generate_kafka_messages()
