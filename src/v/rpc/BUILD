load("@rules_python//python:defs.bzl", "py_binary")

py_binary(
    name = "compiler",
    srcs = ["rpc_compiler.py"],
    main = "rpc_compiler.py",
    visibility = ["//visibility:public"],
    deps = [
        "@python_deps//jinja2",
    ],
)
