# Redpanda clang-tidy plugins

This directory contains sources and bazel targets for custom out-of-tree clang-tidy checks.
It contains a single `redpanda_cc_binary` target, `plugins.so`, which contains a single clang-tidy
module, `redpanda-module`.

## Building

```sh
$ bazel build //bazel/clang_tidy/plugins:plugins.so
```

## Adding a new check

1. Write your check in a separate file or files. e.g. `plugins/redpanda_noop_check.h`.
2. Add your sources directly to `plugins.so.srcs` including any new dependencies.
3. Register the check by name in `plugins/redpanda_tidy_module.h`.
   - The name should start with "redpanda-".

Checks introduced in this way are automatically loaded in `bazel build --config=clang-tidy //...`.

If you want to try your check on some source file in isolation, you can invoke the tool directly:

```sh
$ bazel build //bazel/clang_tidy/plugins:plugins.so
$ ./tools/clang-tidy --checks=-*,redpanda-my-fancy-check -list-checks
$ ./tools/clang-tidy --checks=-*,redpanda-my-fancy-check /path/to/some/foo.cc
```
