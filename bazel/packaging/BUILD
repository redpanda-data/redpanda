load("@bazel_skylib//rules:expand_template.bzl", "expand_template")
load("@rules_pkg//pkg:mappings.bzl", "pkg_attributes", "pkg_files", "strip_prefix")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")

pkg_files(
    name = "redpanda_binary_with_runfiles",
    srcs = ["//src/v/redpanda"],
    # Including runfiles has the effect of copying all of the shared libraries needed to run the binary.
    # Although they always retain the directory structure that is found in bazel build output, meaning
    # that `strip_prefix` has no effect on runfiles here (so we strip_prefix in the other targets).
    include_runfiles = True,
)

pkg_files(
    name = "redpanda_lib",
    srcs = [":redpanda_binary_with_runfiles"],
    # Exclude the redpanda binary, but this will still keep the runfiles, which for the redpanda
    # binary is the shared libraries.
    excludes = ["//src/v/redpanda"],
    prefix = "lib",
    # This flattens the files so that everything is directly in the `lib` folder.
    strip_prefix = strip_prefix.files_only(),
)

pkg_files(
    name = "redpanda_libexec",
    srcs = ["//src/v/redpanda"],
    prefix = "libexec",
    # This flattens the files so that everything is directly in the `libexec` folder.
    strip_prefix = strip_prefix.files_only(),
)

expand_template(
    name = "redpanda_binary_wrapper",
    out = "redpanda",
    substitutions = {
        "%%BINARY%%": "redpanda",
    },
    template = "exec.tmpl.sh",
)

pkg_files(
    name = "redpanda_bin",
    srcs = [":redpanda_binary_wrapper"],
    attributes = pkg_attributes(
        mode = "0755",
    ),
    prefix = "bin",
    # This flattens the files so that everything is directly in the `bin` folder.
    strip_prefix = strip_prefix.files_only(),
)

# TODO(bazel): Pull in all the system libraries that are still depended on and not packaged yet.
# TODO(bazel): Add the other binaries we package (rp_util, hwloc, rpk, etc)
pkg_tar(
    name = "redpanda_tar",
    srcs = [
        ":redpanda_bin",
        ":redpanda_lib",
        ":redpanda_libexec",
    ],
    # This is also needed because it actually specifies compression.
    extension = ".tar.gz",
    package_file_name = "redpanda.tar.gz",
)
