"""
This module contains a function for writing golden file tests so you can see the output of a build step and how it evolves over time.

Credit: https://dev.to/bazel/bazel-can-write-to-the-source-folder-b9b
"""

load("@bazel_skylib//rules:diff_test.bzl", "diff_test")
load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@rules_shell//shell:sh_binary.bzl", "sh_binary")

def redpanda_golden_test(name, srcs):
    """
    Creates a test that compares generated files to golden files in the source tree.

    Golden files are written to a `goldens` directory as to not conflict with the generated
    and checked in files.

    Args:
      name: The name of the test target.
      srcs: A dictionary mapping file names to their expected golden file paths.
              The keys are the names of the files that will be generated, and
              the values are the paths to the golden files in the source tree.
    """

    # Create a test target for each file that Bazel should
    # write to the source tree.
    for [k, v] in srcs.items():
        native.filegroup(
            name = name + "_golden_" + k,
            srcs = ["goldens/" + k],
        )
        diff_test(
            name = name + "_check_" + k,
            # Make it trivial for devs to understand that if
            # this test fails, they just need to run the updater
            failure_message = "Please run:  bazel run //{}:{}_update".format(native.package_name(), name),
            file1 = name + "_golden_" + k,
            timeout = "short",
            file2 = v,
        )

    # Generate the updater script so there's only one target for devs to run,
    # even if many generated files are in the source folder.
    write_file(
        name = name + "_gen_update",
        out = name + "_update.sh",
        content = [
            # This depends on bash, would need tweaks for Windows
            "#!/usr/bin/env bash",
            # Bazel gives us a way to access the source folder!
            "cd $BUILD_WORKSPACE_DIRECTORY",
            "mkdir -p {}/goldens".format(native.package_name()),
        ] + [
            # Paths are now relative to the workspace.
            # We can copy files from bazel-bin to the sources
            "cp --force --copy-contents --verbose bazel-bin/{1}/{0} {1}/goldens/{0}".format(
                k,
                native.package_relative_label(v).package,
            )
            for [k, v] in srcs.items()
        ],
    )

    # This is what you can `bazel run` and it can write to the source folder
    sh_binary(
        name = name + "_update",
        srcs = [":" + name + "_gen_update"],
        data = srcs.values(),
    )
