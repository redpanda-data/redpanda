#!/usr/bin/env python3

import subprocess
import sys
import tempfile
from pathlib import Path

import yaml


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <CLANG_TIDY_BIN> <FAKE_OUTPUT> <CONFIG> [ARGS...]")
        sys.exit(1)

    clang_tidy_bin = sys.argv[1]
    fake_output = sys.argv[2]
    user_config_file = sys.argv[3]
    plugins_lib = sys.argv[4]
    remaining_args = sys.argv[5:]

    # Bazel requires some kind of output file must be specified
    # so always create it
    Path(fake_output).touch(exist_ok=True)

    # Rewrite
    config = yaml.safe_load(Path(user_config_file).read_text())
    strip_warning_checks(config)
    final_config_file = tempfile.NamedTemporaryFile("w+")
    yaml.safe_dump(config, final_config_file.file)

    try:
        extra_args = [
            "--extra-arg=-Wno-error",
            "--extra-arg=-Wno-macro-redefined",
        ]

        verify_command = [
            clang_tidy_bin,
            f"--config-file={final_config_file.name}",
            "--quiet",
            "--verify-config",
            f"--load={plugins_lib}",
        ]
        _ = subprocess.run(verify_command, check=True, capture_output=True, text=True)

        run_command = (
            [
                clang_tidy_bin,
                f"--config-file={final_config_file.name}",
                f"--load={plugins_lib}",
            ]
            + extra_args
            + remaining_args
        )

        _ = subprocess.run(run_command, check=True, capture_output=True, text=True)

    except subprocess.CalledProcessError as e:
        print("clang-tidy command failed.", file=sys.stderr)
        if e.stdout:
            print("\n--- STDOUT ---", file=sys.stderr)
            print(e.stdout, file=sys.stderr)
        if e.stderr:
            print("\n--- STDERR ---", file=sys.stderr)
            print(e.stderr, file=sys.stderr)
        sys.exit(1)


def strip_warning_checks(config) -> None:
    """
    Strip warning checks to keep only error-generating checks for CI speed.
    """
    config["Checks"] = config["WarningsAsErrors"]


if __name__ == "__main__":
    main()
