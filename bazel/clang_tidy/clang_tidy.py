#!/usr/bin/env python3
import sys
import subprocess
from pathlib import Path


def main():
    if len(sys.argv) < 3:
        print(
            f"Usage: {sys.argv[0]} <CLANG_TIDY_BIN> <FAKE_OUTPUT> <CONFIG> [ARGS...]"
        )
        sys.exit(1)

    clang_tidy_bin = sys.argv[1]
    fake_output = sys.argv[2]
    config_file = sys.argv[3]
    remaining_args = sys.argv[4:]

    # Bazel requires some kind of output file must be specified
    # so always create it
    Path(fake_output).touch(exist_ok=True)

    try:
        verify_command = [
            clang_tidy_bin, f"--config-file={config_file}", "--quiet",
            "--verify-config"
        ]
        _ = subprocess.run(verify_command,
                           check=True,
                           capture_output=True,
                           text=True)

        run_command = [clang_tidy_bin, f"--config-file={config_file}"
                       ] + remaining_args

        _ = subprocess.run(run_command,
                           check=True,
                           capture_output=True,
                           text=True)

    except subprocess.CalledProcessError as e:
        print("clang-tidy command failed.", file=sys.stderr)
        if e.stdout:
            print("\n--- STDOUT ---", file=sys.stderr)
            print(e.stdout, file=sys.stderr)
        if e.stderr:
            print("\n--- STDERR ---", file=sys.stderr)
            print(e.stderr, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
