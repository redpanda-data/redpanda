#!/usr/bin/env python3

import os
import subprocess
import sys
import tempfile
import yaml

from pathlib import Path


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <CLANG_TIDY_BIN> <FAKE_OUTPUT> <CONFIG> [ARGS...]")
        sys.exit(1)

    clang_tidy_bin = sys.argv[1]
    fake_output = sys.argv[2]
    user_config_file = sys.argv[3]
    plugins_lib = sys.argv[4]
    fixes_file = sys.argv[6].split("=")[1]
    remaining_args = sys.argv[5:]

    # Bazel requires some kind of output file must be specified
    # so always create it
    Path(fake_output).touch(exist_ok=True)
    Path(fixes_file).touch(exist_ok=True)

    print(f"FIXES: {fixes_file}")

    # Rewrite
    config = yaml.safe_load(Path(user_config_file).read_text())
    strip_warning_checks(config)
    final_config_file = tempfile.NamedTemporaryFile("w+")
    yaml.safe_dump(config, final_config_file.file)

    try:
        verify_command = [
            clang_tidy_bin,
            f"--config-file={final_config_file.name}",
            "--quiet",
            "--verify-config",
            f"--load={plugins_lib}",
        ]
        _ = subprocess.run(verify_command, check=True, capture_output=True, text=True)

        run_command = [
            clang_tidy_bin,
            f"--config-file={final_config_file.name}",
            f"--load={plugins_lib}",
        ] + remaining_args

        _ = subprocess.run(run_command, check=True, capture_output=True, text=True)

    except subprocess.CalledProcessError as e:
        print("clang-tidy command failed.", file=sys.stderr)
        if e.stdout:
            print("\n--- STDOUT ---", file=sys.stderr)
            print(e.stdout, file=sys.stderr)
        if e.stderr:
            print("\n--- STDERR ---", file=sys.stderr)
            print(e.stderr, file=sys.stderr)
        fixes = yaml.safe_load(Path(fixes_file).read_text())
        substitute_fixes(fixes)
        yaml.safe_dump(fixes, open(fixes_file, "w"))
        sys.exit(1)


def strip_warning_checks(config) -> None:
    """
    Strip warning checks to keep only error-generating checks for CI speed.
    """
    config["Checks"] = config["WarningsAsErrors"]


def substitute_fixes(fixes) -> None:
    workspace_root = os.environ.get("WORKSPACE_ROOT")
    if workspace_root is None:
        return

    for d in fixes["Diagnostics"]:
        if "DiagnosticMessage" not in d or "Replacements" not in d["DiagnosticMessage"]:
            continue

        build_root = d["BuildDirectory"]
        for r in d["DiagnosticMessage"]["Replacements"]:
            if "FilePath" not in r:
                continue
            r["FilePath"] = r["FilePath"].replace(build_root, workspace_root)


if __name__ == "__main__":
    main()
