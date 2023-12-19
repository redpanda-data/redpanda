#!/usr/bin/env python3

import argparse
from pathlib import Path
import subprocess
import re

CARGO_WORKSPACE_DIR = Path(__file__).resolve().parent.parent

CARGO_TOML_FILE = CARGO_WORKSPACE_DIR / "Cargo.toml"
SYS_CARGO_TOML_FILE = CARGO_WORKSPACE_DIR / "core-sys" / "Cargo.toml"


def publish_package(pkg: str):
    # must have env CARGO_REGISTRY_TOKEN set or the user must be logged in.
    subprocess.run(
        [
            "cargo",
            "publish",
            # We just update the version
            "--allow-dirty",
            "--locked",
            f"--package={pkg}",
        ],
        check=True,
    )


def publish(version: str):
    # Cargo does not like the `v` prefix we add to tags, so remove it.
    version = version.removeprefix('v')
    # Set the version in the TOML file
    toml = CARGO_TOML_FILE.read_text()
    toml = re.sub(pattern='^version = "[^"]+"',
                  repl=f'version = "{version}"',
                  string=toml,
                  flags=re.MULTILINE)
    toml = re.sub(pattern='{ path = "([^"]+)", version = "=[^"]+" }$',
                  repl=f'{{ path = "\\1", version = "={version}" }}',
                  string=toml,
                  flags=re.MULTILINE)
    CARGO_TOML_FILE.write_text(toml)

    toml = SYS_CARGO_TOML_FILE.read_text()
    toml = re.sub(pattern='{ path = "([^"]+)", version = "=[^"]+" }$',
                  repl=f'{{ path = "\\1", version = "={version}" }}',
                  string=toml,
                  flags=re.MULTILINE)
    SYS_CARGO_TOML_FILE.write_text(toml)

    # The order matters here so that we get the right versions
    for pkg in ["-types", "-sys", ""]:
        publish_package(f"redpanda-transform-sdk{pkg}")


parser = argparse.ArgumentParser()
parser.add_argument('--version', type=str, required=True)

if __name__ == '__main__':
    args = parser.parse_args()
    publish(version=args.version)
