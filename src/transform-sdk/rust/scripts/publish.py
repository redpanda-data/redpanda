#!/usr/bin/env python3
"""
This script runs within GitHub actions and deploys each crate to crates.io

The reason for this script is to turn local dependencies (via paths) into
versions for crates.io
"""

import argparse
from pathlib import Path
import subprocess
import tomlkit

CARGO_WORKSPACE_DIR = Path(__file__).resolve().parent.parent

CARGO_TOML_FILE = CARGO_WORKSPACE_DIR / "Cargo.toml"


def publish_package(pkg: str, dry_run: bool):
    # must have env CARGO_REGISTRY_TOKEN set or the user must be logged in.
    cmd = [
        "cargo",
        "publish",
        # We just update the version
        "--allow-dirty",
        "--locked",
        f"--package={pkg}",
    ]
    if dry_run:
        print(f"$ {' '.join(cmd)}")
    else:
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


def update_dependencies(manifest: tomlkit.TOMLDocument, version: str):
    if "dependencies" not in manifest:
        return
    for meta in manifest["dependencies"].values():
        if not meta.is_inline_table():
            continue
        meta["version"] = version


def publish(version: str, dry_run: bool):
    # Cargo does not like the prefix we add to tags, so remove it.
    version = version.removeprefix('transform-sdk/v')

    # Set the workspace version in the TOML file
    manifest = tomlkit.loads(CARGO_TOML_FILE.read_text())
    manifest["workspace"]["package"]["version"] = version
    CARGO_TOML_FILE.write_text(tomlkit.dumps(manifest))

    # Update versions everywhere
    for path in CARGO_WORKSPACE_DIR.glob("**/*.toml"):
        if "examples" in path.parts:
            continue
        manifest = tomlkit.loads(path.read_text())
        update_dependencies(manifest, version)
        path.write_text(tomlkit.dumps(manifest))

    # The order matters here so that we get the right versions in the registry
    # before deploying the next crate
    for pkg in [
            "-varint", "-sr-types", "-sr-sys", "-sr", "-types", "-sys", ""
    ]:
        publish_package(f"redpanda-transform-sdk{pkg}", dry_run)


parser = argparse.ArgumentParser()
parser.add_argument('--version', type=str, required=True)
parser.add_argument('--dry', action='store_true')

if __name__ == '__main__':
    args = parser.parse_args()
    publish(version=args.version, dry_run=args.dry)
