# ==================================================================
# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
# ==================================================================
#
# Shared helpers for the Antithesis test-packaging scripts
# (ducktape_test_package.py and single_binary_test_package.py).

import argparse
import json
import os
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

from jinja2 import StrictUndefined, Template

LAUNCH_URL = "https://redpanda.antithesis.com/api/v1/launch/basic_test"
RUNS_URL = "https://redpanda.antithesis.com/runs"

_VERBOSE = False


DEFAULT_REGISTRY = "us-central1-docker.pkg.dev/molten-verve-216720/redpanda-repository"

# Antithesis enforces a floor on run length; also our default.
MIN_DURATION_MIN = 5

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PACKAGING_ROOT = REPO_ROOT / "bazel-bin" / "bazel" / "packaging"


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add the registry-push and test-submission flags shared by both
    packaging scripts. Pair with validate_common_args after parsing."""
    group = parser.add_argument_group("registry and submission")
    group.add_argument(
        "--push",
        action="store_true",
        help="Push the built images to the registry (implied by --submit)",
    )
    group.add_argument(
        "--registry",
        default=DEFAULT_REGISTRY,
        help="Docker registry to upload images to",
    )
    group.add_argument(
        "--tag",
        action="append",
        default=[],
        metavar="TAG",
        help="Additional tag applied to every built image and pushed "
        "alongside the implicit image-ID tag (repeatable), e.g. --tag nightly",
    )
    group.add_argument(
        "--submit",
        action="store_true",
        help="Launch an Antithesis test run after pushing "
        "(requires the AT_PASSWORD environment variable)",
    )
    group.add_argument(
        "--description",
        default="",
        help="Antithesis run description (default: none)",
    )
    group.add_argument(
        "--duration",
        type=int,
        default=MIN_DURATION_MIN,
        help=f"Antithesis run duration in minutes "
        f"(default and minimum: {MIN_DURATION_MIN})",
    )
    group.add_argument(
        "--recipients",
        default="",
        help="Semicolon-separated report email recipients (default: none)",
    )
    group.add_argument(
        "--source",
        default="",
        help="antithesis.source: groups property history across runs. Use a "
        "stable key such as the git branch; runs sharing a source share "
        "history. Required for --no-ephemeral runs.",
    )
    group.add_argument(
        "--ephemeral",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Keep the run out of the findings history (antithesis.is_ephemeral). "
        "Default: ephemeral, for ad-hoc runs. Pass --no-ephemeral for a "
        "persistent run recorded to history (requires --source).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Echo the underlying commands and the raw launch response",
    )


def add_build_args(parser: argparse.ArgumentParser) -> None:
    """Add the Bazel-build flags shared by the packaging scripts."""
    group = parser.add_argument_group("build")
    group.add_argument(
        "--instrumented",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Build with --config=antithesis (default: enabled)",
    )
    group.add_argument(
        "--bazel-args",
        default="",
        help="Extra arguments passed to bazel build",
    )
    group.add_argument(
        "--skip-bazel-build",
        action="store_true",
        help="Skip the Bazel build (use existing artifacts)",
    )


def validate_common_args(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> None:
    """Validate the flags added by add_common_args, erroring via the parser
    on contradictory or out-of-range values. --submit implies --push."""
    global _VERBOSE
    _VERBOSE = args.verbose
    if args.submit:
        args.push = True
    if args.submit and args.duration < MIN_DURATION_MIN:
        parser.error(f"--duration must be at least {MIN_DURATION_MIN} minutes")
    if args.submit and not args.ephemeral and not args.source:
        parser.error(
            "--no-ephemeral runs must set --source; use a stable grouping key "
            'such as the git branch, e.g. --source "$(git branch --show-current)"'
        )
    if args.submit and not os.environ.get("AT_PASSWORD"):
        parser.error("--submit requires the AT_PASSWORD environment variable")


def run(
    cmd: list[str],
    *,
    check: bool = True,
    cwd: Path | None = None,
    capture: bool = False,
) -> subprocess.CompletedProcess:
    if _VERBOSE:
        print(f"  $ {' '.join(str(c) for c in cmd)}")
    result = subprocess.run(
        cmd,
        check=False,
        cwd=cwd,
        capture_output=capture,
        text=True,
    )
    if check and result.returncode != 0:
        # Surface captured output before failing, or the error is invisible.
        if capture:
            sys.stdout.write(result.stdout or "")
            sys.stderr.write(result.stderr or "")
        raise subprocess.CalledProcessError(
            result.returncode, cmd, result.stdout, result.stderr
        )
    return result


def render_template(path: Path, **kwargs) -> str:
    """Render a Jinja2 template file. Undefined variables are errors, and
    shquote (usable as a filter or a function) shell-quotes values."""
    template = Template(
        path.read_text(),
        undefined=StrictUndefined,
        keep_trailing_newline=True,
    )
    template.globals["shquote"] = shlex.quote
    template.environment.filters["shquote"] = shlex.quote
    return template.render(**kwargs)


def bazel_build(target: str, *, instrumented: bool, bazel_args: str = "") -> None:
    """Build a Bazel target from the repo root, with --config=antithesis
    when instrumented."""
    print(f"==> Building {target}")
    cmd = ["bazel", "build", target]
    if instrumented:
        cmd.append("--config=antithesis")
    cmd.extend(shlex.split(bazel_args))
    run(cmd, cwd=REPO_ROOT)


def packaging_artifact(
    name: str, *, hint: str = "Run without --skip-bazel-build."
) -> Path:
    """Path of a //bazel/packaging build output, exiting with hint when it
    has not been built."""
    path = PACKAGING_ROOT / name
    if not path.exists():
        sys.exit(f"Error: {path} not found. {hint}")
    return path


def push_image(registry: str, local_ref: str) -> str:
    """Push a local image to the registry, preserving its tag: local_ref
    is pushed as {registry}/{local_ref}. Returns the pushed image's digest
    reference (registry/name@sha256:...), the immutable form recommended
    for antithesis.images."""
    ref = f"{registry}/{local_ref}"
    print(f"==> Pushing image: {ref}")
    run(["docker", "tag", local_ref, ref])
    run(["docker", "push", ref])
    repo = f"{registry}/{local_ref.rsplit(':', 1)[0]}"
    digests = run(
        ["docker", "image", "inspect", ref, "--format", "{{json .RepoDigests}}"],
        capture=True,
    ).stdout
    for digest_ref in json.loads(digests):
        if digest_ref.startswith(f"{repo}@"):
            return digest_ref
    sys.exit(f"Error: no digest recorded for {ref} after push")


def upload_images(registry: str, refs: list[str]) -> dict[str, str]:
    """Push each local image ref to the registry, preserving tags.
    Returns {local_ref: pushed_digest_ref}."""
    return {ref: push_image(registry, ref) for ref in refs}


def tag_images(refs: list[str], tags: list[str]) -> list[str]:
    """Apply each tag to each reference's image: <name>:<x> gains
    <name>:<tag>. Returns the alias references."""
    aliases: list[str] = []
    for ref in refs:
        name = ref.rsplit(":", 1)[0]
        for tag in tags:
            alias = f"{name}:{tag}"
            run(["docker", "tag", ref, alias])
            aliases.append(alias)
    return aliases


def docker_build(name: str, build_args: list[str], context: Path | str) -> str:
    """Run docker build and tag the result by its image ID: <name>:<id12>.
    Image IDs are content-derived, so the reference identifies exactly one
    build and identical inputs rebuild to the identical reference. Returns
    the tagged reference."""
    with tempfile.TemporaryDirectory() as tmpdir:
        iidfile = Path(tmpdir) / "iid"
        run(["docker", "build", "--iidfile", str(iidfile), *build_args, str(context)])
        iid = iidfile.read_text().strip()
    ref = f"{name}:{iid.removeprefix('sha256:')[:12]}"
    run(["docker", "tag", iid, ref])
    return ref


def build_config_image(name: str, compose_content: str) -> str:
    """Package a docker-compose.yaml into a FROM scratch Antithesis config
    image. Returns the built reference."""
    print(f"==> Building config image: {name}")
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        (tmp / "docker-compose.yaml").write_text(compose_content)
        (tmp / "Dockerfile").write_text(
            "FROM scratch\nCOPY docker-compose.yaml /docker-compose.yaml\n"
        )
        return docker_build(name, [], tmp)


def submit_test_run(
    *,
    password: str,
    duration_min: int,
    config_image: str,
    images: list[str],
    test_name: str | None = None,
    description: str | None = None,
    source: str | None = None,
    ephemeral: bool = False,
    recipients: list[str] | None = None,
    extra_params: dict[str, str] | None = None,
) -> None:
    """Launch a basic_test run via the Antithesis API.

    test_name sets antithesis.test_name, the run's name in reports; without it
    runs carry the launch endpoint's built-in name ("Basic Test"). description
    sets antithesis.description, shown in report headers. source sets
    antithesis.source (groups property history across runs); ephemeral sets
    antithesis.is_ephemeral, keeping the run out of that history. config_image
    and images are fully-qualified registry references (the ones produced by
    upload_images). recipients is an optional list of report email addresses.
    extra_params are additional launch parameters (e.g. tenant-custom `custom.*`
    fault-scoping keys) merged into the body.
    """
    params = {
        "antithesis.duration": str(duration_min),
        "antithesis.config_image": config_image,
    }
    if test_name:
        params["antithesis.test_name"] = test_name
    if description:
        params["antithesis.description"] = description
    if source:
        params["antithesis.source"] = source
    if ephemeral:
        params["antithesis.is_ephemeral"] = "true"
    if images:
        params["antithesis.images"] = ";".join(images)
    if recipients:
        params["antithesis.report.recipients"] = ";".join(recipients)
    if extra_params:
        params.update(extra_params)

    body = json.dumps({"params": params})

    print("==> Submitting Antithesis test run")
    if _VERBOSE:
        print(f"  $ curl --fail -u redpanda:*** -X POST {LAUNCH_URL} -d {body}")
    result = subprocess.run(
        [
            "curl",
            "--fail",
            "-sS",
            "-u",
            f"redpanda:{password}",
            "-X",
            "POST",
            LAUNCH_URL,
            "-d",
            body,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if _VERBOSE and result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        sys.exit(f"\nError: test run submission failed: {detail}")

    try:
        run_id = json.loads(result.stdout).get("runId", "")
    except (json.JSONDecodeError, AttributeError):
        run_id = ""

    lines = [
        "",
        "Submitted Antithesis test run:",
        f"  name:      {test_name or '(unnamed)'}",
    ]
    if run_id:
        lines.append(f"  run id:    {run_id}")
    lines.append(f"  duration:  {duration_min} min")
    if source:
        lines.append(f"  source:    {source}")
    if ephemeral:
        lines.append("  ephemeral: yes (not recorded in the findings history)")
    lines += [f"  runs:      {RUNS_URL}", ""]
    print("\n".join(lines))


def maybe_submit(
    args: argparse.Namespace,
    *,
    test_name: str,
    pushed: dict[str, str],
    config_ref: str,
    extra_params: dict[str, str] | None = None,
) -> None:
    """Launch a test run from parsed args when --submit was given.

    pushed maps local image refs to their pushed references (as returned by
    upload_images); config_ref names the entry that is the Antithesis config
    image (every other entry is sent as a workload image).
    """
    if not args.submit:
        return
    submit_test_run(
        password=os.environ["AT_PASSWORD"],
        test_name=test_name,
        description=args.description or None,
        source=args.source or None,
        ephemeral=args.ephemeral,
        duration_min=args.duration,
        config_image=pushed[config_ref],
        images=[remote for local, remote in pushed.items() if local != config_ref],
        recipients=[r for r in args.recipients.split(";") if r] or None,
        extra_params=extra_params,
    )


def registry_help_str(registry: str, *, pushed: bool, refs: list[str]) -> str:
    """Render the closing registry status block for a script's summary:
    a one-line confirmation when images were pushed, or the manual docker
    tag/push commands to push them later."""
    if pushed:
        return f"Pushed to the Antithesis registry: {registry}"
    lines = [
        "Images were not pushed to the Antithesis registry. Rerun with --push to push,",
        "or with --submit to also launch an Antithesis test run.",
        "Alternatively, push manually:",
        "",
        f"  REGISTRY={registry}",
    ]
    for ref in refs:
        lines.append(f"  docker tag {ref} $REGISTRY/{ref}")
        lines.append(f"  docker push $REGISTRY/{ref}")
    return "\n".join(lines)
