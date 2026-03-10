#!/usr/bin/python3
#
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
# Validate an Antithesis-packaged Docker image locally, checking all
# requirements documented by Antithesis for container images.
#
# Usage:
#   ./tools/antithesis/single_binary_test_validate.py memtable_test
#   ./tools/antithesis/single_binary_test_validate.py memtable_test --instrumented
#
# Runs the same checks Antithesis performs on image submission:
#   - Container starts and stays running (entrypoint sleeps)
#   - docker-compose.yaml is well-formed
#   - hostname == container_name, no underscores
#   - init: true is set
#   - setup_complete signal emitted to $ANTITHESIS_OUTPUT_DIR/sdk.jsonl
#   - Singleton driver exists and runs the test successfully
#   - /symbols directory present for symbolization
#   - No fixed RNG seeding
#   - Instrumentation symbols present (when --instrumented)
#

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path


def _config_tag(image_tag: str) -> str:
    """Derive the config image tag by inserting '-config' before the :tag suffix."""
    if ":" in image_tag:
        repo, tag = image_tag.rsplit(":", 1)
        return f"{repo}-config:{tag}"
    return f"{image_tag}-config"


def _compose_path(target_name: str) -> Path:
    return (
        Path(__file__).resolve().parent.parent.parent
        / "bazel-bin"
        / ".antithesis"
        / target_name
        / "docker-compose.yaml"
    )


def run(
    cmd: list[str], *, check: bool = True, capture: bool = False, timeout: int = 120
) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        capture_output=capture,
        text=True,
        check=check,
        timeout=timeout,
    )


def docker_exec(
    container: str, cmd: str, *, check: bool = True, timeout: int = 60
) -> str:
    result = subprocess.run(
        ["docker", "exec", container, "sh", "-c", cmd],
        capture_output=True,
        text=True,
        check=check,
        timeout=timeout,
    )
    return result.stdout.strip()


class Validator:
    def __init__(self, image_tag: str, target_name: str, instrumented: bool):
        self.image_tag = image_tag
        self.target_name = target_name
        self.compose_path = _compose_path(target_name)
        self.instrumented = instrumented
        self.passed = 0
        self.failed = 0
        self.container: str | None = None
        self.binary_name: str | None = None

    def check(self, name: str, ok: bool, detail: str = ""):
        if ok:
            self.passed += 1
            print(f"  PASS  {name}")
        else:
            self.failed += 1
            msg = f"  FAIL  {name}"
            if detail:
                msg += f" -- {detail}"
            print(msg)

    def run_all(self) -> bool:
        print(f"Validating image: {self.image_tag}")
        if self.instrumented:
            print("Mode: instrumented (--config=antithesis)")
        else:
            print("Mode: default (no instrumentation)")

        try:
            self._check_compose_file()
            if self.container is None:
                print("Cannot continue: compose file missing or invalid")
                return False
            self._check_config_image()
            self._start_container()
            self._check_container_running()
            self._check_hostname()
            self._check_image_layout()
            self._check_setup_signal()
            self._check_singleton_driver()
            self._check_no_fixed_seed()
            self._check_symbols()
            if self.instrumented:
                self._check_instrumentation()
        finally:
            self._cleanup()

        print(f"Results: {self.passed} passed, {self.failed} failed")
        return self.failed == 0

    def _check_config_image(self):
        """Validate the config image by extracting docker-compose.yaml from it,
        simulating what Antithesis does to orchestrate the workload."""
        print("[config image]")
        config_tag = _config_tag(self.image_tag)

        # Check the config image exists locally.
        result = run(
            ["docker", "image", "inspect", config_tag],
            capture=True,
            check=False,
        )
        self.check(
            "config image exists",
            result.returncode == 0,
            f"{config_tag} not found in local Docker",
        )
        if result.returncode != 0:
            return

        # Extract docker-compose.yaml from the config image (FROM scratch,
        # no shell — create a container with a dummy entrypoint to copy).
        container_name = "antithesis_config_validate"
        run(["docker", "rm", "-f", container_name], capture=True, check=False)
        result = run(
            [
                "docker",
                "create",
                "--name",
                container_name,
                "--entrypoint=",
                config_tag,
                "/dev/null",
            ],
            capture=True,
            check=False,
        )
        self.check("config container created", result.returncode == 0)

        if result.returncode == 0:
            # Copy the compose file out of the config image.
            with tempfile.TemporaryDirectory() as tmpdir:
                cp_result = run(
                    [
                        "docker",
                        "cp",
                        f"{container_name}:/docker-compose.yaml",
                        f"{tmpdir}/docker-compose.yaml",
                    ],
                    capture=True,
                    check=False,
                )
                self.check(
                    "docker-compose.yaml at / in config image",
                    cp_result.returncode == 0,
                )

                if cp_result.returncode == 0:
                    extracted = Path(f"{tmpdir}/docker-compose.yaml").read_text()
                    expected = (
                        self.compose_path.read_text()
                        if self.compose_path.exists()
                        else None
                    )

                    self.check(
                        "config image compose matches build output",
                        extracted == expected,
                        "content mismatch",
                    )

                    # Use the extracted compose to start a container, proving
                    # the config image is sufficient for orchestration.
                    compose_file = Path(tmpdir) / "docker-compose.yaml"
                    up_result = run(
                        ["docker", "compose", "-f", str(compose_file), "up", "-d"],
                        capture=True,
                        check=False,
                    )
                    self.check(
                        "workload starts from config image compose",
                        up_result.returncode == 0,
                        up_result.stderr.strip() if up_result.returncode != 0 else "",
                    )

                    if up_result.returncode == 0:
                        ps_result = run(
                            [
                                "docker",
                                "compose",
                                "-f",
                                str(compose_file),
                                "ps",
                                "--format",
                                "{{.Status}}",
                            ],
                            capture=True,
                        )
                        status = ps_result.stdout.strip()
                        self.check(
                            "workload running via config image compose",
                            status.startswith("Up"),
                            f"status={status!r}",
                        )

                        # Tear down the config-image-started container so the
                        # main validation can start fresh.
                        run(
                            ["docker", "compose", "-f", str(compose_file), "down"],
                            capture=True,
                            check=False,
                        )

            run(["docker", "rm", "-f", container_name], capture=True, check=False)

    def _check_compose_file(self):
        print("[compose file]")
        self.check(
            "compose file exists", self.compose_path.exists(), str(self.compose_path)
        )
        if not self.compose_path.exists():
            return

        content = self.compose_path.read_text()

        self.check(
            "image tag matches",
            f"image: {self.image_tag}" in content,
            f"expected 'image: {self.image_tag}'",
        )

        self.check("init: true present", "init: true" in content)

        # Extract container_name and hostname
        cn = hn = None
        for line in content.splitlines():
            stripped = line.strip()
            if stripped.startswith("container_name:"):
                cn = stripped.split(":", 1)[1].strip()
            elif stripped.startswith("hostname:"):
                hn = stripped.split(":", 1)[1].strip()

        self.check(
            "container_name == hostname",
            cn is not None and cn == hn,
            f"container_name={cn!r}, hostname={hn!r}",
        )

        self.check(
            "no underscores in hostname",
            hn is not None and "_" not in hn,
            f"hostname={hn!r}",
        )

        self.check("no pull_policy", "pull_policy" not in content)

        self.container = cn

    def _start_container(self):
        print("[container startup]")
        run(
            ["docker", "compose", "-f", str(self.compose_path), "up", "-d"],
            capture=True,
        )

    def _check_container_running(self):
        result = run(
            [
                "docker",
                "ps",
                "--filter",
                f"name={self.container}",
                "--format",
                "{{.Status}}",
            ],
            capture=True,
        )
        status = result.stdout.strip()
        self.check(
            "container is running", status.startswith("Up"), f"status={status!r}"
        )

    def _check_hostname(self):
        print("[hostname]")
        actual = docker_exec(self.container, "hostname")
        self.check(
            "hostname matches container_name",
            actual == self.container,
            f"got {actual!r}, expected {self.container!r}",
        )

    def _check_image_layout(self):
        print("[image layout]")

        self.check(
            "/usr/bin/entrypoint.sh exists", self._file_exists("/usr/bin/entrypoint.sh")
        )

        # Find the binary name
        bins = docker_exec(self.container, "ls /opt/antithesis/bin/")
        binary_names = bins.split()
        self.check(
            "/opt/antithesis/bin/ has binary",
            len(binary_names) > 0,
            f"contents: {bins!r}",
        )

        if binary_names:
            self.binary_name = binary_names[0]

        self.check(
            "/opt/antithesis/lib/ has shared libs",
            bool(docker_exec(self.container, "ls /opt/antithesis/lib/ | head -1")),
        )

        driver_path = (
            f"/opt/antithesis/test/v1/quickstart/singleton_driver_{self.binary_name}.sh"
        )
        self.check(f"singleton_driver exists", self._file_exists(driver_path))

    def _check_setup_signal(self):
        print("[setup_complete signal]")
        output = docker_exec(
            self.container,
            "ANTITHESIS_OUTPUT_DIR=/tmp/_av "
            "/usr/bin/sh /usr/bin/entrypoint.sh & "
            "PID=$!; sleep 1; "
            "cat /tmp/_av/sdk.jsonl 2>/dev/null; "
            "kill $PID 2>/dev/null; true",
            check=False,
        )

        valid_signal = False
        if output:
            try:
                obj = json.loads(output.splitlines()[0])
                valid_signal = (
                    obj.get("antithesis_setup", {}).get("status") == "complete"
                )
            except (json.JSONDecodeError, IndexError):
                pass

        self.check("setup_complete signal emitted", valid_signal, f"output={output!r}")

    def _check_singleton_driver(self):
        print("[singleton_driver execution]")
        driver = (
            f"/opt/antithesis/test/v1/quickstart/singleton_driver_{self.binary_name}.sh"
        )
        result = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(self.compose_path),
                "exec",
                "-T",
                "workload",
                driver,
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )
        output = result.stdout + result.stderr
        self.check(
            "singleton_driver exits 0",
            result.returncode == 0,
            f"exit code {result.returncode}",
        )

        self.check(
            "binary was executed",
            self.binary_name in output,
            f"no mention of '{self.binary_name}' in output",
        )

    def _check_no_fixed_seed(self):
        print("[random seeding]")
        driver = (
            f"/opt/antithesis/test/v1/quickstart/singleton_driver_{self.binary_name}.sh"
        )
        content = docker_exec(self.container, f"cat {driver}")
        has_fixed = "REDPANDA_RNG_SEEDING_MODE" in content
        self.check(
            "no fixed RNG seed in driver env",
            not has_fixed,
            "REDPANDA_RNG_SEEDING_MODE found in driver script",
        )

    def _check_symbols(self):
        print("[symbolization]")
        self.check("/symbols/ directory exists", self._file_exists("/symbols"))

        link_target = docker_exec(
            self.container,
            f"readlink /symbols/{self.binary_name} 2>/dev/null || true",
        )
        expected = f"/opt/antithesis/bin/{self.binary_name}"
        self.check(
            f"/symbols/{self.binary_name} -> binary",
            link_target == expected,
            f"got {link_target!r}, expected {expected!r}",
        )

    def _check_instrumentation(self):
        print("[instrumentation]")

        # Verify instrumentation symbols are linked into the binary inside
        # the container image. The slim base image lacks nm/readelf, so
        # search for the symbol name strings directly in the binary.
        binary_path = f"/opt/antithesis/bin/{self.binary_name}"
        has_symbol = docker_exec(
            self.container,
            f"grep -qc __sanitizer_cov_trace_pc_guard {binary_path} && echo found || true",
        )
        self.check(
            "instrumentation symbols in binary",
            has_symbol.strip() == "found",
            f"no instrumentation symbols found in {binary_path}",
        )

    def _file_exists(self, path: str) -> bool:
        result = subprocess.run(
            ["docker", "exec", self.container, "test", "-e", path],
            capture_output=True,
        )
        return result.returncode == 0

    def _cleanup(self):
        print("[cleanup]")
        run(
            ["docker", "compose", "-f", str(self.compose_path), "down"],
            capture=True,
            check=False,
        )
        print("  containers stopped and removed")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate an Antithesis-packaged Docker image locally."
    )
    parser.add_argument("target_name", help="Target name (e.g. memtable_test)")
    parser.add_argument(
        "--tag", default="", help="Docker image tag (default: <target_name>:latest)"
    )
    parser.add_argument(
        "--instrumented",
        action="store_true",
        help="Also validate instrumentation symbols",
    )
    args = parser.parse_args()

    image_tag = args.tag or f"{args.target_name}:latest"
    validator = Validator(image_tag, args.target_name, args.instrumented)
    ok = validator.run_all()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
