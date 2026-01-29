#!/usr/bin/env python3
from dataclasses import dataclass
import shutil
import subprocess
import time
from pathlib import Path
from typing import List, Any
import sys
import argparse
import tempfile

# pyright: strict

# Always add type hints unless told otherwise

BAZEL_CMD = "bazel"


def info(*args: Any, **kwargs: Any) -> None:
    print(*args, file=sys.stderr, **kwargs)


@dataclass
class Options:
    quiet_build: bool
    target: str
    skip_delete: bool
    target2: str


@dataclass
class BuildResult:
    label: str
    elapsed: float
    size_mb: float
    redpanda_size_mb: float
    test_elapsed: float
    test_size_mb: float


class Main:
    def __init__(self, repo_root: Path, options: Options) -> None:
        self.options: Options = options
        self.repo_root: Path = repo_root
        self.logfile_path: str | None = None
        self.logfile: Any = None
        self.build_times: List[BuildResult] = []
        if self.options.quiet_build:
            self.logfile = tempfile.NamedTemporaryFile(
                delete=False,
                mode="w+",
                prefix="build-timer-bazel-",
                suffix=".log",
                dir="/tmp",
            )
            self.logfile_path = self.logfile.name
            info(f"Bazel output will be written to: {self.logfile_path}")

    def run_bazel(
        self, desc: str, label: str, opts: List[str], target: str | None = None
    ) -> tuple[float, float]:
        cmd = (
            [BAZEL_CMD, "build"] + opts + [(target if target else self.options.target)]
        )
        info(f"Running: {' '.join(cmd)}")
        start: float = time.perf_counter()
        try:
            if self.logfile:
                subprocess.run(
                    cmd, check=True, stdout=self.logfile, stderr=self.logfile
                )
            else:
                subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError:
            if self.logfile is not None:
                self.logfile.flush()
                try:
                    with open(self.logfile.name, "r") as f:
                        lines = f.readlines()
                        tail = lines[-50:] if len(lines) > 50 else lines
                        info("Last 50 lines of Bazel output:")
                        for line in tail:
                            info(line.rstrip())
                except Exception as read_exc:
                    info(f"Could not read logfile: {read_exc}")
            raise
        elapsed: float = time.perf_counter() - start
        # Calculate size of bazel-bin/src/v
        output_dir = self.repo_root / "bazel-bin" / "src" / "v"
        size_bytes: float = 0.0
        if output_dir.exists():
            for p in output_dir.rglob("*"):
                if p.is_file():
                    size_bytes += p.stat().st_size
        size_mb: float = size_bytes / 1e6
        info(f"Output size after build: {size_mb:,.2f} MB")
        return elapsed, size_mb

    def _run_single_build_option(self, opts: str) -> None:
        label = "default" if not opts else opts
        info(f"===== Timing build for label: {label} ======")

        if not self.options.skip_delete:
            elapsed, size_mb = self.run_bazel("initial build", label, opts.split())
            print(
                f"Initial finished: {label} in {elapsed:.2f} seconds, size: {size_mb:,.0f} MB"
            )
            # Delete output directory to ensure a clean build
            output_dir = self.repo_root / "bazel-bin" / "src" / "v"
            if not output_dir.exists():
                raise RuntimeError(
                    f"Output directory {output_dir} does not exist. Aborting."
                )
            shutil.rmtree(output_dir)
            info(f"Deleted output directory: {output_dir}")

        elapsed, size_mb = self.run_bazel("Redpanda rebuild", label, opts.split())
        # Record redpanda binary size in MB
        rp_bin = self.repo_root / "bazel-bin/src/v/redpanda/redpanda"
        redpanda_size_mb = (rp_bin).stat().st_size / 1e6
        info(f"Redpanda binary size: {redpanda_size_mb:,.2f} MB")
        print(
            f"Rebuild finished: {label} in {elapsed:.2f} seconds, size: {size_mb:,.0f} MB, redpanda: {redpanda_size_mb:,.0f} MB"
        )
        # Build the test target (no delete)
        test_elapsed, test_size_mb = self.run_bazel(
            "Test build", label, opts.split(), target=self.options.target2
        )
        print(
            f"Test build finished: {label} in {test_elapsed:.2f} seconds, size: {test_size_mb:,.0f} MB"
        )
        self.build_times.append(
            BuildResult(
                label, elapsed, size_mb, redpanda_size_mb, test_elapsed, test_size_mb
            )
        )

    def run_bazel_builds(self, build_options_list: List[str]) -> None:
        for opts in build_options_list:
            try:
                self._run_single_build_option(opts)
            except subprocess.CalledProcessError as e:
                info(f"=========\nERROR: Build failed for options '{opts}': {e}")
        if self.logfile:
            self.logfile.close()
        info("\nBuild time summary:")
        for result in self.build_times:
            print(
                f"  {result.label:20} {result.elapsed:.2f} seconds, {result.size_mb:,.0f} MB, redpanda: {result.redpanda_size_mb:,.0f} MB, test: {result.test_elapsed:.2f} seconds, test size: {result.test_size_mb:,.0f} MB"
            )
        # Output summary as a markdown table
        print(
            "\n| Label                  | Time (seconds) | size (MB) | Redpanda size (MB) | Time test (s) | size (MB) |"
        )
        print(
            "|------------------------|---------------:|------------:|--------------:|------------------:|---------------:|"
        )
        for result in self.build_times:
            print(
                f"| {result.label:22} | {result.elapsed:13.2f} | {result.size_mb:11,.0f} | {result.redpanda_size_mb:13,.0f} | {result.test_elapsed:16.2f} | {result.test_size_mb:13,.0f} |"
            )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--quiet-build",
        action="store_true",
        help="Redirect Bazel output to a logfile in /tmp",
    )
    parser.add_argument(
        "--target",
        type=str,
        default="//:redpanda",
        help="Bazel build target (default: //:redpanda)",
    )
    parser.add_argument(
        "--target2",
        type=str,
        default="//src/v/kafka/server/tests:fetch_test",
        help="Second Bazel build target (default: //src/v/kafka/server/tests:fetch_test)",
    )
    parser.add_argument(
        "--skip-delete",
        action="store_true",
        help="Skips the warmup + delete + rebuild dance"
        " and just builds, useful for debugging",
    )
    args = parser.parse_args()

    options = Options(
        quiet_build=args.quiet_build,
        target=args.target,
        skip_delete=args.skip_delete,
        target2=args.target2,
    )

    repo_root = Path(__file__).resolve().parents[2]
    user_bazelrc = repo_root / "user.bazelrc"
    backup_bazelrc = repo_root / "user.bazelrc.backup"

    # Move user.bazelrc out of the way
    if user_bazelrc.exists():
        shutil.move(str(user_bazelrc), str(backup_bazelrc))
        info("Moved user.bazelrc out of the way.")
    else:
        info("user.bazelrc not found, continuing.")

    def sanopts(sans: List[str]):
        sanlist = ",".join(sans)
        base = "--copt -O1 "
        sanpart = f"--copt -fsanitize={sanlist} --linkopt -fsanitize={sanlist} --@krb5//:sanitizers={sanlist} --linkopt -fsanitize-link-c++-runtime"
        return (base + sanpart) if sanlist else base

    try:
        build_options_list: List[str] = [
            "--config=release",
            "",
            "--strip=never",
            # "--strip=never --copt -gmlt",
            # "--strip=never --copt -gline-tables-only",
            # "--copt -O1",
            # "--copt -Os",
            # "--copt -Oz",
            # "--config=sanitizer",
            # sanopts(["address"]),
            # sanopts(["undefined"]),
            # sanopts(["vptr"]),
            # sanopts(["function"]),
            # sanopts(["alignment"]),
            # sanopts([]),
            # "--compilation_mode=dbg",
            # "--copt -fno-function-sections",
            # "",
            # "--config=release_base",
            # "--config=release_base --config=lto",
            # "--config=release_base --config=full-debug",
            # "--config=release_base --copt -mllvm --copt -inline-threshold=2500",
            # "--config=full-debug",
            # "--config=full-debug --copt -fstandalone-debug",
            # ""  # default build
        ]
        main_obj = Main(repo_root=repo_root, options=options)
        main_obj.run_bazel_builds(build_options_list)
    finally:
        # Restore user.bazelrc
        if backup_bazelrc.exists():
            shutil.move(str(backup_bazelrc), str(user_bazelrc))
            info("Restored user.bazelrc.")


if __name__ == "__main__":
    main()
