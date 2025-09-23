#!/usr/bin/env python3

import argparse
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any

# pyright: strict

SCRIPT_RELPATH = "/".join(Path(__file__).parts[-2:])


class Level(Enum):
    SKIP = "skip"
    OFF = "off"
    BASIC = "basic"
    STANDARD = "standard"
    STRICT = "strict"

    def __str__(self) -> str:
        return self.value


@dataclass
class Diagnostic:
    file: Path
    severity: str
    message: str
    rule: str
    level: Level

    @staticmethod
    def from_json(input: dict[str, Any], level: Level):
        return Diagnostic(
            Path(input["file"]),
            input["severity"],
            input["message"],
            input["rule"],
            level,
        )


DiagMap = dict[Path, list[Diagnostic]]


class TypeCheck:
    def __init__(self, args: Any):
        self._args = args
        self._tests_root: Path = args.tests_root

        self._config: Path = self._tests_root / args.config
        self.pyright: Path = args.pyright_path
        self._input_files: str | None = args.input_files or None
        self._force_level: Level | None = (
            Level(args.force_level) if args.force_level else None
        )

        # Set up color handling
        if args.color == "always":
            self._use_color = True
        elif args.color == "never":
            self._use_color = False
        else:  # auto
            assert args.color == "auto"
            # Auto-detect if output is a terminal
            self._use_color = sys.stdout.isatty()

        self.error = self._red("ERROR:")
        self.info = self._blue("INFO: ")

        self.vprint("Tests root:", self._tests_root)

        if not self._config.is_file():
            raise RuntimeError(f"pyright config file not found: {self._config}")

    def _run_json(self, config: Path, extra_args: list[str]):
        """Return the json output for running the type checker on the provided args,
        raises an exception if the error is not in 0, 1 (1 being returned when there are
        type checking failures, which is expected here)."""
        res = self._run(config, ["--outputjson"] + extra_args)
        self.vprint(f"pyright returned: {res.returncode}")
        if res.returncode not in {0, 1}:
            print(res.stdout)
            res.check_returncode()
        return json.loads(res.stdout)

    def _run(
        self, config: Path, extra_args: list[str] = [], *, pipe_stdout: bool = True
    ):
        args = [self.pyright, "--project", config, "--warnings"]
        pargs = list(map(str, args)) + extra_args

        cmd_str = " ".join(pargs)
        if len(cmd_str) > 500:
            cmd_str = cmd_str[:500] + "[... truncated]"
        self.vprint(f"Running pyright command: {cmd_str}")

        env: dict[str, str] = dict(os.environ)

        # pass through the CLI force color setting to pyright (uses chalk)
        if self._args.color == "never":
            env["FORCE_COLOR"] = "0"
        if self._args.color == "always":
            env["FORCE_COLOR"] = "1"

        # Set up environment to use the .venv virtual environment
        if self._args.no_venv:
            self.vprint("Skipping virtual environment (--no-venv specified)")
        else:
            venv_path = self._tests_root / ".venv"
            if venv_path.exists():
                env["VIRTUAL_ENV"] = str(venv_path)
                env["PATH"] = f"{venv_path / 'bin'}:{env.get('PATH', '')}"
                # Remove PYTHONHOME if set, as it can interfere with venv
                env.pop("PYTHONHOME", None)
                self.vprint(f"Using virtual environment: {venv_path}")
            else:
                self.vprint("Virtual environment not found, using system environment")

        sys.stdout.flush()  # ensure any pending output is printed before subprocess

        stdout = subprocess.PIPE if pipe_stdout else None
        res = subprocess.run(pargs, text=True, stdout=stdout, env=env)
        return res

    def _generate_config(self, target_level: Level) -> Path:
        """Generate a temporary pyrightconfig.json file with the desired typeCheckingMode
        and return its path."""

        # SKIP level should not call pyright, so this should not be called for SKIP
        if target_level == Level.SKIP:
            raise RuntimeError("_generate_config should not be called for SKIP level")

        config = self._read_config()
        level = config["typeCheckingMode"]
        if level not in {"basic", "standard", "strict"}:
            raise RuntimeError(f"Unknown typeCheckingMode in {self._config}: {level}")

        # Map our internal levels to pyright's valid levels
        target_string = target_level.value

        config["typeCheckingMode"] = target_string

        tmp_config = self._tests_root / f".temp.pyrightconfig.{target_level.value}.json"
        tmp_config.write_text(json.dumps(config, indent=4))
        self.vprint(f"Wrote temporary pyright config: {tmp_config}")
        return tmp_config

    def _run_diagnostics(
        self,
        level: Level | None = None,
        input_files: dict[Level, list[Path]] | None = None,
    ) -> DiagMap:
        """Return a map of filename to diagnostics for the command line inputs files,
        if any, or all files in tests/**/*.py otherwise."""
        strictness_map = (
            input_files if input_files is not None else self._get_input_files()
        )
        dmap: dict[Path, list[Diagnostic]] = defaultdict(list)
        for level_str, files in strictness_map.items():
            if len(files) == 0:
                print(f"No files in {level_str}.")
                continue

            level = Level(level_str)

            # Skip pyright execution for SKIP level files
            if level == Level.SKIP:
                self.vvprint(
                    f"Skipping pyright execution for {len(files)} files in level {level_str}."
                )
                # Add empty entries for all files at SKIP level (no diagnostics)
                for f in files:
                    dmap[f]
                continue

            config = self._generate_config(level)
            self.vprint(f"Processing {len(files)} files in level {level_str}.")
            output = self._run_json(config, list(map(str, files)))
            djson: list[Any] = output["generalDiagnostics"]
            for dj in djson:
                d = Diagnostic.from_json(dj, level)
                assert d.file in files, (
                    f"diagnostic returned for non-input file {d.file}"
                )
                dmap[d.file].append(d)

            # this adds empty entries for all the files that didn't have any errors
            for f in files:
                dmap[f]

        dmap = dict(dmap)

        def error_count(item: Any):
            return len(item[1])

        return dict(sorted(dmap.items(), key=error_count))

    def check(self):
        """Run pyright on the input set, printing errors to stdout."""
        strictness_map = self._get_input_files()

        failed = False
        for level, files in strictness_map.items():
            if len(files) == 0:
                self.vprint(f"No files in {level}.")
                continue
            if level == Level.SKIP:
                self.vprint(f"Skipping {len(files)} files in SKIP level.")
                continue

            print(
                f"\nType-checking {len(files)} files using pyright files at level {level}"
            )
            config = self._generate_config(level)
            res = self._run(config, list(map(str, files)), pipe_stdout=False)
            # print(res.stdout)
            if res.returncode:
                failed = True

        if failed:
            print(f"{self.error} type-checking failed, see above for pyright output")
        else:
            print(f"Type checking passed!")

        return not failed

    def promotion_check(self):
        """Check if files can be promoted to stricter levels than their current assignment."""

        # Group files by their current level for efficient batch testing
        input_files: dict[Level, list[Path]] = self._get_input_files()
        file_count = sum(map(len, input_files.values()))

        print(
            f"Checking {file_count} files for potential promotion to stricter levels..."
        )

        promotable_files, _ = self._check_file_promotions(input_files)

        # Report promotable files
        if promotable_files:
            print(
                f"{self.error} Found {len(promotable_files)} files in the wrong level:\n"
                f"{self.info} List of promotions:"
            )
            for file_path, current_level, strictest_passing in sorted(promotable_files):
                rel_path = file_path.relative_to(self._tests_root)
                print(
                    f"  {rel_path}: {current_level.value} → {strictest_passing.value}"
                )
            print(
                f"{self.info} See https://github.com/redpanda-data/redpanda/blob/dev/tests/type-checking/README.md#failed-promotion-check\n"
            )
        else:
            print("No files can be promoted to stricter levels.")

        # Update strictness file if requested and there are promotable files
        if self._args.update and promotable_files:
            self._update_strictness_file(promotable_files)
            return True
        else:
            return not promotable_files

    def fruit(self):
        """Show files with fewest errors at each target level (low-hanging fruit for type checking improvements)."""

        # Group files by their current level for efficient batch testing
        input_files: dict[Level, list[Path]] = self._get_input_files()
        file_count = sum(map(len, input_files.values()))

        print(
            f"Finding low-hanging fruit among {file_count} files for type checking improvements..."
        )
        print()

        _, files_by_next_level = self._check_file_promotions(input_files)

        # Use the --top argument to control how many files to show
        top_n = self._args.top

        # For each target level, show the files with fewest errors
        for target_level in sorted(files_by_next_level.keys(), key=lambda x: x.value):
            file_diagnostics_list = files_by_next_level[target_level]

            if not file_diagnostics_list:
                continue

            # Sort by number of errors (ascending)
            sorted_files = sorted(file_diagnostics_list, key=lambda x: len(x[1]))

            print(
                f"=== {target_level.value.upper()} level (top {min(top_n, len(sorted_files))} files with fewest errors) ==="
            )

            for i, (file_path, diagnostics) in enumerate(sorted_files[:top_n]):
                rel_path = file_path.relative_to(self._tests_root)
                error_count = len(diagnostics)

                if error_count == 1:
                    error_text = "1 error"
                else:
                    error_text = f"{error_count} errors"

                print(f"{i + 1:2}. {rel_path} ({error_text})")

                # Show first few errors for very verbose mode
                if self.verbose >= 2 and diagnostics:
                    for diag in diagnostics[:3]:  # Show max 3 errors
                        print(f"      {diag.rule}: {diag.message}")
                    if len(diagnostics) > 3:
                        print(f"      ... and {len(diagnostics) - 3} more errors")

            total_files = len(sorted_files)
            if total_files > top_n:
                print(f"   ... and {total_files - top_n} more files")
            print()

        return True

    def _check_file_promotions(
        self, files_by_level: dict[Level, list[Path]]
    ) -> tuple[
        list[tuple[Path, Level, Level]],
        dict[Level, list[tuple[Path, list[Diagnostic]]]],
    ]:
        """Check which files can be promoted to stricter levels.

        Returns:
            Tuple of (promotable_files, files_by_next_level)
            - promotable_files: List of (file_path, current_level, target_level)
            - files_by_next_level: Map from next stricter level to list of (file_path, diagnostics)
        """
        next_level_map = {
            Level.SKIP: Level.OFF,
            Level.OFF: Level.BASIC,
            Level.BASIC: Level.STANDARD,
            Level.STANDARD: Level.STRICT,
            Level.STRICT: None,  # Already at the strictest level
        }

        promotable_files: list[tuple[Path, Level, Level]] = []
        files_by_next_level: dict[Level, list[tuple[Path, list[Diagnostic]]]] = (
            defaultdict(list)
        )

        # Test each group at their next strictest level
        for current_level, files in files_by_level.items():
            if (next_level := next_level_map[current_level]) is None:
                continue  # Already at strictest level, nothing to do

            self.vprint(
                f"Testing {len(files)} files at {current_level.value} level for promotion to {next_level.value}"
            )

            # Temporarily override force level to test at the next strictest level
            original_force_level = self._force_level
            self._force_level = next_level

            try:
                # Run diagnostics on files at the next level
                # Create a temporary strictness map with only these files at the target level
                temp_strictness_map: dict[Level, list[Path]] = {next_level: files}

                dmap = self._run_diagnostics(input_files=temp_strictness_map)

                # Check which files pass at the next level
                for file_path in files:
                    diagnostics = dmap.get(file_path, [])
                    if len(diagnostics) == 0:
                        # File passes at next level, can be promoted
                        promotable_files.append((file_path, current_level, next_level))
                    else:
                        # File fails at next level, record diagnostics for that level
                        files_by_next_level[next_level].append((file_path, diagnostics))

            finally:
                # Restore original force level
                self._force_level = original_force_level

        return promotable_files, dict(files_by_next_level)

    def ci(self):
        """The thing that runs in ci, works locally too"""

        # Run both promotion-check and check commands, failing if either fails
        # this gets its own command so that it can be careful with the output
        # and ensure the entire type checking run executes (to avoid repeated
        # pushes fixing one issue at a time).

        check = self._green("✓")

        print()
        print(self._blue("== Running type check =="))
        check_result = self.check()
        check_passed = check_result is not False
        if check_passed:
            print(f"{check} type check passed")
        else:
            print(f"{self._red('✗ check failed')}")
            print(f"{self.info} run this command locally to reproduce:")
            print(f"{SCRIPT_RELPATH} check")

        print()
        print(self._blue("== Running promotion-check =="))
        if promotion_check_passed := self.promotion_check():
            print(f"{check} promotion-check passed\n")
        else:
            print(f"{self._red('✗ promotion-check failed')}")
            print(f"{self.info} run this command locally to fix:")
            print(f"{SCRIPT_RELPATH} promotion-check --update")

        if promotion_check_passed and check_passed:
            print(self._green("✓ All CI checks passed"))
            return True
        else:
            print(self._red("✗ Some checks failed, see above for errors"))
            return False

    def _update_strictness_file(
        self, promotable_files: list[tuple[Path, Level, Level]]
    ):
        """Update the type-check-strictness.json file with promoted files."""
        strictness_config_path = (
            self._tests_root / "type-checking" / "type-check-strictness.json"
        )

        print(f"Updating {strictness_config_path}...")

        # Load current config
        with open(strictness_config_path, "r") as f:
            config: dict[str, list[str]] = json.load(f)

        # Remove files from their current levels
        for level_str, patterns in config.items():
            if level_str == "comment":
                continue
            level = Level(level_str)

            # Files being moved out of this level
            files_being_moved: list[str] = []
            for promoted_file_path, current_level, new_level in promotable_files:
                if current_level == level:
                    rel_path = str(promoted_file_path.relative_to(self._tests_root))
                    files_being_moved.append(rel_path)

            # Remove the patterns for files being moved
            if files_being_moved:
                updated_patterns: list[str] = []
                for pattern in patterns:
                    # Check if this pattern matches any of the files being moved
                    matching_files = list(self._tests_root.glob(pattern))
                    pattern_matches_moved_file = any(
                        str(f.relative_to(self._tests_root)) in files_being_moved
                        for f in matching_files
                    )

                    if not pattern_matches_moved_file:
                        updated_patterns.append(pattern)
                    else:
                        self.vprint(f"Removing pattern '{pattern}' from {level_str}")

                config[level_str] = sorted(updated_patterns)

                # Create sets of patterns to remove and add
        files_to_move: dict[Level, list[str]] = defaultdict(list)

        # Convert file paths to patterns relative to test root
        for file_path, current_level, new_level in promotable_files:
            rel_path = str(file_path.relative_to(self._tests_root))
            files_to_move[new_level].append(rel_path)
            self.vprint(
                f"Moving {rel_path} from {current_level.value} to {new_level.value}"
            )

        # Add files to their new levels
        for new_level, file_paths in files_to_move.items():
            if new_level == Level.STRICT:
                continue  # strict is implicit if the file is not listed
            level_str = new_level.value
            if level_str not in config:
                config[level_str] = []

            for file_path in file_paths:
                if file_path not in config[level_str]:
                    config[level_str].append(file_path)
                    self.vprint(f"Adding '{file_path}' to {level_str}")

        for level, files in config.items():
            if level == "comment":
                continue
            config[level] = sorted(files)

        # Write updated config back to file
        with open(strictness_config_path, "w") as f:
            json.dump(config, f, indent=4)
            print(file=f)  # trailing newline to make IDEs happy

        print(
            f"Updated {len(promotable_files)} file assignments in {strictness_config_path}"
        )
        print("Re-run the command to see if further promotions are possible.")

    def _get_input_files(self):
        """Return a map of strictness level to a list of files which are in that level."""
        input_files = self._input_files or "rptest/**/*.py"
        if (abs_input := Path(self._tests_root / input_files)).is_file():
            self.vprint(f"treating {input_files} as a file")
            files = [abs_input]
        else:
            self.vprint(f"treating {input_files} as a glob")
            files = list(self._tests_root.glob(input_files))
        if not files:
            raise RuntimeError(f"No files matched input glob: {input_files}")

        # If force level is specified, put all files at that level
        if self._force_level is not None:
            strictness_map = {self._force_level: files}
            self.vprint(
                f"Force level {self._force_level.value} specified, assigning all {len(files)} files to that level"
            )
            return strictness_map

        # Load strictness configuration
        strictness_config_path = (
            self._tests_root / "type-checking" / "type-check-strictness.json"
        )
        assert strictness_config_path.exists(), f"missing: {strictness_config_path}"

        with open(strictness_config_path, "r") as f:
            strictness_config = json.load(f)

        # Initialize result map with all strictness levels
        strictness_map: dict[Level, list[Path]] = defaultdict(list)

        # Track which files have been assigned to check for duplicates
        assigned_files: dict[Path, str] = {}

        # Process each strictness level, skipping comments and unknown levels
        for level_str, patterns in strictness_config.items():
            if level_str == "comment":
                continue
            level = Level(level_str)
            for pattern in patterns:
                matching_files = list(self._tests_root.glob(pattern))
                for file_path in matching_files:
                    if (
                        file_path in files
                    ):  # Only include files that match our input criteria
                        if file_path in assigned_files:
                            prev_level = assigned_files[file_path]
                            raise RuntimeError(
                                f"File {file_path} appears in multiple strictness levels: "
                                f"{prev_level} and {level_str}"
                            )
                        assigned_files[file_path] = level_str
                        strictness_map[level].append(file_path)
                        self.vprint(f"File {file_path} assigned to level {level_str}")

        # Files not explicitly assigned to any level default to strict
        unassigned_files = set(files) - set(assigned_files.keys())
        strictness_map[Level.STRICT] = list(unassigned_files)

        return dict(strictness_map)

    @property
    def verbose(self) -> int:
        return self._args.verbose

    def vprint(self, *args: Any, **kwargs: Any):
        if self.verbose > 0:
            print(*args, **kwargs)

    def vvprint(self, *args: Any, **kwargs: Any):
        if self.verbose > 1:
            print(*args, **kwargs)

    def _red(self, text: str) -> str:
        """Return the text wrapped in ANSI red and bold escape codes if color is enabled."""
        return self._colorize(text, "1;31")  # 1 = bold, 31 = red

    def _blue(self, text: str) -> str:
        """Return the text wrapped in ANSI blue escape codes if color is enabled."""
        return self._colorize(text, 34)

    def _green(self, text: str) -> str:
        """Return the text wrapped in ANSI green escape codes if color is enabled."""
        return self._colorize(text, 32)

    def _colorize(self, text: str, color_code: int | str) -> str:
        """Return the text wrapped in ANSI escape codes if color is enabled."""
        if self._use_color:
            return f"\033[{color_code}m{text}\033[0m"
        return text

    def _read_config(self):
        # read the config stripping out comments
        t = re.sub(r"\s*//.*", "", self._config.read_text())
        try:
            return json.loads(t)
        except json.JSONDecodeError as e:
            print(f"Failed to parse\n{self._config}:\n{t}\n: {e}")
            raise


CMDS = [
    "check",
    "promotion-check",
    "fruit",
    "ci",
]

COMMAND_DOC = """Commands:\nmissing-check: Find files and directories which are not included in the pyrightconfig.json
check: Run the type checker on the input set and print any errors to stdout
promotion-check: Check if files can be promoted to stricter levels than their current assignment
fruit: Show files with fewest errors at each target level (low-hanging fruit for type checking improvements)
ci: Run both promotion-check and check commands, failing if either fails"""


def main():
    p = argparse.ArgumentParser(
        description="Wrapper around pyright to type check our rptest python codebase",
        epilog=COMMAND_DOC,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    p.add_argument("command", type=str, help="Command to run", choices=CMDS)
    p.add_argument(
        "--tests-root",
        type=Path,
        help="Path to tests directory",
        default=Path(__file__).parent.parent,
    )
    p.add_argument(
        "--config",
        type=Path,
        help="Relative path to pyrightconfig.json (relative to tests-root)",
        default="pyrightconfig.json",
    )

    p.add_argument(
        "--pyright-path",
        type=Path,
        help="Path to (or bare name of) pyright",
        default="pyright",
    )

    p.add_argument("--input-files", help="Path (glob) to input file(s)", type=str)

    p.add_argument(
        "--force-level",
        type=str,
        choices=[level.value for level in Level],
        help="Force all files to be checked at this strictness level, ignoring type-check-strictness.json",
    )

    p.add_argument(
        "-v",
        "--verbose",
        help="Enable verbose output. Optional integer value for verbosity level 0-2 (default: 1 when flag used, 0 when not)",
        nargs="?",
        const=1,
        default=0,
        type=int,
    )

    p.add_argument(
        "--update",
        help="Update type-check-strictness.json in-place when running promotion-check",
        action="store_true",
    )

    p.add_argument(
        "--no-venv",
        help="Do not use the virtual environment when running pyright",
        action="store_true",
    )

    p.add_argument(
        "--color",
        choices=["never", "auto", "always"],
        default="auto",
        help="Control color output: 'never' disables color, 'always' forces color, 'auto' uses color when outputting to a terminal (default: auto)",
    )

    p.add_argument(
        "--top",
        type=int,
        default=10,
        help="Number of files to show per target level when using 'fruit' command (default: 10)",
    )

    args = p.parse_args()

    tc = TypeCheck(args)

    cmd: str = args.command.replace("-", "_")

    # run the command, map False to rc=1
    if getattr(tc, cmd)() is False:
        sys.exit(1)


if __name__ == "__main__":
    main()
