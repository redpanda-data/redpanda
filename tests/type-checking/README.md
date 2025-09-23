# Ducktape Type Checking

This directory contains the infrastructure for running PEP-484 type checking on Redpanda's Python test suite (ducktape) using [pyright](https://github.com/microsoft/pyright).

## Overview

The type checking system provides:
- **Gradual type checking adoption** with per-file strictness levels
- **Automated promotion detection** to identify files ready for stricter checking
- **CI integration** to prevent type regressions
- **Docker-based execution** for consistent environments

## Files

### `type-check.py`

The main type checking orchestration script. Provides multiple commands:

- **`check`** - Run type checking on files and report errors
- **`promotion-check`** - Identify files that can be promoted to stricter type checking levels
- **`fruit`** - Show files with fewest errors at each target level (low-hanging fruit for type checking improvements)
- **`ci`** - Run both `check` and `promotion-check` and decorates the results (used in CI)

### `type-check-strictness.json`

Configuration file that maps Python files to their current type checking strictness level. Files not listed default to `strict` level.

**Strictness levels** (from most to least strict):
- `strict` - Full type checking (default for unlisted files)
- `standard` - Standard type checking
- `basic` - Basic type checking
- `off` - Minimal checking (syntax, imports, etc.)
- `skip` - No type checking at all

### `Dockerfile`
Multi-stage Docker image for running type checking in a consistent environment. Installs Python dependencies and pyright.

### `type-check.sh`
Shell script that builds the Docker image and runs type checking inside a container.

### `requirements.txt`
Python dependencies for type checking (primarily pyright).

## Usage

### Local Development

Locally you should be able to run the .py script directly if you have run [local_venv.sh](./local_venv.sh). Alternately, in any of the commands below you can replace `type-check.py` with `type-check.sh` to use docker to run the checker, which does not require any local setup.

```bash
# Run type checking on all files
type-checking/type-check.py check

# Check for files ready to be promoted to stricter levels
type-checking/type-check.py promotion-check

# Find low-hanging fruit - files with fewest errors at each target level
type-checking/type-check.py fruit

# Show top 25 fruit candidates
type-checking/type-check.py fruit --top 25

# Show detailed error information with verbose output
type-checking/type-check.py fruit --verbose 2

# Automatically promote eligible files
type-checking/type-check.py promotion-check --update

# Run CI checks (both check and promotion-check)
type-checking/type-check.py ci

# Check specific files
type-checking/type-check.py check --input-files "rptest/tests/my_test.py"

# Force all files to be checked at a specific level
type-checking/type-check.py check --force-level strict
```

## Strictness Level Management

In order to have type checking useful immediately, without needing to fix 1000s of existing type errors, the system uses a progressive approach to type checking adoption:

1. **New files** default to `strict` type checking
2. **Existing files** are assigned to the strictest level they currently pass
3. **Promotion detection** automatically identifies when files can move to stricter levels
4. **Automated updates** can promote files with `--update` flag

### Adding Type Hints to Files

When adding type hints to improve a file:

1. Run `type-checking/type-check.py promotion-check --update`
2. This will automatically move the file to the strictest level it now passes
3. Commit both your type hint changes and the updated `type-check-strictness.json`

### Finding Low-Hanging Fruit

The `fruit` command helps identify files that are closest to being promotable to stricter type checking levels by showing files with the fewest errors at each target level:

```bash
# Show top 10 files with fewest errors per target level
type-checking/type-check.py fruit

# Show top 20 files with fewest errors per target level
type-checking/type-check.py fruit --top 20

# Show detailed error information with verbose output
type-checking/type-check.py fruit --verbose 2
```

This is useful for:
- **Prioritizing type checking improvements** - start with files that need minimal fixes
- **Setting realistic goals** - see how many errors need to be fixed for each promotion
- **Understanding error distribution** - identify patterns in type checking issues

The command groups files by their potential promotion target (off→basic, basic→standard, etc.) and sorts them by error count, making it easy to find files that are closest to passing at the next strictness level.

### Failed Promotion Check

You may be here because CI failed with a "promotion check" error. This means that the strictness level for some files are set too low, i.e., your change has improved the state of the Python code base and as a reward you can promote some files to higher strictness levels. Pat yourself on the back, then use the following command to automatically update the strictness file:

```bash
# Automatically update the configuration
type-checking/type-check.py promotion-check --update

# Commit the updated type-check-strictness.json
git add type-checking/type-check-strictness.json
git commit -m "chore: update type checking strictness"
```

## Command Line Options

- `--tests-root PATH` - Path to tests directory (auto-detected by default)
- `--config PATH` - Path to pyrightconfig.json
- `--input-files GLOB` - Glob pattern for files to check (default: `rptest/**/*.py`)
- `--force-level LEVEL` - Override strictness levels and check all files at specified level
- `--no-venv` - Don't use virtual environment when running pyright
- `--verbose [LEVEL]` - Enable verbose output (0=off, 1=verbose, 2=very verbose)
- `--update` - Update type-check-strictness.json in-place (for promotion-check)
- `--top N` - Number of files to show per target level in fruit command (default: 10)

## CI Integration

The type checking runs automatically on pull requests via GitHub Actions (`.github/workflows/python-type-check.yml`). The CI:

1. Builds the Docker image from `Dockerfile`
2. Runs `type-check.py ci` which executes both type checking and promotion detection
3. Fails if there are type errors or files that should be promoted

This check tries to be as fast as possible, and usually runs in about 3 minutes if the docker image build is fully cached (usually the case), or a few minutes more if not.

## Architecture

The system is designed around Microsoft's pyright type checker with these key features:

- **Segmented checking**: Files are grouped by strictness level and checked separately
- **Temporary configs**: Generates temporary pyrightconfig.json files for each strictness level
- **Batch processing**: Efficiently checks multiple files at once when possible
- **Virtual environment integration**: Uses `.venv` if available for consistent Python environment

## Adding New Features

The `type-check.py` script is modular and can be extended with new commands by:

1. Adding the command name to the `CMDS` list
2. Adding documentation to `COMMAND_DOC`
3. Implementing a method on the `TypeCheck` class with the command name (with hyphens converted to underscores)

The script follows the pattern where each command method returns `True` for success, `False` for failure, or `None` for success (which is treated as `True`).
