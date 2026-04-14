# /// script
# dependencies = ["anthropic==0.50.0"]
# ///

import fnmatch
import os
import subprocess
import sys
import time

import anthropic

GENERATED_PATTERNS = [
    "*.pb.go", "*.pb.h", "*.pb.cc", "*.pb.rs", "*_pb2.py",
    "MODULE.bazel", "*.lock", "go.sum", "package-lock.json", "Cargo.lock",
]

MODEL = "claude-opus-4-6"
MAX_TOKENS = 2048
UNCERTAIN_RESPONSE = "UNCERTAIN"
CONFLICT_MARKERS = ["<<<<<<<", "=======", ">>>>>>>"]

SYSTEM_PROMPT = (
    "You are a git conflict resolver. Return the resolved file with all conflict "
    f"markers removed. If you cannot resolve it confidently, respond with: {UNCERTAIN_RESPONSE}"
)

BACKPORT_COMMITS = os.environ["BACKPORT_COMMITS"]
BACKPORT_BRANCH = os.environ["BACKPORT_BRANCH"]

client = anthropic.Anthropic()


def matches_any(path: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(os.path.basename(path), p) for p in patterns)


def call_with_retry(path: str, diff: str, file_content: str):
    prompt = (
        f"Target branch: {BACKPORT_BRANCH}\nFile: {path}\n\n"
        f"--- commit diff (file-specific) ---\n{diff}\n"
        f"--- conflicted file ---\n{file_content}"
    )
    for attempt in range(2):
        try:
            return client.messages.create(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                timeout=30,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
        except anthropic.RateLimitError:
            if attempt == 0:
                print(f"{path}: rate limited, retrying in 10s...")
                time.sleep(10)
                continue
            raise


def difficulty_rating(resolved_count: int, total_diff_lines: int) -> str:
    if resolved_count >= 3 or total_diff_lines > 150:
        return "hairy"
    if resolved_count == 1 and total_diff_lines <= 50:
        return "easy"
    return "medium"


conflicted = subprocess.check_output(
    ["git", "diff", "--name-only", "--diff-filter=U"]
).decode().splitlines()

eligible = [f for f in conflicted if not matches_any(f, GENERATED_PATTERNS)]
skipped = [f for f in conflicted if matches_any(f, GENERATED_PATTERNS)]

if skipped:
    print(f"Skipping generated files: {skipped}")
if not eligible:
    print("No eligible files. Falling back.")
    sys.exit(1)

resolved = []
total_diff_lines = 0

for path in eligible:
    diff = subprocess.check_output(
        ["git", "show", "-U0"] + BACKPORT_COMMITS.split() + ["--", path]
    ).decode(errors="replace")

    diff_lines = diff.splitlines()
    if len(diff_lines) > 200:
        print(f"{path}: diff too large ({len(diff_lines)} lines). Skipping.")
        continue

    file_content = open(path, encoding="utf-8", errors="replace").read()

    try:
        response = call_with_retry(path, diff, file_content)
    except Exception as e:
        print(f"{path}: API error ({type(e).__name__}: {e}). Skipping.")
        continue

    if response.stop_reason != "end_turn":
        print(f"{path}: response truncated (stop_reason={response.stop_reason}). Skipping.")
        continue

    text = response.content[0].text.strip()

    if text.upper() == UNCERTAIN_RESPONSE:
        print(f"{path}: model uncertain. Skipping.")
        continue

    if any(m in text for m in CONFLICT_MARKERS):
        print(f"{path}: model output still contains conflict markers. Skipping.")
        continue

    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    subprocess.run(["git", "add", "--", path], check=True)
    resolved.append(path)
    total_diff_lines += len(diff_lines)
    print(f"{path}: resolved and staged.")

if not resolved:
    print("No files were resolved. Falling back.")
    sys.exit(1)

with open(os.environ["RESOLVED_FILES_OUT"], "w") as f:
    f.write("\n".join(resolved))

rating = difficulty_rating(len(resolved), total_diff_lines)
with open(os.environ["DIFFICULTY_OUT"], "w") as f:
    f.write(rating)

print(f"Difficulty: {rating}")
sys.exit(0)
