#!/usr/bin/env python3

import subprocess
import sys

git_dir = sys.argv[1]
template = sys.argv[2]
output = sys.argv[3]

revision = subprocess.check_output(
    ["git", "-C", git_dir, "rev-parse", "--verify", "HEAD"],
    text=True).strip()

version = subprocess.check_output(
    ["git", "-C", git_dir, "describe", "--dirty", "--always"],
    text=True).strip()

dirty = "-dirty" if "dirty" in version else ""

with open(template, "r") as tmpl:
    content = tmpl.read()

content = content.replace("@GIT_VER@", version)
content = content.replace("@GIT_SHA1@", revision)
content = content.replace("@GIT_CLEAN_DIRTY@", dirty)

try:
    with open(output, "r") as out:
        prev_content = out.read()
except FileNotFoundError:
    prev_content = None

# avoid unnecessary rebuild
if content == prev_content:
    sys.exit()

with open(output, "w") as out:
    out.write(content)
