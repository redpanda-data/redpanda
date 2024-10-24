#!/usr/bin/env bash

# type-branch job
# Extract Required Backports from PR Body step

# shellcheck disable=SC1091
source "$SCRIPT_DIR/gh_wrapper.sh"

required_backport_branches=$(gh issue view $PR_NUMBER | grep -i "\[x\]" | sed 's/- \[x\] //i' | grep "^v")

for branch in $(echo $required_backport_branches); do
  gh pr comment $PR_NUMBER -b "/backport $branch"
done
