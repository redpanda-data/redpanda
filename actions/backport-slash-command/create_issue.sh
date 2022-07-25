#!/usr/bin/env bash

# type-branch job
# Create issue step

set -e

# shellcheck disable=SC1091
source "$SCRIPT_DIR/gh_wrapper.sh"

if [[ -n $(echo "$ORIG_LABELS" | jq '.[] | select(.name == "kind/backport")') ]]; then
  msg="The issue is already a backport."
  echo "BACKPORT_ERROR=$msg" >>"$GITHUB_ENV"
  backport_failure "$msg"
fi

backport_issue_url=$(gh_issue_url)
if [[ -z $backport_issue_url ]]; then
  gh issue create --title "[$BACKPORT_BRANCH] $ORIG_TITLE" \
    --label "kind/backport" \
    --repo "$TARGET_ORG/$TARGET_REPO" \
    --assignee "$ORIG_ASSIGNEES" \
    --milestone "$TARGET_MILESTONE" \
    --body "Backport $ORIG_ISSUE_URL to branch $BACKPORT_BRANCH"
else
  msg="Backport issue already exists: $backport_issue_url"
  echo "BACKPORT_ERROR=$msg" >>"$GITHUB_ENV"
fi
