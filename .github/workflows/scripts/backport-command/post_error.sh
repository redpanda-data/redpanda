#!/usr/bin/env bash

# post error message to issue or pr

set -e

default_error="Oops! Something went wrong."

msg="${BACKPORT_ERROR-$default_error}

[Workflow run logs]($GITHUB_SERVER_URL/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID)."

case $COMMENTED_ON in
  issue)
    link=$ORIG_ISSUE_URL
    ;;
  pr)
    link=$PR_NUMBER
    ;;
esac

gh "$COMMENTED_ON" comment "$link" \
  --repo "$TARGET_FULL_REPO" \
  --body "$msg"
