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

additional_body=""
orig_assignees=$ORIG_ASSIGNEES

if [[ -n $CREATE_ISSUE_ON_ERROR ]]; then
  additional_body="Note that this issue was created as a placeholder, since the original PR's commit(s) could not be automatically cherry-picked."

  additional_body="$additional_body
  Command I attempted to execute:
  gh pr create
    --title \"[$BACKPORT_BRANCH] $ORIG_TITLE\"
    --base \"$BACKPORT_BRANCH\"
    --label \"kind/backport\"
    --head \"$GIT_USER:$HEAD_BRANCH\"
    --draft
    --repo \"$TARGET_ORG/$TARGET_REPO\"
    --reviewer \"$ORIG_REVIEWERS\"
    --milestone \"$TARGET_MILESTONE\"
    --body \"Backport of PR $ORIG_ISSUE_URL \""

  orig_assignees=$(gh issue view $PR_NUMBER --json author --jq .author.login)
fi

backport_issue_url=$(gh_issue_url)
if [[ -z $backport_issue_url ]]; then
  gh issue create --title "[$BACKPORT_BRANCH] $ORIG_TITLE" \
    --label "kind/backport" \
    --repo "$TARGET_ORG/$TARGET_REPO" \
    --assignee "$orig_assignees" \
    --milestone "$TARGET_MILESTONE" \
    --body "Backport $ORIG_ISSUE_URL to branch $BACKPORT_BRANCH. $additional_body"
else
  msg="Backport issue already exists: $backport_issue_url"
  echo "BACKPORT_ERROR=$msg" >>"$GITHUB_ENV"
fi
