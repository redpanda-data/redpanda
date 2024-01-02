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
if [[ -n $CREATE_ISSUE_ON_ERROR ]]; then
  additional_body="Note that this issue was created as a placeholder, since the original PR's commit(s) could not be automatically cherry-picked."

  local_user=$(gh api user --jq .login)
  local_branch="$local_user/backport-$PR_NUMBER-$BACKPORT_BRANCH-$((RANDOM % 1000))"

  additional_body="$additional_body
  Here are the commands to execute:
  \`\`\`
  git checkout $BACKPORT_BRANCH
  git checkout -b $local_branch
  git cherry-pick -x $BACKPORT_COMMITS

  git push origin $local_branch
  gh pr create \\
    --title \"[$BACKPORT_BRANCH] $ORIG_TITLE\" \\
    --base \"$BACKPORT_BRANCH\" \\
    --label \"kind/backport\" \\
    --head \"$local_branch\" \\
    --draft \\
    --repo \"$TARGET_ORG/$TARGET_REPO\" \\
    --milestone \"$TARGET_MILESTONE\" \\
    --body \"Backport of PR $ORIG_ISSUE_URL \""
fi

backport_issue_url=$(gh_issue_url)
if [[ -z $backport_issue_url ]]; then
  gh issue create --title "[$BACKPORT_BRANCH] $ORIG_TITLE" \
    --label "kind/backport" \
    --repo "$TARGET_ORG/$TARGET_REPO" \
    --assignee "$ASSIGNEES" \
    --milestone "$TARGET_MILESTONE" \
    --body "Backport $ORIG_ISSUE_URL to branch $BACKPORT_BRANCH. $additional_body"
else
  msg="Backport issue already exists: $backport_issue_url"
  echo "BACKPORT_ERROR=$msg" >>"$GITHUB_ENV"
fi
