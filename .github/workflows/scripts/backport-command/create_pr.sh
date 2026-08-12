#!/usr/bin/env bash

# type-branch job
# Create pull request step

set -e

# shellcheck disable=SC1091
source "$SCRIPT_DIR/gh_wrapper.sh"

cd "$GITHUB_WORKSPACE/target-repo" || exit 1

backport_issue_urls=""
# shellcheck disable=SC2153
if [[ $FIXING_ISSUE_URLS != "" ]]; then
  for FIXING_ISSUE_URL in $FIXING_ISSUE_URLS; do
    orig_issue_number=$(echo "$FIXING_ISSUE_URL" | awk -F/ '{print $NF}')
    orig_org=$(echo "$FIXING_ISSUE_URL" | awk -F/ '{print $4}')
    orig_repo=$(echo "$FIXING_ISSUE_URL" | awk -F/ '{print $5}')

    if [[ $orig_repo != "$TARGET_REPO" || $orig_org != "$TARGET_ORG" ]]; then
      break
    fi

    orig_issue_title=$(gh issue view "$orig_issue_number" \
      --repo "$orig_org/$orig_repo" \
      --json title --jq .title)

    backport_issue_url=$(gh_issue_url -r "$orig_org/$orig_repo" \
      -s "[$BACKPORT_BRANCH] $orig_issue_title in:title")

    # backport issue does not exist and will be created
    if [[ $backport_issue_url == "" ]]; then
      # get orig issue assignees
      orig_issue_assignees=$(gh issue view "$orig_issue_number" \
        --repo "$orig_org/$orig_repo" \
        --json assignees --jq .assignees.[].login | paste -s -d ',' -)

      # create issue
      backport_issue_url=$(gh issue create --title "[$BACKPORT_BRANCH] $orig_issue_title" \
        --label "kind/backport" \
        --repo "$orig_org/$orig_repo" \
        --assignee "$orig_issue_assignees" \
        --milestone "$TARGET_MILESTONE" \
        --body "Backport $FIXING_ISSUE_URL to branch $BACKPORT_BRANCH. Requested by PR $ORIG_PR_URL")
    fi
    backport_issue_urls+="Fixes: $backport_issue_url, "
  done
  # shellcheck disable=SC2001
  backport_issue_urls=$(echo "$backport_issue_urls" | sed 's/.$//')
fi

# If the AI path produced a skill report, use it as the PR body but still
# append the Fixes: lines so the backport PR auto-closes the backport issues
# this script just created. Otherwise fall back to the plain one-line summary.
if [[ -n ${AI_REPORT_FILE:-} && -s $AI_REPORT_FILE ]]; then
  combined_body=$(mktemp)
  cat "$AI_REPORT_FILE" >"$combined_body"
  if [[ -n $backport_issue_urls ]]; then
    printf '\n\n%s\n' "$backport_issue_urls" >>"$combined_body"
  fi
  body_args=(--body-file "$combined_body")
else
  body_args=(--body "Backport of PR $ORIG_ISSUE_URL
$backport_issue_urls")
fi

gh pr create --title "[$BACKPORT_BRANCH] $ORIG_TITLE" \
  --base "$BACKPORT_BRANCH" \
  --label "kind/backport" \
  --head "$HEAD_BRANCH" \
  --repo "$TARGET_ORG/$TARGET_REPO" \
  --reviewer "$AUTHOR" \
  --milestone "$TARGET_MILESTONE" \
  "${body_args[@]}"
