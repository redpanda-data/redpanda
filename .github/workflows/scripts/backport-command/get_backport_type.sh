#!/usr/bin/env bash

# backport-type job
# "Get type of backport (issue or pr)" step

set -e

# shellcheck disable=SC1091
source "$SCRIPT_DIR/gh_wrapper.sh"

gh_branch_exists() {
  [[ -n $(gh api "/repos/$TARGET_FULL_REPO/branches" --paginate --jq \
    '.[] | select(.name == '\""$1"\"')') ]] && return 0
  return 1
}

if [[ $(echo "$CLIENT_PAYLOAD" | jq 'has("pull_request")') == true ]]; then
  commented_on="pr"
else
  commented_on="issue"
fi
echo "::set-output name=commented_on::$commented_on"

if ! gh_branch_exists "$ARG1"; then
  new_arg=$ARG1
  if [[ ${new_arg:0:1} != "v" ]]; then new_arg="v$ARG1"; fi
  major=$(echo "$new_arg" | grep -Eo '^v[0-9]{2}\.[0-9]{1,2}\.')

  if gh_branch_exists "$new_arg"; then
    backport_branch=$new_arg
  elif [[ $MILESTONE_ARG == "auto" || $MILESTONE_ARG == "$ARG1" ]] && gh_branch_exists "${major}x"; then
    echo "Milestone given instead of branch. Using \"${major}x\" branch."
    backport_branch="${major}x"
    target_milestone="$new_arg"
  else
    msg="Branch name \"$ARG1\" not found."

    echo "BACKPORT_ERROR=$msg" >>"$GITHUB_ENV"
    backport_failure "$msg"
  fi
fi

echo "::set-output name=backport_branch::${backport_branch-$ARG1}"
echo "::set-output name=target_milestone::${target_milestone-$MILESTONE_ARG}"
