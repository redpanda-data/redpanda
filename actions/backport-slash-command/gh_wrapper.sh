#!/usr/bin/env bash

# Wrapper functions for gh and git to make them reusable.

# Many functions take optional parameters. When these are not provided,
# the script attempts to get necessary details from env variables. Most of the convenience
# of these functions is the default env variables it uses.
# If the env variable is also not set, `set -u` should throw an unbound variable error.
set -u

backport_failure() {
  echo "$1" >&2
  exit 1
}

gh_issue_url() {
  while getopts :s:m:r: opt; do
    case $opt in
      s)
        search=$OPTARG
        ;;
      m)
        milestone=$OPTARG
        ;;
      r)
        repo=$OPTARG
        ;;
      \?)
        backport_failure "Invalid option: -$OPTARG"
        ;;
      :)
        backport_failure "-$OPTARG option requires an argument"
        ;;
    esac
  done
  shift $((OPTIND - 1))

  search=${search-"[$BACKPORT_BRANCH] $ORIG_TITLE in:title"}
  milestone=${milestone-$TARGET_MILESTONE}
  repo=${repo-$TARGET_ORG/$TARGET_REPO}

  gh issue list \
    --state open \
    --search "$search" \
    --json url \
    --milestone "$milestone" \
    --repo "$repo" \
    --jq '.[0].url'
}

# Do nothing if being imported in another script via `source`.
if [[ ${BASH_SOURCE[0]} != "${0}" ]]; then
  :

# Allows functions to be called directly from github actions yaml.
elif declare -f "$1" >/dev/null; then
  "$@"

else
  backport_failure "No function named '$1'"
fi
