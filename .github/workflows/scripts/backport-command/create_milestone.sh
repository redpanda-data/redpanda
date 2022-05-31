#!/usr/bin/env bash

# type-branch job
# Discover and create milestone step

set -e

if [[ $TARGET_MILESTONE == "auto" ]]; then
  assignee_milestone="$BACKPORT_BRANCH-next"
else
  assignee_milestone=$TARGET_MILESTONE
fi
if [[ $(gh api "repos/$TARGET_ORG/$TARGET_REPO/milestones" --jq .[].title | grep "$assignee_milestone") == "" ]]; then
  # The below fails if something goes wrong
  gh api "repos/$TARGET_ORG/$TARGET_REPO/milestones" --silent --method POST -f title="$assignee_milestone"
  sleep 20 # wait for milestone creation to be propagated
fi
echo "::set-output name=milestone::$assignee_milestone"
