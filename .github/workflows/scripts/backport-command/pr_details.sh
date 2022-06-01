#!/usr/bin/env bash

# type-branch job
# Backport commits and get details step

set -e

# shellcheck disable=SC1091
source "$SCRIPT_DIR/gh_wrapper.sh"

cd "$GITHUB_WORKSPACE/fork"

if [[ $IS_MERGED != true ]]; then
  msg="The pull request is not merged yet. Cancelling backport..."
  echo "BACKPORT_ERROR=$msg" >>"$GITHUB_ENV"
  backport_failure "$msg"

elif [[ $PR_BASE_BRANCH != "$REPO_DEFAULT_BRANCH" ]]; then
  msg="The pull request's base branch is not the default one. Cancelling backport..."
  echo "BACKPORT_ERROR=$msg" >>"$GITHUB_ENV"
  backport_failure "$msg"
fi

fixing_issue_urls=$(gh api graphql -f query='{
  resource(url: "https://github.com/'"$TARGET_FULL_REPO"'/pull/'"$PR_NUMBER"'") {
    ... on PullRequest {
      closingIssuesReferences(first: 20) {
        nodes {
          url
        }
      }
    }
  }
}' --jq .data.resource.closingIssuesReferences.nodes.[].url)

suffix=$((RANDOM % 1000))
git config --global user.email "$GIT_EMAIL"
git config --global user.name "$GIT_USER"
git remote add upstream "https://github.com/$TARGET_FULL_REPO.git"
git fetch --all
git remote set-url origin "https://$GIT_USER:$GITHUB_TOKEN@github.com/$GIT_USER/$TARGET_REPO.git"

backport_issues_numbers=""
for issue_url in $fixing_issue_urls; do
  backport_issues_numbers+=$(echo "$issue_url" | awk -F/ '{print $NF"-"}')
done
if [[ $backport_issues_numbers == "" ]]; then
  backport_issues_numbers="fixes-to-"
fi
head_branch=$(echo "backport-$backport_issues_numbers$BACKPORT_BRANCH-$suffix" | sed 's/ /-/g')
git checkout -b "$head_branch" "remotes/upstream/$BACKPORT_BRANCH"

if ! git cherry-pick -x $BACKPORT_COMMITS; then
  msg="Failed to run cherry-pick command. I executed the below command:\n
\`\`\`\r
git cherry-pick -x $BACKPORT_COMMITS
\`\`\`"

  # Multiline workaround for GitHub Actions.
  {
    echo 'BACKPORT_ERROR<<EOF'
    echo -e "$msg"
    echo 'EOF'
  } >>"$GITHUB_ENV"

  backport_failure "$msg"
fi

git push --set-upstream origin "$head_branch"
git remote rm upstream
echo "::set-output name=head_branch::$head_branch"
echo "::set-output name=fixing_issue_urls::$fixing_issue_urls"
