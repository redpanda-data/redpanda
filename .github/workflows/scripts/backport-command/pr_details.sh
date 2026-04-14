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
}' --jq '.data.resource.closingIssuesReferences.nodes | map(.url) | join(" ")')

suffix=$((RANDOM % 1000))
git config --global user.email "$GIT_EMAIL"
git config --global user.name "$GIT_USER"
git remote add upstream "https://github.com/$TARGET_FULL_REPO.git"
git fetch --all
git remote set-url origin "https://$GIT_USER:$GITHUB_TOKEN@github.com/$GIT_USER/$TARGET_REPO.git"

head_branch=$(echo "backport-pr-$PR_NUMBER-$BACKPORT_BRANCH-$suffix" | sed 's/ /-/g')
git checkout -b "$head_branch" "remotes/upstream/$BACKPORT_BRANCH"

if ! git cherry-pick -x $BACKPORT_COMMITS; then
  echo "Cherry-pick failed. Attempting AI conflict resolution..."
  RESOLVED_OUT=$(mktemp)
  DIFFICULTY_OUT=$(mktemp)
  DIFFICULTY_COMMENT_OUT=$(mktemp)
  trap 'rm -f "$RESOLVED_OUT" "$DIFFICULTY_OUT" "$DIFFICULTY_COMMENT_OUT"' EXIT
  export RESOLVED_FILES_OUT="$RESOLVED_OUT"
  export DIFFICULTY_OUT
  export DIFFICULTY_COMMENT_OUT
  if uv run "$SCRIPT_DIR/ai_resolve.py"; then
    ai_resolved_files=$(cat "$RESOLVED_OUT")
    ai_difficulty=$(cat "$DIFFICULTY_OUT")
    ai_difficulty_comment=$(cat "$DIFFICULTY_COMMENT_OUT")
    if ! git cherry-pick --continue --no-edit; then
      git cherry-pick --abort 2>/dev/null || true
      msg="AI resolution staged changes but cherry-pick --continue failed (unresolved conflicts remain). Manual backport required."
      {
        echo 'BACKPORT_ERROR<<EOF'
        echo -e "$msg"
        echo 'EOF'
      } >>"$GITHUB_ENV"
      backport_failure "$msg"
    fi
  else
    git cherry-pick --abort 2>/dev/null || true
    msg="Cherry-pick failed and AI resolution could not resolve conflicts automatically. Manual backport required."
    {
      echo 'BACKPORT_ERROR<<EOF'
      echo -e "$msg"
      echo 'EOF'
    } >>"$GITHUB_ENV"
    backport_failure "$msg"
  fi
fi

git push --set-upstream origin "$head_branch"
git remote rm upstream
echo "head_branch=$head_branch" >>$GITHUB_OUTPUT
echo "fixing_issue_urls=$fixing_issue_urls" >>$GITHUB_OUTPUT
{
  echo 'ai_resolved_files<<EOF'
  echo "${ai_resolved_files:-}"
  echo 'EOF'
} >>"$GITHUB_OUTPUT"
echo "ai_difficulty=${ai_difficulty:-}" >>"$GITHUB_OUTPUT"
{
  echo 'ai_difficulty_comment<<EOF'
  echo "${ai_difficulty_comment:-}"
  echo 'EOF'
} >>"$GITHUB_OUTPUT"
