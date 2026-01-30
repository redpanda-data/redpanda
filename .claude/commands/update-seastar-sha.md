---
description: Update Seastar SHA to latest commit from v26.1.x branch
---

# Update Seastar SHA

Update the Seastar dependency in `bazel/repositories.bzl` to the latest commit from the `v26.1.x` branch of the Redpanda Seastar fork, then open a PR.

## Instructions

1. **Get the latest SHA from the v26.1.x branch**
   ```bash
   gh api repos/redpanda-data/seastar/commits/v26.1.x --jq '.sha'
   ```
   Store this as `NEW_SHA`.

2. **Read the current configuration**
   - Use the Read tool to read `bazel/repositories.bzl`
   - Find the current Seastar SHA in the `strip_prefix` line (format: `seastar-<SHA>`)
   - Store this as `OLD_SHA`

3. **Check if update is needed**
   - If `NEW_SHA` equals `OLD_SHA`, inform the user: "Seastar is already at the latest SHA: `<SHA>`"
   - Exit without changes

4. **Compute the sha256 hash of the new archive**
   ```bash
   curl -sL "https://github.com/redpanda-data/seastar/archive/<NEW_SHA>.tar.gz" | openssl dgst -sha256 | awk '{print $2}'
   ```
   Store this as `NEW_HASH`.

5. **Update `bazel/repositories.bzl`**
   Use the Edit tool to update the seastar `http_archive` block:
   - Add or update the comment line immediately before `http_archive(` to read: `# branch: v26.1.x`
   - Update `sha256 = "<NEW_HASH>"`
   - Update `strip_prefix = "seastar-<NEW_SHA>"`
   - Update `url = "https://github.com/redpanda-data/seastar/archive/<NEW_SHA>.tar.gz"`

6. **Run buildifier to format the file (this also updates MODULE.bazel.lock)**
   ```bash
   bazel run //tools:buildifier.fix
   ```

7. **Build to verify the change**
   ```bash
   bazel build //src/v/base
   ```
   - If the build fails, revert the changes and report the error to the user
   - Do not proceed with the PR if the build fails

8. **Get the list of changes since the old SHA**
   ```bash
   gh api "repos/redpanda-data/seastar/compare/<OLD_SHA>...<NEW_SHA>" --jq '.commits[].commit.message' | head -1 | while read line; do echo "- $line"; done
   ```
   Or use:
   ```bash
   gh api "repos/redpanda-data/seastar/compare/<OLD_SHA>...<NEW_SHA>" --jq '.commits[] | "- " + (.commit.message | split("\n")[0])'
   ```
   Store the list of oneline commit messages as `CHANGELOG`.

9. **Create a branch and commit**
   - Create branch: `seastar-update-<first 8 chars of NEW_SHA>`
   - Stage both `bazel/repositories.bzl` and `MODULE.bazel.lock`
   - Commit message should be multi-line:
     ```
     bazel: update seastar to <first 8 chars of NEW_SHA>

     Changes:
     <CHANGELOG>
     ```

10. **Open a PR**
    Use `gh pr create` with:
    - Title: `bazel: update seastar to <first 8 chars of NEW_SHA>`
    - Body should include:
      - Previous SHA (first 12 chars)
      - New SHA (first 12 chars)
      - Link to the commit comparison on GitHub

11. **Report success**
   - Print the old and new SHA
   - Print the PR URL
   - Print the GitHub comparison link: `https://github.com/redpanda-data/seastar/compare/<OLD_SHA>...<NEW_SHA>`

## Example

Running `/update-seastar-sha` when there's a new commit:

```
Updating Seastar SHA...
Old SHA: 804949ce1b42
New SHA: abc123def456
Build succeeded.
PR created: https://github.com/redpanda-data/redpanda/pull/12345
Compare changes: https://github.com/redpanda-data/seastar/compare/804949ce1b42...abc123def456
```
