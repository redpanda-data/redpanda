---
name: lint-pr
description: >
  Lint pull request commits against project standards. Checks commit
  message formatting and commit diffs for issues. Use with no arguments
  to lint the current branch against upstream/dev, or pass a GitHub PR
  number to lint a remote PR.
disable-model-invocation: true
argument-hint: "[pr-number]"
allowed-tools: Bash(git *) Bash(gh *)
---

# Lint PR

Check that pull request commits meet project standards.

## Step 1: Gather commits

Determine which commits to lint based on how the command was invoked.

**If `$ARGUMENTS` is empty or blank** (local branch mode):

Run:

```
git log --format='%H %s' upstream/dev..HEAD
```

This produces the list of commits on the current branch that are not yet
in upstream/dev. If the output is empty, tell the user there are no
commits to lint and stop.

**If `$ARGUMENTS` is a PR number** (remote PR mode):

Run:

```
gh api repos/{owner}/{repo}/pulls/$ARGUMENTS/commits --paginate --jq '.[] | .sha + " " + (.commit.message | split("\n") | .[0])'
```

The `{owner}/{repo}` should be derived from the `upstream` remote URL.
If there is no `upstream` remote, fall back to `origin`. If the `gh`
command fails, report the error and stop.

## Step 2: Lint each commit

For every commit collected in step 1, run the checks described below.
Track each warning so you can present a full report at the end.

### Gather per-commit data

For each commit, obtain the full commit message and the diffstat.

**Commit message:**

- Local branch mode: `git log -1 --format='%B' <sha>`
- Remote PR mode: `gh api repos/{owner}/{repo}/pulls/$ARGUMENTS/commits --paginate --jq '.[] | select(.sha == "<sha>") | .commit.message'`

**Diffstat:**

- Local branch mode: `git show --stat --format='' <sha>`
- Remote PR mode: `gh api repos/{owner}/{repo}/commits/<sha> --jq '.stats.total as $t | .files | map(.filename + " | " + (.changes | tostring) + " (+\(.additions)/-\(.deletions))") | .[] ' ` and `gh api repos/{owner}/{repo}/commits/<sha> --jq '.stats | "\(.total) lines changed (+\(.additions)/-\(.deletions))"'`

The diffstat is used by the M5 and M7 checks. The M7 check (commit
atomicity) always requires the full diff — fetch it for every commit:

- Local branch mode: `git show --format='' <sha>`
- Remote PR mode: `gh api repos/{owner}/{repo}/commits/<sha> --jq '.files[] | "\(.filename) \(.status)\n\(.patch)"'`

### Commit message checks

Apply the following checks to each commit message:

#### M1: Subject line length

The first line of the commit message (the subject) must be at most 72
characters, with an exception for long identifiers.

A "long identifier" is either:
- A verbatim string enclosed in single backticks (e.g. `` `some_long_name` ``)
- A CamelCase word (a single word with no spaces containing at least two
  capital letters, e.g. `RaftGroupReconfigurationTable`)

If the subject exceeds 72 characters, check whether it contains a long
identifier. If it does, allow up to 90 characters. If it does not, or if
it exceeds 90 characters even with a long identifier, it is flagged.

- **Pass** if `length <= 72`
- **Pass** if `length > 72 AND length <= 90 AND subject contains a long identifier`
- **Warn** otherwise, reporting the actual length and the applicable limit

#### M2: Body line wrapping

The commit message body (everything after the first blank line following
the subject) is optional. If present, every line in the body must be at
most 72 characters, with the following exceptions:

**Fenced code blocks:** Lines between a pair of `` ``` `` markers
(triple backtick opening and closing lines) are exempt. The backtick
markers themselves are also exempt.

**Indented code blocks:** Lines that begin with 3 or more leading spaces
(common indent-style verbatim blocks) are exempt.

**URLs and long tokens:** Lines whose only content exceeding 72
characters is a URL (http://, https://) or a single unbreakable token
(no spaces) are exempt.

For lines that are not exempt:
- **Pass** if `length <= 72`
- **Warn** if `length > 72`, reporting the line number within the
  commit message, the actual length, and a snippet of the offending line

#### M3: Blank line after subject

If the commit message has a body, there must be exactly one blank line
separating the subject from the body.

- **Pass** if there is no body, or if line 2 is blank
- **Warn** if line 2 is non-blank and the message has more than one line

#### M4: Subject line format

The subject line must follow the project convention:

```
area[/detail]: short description
```

Check the following properties:

**Has area prefix:** The subject must contain a `: ` (colon-space)
separator. Everything before the first `: ` is the area prefix, and
everything after it is the description.

- **Warn** if there is no `: ` in the subject

**Area syntax:** The area prefix (before `: `) must match one of these
forms:

- Plain area: one or more segments separated by `/`, where each segment
  consists of letters, digits, underscores, or hyphens
  (e.g. `cluster`, `build/version`, `c/tests/rm_stm`)
- Backtick-quoted area: same as above but wrapped in backticks
  (e.g. `` `kafka` ``, `` `ct/l1` ``)

- **Warn** if the area prefix does not match either form

**Description starts lowercase:** The first character of the description
(after `: `) must be a lowercase letter.

- **Warn** if the first character is uppercase
- Exempt if the first character is a backtick (`` ` ``), indicating a
  verbatim identifier

**No trailing period:** The subject must not end with a period.

- **Warn** if the last character is `.`

#### M5: Commit message adequately explains the change

The commit message (subject + optional body) should give a reviewer or
future developer enough context to understand what the change does and
why. This check evaluates whether the message is adequate relative to
the complexity of the diff.

This is the general "is the commit message sufficient?" check. M6
provides an additional specialized lens for bug fix commits specifically.

**This is a judgment call.** Err heavily on the side of PASS. Only warn
when the gap between the complexity of the change and the information in
the commit message is obvious. When in doubt, pass.

To make this judgment, examine the subject line, body (if any), and the
diffstat (total lines changed and which files are touched). If you need
more context, look at the full diff, but the diffstat is usually enough.

**Always PASS (do not even examine the diff) when the subject indicates
a routine or mechanical change.** Examples include but are not limited
to:

- Formatting/style: "apply clang-format", "fix formatting", "fix
  whitespace", "reformat ..."
- Typos and comments: "fix typo", "fix spelling", "update comments",
  "clarify comment", "add doc comment"
- Dependency bumps: "bump dependencies", "bump X to Y", "update X
  version"
- Automated transforms: "run go fix", "run gofmt", "apply codemod"
- Renames: "rename X to Y", "s/old/new"
- Config and boilerplate: "add config option", "add BUILD target",
  "update .gitignore", "update CODEOWNERS"
- CI changes: "update CI workflow", "fix CI", "add CI step"

The above list is not exhaustive. Generalize: if the subject describes a
category of change that is inherently self-explanatory, pass regardless
of diff size.

**PASS for small changes.** If the diffstat shows a small number of
lines changed (roughly 20 or fewer), the subject is almost always
sufficient. Pass without further analysis.

**PASS for new-file-only commits.** If the diffstat shows only new files
being added (no modifications to existing files), this is likely
greenfield code. The code itself (including its comments and
documentation) serves as the explanation. Pass.

**No body: examine more closely when the change is large and touches
existing code.** Look at the diffstat: if the commit modifies many lines
across existing files, and the subject is vague or generic relative to
the scope of the change, it is a candidate for a warning. Examples of
subjects that are too vague for a large change:

- "fix bug" (what bug? in what component? what was the root cause?)
- "update logic" (what logic? why?)
- "refactor" (what was refactored? why?)
- "improvements" (to what?)

Even for large changes, pass if the subject is specific enough to
understand the intent, e.g. "raft: fix follower crash on empty batches"
is fine for a 100-line change without a body — the subject names the
component, the failure mode, and the trigger.

**Thin body: check whether the body adds meaningful context.** A body
that exists but adds little beyond the subject is not much better than
no body at all. Common patterns that warrant a warning:

- Body is a single vague sentence that restates the subject in
  different words
- Body says "no semantic change" or "just a refactor" for a non-trivial
  diff without explaining what the refactor does or why it is
  semantics-preserving
- Body says "see subsequent commit" or "preparation for future work"
  without explaining what the current commit actually does
- Body lists what changed (which the diff already shows) without
  explaining why

A short body is fine when it adds genuine context. "Needed because X
depends on Y being initialized first" is one sentence but it tells you
something the diff doesn't. The question is whether the body helps a
reviewer understand the change, not whether it is long.

**Result format:**

- **Pass** — the commit message adequately explains the change
- **Warn** — briefly explain what context is missing (1 sentence), e.g.
  "large change (145 lines across 8 files) with vague subject — a body
  explaining the motivation would help reviewers" or "body says 'no
  semantic change' but the diff restructures conditional logic — explain
  why this is semantics-preserving"

#### M6: Bug fix commits explain bug, fix, and rationale

When a commit fixes a bug, the commit message should help reviewers and
future developers understand three things:

1. **The bug:** What was going wrong? (crash, data corruption, wrong
   result, deadlock, etc. — and under what conditions)
2. **The fix:** What does the code change do to address it?
3. **The rationale:** Why is this the right fix? (Why this approach
   rather than alternatives? Why does this actually solve the problem?)

All three are needed for non-trivial bug fixes. Reviewers need the bug
description to know what to test. Future developers hitting `git blame`
need the rationale to know whether the fix is still load-bearing or can
be safely reworked.

**Detecting bug-fix commits:** This check applies to commits whose
subject line suggests a bug fix. Indicators include:

- Keywords in the subject: "fix", "crash", "segfault", "panic",
  "assert", "bug", "handle", "prevent", "avoid", "correct", "repair",
  "resolve"
- The diff adds/modifies error handling, null checks, bounds checks,
  assertions, or conditional guards

If the subject does not suggest a bug fix, **SKIP** this check.

**Trivial fixes — subject only is fine.** Some bug fixes are simple
enough that the subject line communicates all three components
implicitly. Examples:

- "kafka: fix division by zero in fetch response" — the bug (division
  by zero), the fix (implied: add a zero check or avoid the division),
  and the rationale (implied: don't divide by zero) are all obvious
- "raft: fix off-by-one in batch offset calculation" — similarly
  self-contained
- "cluster: fix null pointer dereference in partition_manager" — the
  bug and fix are clear from context

For these, the subject names the specific failure mode precisely enough
that a reviewer can understand what happened. PASS.

**Non-trivial fixes — the body must cover all three components.** A bug
fix is non-trivial when any of these apply:

- The root cause is not obvious from the subject or the diff
- The fix involves a design choice (e.g. switching data structures,
  changing concurrency strategy, reordering operations)
- The failure mode is subtle (e.g. race condition, state corruption
  that manifests later, incorrect result under specific conditions)
- The diff is large or touches multiple subsystems

For non-trivial fixes, read the commit body and check whether it
addresses the three components. Common deficiencies:

**Body describes the fix but not the bug.** The reviewer can see *what*
the code does but not *why* it was wrong before. Example of a bad
message:

```
raft: fix consensus state handling

Switch from unordered_map to btree_map for tracking follower state.
```

This tells us what changed but not what was broken. What symptom was
observed? Under what conditions? A reviewer cannot evaluate the fix
without understanding the problem.

**Body describes the bug but not the fix.** The reviewer understands
what was wrong but cannot easily evaluate whether the change addresses
it. Example of a bad message:

```
raft: fix consensus state handling

Under heavy load, followers could receive stale state because iteration
order over the follower map was non-deterministic, causing updates to be
applied out of order.
```

This explains the bug well, but doesn't explain why switching to an
ordered map is the right solution (as opposed to, say, sorting updates
by sequence number, or using a different synchronization strategy).

**Body describes bug and fix but not the rationale.** The reviewer can
see the problem and the code change, but it's unclear why this specific
approach was chosen. This matters most when the fix involves a
non-obvious design choice.

**This is a judgment call.** Err on the side of PASS. The goal is to
catch cases where reviewers or future developers would clearly struggle
to understand the change. Some specific guidance:

- If the fix is a one-line change and the subject is specific, PASS
  even without a body
- If the body covers 2 out of 3 components and the missing one is
  inferable from context, PASS
- If the rationale is "this is the obviously correct thing to do" (e.g.
  adding a null check for a null dereference), it doesn't need to be
  stated — PASS
- Only warn when the gap is clear and would genuinely hinder review or
  future understanding

**Result format:**

- **Skip** — commit does not appear to be a bug fix
- **Pass** — bug fix is trivial, or body adequately covers bug/fix/rationale
- **Warn** — state which components are missing (1 sentence), e.g.
  "body describes the fix but not the bug — a reviewer cannot evaluate
  the change without understanding what was broken and under what
  conditions"

#### M7: Commit atomicity

A commit should do one logical thing. Combining unrelated changes in a
single commit makes the change harder to review, harder to bisect, and
harder to understand in `git blame`. This check examines both the commit
message and the diff to identify commits that bundle multiple unrelated
changes.

**This is a judgment call.** Examine the full diff to determine whether
the changes form a coherent, single-purpose unit of work. The commit
message can provide clues, but the diff is the source of truth.

**What counts as "one thing":**

A commit does one thing when all of its changes serve a single logical
purpose. Examples of coherent commits:

- Rename a function and update all its call sites
- Fix a bug and add a test for that bug
- Add a new feature across the files needed to implement it
- Refactor a module's internals without changing behavior
- Move code between files as part of a reorganization

The changes may touch many files and many lines, but they are all in
service of the same goal.

**Drive-by changes are OK.** A commit that fixes a bug but also
corrects a nearby typo, updates an adjacent comment, or fixes minor
formatting in the files it touches is fine. These are incidental
improvements that a developer naturally makes while working in the area.

The key distinction: drive-by changes are things that would **not**
deserve their own explanation or commit body. They are trivial,
localized, and obviously correct. If a "drive-by" change would itself
warrant a commit message body to explain, it is not a drive-by — it is
a separate logical change that belongs in its own commit.

**What to look for in the diff:**

- Changes to unrelated subsystems or components that don't interact
- Multiple independent bug fixes in the same commit
- A behavioral change mixed with an unrelated refactor
- Feature work mixed with unrelated cleanup in distant parts of the
  codebase
- Test changes that test something different from what the main change
  does

**What to look for in the commit message:**

The commit message can reveal atomicity problems. Watch for:

- The body lists multiple distinct changes as bullet points with no
  connecting thread
- The subject uses "and" to join unrelated actions (e.g. "fix X and
  refactor Y" where X and Y are unrelated)
- The body reads like a changelog for multiple independent items

However, a commit message that lists multiple changes is **not
automatically a problem**. Sometimes multiple steps are part of one
logical change. For example, "add new RPC endpoint" might involve
adding the protobuf definition, the handler, the client, and the test —
those are multiple things in the message but one logical change.

Conversely, a clean-looking commit message does not excuse a
non-atomic diff. A commit with a tidy one-line subject can still bundle
unrelated changes.

**When the commit message justifies combining changes:**

Sometimes the commit body explicitly acknowledges that multiple changes
are combined and explains why they are hard to separate. Evaluate this
claim critically:

- If the explanation is convincing (e.g. the changes have a genuine
  circular dependency, or separating them would require an intermediate
  state that doesn't compile), and the commit body thoroughly explains
  each change, PASS — but note it in the output as "PASS (combined
  changes justified in message)"
- If the explanation is thin (e.g. "easier to review together" or
  "these are related") but the changes are clearly separable, still
  warn. "Related" is not the same as "inseparable"
- If the complexity is very high — many independent changes each
  requiring explanation — warn even with justification. It is rarely
  true that complex changes genuinely cannot be split. With modern
  tooling (including AI coding agents that can efficiently perform
  mechanical refactoring), the bar for "too hard to separate" is high

**Assessing severity:**

Not all atomicity violations are equal. Calibrate your response:

- Two small, loosely related changes in one commit: mild concern, still
  worth a PASS if the overall commit is easy to follow
- A bug fix combined with an unrelated refactor, each non-trivial:
  clear warn
- Three or more orthogonal non-trivial changes in one commit: strong
  warn — this significantly hinders review and bisectability

**Result format:**

- **Pass** — the commit does one logical thing (possibly with trivial
  drive-by changes)
- **Pass** (combined changes justified in message) — multiple changes
  are combined but convincingly justified and well-explained
- **Warn** — briefly describe the distinct changes found (1-2
  sentences), e.g. "commit combines an unrelated rename of
  partition_allocator methods with a fix for stale follower state —
  these should be separate commits"

#### M8: Commit message accuracy

The commit message must accurately describe what the diff actually does.
A message that misrepresents the change is worse than a missing message
— it actively misleads reviewers and future developers reading
`git blame`.

This check compares the claims in the commit message (both subject and
body) against the actual diff. It requires reading the full diff.

**What to check:**

**Subject area prefix matches the change.** The area prefix (e.g.
`kafka:`, `raft:`, `cloud_storage:`) should correspond to the code
actually modified. If the subject says `kafka:` but the diff only
touches `cluster/` files, that is a mismatch.

Minor exceptions: a commit to `kafka/` code that also touches a shared
header in `utils/` is fine — the area reflects the primary intent. Only
warn when the area is clearly wrong for the change.

**Subject description matches the change.** The description (after the
area prefix) should accurately characterize what the diff does. Common
mismatches:

- Subject says "fix" but the diff is a refactor with no behavioral
  change
- Subject says "refactor" but the diff changes behavior
- Subject says "add" but the diff modifies or removes existing
  functionality
- Subject names a specific component or mechanism (e.g. "multipart
  upload") but the diff doesn't touch that mechanism
- Subject says "rename X to Y" but the diff does more than a rename

**Body claims match the diff.** If the commit body makes specific
claims about what the change does, verify them against the diff:

- If the body says "this replaces X with Y", check that X is actually
  removed and Y is actually added
- If the body says "no behavioral change", check that the diff is
  truly a refactor
- If the body describes a specific failure mode being fixed, check
  that the diff plausibly addresses that failure mode
- If the body says the change affects component A, check that
  component A is actually modified

**Drive-by changes don't count as mismatches.** A commit described as
"fix null dereference in fetch handler" that also fixes a nearby typo
is not inaccurate — the subject describes the primary change. Only
warn when the primary characterization is wrong.

**This is a judgment call.** Only warn when the mismatch is clear and
would mislead someone reading the message without the diff. Minor
imprecision in natural language is fine. The goal is to catch cases
where a reviewer or future developer would form a *wrong mental model*
of the change based on the message.

**Result format:**

- **Pass** — the commit message accurately describes the change
- **Warn** — describe the mismatch (1 sentence), e.g. "subject says
  'multipart upload' but the diff modifies single-part upload logic —
  the message mischaracterizes the change"

<!-- Future commit message checks go here -->

### Commit diff checks

<!-- Future diff checks go here -->

## Step 3: Report results

After checking all commits, print a summary report. Use this exact
format:

```
## lint-pr results

Checked N commits.

### <commit-sha-short> <subject (truncated to 60 chars if needed)>
  - M1 subject line length: PASS
  - M2 body line wrapping: PASS (or SKIP if no body)
  - M3 blank line after subject: PASS (or SKIP if no body)
  - M4 subject line format: PASS
  - M5 body explains non-trivial changes: PASS
  - M6 bug fix explanation: SKIP (not a bug fix)
  - M7 commit atomicity: PASS
  - M8 commit message accuracy: PASS

### <commit-sha-short> <subject>
  - M1 subject line length: WARN (84 chars, max 72)
  - M2 body line wrapping: WARN (line 5: 95 chars — "the offending line conten...")
  - M3 blank line after subject: PASS
  - M4 subject line format: WARN (missing area prefix — expected "area: description")
  - M5 body explains non-trivial changes: WARN (large change — 145 lines across 8 files — with vague subject; a body explaining the motivation would help reviewers)
  - M6 bug fix explanation: WARN (body describes the fix but not the bug — a reviewer cannot evaluate the change without understanding what was broken)
  - M7 commit atomicity: WARN (commit combines an unrelated rename of partition_allocator methods with a fix for stale follower state — these should be separate commits)
  - M8 commit message accuracy: WARN (subject says "multipart upload" but the diff modifies single-part upload logic)

## Summary: X passed, Y warnings, Z skipped
```

If all checks pass, end with:

```
All checks passed.
```

If any check produced a warning, end with:

```
Some checks flagged warnings. See above for details.
```
