---
name: doc-strings
description: Writing standards for user-facing doc strings in this repo — config property descriptions in src/v/config/, sm::description(...) metric help, and rpk cobra Short/Long/flag usage strings in src/go/rpk/pkg/cli/. Apply when adding or editing any of those strings, or reviewing a diff that touches them. These strings are published documentation, generated verbatim onto docs.redpanda.com.
user-invocable: true
---

# Doc strings

Config property descriptions, metric help text, and rpk help strings in this
repo are not code comments. Doc generators publish them verbatim: property
descriptions become the property reference pages and the docs-site hover
tooltips, `sm::description` strings become the Prometheus `# HELP` text and
the metrics reference, and rpk `Short`/`Long`/flag usage strings become the
rpk command reference and `--help` output. Write them as documentation for
an operator, not as notes for a maintainer.

The authoritative standard is `resources/writing-style/embedded-reference-strings.md`
in redpanda-data/docs-team-standards (private; fetch it, never vendor a copy —
it carries the per-surface contracts and the full quality bar). The rules
below are the local summary.

## What this skill looks for

- Every new user-facing property, metric, command, or flag carries a
  description. An empty or missing string ships an empty docs page.
- The description is a complete thought that states the effect and when an
  operator would change the setting — never a restatement of the name
  ("Cluster identifier." for `cluster_id` documents nothing).
- Defaults, units, and valid ranges are stated in prose. Spell out units.
- A changed default or unit updates the description string in the same diff,
  or the published docs state the old behavior the moment this merges.
- No internal jargon in user-facing text: users set `null`, not `nullopt`;
  expand or avoid NTP, shard, seastar, stm.

## Per-surface rules

| Surface | Rules |
|---|---|
| Properties (`src/v/config/`) | Verbatim AsciiDoc: keep backticks balanced; no raw `\|` or `{attr}`. Sentence case, terminal period. The 4th ctor arg's `.example` field is where example values go. |
| Metrics (`sm::description`) | Zero escaping downstream — no `\|` or `{attr}` at all. Capitalized, NO terminal period. Never echo the metric name. |
| rpk (`src/go/rpk/pkg/cli/`) | The docs formatter rewrites these: `Short` = one line, capitalized, no period; flag usage = capitalized, no period, never an echo of the flag name; in `Long`, an ALLCAPS line becomes a section heading, so never use ALLCAPS for emphasis. Tie CLI defaults to their server property ("-1 defaults to the cluster's `default_topic_partitions`"). |

## Tools

When the redpanda-doc-tools-assistant MCP server is available, use
`lint_doc_strings` (deterministic rule check over a repo or diff) and
`preview_doc_string` (renders one declaration exactly as it will ship,
including whether a docs-side override currently masks it). Without MCP:
`npx doc-tools lint-strings --repo . --diff origin/dev`.

## Check published content

Changing a default, unit, or behavior, removing or renaming a surface, or
adding a new one can make published docs wrong or leave a gap. Search
docs.redpanda.com for the surface name before finalizing the change (the
public docs MCP at https://docs.redpanda.com/mcp exposes search and Q&A
tools; plain web search works too). If a published page states the old
behavior, say so in the PR description and update the doc string in the
same diff; the PR review automation routes high-impact cases to the docs
team's Jira intake (comments on the existing DOC ticket or files one).

## What NOT to flag

- Subjective wording or polish on a string that already states effect,
  default, and units. Never bikeshed phrasing.
- Internal comments, log messages, or test strings — this skill covers only
  strings the doc generators consume.
- Style of the surrounding C++/Go code (other reviews own that).

## Severity (for reviews)

- **high**: changed default/unit/behavior with the string left stating the
  old one (published docs become wrong on merge).
- **medium** (default): new user-facing surface with an empty, missing, or
  name-echo description.
- **low**: present and accurate but incomplete (no default, no units).

Zero findings on a diff that adds user-facing surfaces is a claim: state
which surfaces you checked.
