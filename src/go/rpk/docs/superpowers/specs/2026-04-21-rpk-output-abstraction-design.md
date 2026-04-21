# rpk Output Abstraction — Design

**Status:** RFC draft
**Author:** Graham Smith
**Date:** 2026-04-21

## Goal

Collapse the per-command boilerplate around table / JSON / YAML output in `rpk` behind a single declarative renderer. Today every command with structured output hand-rolls the same pattern: a row struct with `json` / `yaml` tags, an `if isText, _, t, err := f.Format(results); !isText { print t; return }` dispatch block, and an explicit `NewTable(headers...) + loop + Print(cells...)` text path. Across ~135 list-style commands and ~80 commands still printing tables by hand, this adds up to several thousand lines of near-identical code.

## Non-goals

- Changing the `--format` flag vocabulary. `json` / `yaml` / `text` / `wide` / `long` / `short` / `help` stays identical.
- Changing the JSON / YAML output shape of any migrated command. Byte-for-byte backward compatibility is a hard constraint.
- Replacing `pkg/out.NewTable`. Commands outside the declarative path keep using it.
- Declarative support for output that mixes structured sections with free-form `fmt.Printf` prose between them. Commands that need that can call the renderer per-section and interleave their own text.

## Design

### Public API

One function:

```go
package out

// Render emits v in the format selected by f, writing to w.
// The output shape is determined by reflection on v.
func Render(f config.OutFormatter, w io.Writer, v any) error
```

Typical usage:

```go
out.Render(&p.Formatter, os.Stdout, data)
```

### Tag vocabulary

```go
type topicRow struct {
    Name       string `json:"name"       yaml:"name"       table:"NAME"`
    Partitions int    `json:"partitions" yaml:"partitions" table:"PARTITIONS"`
    LogBytes   int64  `json:"log_bytes"  yaml:"log_bytes"  table:"LOG BYTES,wide"`
    isInternal bool   // unexported, used via FormatTableRow for decoration
}
```

- `json:"..."` / `yaml:"..."` — existing convention. Authors keep writing these exactly as today; JSON / YAML output derives from them.
- `table:"HEADER"` — field appears as a column in list mode (shape 1) or as a label in object mode (shape 2).
- `table:"HEADER,wide"` — field only appears when `--format` is `wide` or `long`.
- `table:"HEADER,omitempty"` — in object mode only, skip the label / value line when the field's value is the zero value. Ignored in list mode (columns are not per-row elided). May be combined: `table:"HEADER,wide,omitempty"`.
- `table:"-"` — field is never rendered in text; still serialized in JSON / YAML via its existing tags.
- `header:"SECTION TITLE"` — on a struct field whose enclosing struct is a composite (shape 3). The field becomes a titled section.
- `header:"SECTION TITLE,omitempty"` — section is skipped in text when the field's value is zero, nil, or an empty slice.

### Dispatch rules

`Render(f, w, v)`:

1. Call `f.Format(v)` → `(isText, isWide, s, err)`. (The actual return variable names in `pkg/config/format.go` are `isShort, isLong`; the semantics are "is text mode" and "is wide mode".)
2. If not text — write `s`, return `err`. Done.
3. Dereference `v` if pointer.
4. If `v` is a slice or array → list renderer (shape 1).
5. If `v` is a struct with at least one field tagged `header:"..."` → composite renderer (shape 3).
6. Any other struct → object renderer (shape 2).
7. Anything else → error.

### Shape 1 — list

For each element of the slice:

- If the element type implements `FormatTableRow() []any`, call it for the cells.
- Otherwise, reflect over `table:"..."` tags in struct declaration order, skipping `table:"-"` and (unless wide) `,wide` fields.

Emit via `out.NewTableTo(w, headers...)`. Cells stringify via `fmt.Sprint`.

### Shape 2 — object

Same tag iteration as shape 1, but laid out vertically as `LABEL  VALUE` rows in a two-column table. `table:"-"` and wide-filtering apply identically.

### Shape 3 — composite

Walk the struct's fields in declaration order:

- Skip fields without `header:"..."`.
- Skip if `,omitempty` and the field's value is zero / nil / empty.
- Print header text, an `=` underline of matching length, a blank line.
- Recurse via the dispatch rules (slice → list, struct → object or composite).
- Blank line between sections.

### Escape hatch for per-row decoration

```go
type TableRowFormatter interface {
    FormatTableRow() []any
}
```

When a row type implements this, the list renderer uses its return slice instead of reflection. Intended for cells that need per-row decoration (e.g., appending ` (internal)` when a flag is set).

The return slice contains every `table:"..."` cell in struct declaration order, including `,wide` cells. The renderer selects which indices to emit based on the current mode — the method does not need to know about short vs wide. A `table:"-"` field contributes no cell.

On length mismatch (against the count of `table:"..."` tags excluding `-`), the renderer softens rather than panicking:

- Fewer cells than expected → pad with `""`.
- More cells than expected → drop the extras.
- Either way → emit a `zap.L().Warn(...)` so the drift shows up in `--verbose`.

No panic. Output still renders. CI format tests are expected to catch shape drift via output comparison.

### Format → rendering mapping

| `--format` | `isText` | `isWide` | Text rendering |
| --- | --- | --- | --- |
| `text`, `short` | true | false | tagged fields, excluding `,wide` |
| `wide`, `long` | true | true | all tagged fields |
| `json`, `yaml` | false | — | handed to `f.Format`, written as-is |

### Empty data

- Empty slice passed to the list renderer → the header row is still printed, with no data rows. Commands wanting "No foo found." print it themselves before calling `Render` when `len(data) == 0`. The renderer stays context-free.
- Empty struct passed to the object renderer → fields render with their zero values unless tagged `table:"-"`.
- A composite section with a zero / nil / empty value and `,omitempty` → skipped.

## Backward compatibility

- JSON / YAML output is produced by the same `f.Format(v)` call the current code uses, marshalling `v` directly. As long as a migrated command's struct keeps its existing field names, types, and `json` / `yaml` tags, output is byte-for-byte identical.
- Each migration PR must include a before / after JSON diff for at least one realistic fixture as part of its verification.
- Text output may drift slightly on migration (header casing, column spacing, empty-result behavior). This is acceptable: text output was never a stable contract. We document that consumers parsing output should use `--format json` or `yaml`.

## Rollout

**Step 1 — RFC PR.**

- Add `pkg/out/render.go` with `Render` and three shape renderers.
- Add `pkg/out/render_test.go` covering the full tag vocabulary and edge cases (see Testing).
- Migrate `rpk topic describe` as the demo. It exercises all three shapes (summary = object, configs + partitions = lists, whole command = composite) and is high-visibility enough that any JSON regression will be caught immediately.
- PR description shows a before / after JSON diff for `topic describe` against a real topic fixture.
- Include a short godoc block on `Render` with one list, one object, and one composite example.

**Step 2 — Opportunistic migration.**

- Once merged, `out.Render` is the recommended path for new commands.
- Existing commands migrate as authors touch them for other reasons or as anyone volunteers a batch.
- No forced timeline. The old pattern continues to work; the two coexist until everything has moved.

## Testing

- **Renderer unit tests** in `pkg/out/render_test.go`, table-driven, covering:
  - Primitive cells, nested structs, pointer deref.
  - `table:"-"` hidden in text, present in JSON / YAML.
  - `,wide` filtering toggled by `isWide`.
  - `FormatTableRow` override including short / long mismatch (verifies pad / drop behavior and warn log).
  - `header:"..."` composite with mixed shape children.
  - `header:"...,omitempty"` for zero struct, nil slice, empty slice.
  - Empty slice to list renderer emits headers only.
- **Author helper**: `out.AssertRowShape[T](t *testing.T, zero T)` — validates the number of cells returned by `FormatTableRow` matches the number of visible `table:"..."` tags on `T`. Opt-in, but useful for commands with non-literal return slices where static inspection can't help.
- **Existing per-command format tests** continue to run. The "rework tests" commit on this branch established format coverage for most migrated commands; if a `FormatTableRow` implementation drifts against its tags, the expected-output comparison in those tests fails.

## Alternatives considered

- **Per-column functional spec (kubectl / gh pattern):** `[]Column[T]{Header, Key, Get}`. Rejected because it decouples JSON shape from the Go struct, making backward compatibility harder to verify. Struct-tag approach keeps the existing data structures authoritative.
- **Cell-level override `FormatCell(fieldName string) (any, bool)` instead of row-level:** Rejected because string-based field dispatch is fragile under refactors (typos fail silently). Row-level return is positional and obvious.
- **Custom `go/analysis` linter** for the `FormatTableRow` length contract: Rejected as overkill for one rule. Runtime pad / truncate + warn log + existing per-command format tests provides adequate coverage.
- **Big-bang migration in one PR**: Rejected. JSON backward compatibility is the biggest risk, and ~200 commands is not reviewable at once.

## Open questions

- Are any existing commands relying on `--format text` output shape being parse-stable? Worth a quick audit during the RFC PR review and, if any are found, explicitly calling them out.
- Should `TableRowFormatter` be exported from `pkg/out`, or kept unexported and matched structurally? Exporting is more discoverable. Tentatively exported.
