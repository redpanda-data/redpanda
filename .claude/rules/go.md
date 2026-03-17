---
paths:
  - "src/go/**/*.go"
---

# Go Conventions (rpk)

## Error handling
- Define sentinel errors as package-level vars: `var ErrFoo = errors.New("...")`
- Wrap errors with context: `fmt.Errorf("unable to do X %q: %w", val, err)`
- Check wrapped errors with `errors.Is()`, not string comparison
- Return early; avoid deeply nested error checks

## Logging
- No logging in utility/library functions — log only in command handlers
- Structured logging via `zap.L().Sugar()` or the config logger `p.Logger()`
- CLI output (tables, JSON, YAML) goes through the `out` package, not fmt/log

## Structure
- One package per feature area (e.g. `oauth/`, `schemaregistry/`, `adminapi/`)
- Config flows through `*config.Params` passed to command handlers
- Constructor pattern: `NewClient(fs afero.Fs, p *config.Params) (*Client, error)`
