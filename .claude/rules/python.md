---
paths:
  - "tests/**/*.py"
  - "tools/**/*.py"
---

# Python Conventions

- Use `except Exception:` not bare `except:` — bare except swallows signals used for test timeouts
- Use `|` for type unions, not `Union`/`Optional`
