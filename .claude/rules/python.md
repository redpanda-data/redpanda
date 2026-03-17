---
paths:
  - "tests/**/*.py"
  - "tools/**/*.py"
---

# Python Conventions

## Style
- Use `except Exception:` not bare `except:` — bare except swallows signals used for test timeouts
- Use `|` for type unions, not `Union`/`Optional`

## Integration tests (tests/rptest/)
Tests run against a live cluster via the ducktape framework — there are no mocks.

Test structure:
- Inherit from `RedpandaTest`; cluster starts in `setUp()` automatically
- Declare cluster size with `@cluster(num_nodes=N)` on each test method
- Use `wait_until(fn, timeout_sec=N, backoff_sec=N)` for async assertions — never sleep
- Access the broker via `self.redpanda` (service) and `self.client()` (Kafka client)
- Use `self.logger.info()` for test logging
- Parameterize with `@parametrize(key=value)` rather than duplicating test methods
