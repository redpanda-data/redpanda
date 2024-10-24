---
name: Create ci-failure issue (rpunit)
about: An unit/fixure test is failing in CI
title: CI Failure (key symptom) in `_rpunit`
labels: kind/bug, rpunit
assignees: ''

---

<!-- Before creating an issue look through existing rpunit to avoid duplicates
https://github.com/redpanda-data/redpanda/issues?q=is%3Aissue+is%3Aopen+label%3Arpunit -->

https://link.to/failing/build

<!-- Copy the stacktrace and/or failing assertion -->

```
/redpanda/src/v/foo/tests/foo_test.cc:42: Failure
Value of: returned
Expected: contains 1 values, where each value and its corresponding value in {} same record eq
  Actual: { {}, {} }, which contains 2 values
```