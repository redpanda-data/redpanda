---
name: Create ci-failure issue (ducktape)
about: A ducktape test is failing in CI
title: CI Failure (key symptom) in `Class.method`
labels: kind/bug, ci-failure
assignees: ''

---

<!-- Before creating an issue look through existing ci-issues to avoid duplicates
https://github.com/redpanda-data/redpanda/issues?q=is%3Aissue+is%3Aopen+label%3Aci-failure -->

https://link.to/failing/build

<!-- Copy the summary from the "Failed Tests" section of report.html -->

```
Module: rptest.tests.module
Class:  ClassTest
Method: test_methods
Arguments:
{
  "if_present": true
}
```

<!-- Copy the summary from report.txt if it's too vague to differentiate key symptoms (e.g. TimeoutError is thrown for multiple reason so it makes to dig deeper) - fetch the tarball, check debug and redpanda logs -->

```
test_id:    rptest.tests.module.ClassTest.test_methods.if_present=True
status:     FAIL
run time:   4 minutes 21.145 seconds
 
    TimeoutError("Something is wrong")
Traceback (most recent call last):
  File "/usr/local/lib/python3.10/dist-packages/ducktape/tests/runner_client.py", line 135, in run
    data = self.run_test()
  File "/usr/local/lib/python3.10/dist-packages/ducktape/tests/runner_client.py", line 227, in run_test
    return self.test_context.function(self.test)
  File "/usr/local/lib/python3.10/dist-packages/ducktape/mark/_mark.py", line 476, in wrapper
    return functools.partial(f, *args, **kwargs)(*w_args, **w_kwargs)
  File "/root/tests/rptest/utils/mode_checks.py", line 63, in f
    return func(*args, **kwargs)
  File "/root/tests/rptest/services/cluster.py", line 35, in wrapped
    r = f(self, *args, **kwargs)
  File "/root/tests/rptest/tests/module.py", line 105, in test_methods
    raise TimeoutError("Something is wrong")
ducktape.errors.TimeoutError
```