The testing framework can run executables in the build tree. Given an up-to-date
build, the build must be packaged into a staging directory. For example:

    vtools build pkg --format dir --build-type debug --clang

The `dir` packaging format will only operate on files that have changed
since the last packaging step was run, allowing fast iteration of tests.

compacted topic verifier and its java dependencies have to be built for all tests
to be able to run:

    vtools install java
    vtools install maven
    vtools build java

To run all tests from the root of the source tree invoke:

    tests/docker/run_tests.sh

Once all tests have completed a report will be generated:

```
================================================================================
SESSION REPORT (ALL TESTS)
ducktape version: 0.7.8
session_id:       2020-08-05--009
run time:         2 minutes 26.877 seconds
tests run:        6
passed:           6
failed:           0
ignored:          0
================================================================================
test_id:    rptest.tests.compaction_recovery_test.CompactionRecoveryTest.test_index_recovery
status:     PASS
run time:   46.861 seconds
--------------------------------------------------------------------------------
test_id:    rptest.tests.controller_recovery_test.ControllerRecoveryTest.test_controller_recovery
status:     PASS
run time:   8.365 seconds
--------------------------------------------------------------------------------

...
```

And a local `results` directory will be populated will all of the results
including log messages from the testing framework and redpanda logs collected
from each node.

To run a single test suite invoke:

    TC_PATHS="tests/rptest/tests/controller_recovery_test.py" tests/docker/run_tests.sh

To run a single test, specify the path of the method:

    TC_PATHS="tests/rptest/tests/controller_recovery_test.py::ControllerRecoveryTest.test_controller_recovery" tests/docker/run_tests.sh

To enable debug logging of ducktape:

    _DUCKTAPE_OPTIONS="--debug" tests/docker/run_tests.sh
