#!/bin/bash

# This wrapper exists to run ducktape and the output processing
# together, in a way that preserves the exit status is that of ducktape.

set +e

ducktape $@
DUCKTAPE_ERR=$?
ls -lh /build/tests/results/latest
/root/tests/rptest/trim_results.py /build/tests/results/latest
exit $DUCKTAPE_ERR
