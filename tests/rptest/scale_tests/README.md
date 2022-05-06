
# scale_tests

Tests in this directory validate redpanda at higher
scale than the regular functional tests in tests/.

They require "fast" test nodes, where fast means dedicated
CPU/memory/storage for each node (not that the nodes necessarily
have to be especially high performance).

In practice, these are tests that we run using "clustered ducktape", i.e.
ducktape on EC2 instances, rather than running ducktape within docker.
