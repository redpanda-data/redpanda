from typing import Any
from rptest.services.cluster import cluster
from rptest.services.redpanda import LoggingConfig, get_cloud_storage_type
from rptest.tests.random_node_operations_smoke_test import (
    RNOT_ALLOW_LIST,
    CompactionMode,
    RandomNodeOperationsBase,
)
from rptest.utils.mode_checks import skip_debug_mode, skip_fips_mode

from ducktape.mark import matrix

# This is the "full fat" version of the Random Node Operations Test (RNOT).
# It runs the test with a variety of parameters, including:
# - with and without simulated failures
# - mixed versions (rolling upgrade/downgrade)
# - with and without iceberg
# - different compaction modes
# - different cloud storage types
#
# It is a fuzz test since it runs randomized inputs and failures.
#
# See also RandomNodeOperationsSmokeTest which is a much more limited version of
# the test that is intended to be run more frequently (e.g. in PRs) to catch
# obvious issues in the RNOT test itself.


class RandomNodeOperationsTest(RandomNodeOperationsBase):
    """
    Main test for RNOT test with all the parameterization.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(
            *args,
            **kwargs,
            is_smoke_test=False,
            log_config=LoggingConfig(
                "info",
                {
                    "storage-resources": "warn",
                    "storage-gc": "debug",
                    "storage": "trace",
                    "raft": "trace",
                    "offset_translator": "trace",
                    "cluster": "debug",
                    "datalake": "trace",
                    "cloud_storage": "debug",
                    "cloud_io": "debug",
                    "kafka": "debug",
                    "reconciler": "debug",
                    "cloud_topics": "debug",
                    "cloud_topics_compaction": "debug",
                },
            ),
        )

    # before v24.2, dns query to s3 endpoint do not include the bucketname, which is required for AWS S3 fips endpoints
    @skip_fips_mode
    @skip_debug_mode
    @cluster(num_nodes=9, log_allow_list=RNOT_ALLOW_LIST)
    @matrix(
        enable_failures=[True, False],
        mixed_versions=[True, False],
        with_iceberg=[True, False],
        compaction_mode=[
            CompactionMode.SLIDING_WINDOW,
            CompactionMode.CHUNKED_SLIDING_WINDOW,
            CompactionMode.ADJACENT_MERGE,
        ],
        cloud_storage_type=get_cloud_storage_type(),
    )
    def test_node_operations(self, **kwargs: Any):
        self._do_test_node_operations(**kwargs)
