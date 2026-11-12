# Scratch repro for the SR-sync shutdown hang (do not commit).
#
# Forces the race behind "Redpanda node docker-rp-X failed to stop in 30
# seconds" in the SR sync suite: register a large flat subject set on the
# source, create the link, wait until the destination import is underway, then
# return. The @cluster decorator's post-test stop SIGTERMs the destination
# brokers while the full-sync run is mid-import, so task::stop()'s runner-gate
# close must drain an in-flight run whose awaits are not abort-responsive.
#
# Expected: hangs (>30s stop -> TimeoutError) at 1cbb0dbcee; the question under
# test is whether PR #31101's stop-ordering fixes drain it cleanly.

from typing import Any

from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.services.cluster import cluster
from rptest.services.multi_cluster_services import SecondaryClusterArgs
from rptest.services.redpanda import SchemaRegistryConfig
from rptest.tests.cluster_linking_schema_registry_sync_test import (
    SchemaRegistrySyncMixin,
)
from rptest.tests.cluster_linking_test_base import ShadowLinkTestBase
from rptest.tests.schema_registry_test import SchemaRegistryRedpandaClient


class SchemaRegistrySyncStopHangReproTest(ShadowLinkTestBase, SchemaRegistrySyncMixin):
    SUBJECTS = 600

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        source_sr_config = SchemaRegistryConfig()
        source_sr_config.mode_mutability = True
        super().__init__(
            test_context,
            secondary_cluster_args=SecondaryClusterArgs(
                schema_registry_config=source_sr_config
            ),
            schema_registry_config=SchemaRegistryConfig(),
            extra_rp_conf={"enable_leader_balancer": False},
            *args,
            **kwargs,
        )

    def _make_source_client(self) -> SchemaRegistryRedpandaClient:
        return SchemaRegistryRedpandaClient(self.source_cluster_service)

    @cluster(num_nodes=6)
    def test_stop_mid_import(self):
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        for i in range(self.SUBJECTS):
            self._add_leaf(src, i)

        self._create_sr_link()

        def import_underway() -> bool:
            resp = dest.get_subjects()
            if resp.status_code != 200:
                return False
            n = len(resp.json())
            self.logger.info(f"destination has imported {n} subjects")
            # Import has begun but the bulk is still pending: returning now
            # guarantees the post-test stop lands with many import fibers
            # inside destination seq_writer operations.
            return 5 <= n < 150

        wait_until(
            import_underway,
            timeout_sec=120,
            backoff_sec=0.2,
            err_msg="import never reached the mid-flight window",
        )
        self.logger.info("returning with import in flight; teardown stops the cluster")
