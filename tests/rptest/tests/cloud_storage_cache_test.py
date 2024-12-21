import pytest
from ducktape.utils.util import wait_until
from requests import HTTPError

from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings
from rptest.tests.redpanda_test import RedpandaTest


class CloudStorageCacheAdminApisNoCacheTest(RedpandaTest):
    """
    Test the Cloud Storage Cache Admin APIs when tiered storage is not configured.
    """
    def __init__(self, test_context):
        super().__init__(test_context, num_brokers=1)
        self.admin = Admin(self.redpanda)

    @cluster(num_nodes=1)
    def test_admin_apis(self):
        for node in self.redpanda.nodes:
            with pytest.raises(HTTPError) as excinfo:
                self.admin.cloud_storage_trim(byte_limit=None,
                                              object_limit=None,
                                              node=node)

            assert "Cloud Storage Cache is not available. Is cloud storage enabled?" == excinfo.value.response.json(
            )['message']


class CloudStorageCacheAdminApisTest(RedpandaTest):
    """
    Test the Cloud Storage Cache Admin APIs when tiered storage is configured.
    """
    def __init__(self, test_context):
        super().__init__(test_context,
                         num_brokers=1,
                         si_settings=SISettings(test_context))
        self.admin = Admin(self.redpanda)

    def setUp(self):
        pass

    @cluster(num_nodes=1,
             log_allow_list=["Free space information is not available"])
    def test_admin_apis(self):
        for node in self.redpanda.nodes:
            node.account.ssh(
                f"mkdir -p {self.redpanda.cache_dir} ; "
                "for n in `seq 1 100`; do "
                f"dd if=/dev/urandom bs=4k count=1 of={self.redpanda.cache_dir}/garbage_$n.bin ; done",
            )

            self.redpanda.start(clean_nodes=False)

            # Assert initial conditions.
            usage = self.admin.get_local_storage_usage(node)
            assert usage['cloud_storage_cache_objects'] == 100, usage

            # Trim with default settings. Nothing should be trimmed as we are well
            # below reasonable limits.
            # Wrapped with wait_until as it will fail until a background fiber
            # updates information about free disk space.
            wait_until(lambda: self.admin.cloud_storage_trim(
                byte_limit=None, object_limit=None, node=node),
                       timeout_sec=30,
                       backoff_sec=1,
                       retry_on_exc=True)

            usage = self.admin.get_local_storage_usage(node)
            assert usage['cloud_storage_cache_objects'] == 100, usage

            # Trim with byte limit. We should trim half of objects.
            self.admin.cloud_storage_trim(byte_limit=4096 * 50,
                                          object_limit=None,
                                          node=node)
            usage = self.admin.get_local_storage_usage(node)

            # Although we set the limit to size of 50 objects, the value
            # gets multiplied by 0.8 internally so we end up with 40 objects left.
            assert usage['cloud_storage_cache_objects'] == 40, usage

            # Trim with object limit. We should trim 20 objects.
            self.admin.cloud_storage_trim(byte_limit=None,
                                          object_limit=20,
                                          node=node)
            usage = self.admin.get_local_storage_usage(node)

            # Although we set the limit to 20 objects, the value
            # gets multiplied by 0.8 internally so we end up with 16 objects left.
            assert usage['cloud_storage_cache_objects'] == 16, usage
