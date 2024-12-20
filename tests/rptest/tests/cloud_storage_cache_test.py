import pytest
from requests import HTTPError

from rptest.services.admin import Admin
from rptest.services.cluster import cluster
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
