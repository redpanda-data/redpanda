# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster

from rptest.services.polaris_catalog import PolarisCatalog
from rptest.tests.polaris_catalog_test import PolarisCatalogTest
import polaris.catalog

from polaris.management.api.polaris_default_api import PolarisDefaultApi
from polaris.management.models.create_catalog_request import CreateCatalogRequest
from polaris.management.models.catalog_properties import CatalogProperties
from polaris.management.models.catalog import Catalog
from polaris.management.models.storage_config_info import StorageConfigInfo


class PolarisCatalogSmokeTest(PolarisCatalogTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(PolarisCatalogSmokeTest, self).__init__(test_ctx,
                                                      num_brokers=1,
                                                      *args,
                                                      extra_rp_conf={},
                                                      **kwargs)

    """
    Validates if the polaris catalog is accessible from ducktape tests harness
    """

    @cluster(num_nodes=2)
    def test_creating_catalog(self):
        """The very basic test checking interaction with polaris catalog
        """
        polaris_api = PolarisDefaultApi(self.polaris.management_client())
        catalog = Catalog(
            type="INTERNAL",
            name="test-catalog",
            properties=CatalogProperties(
                default_base_location=
                f"file://{PolarisCatalog.PERSISTENT_ROOT}/catalog_data",
                additional_properties={}),
            storageConfigInfo=StorageConfigInfo(storageType="FILE"))

        polaris_api.create_catalog(CreateCatalogRequest(catalog=catalog))
        resp = polaris_api.list_catalogs()

        assert len(resp.catalogs) == 1
        assert resp.catalogs[0].name == "test-catalog"
