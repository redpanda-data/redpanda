# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import uuid
from rptest.services.apache_iceberg_catalog import IcebergRESTCatalog
from rptest.services.redpanda import SISettings
from rptest.tests.redpanda_test import RedpandaTest


class IcebergRESTCatalogTest(RedpandaTest):
    """Create a iceberg REST catalog with a shared bucket as Redpanda"""
    def __init__(self, test_ctx, *args, **kwargs):
        si_settings = SISettings(test_context=test_ctx)
        super(IcebergRESTCatalogTest, self).__init__(test_ctx,
                                                     si_settings=si_settings,
                                                     *args,
                                                     **kwargs)
        self.catalog_service = IcebergRESTCatalog(
            test_ctx,
            cloud_storage_bucket=si_settings.cloud_storage_bucket,
            filesystem_wrapper_mode=False)

    def setUp(self):
        super().setUp()
        self.catalog_service.start()

    def tearDown(self):
        self.catalog_service.stop()
        return super().tearDown()
