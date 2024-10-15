# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.apache_iceberg_catalog import IcebergRESTCatalog
from rptest.tests.redpanda_test import RedpandaTest


class IcebergRESTCatalogTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        self.catalog_service = IcebergRESTCatalog(test_ctx)
        super(IcebergRESTCatalogTest, self).__init__(test_ctx, *args, **kwargs)

    def setUp(self):
        self.catalog_service.start()
        return super().setUp()
