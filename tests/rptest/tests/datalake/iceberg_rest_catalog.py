# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import uuid
from rptest.archival.s3_client import S3Client
from rptest.services.apache_iceberg_catalog import IcebergRESTCatalog
from rptest.tests.redpanda_test import RedpandaTest


class IcebergRESTCatalogTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(IcebergRESTCatalogTest, self).__init__(test_ctx, *args, **kwargs)
        self.s3_client = S3Client(region="panda-region",
                                  access_key="panda-user",
                                  secret_key="panda-secret",
                                  endpoint="http://minio-s3:9000",
                                  logger=self.logger)
        self.warehouse = f"warehouse-{uuid.uuid1()}"
        self.catalog_service = IcebergRESTCatalog(
            test_ctx, cloud_storage_warehouse=self.warehouse)

    def setUp(self):
        self.s3_client.create_bucket(self.warehouse)
        self.catalog_service.start()
        return super().setUp()

    def tearDown(self):
        self.catalog_service.stop()
        self.s3_client.empty_and_delete_bucket(self.warehouse)
        return super().tearDown()
