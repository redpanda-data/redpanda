# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.polaris_catalog import PolarisCatalog
from rptest.tests.redpanda_test import RedpandaTest


class PolarisCatalogTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        self.polaris = PolarisCatalog(test_ctx)
        super(PolarisCatalogTest, self).__init__(test_ctx, *args, **kwargs)

    """
    Base class for tests using polaris catalog
    """

    def setUp(self):
        self.polaris.start()
        return super().setUp()
