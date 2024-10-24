# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import tempfile

from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool


class RpkClusterQuotaTest(RedpandaTest):
    def __init__(self, ctx):
        super(RpkClusterQuotaTest, self).__init__(test_context=ctx)
        self._ctx = ctx
        self._rpk = RpkTool(self.redpanda)

    @cluster(num_nodes=1)
    def test_import_describe_quotas(self):
        """
        Test that asserts the correct handling of imported quotas
        either using the source as a string, or passing a file
        to rpk.
        """
        q1 = self._rpk.describe_cluster_quotas()
        assert len(q1) == 0

        self._rpk.alter_cluster_quotas(default=["client-id"],
                                       add=["producer_byte_rate=11111"])
        self._rpk.alter_cluster_quotas(name=["client-id-prefix=foo-"],
                                       add=["consumer_byte_rate=2222"])

        q2 = self._rpk.describe_cluster_quotas()
        assert len(
            q2["quotas"]) == 2  # Quick check, just that we have something.

        # Same values, NoOp:
        import1 = '''
{
   "quotas":[
      {
         "entity":[
            {
               "name":"foo-",
               "type":"client-id-prefix"
            }
         ],
         "values":[
            {
               "key":"consumer_byte_rate",
               "value":"2222"
            }
         ]
      },
      {
         "entity":[
            {
               "name":"<default>",
               "type":"client-id"
            }
         ],
         "values":[
            {
               "key":"producer_byte_rate",
               "value":"11111"
            }
         ]
      }
   ]
}
'''
        out = self._rpk.import_cluster_quota(import1, output_format="text")
        assert "No changes detected from import" in out

        # Remove 1 (default client-id), Add 1 (producer byte rate).
        import2 = '''
{
   "quotas":[
      {
         "entity":[
            {
               "name":"foo-",
               "type":"client-id-prefix"
            }
         ],
         "values":[
            {
               "key":"consumer_byte_rate",
               "value":"2222"
            },
            {
               "key":"producer_byte_rate",
               "value":"3333"
            }
         ]
      }
   ]
}
'''

        def assertChanges(quotas, entity, quotaType, old, new):
            found = False
            for q in quotas:
                if q["entity"] == entity and q["quota-type"] == quotaType:
                    assert q["old-value"] == old
                    assert q["new-value"] == new
                    found = True
            if not found:
                raise Exception(f"unable to find quota entity {entity}")

        with tempfile.NamedTemporaryFile() as tf:
            tf.write(bytes(import2, 'UTF-8'))
            tf.seek(0)
            out = self._rpk.import_cluster_quota(tf.name)
            assertChanges(out,
                          "client-id=<default>",
                          "producer_byte_rate",
                          old="11111",
                          new="-")  # New Value as '-' means it was deleted
            assertChanges(out,
                          "client-id-prefix=foo-",
                          "producer_byte_rate",
                          old="-",
                          new="3333")

        # Retry with same value, it should not detect any changes:
        out = self._rpk.import_cluster_quota(import2, output_format="text")
        assert "No changes detected from import" in out

        # Assert that the imported quotas match what we can describe:
        q3 = self._rpk.describe_cluster_quotas()
        quotas = q3["quotas"]
        assert len(quotas) == 1 and len(quotas[0]["entity"]) == 1
        assert quotas[0]["entity"][0]["type"] == "client-id-prefix" and quotas[
            0]["entity"][0]["name"] == "foo-"

        def assertValues(values, key, expectedVal):
            found = False
            for v in values:
                if v["key"] == key:
                    assert v["value"] == expectedVal
                    found = True
            if not found:
                raise Exception(f"unable to find {key}")

        values = quotas[0]["values"]
        assert len(values) == 2
        assertValues(values, "consumer_byte_rate", "2222")
        assertValues(values, "producer_byte_rate", "3333")
