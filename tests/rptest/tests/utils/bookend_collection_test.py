# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import resource
from ducktape.tests.test import Test

from rptest.utils.bookend_collection import BookendCollection


class BookendCollectionTest(Test):
    @resource.cluster(num_nodes=0)
    def test_only_head(self):
        collection = BookendCollection(head=2, tail=2)
        collection.append(0)
        assert list(collection) == [0], f"got {list(collection)}"

    @resource.cluster(num_nodes=0)
    def test_limit(self):
        collection = BookendCollection(head=2, tail=2)
        for i in range(4):
            collection.append(i)
        assert list(collection) == [0, 1, 2, 3], f"got {list(collection)}"

    @resource.cluster(num_nodes=0)
    def test_dropping(self):
        collection = BookendCollection(head=2, tail=2)
        for i in range(10):
            collection.append(i)
        assert list(collection) == [0, 1, BookendCollection.Gap(6), 8, 9], (
            f"got {list(collection)}"
        )
