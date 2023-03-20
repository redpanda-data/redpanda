# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from functools import reduce

from kafka import TopicPartition
from enum import Enum


class BasicKafkaRecord:
    def __init__(self,
                 topic=None,
                 partition=None,
                 key=None,
                 value=None,
                 offset=0):
        self.topic = topic
        self.partition = partition
        self.key = key
        self.value = value
        self.offset = offset

    def to_tuple(self):
        return (self.topic, self.partition, self.key, self.value, self.offset)

    def to_tuple_without_offset(self):
        return (self.topic, self.partition, self.key, self.value)

    def hash_without_offset(self):
        return hash(self.to_tuple_without_offset())

    def __eq__(self, o):
        return self.to_tuple() == o.to_tuple()

    def __hash__(self):
        return hash(self.to_tuple())


class TopicsResultSet:
    """
    A high level wrapper around a result set from a read of a set of kafka
    topic partitions.
    """
    def __init__(self, rset=None):
        # Key: TopicPartition(topic='XYZ', partition=X)
        # Value: list<BasicKafkaRecord>
        self.rset = rset or {}

    def num_records(self):
        return reduce(lambda acc, kv: acc + len(kv[1]), self.rset.items(), 0)

    def num_records_for_topic(self, topic):
        new_trs = self.filter(lambda x: x.topic == topic)
        return new_trs.num_records()

    def deduplicate(self):
        """
        Returns a new set without any duplicates
        """
        def ddup(records):
            d = {r.hash_without_offset(): r for r in records}
            return list(sorted(d.values(), key=lambda x: x.offset))

        return TopicsResultSet(
            dict([(k, ddup(v)) for k, v in self.rset.items()]))

    def filter(self, predicate):
        """
        Returns a set containing the keys & values passing 'predicate'
        predicate: Lambda returning boolean
        """
        new_rset = dict([(k, v) for k, v in self.rset.items()
                         if predicate(k) is True])
        return TopicsResultSet(new_rset)

    def map(self, lambda_fn):
        """
        Modify each BasicKafkaRecord across all partitions
        """
        new_rset = dict([(k, [lambda_fn(x) for x in v])
                         for k, v in self.rset.items()])
        return TopicsResultSet(new_rset)

    def append(self, r):
        def to_basic_records(records):
            return list([
                BasicKafkaRecord(x.topic, x.partition, x.key, x.value,
                                 x.offset) for x in records
            ])

        r = dict([(kv[0], to_basic_records(kv[1])) for kv in r.items()])
        for tp, records in r.items():
            rs = self.rset.get(tp)
            if rs is None:
                self.rset[tp] = records
            else:
                rs += records
