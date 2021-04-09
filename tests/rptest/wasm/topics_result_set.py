# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.wasm.topic import get_source_topic, is_materialized_topic

from functools import reduce

from kafka import TopicPartition


class BasicKafkaRecord:
    def __init__(self, topic=None, partition=None, key=None, value=None):
        self.topic = topic
        self.partition = partition
        self.key = key
        self.value = value

    def __eq__(self, o):
        return self.topic == o.topic and self.partition == o.partition \
            and self.key == o.key and self.value == o.value


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

    def filter(self, predicate):
        """
        Returns a set containing the keys & values passing 'predicate'
        predicate: Lambda returning boolean
        """
        new_rset = dict([(k, v) for k, v in self.rset.items()
                         if predicate(k) is True])
        return TopicsResultSet(new_rset)

    def append(self, r):
        def filter_control_record(records):
            # Unfortunately the kafka-python API abstracts away the notion of a
            # record batch, leaving us unable to check the control attribute
            # within the record_batch header.
            # https://github.com/dpkp/kafka-python/issues/1853
            if len(records) == 0 or records[0].offset != 0:
                return records
            r = records[0]
            if r.checksum is None and r.value is not None and \
               r.headers == [] and r.serialized_key_size == 4:
                return records[1:]
            return records

        def to_basic_records(records):
            return list([
                BasicKafkaRecord(x.topic, x.partition, x.key, x.value)
                for x in records
            ])

        # Filter out control records and unwanted data
        r = dict([(kv[0], filter_control_record(kv[1])) for kv in r.items()])
        r = dict([(kv[0], to_basic_records(kv[1])) for kv in r.items()])
        for tp, records in r.items():
            rs = self.rset.get(tp)
            if rs is None:
                self.rset[tp] = records
            else:
                rs += records


def materialized_result_set_compare(oset, materialized_set):
    """
    Compares the actual data (keys, values) between two result sets. 'oset'
    must contain normal topics, while 'materaizlied_set' contains
    materialized_topics
    """
    if len(oset.rset.keys()) != len(materialized_set.rset.keys()):
        return False
    if oset.num_records() != materialized_set.num_records():
        return False

    def strip_topic(bkr):
        bkr.topic = None
        return bkr

    for tp, records in materialized_set.rset.items():
        if not is_materialized_topic(tp.topic):
            raise Exception(
                "'materialized_set' must contain only materialized topics")
        input_data = oset.rset.get(
            TopicPartition(topic=get_source_topic(tp.topic),
                           partition=tp.partition))
        if input_data is None:
            return False
        mat_records = [strip_topic(x) for x in records]
        src_recs = [strip_topic(x) for x in input_data]
        if mat_records != src_recs:
            return False
    return True
