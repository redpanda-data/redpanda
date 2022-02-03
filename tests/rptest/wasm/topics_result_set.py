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


CmpErr = Enum('CmpErr', 'Success NonEqKeys NonEqRecords CmpFailed DataInvalid')


def cmp_err_to_str(cmp_err):
    if cmp_err == CmpErr.NonEqKeys:
        return 'Mismatch in number of keys'
    elif cmp_err == CmpErr.NonEqRecords:
        return 'Mistmatch in number of records'
    elif cmp_err == CmpErr.CmpFailed:
        return 'Mismatch in expected topic data'
    elif cmp_err == CmpErr.DataInvalid:
        return 'Unexpected non-materialized record in output set'
    else:
        raise Exception('Unimplemented enum case')


def _materialized_topic_set_compare(a, b, comparator):
    for tp, records in b.rset.items():
        if not is_materialized_topic(tp.topic):
            return CmpErr.DataInvalid
        input_data = a.rset.get(
            TopicPartition(topic=get_source_topic(tp.topic),
                           partition=tp.partition))
        if input_data is None or not comparator(input_data, records):
            return CmpErr.CmpFailed
    return CmpErr.Success


def materialized_result_set_compare(oset, materialized_set):
    """
    Compares the actual data (keys, values) between two result sets. 'oset'
    must contain normal topics, while 'materaizlied_set' contains
    materialized_topics
    """
    if len(oset.rset.keys()) != len(materialized_set.rset.keys()):
        return CmpErr.NonEqKeys
    if oset.num_records() != materialized_set.num_records():
        return CmpErr.NonEqRecords

    def strip_topic(bkr):
        bkr.topic = None
        return bkr

    def strict_cmp(input_data, records):
        """ 1:1 direct comparison between all records across ntp """
        mat_records = [strip_topic(x) for x in records]
        src_recs = [strip_topic(x) for x in input_data]
        return mat_records == src_recs

    return _materialized_topic_set_compare(oset, materialized_set, strict_cmp)


def materialized_at_least_once_compare(oset, materialized_set):
    """
    Within the wasm framework since offsets are periodically written to
    disk it may be the case that duplicate records are observed within the
    materialized topics.
    """
    def cmp_duplicates_allowed(input_data, records):
        """ Ensures all keys from input exist in output, and values match,
        also ensures missing keys from either set in the others set.
        """
        input_map = dict((x.key, x.value) for x in records)
        for record in records:
            input_value = input_map.get(record.key)
            if input_value is None or record.value != input_value:
                return False

        # one last verification, ensure that there are no extra keys in input
        # space that is missing from the output space of results
        unique_keys = set(x.key for x in records)
        return all(x in unique_keys for x in input_map)

    return _materialized_topic_set_compare(oset, materialized_set,
                                           cmp_duplicates_allowed)
