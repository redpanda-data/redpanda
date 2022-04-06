# Copyright 2021 Redpanda Data, Inc.
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
    Performs the same checks as 'materialized_result_set_compare' but removes
    duplicate records from the 'materialized_set', as they are expected to exist in the
    case of failures.

    'oset' remains unchanged because duplicates are not expected since records were
    produced with idempotency enabled
    """
    def strip_offset(r):
        return BasicKafkaRecord(topic=r.topic,
                                partition=r.partition,
                                key=r.key,
                                value=r.value,
                                offset=0)

    deduplicated = materialized_set.deduplicate()
    return materialized_result_set_compare(oset.map(strip_offset),
                                           deduplicated.map(strip_offset))


def group_fan_in_verifier(topics, input_results, output_results):
    """
    A materialized topic will contain Nx the input records where N is the number
    of scripts deployed. Since it cannot be determined which script came from what
    coprocessor (in order to split into distinct result sets and compare) just compare
    totals for now
    """
    def compare(topic):
        iis = input_results.filter(lambda x: x.topic == topic)
        oos = output_results.filter(
            lambda x: get_source_topic(x.topic) == topic)
        return iis.num_records() == oos.num_records()

    return all(compare(topic) for topic in topics)
