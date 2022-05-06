# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import re
import itertools


class Segment:
    def __init__(self, partition, name):
        self.partition = partition
        self.name = name
        self.data_file = None
        self.base_index = None
        self.compaction_index = None

    def add_file(self, fn, ext):
        assert fn
        assert ext
        if ext == ".log":
            self.data_file = fn
        elif ext == ".base_index":
            self.base_index = fn
        elif ext == ".compaction_index":
            self.compaction_index = fn

    def delete_indices(self, allow_fail=False):
        paths = map(lambda fn: os.path.join(self.partition.path, fn),
                    filter(bool, (self.base_index, self.compaction_index)))
        for path in paths:
            self.partition.node.account.remove(path, allow_fail)

    def recovered(self):
        files = map(lambda ext: self.name + ext,
                    (".log", ".base_index", ".compaction_index"))
        paths = map(lambda fn: os.path.join(self.partition.path, fn), files)
        return all(
            map(lambda path: self.partition.node.account.isfile(path), paths))

    def __repr__(self):
        return "{}:{}{}{}".format(self.name, "D" if self.data_file else "d",
                                  "B" if self.base_index else "b",
                                  "C" if self.compaction_index else "c")


class Partition:
    def __init__(self, idx, rev, node, path):
        self.num = idx
        self.rev = rev
        self.node = node
        self.path = path
        self.files = set()
        self.segments = dict()

    def add_files(self, files):
        self.files = set(files)
        for fn in self.files:
            seg, ext = os.path.splitext(fn)
            if not re.match(r"^\d+\-\d+\-v\d+$", seg):
                continue
            if seg not in self.segments:
                self.segments[seg] = Segment(self, seg)
            seg = self.segments[seg]
            seg.add_file(fn, ext)

    def delete_indices(self, allow_fail=False):
        for _, segment in self.segments.items():
            segment.delete_indices(allow_fail)

    def recovered(self):
        n_recovered = sum(1 for s in map(lambda s: s.recovered(), (
            kv[1] for kv in self.segments.items())) if s is True)

        # All but one should have index files: the one that doesn't is
        # the currently open segment (segments don't get indices on disk
        # until they're sealed)
        return n_recovered >= len(self.segments) - 1

    def __repr__(self):
        return "part-{}-{}-{}".format(self.node.name, self.num, self.segments)


class Topic:
    def __init__(self, name, path):
        self.name = name
        self.path = path
        self.partitions = dict()

    def add_partition(self, num, node_id, path):
        (idx, rev) = num.split("_")
        p = Partition(int(idx), int(rev), node_id, path)
        self.partitions[num] = p
        return p

    def __repr__(self):
        return self.name


class Namespace:
    def __init__(self, name, path):
        self.name = name
        self.path = path
        self.topics = dict()

    def add_topic(self, topic, path):
        t = Topic(topic, path)
        self.topics[topic] = t
        return t

    def __repr__(self):
        return self.name


class NodeStorage:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.ns = dict()

    def add_namespace(self, ns, path):
        n = Namespace(ns, path)
        self.ns[ns] = n
        return n

    def partitions(self, ns, topic):
        if ns in self.ns:
            if topic in self.ns[ns].topics:
                parts = self.ns[ns].topics[topic].partitions
                return [p[1] for p in parts.items()]
        return []


class ClusterStorage:
    def __init__(self):
        self.nodes = []

    def add_node(self, node_storage):
        self.nodes.append(node_storage)

    def partitions(self, ns, topic):
        return itertools.chain(
            *map(lambda n: n.partitions(ns, topic), self.nodes))
