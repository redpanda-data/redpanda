#!/usr/bin/env python3
#
# Examples:
# Say you have a segment file like: /var/lib/redpanda/data/kafka/test-si3/0_105/0-1-v1.log
# run the command like below:
#
# $ ./cloud_storage_name_mapper.py kafka/test-si3/0_105/0-1-v1.log
#
# and the expected output looks like:
#
# manifest path:       00000000/meta/kafka/test-si3/0_105/manifest.json
# topic manifest path: 60000000/meta/kafka/test-si3/topic_manifest.json
# segment path:        924dec9f/kafka/test-si3/0_105/0-1-v1.log.*
#

import abc
import sys
from typing import Optional

import xxhash


def main():
    import argparse

    def generate_options():
        parser = argparse.ArgumentParser(
            description="Redpanda Tiered Storage Remote Write Naming Mapper"
        )
        parser.add_argument(
            "--cluster-uuid",
            type=str,
            required=True,
            help="Cluster UUID which created the topic. It might be different from the current "
            "cluster UUID if cluster was recovered or topic was mounted from a different cluster."
            'For legacy topics which didn\'t have cluster UUID, use value "0".',
        )
        parser.add_argument(
            "path", type=str, help="Path to the file, starting with 'kafka/'"
        )

        return parser

    parser = generate_options()
    options, _ = parser.parse_known_args()
    if not options.path.startswith("kafka"):
        print("The path should start with 'kafka'")
        sys.exit(1)

    p = options.path.split("/")

    if options.cluster_uuid != "0":
        path_utils = LabeledPathUtils(options.cluster_uuid)
    else:
        path_utils = PrefixedPathUtils()

    opt_initial_revision: Optional[int] = None
    partition_id, revision = p[2].split("_")
    if partition_id == "0":
        # We can infer the initial revision for the topic manifest.
        opt_initial_revision = int(revision)

    print(
        f"manifest path:       {path_utils.partition_manifest_path(p[0], p[1], p[2])}"
    )
    print(
        f"topic manifest path: {path_utils.topic_manifest_path(p[0], p[1], opt_initial_revision)}"
    )
    print(f"segment path:        {path_utils.segment_path(options.path)}")


class PathUtils(abc.ABC):
    @abc.abstractmethod
    def topic_manifest_path(
        self, namespace: str, topic: str, initial_revision: int
    ) -> str: ...

    @abc.abstractmethod
    def partition_manifest_path(
        self, namespace: str, topic: str, partition_with_rev: str
    ) -> str: ...

    @abc.abstractmethod
    def segment_path(self, segment_path_suffix: str) -> str: ...


class PrefixedPathUtils(PathUtils):
    def topic_manifest_path(
        self, namespace: str, topic: str, initial_revision: Optional[int]
    ) -> str:
        x = xxhash.xxh32()
        path = namespace + "/" + topic
        x.update(path.encode("ascii"))
        topic_manifest_hash = x.hexdigest()[0] + "0000000"
        return f"{topic_manifest_hash}/meta/{namespace}/{topic}/topic_manifest.{{json,bin}}"

    def partition_manifest_path(
        self, namespace: str, topic: str, partition_with_rev: str
    ) -> str:
        x = xxhash.xxh32()
        path = namespace + "/" + topic + "/" + partition_with_rev
        x.update(path.encode("ascii"))
        manifest_hash = x.hexdigest()[0] + "0000000"
        return f"{manifest_hash}/meta/{namespace}/{topic}/{partition_with_rev}/manifest.{{json,bin}}"

    def segment_path(self, segment_path_suffix: str) -> str:
        x = xxhash.xxh32()
        x.update(segment_path_suffix.encode("ascii"))
        hash = x.hexdigest()
        return f"{hash}/{segment_path_suffix}.*"


class LabeledPathUtils(PathUtils):
    def __init__(self, cluster_uuid: str):
        super().__init__()
        self.cluster_uuid = cluster_uuid

    def topic_manifest_path(
        self, namespace: str, topic: str, initial_revision: Optional[int]
    ) -> str:
        if initial_revision is None:
            return "??? (can be inferred only for a path with partition id 0)"
        return f"meta/{namespace}/{topic}/{self.cluster_uuid}/{initial_revision}/topic_manifest.bin"

    def partition_manifest_path(
        self, namespace: str, topic: str, partition_with_rev: str
    ) -> str:
        return f"{self.cluster_uuid}/meta/{namespace}/{topic}/{partition_with_rev}/manifest.bin"

    def segment_path(self, segment_path_suffix: str) -> str:
        return f"{self.cluster_uuid}/{segment_path_suffix}.*"


if __name__ == "__main__":
    main()
