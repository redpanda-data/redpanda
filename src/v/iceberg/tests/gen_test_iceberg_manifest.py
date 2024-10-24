#!/usr/bin/python

# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import argparse
from pyiceberg.io.fsspec import FsspecFileIO
from pyiceberg.manifest import write_manifest, DataFile, DataFileContent, ManifestEntry
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, ListType, IntegerType, StructType, BooleanType, NestedField, MapType, FloatType

# TODO: support some other schemas.
# Define the nested schema. This matches the one at
# src/v/iceberg/tests/test_schemas.cc
nested_schema = Schema(
    NestedField(field_id=1,
                required=False,
                name="foo",
                field_type=StringType()),
    NestedField(field_id=2,
                required=True,
                name="bar",
                field_type=IntegerType()),
    NestedField(field_id=3,
                required=False,
                name="baz",
                field_type=BooleanType()),
    NestedField(field_id=4,
                required=True,
                name="qux",
                field_type=ListType(element_id=5,
                                    element_required=True,
                                    element_type=StringType())),
    NestedField(field_id=6,
                required=True,
                name="quux",
                field_type=MapType(key_id=7,
                                   key_type=StringType(),
                                   value_id=8,
                                   value_type=MapType(
                                       key_id=9,
                                       key_type=StringType(),
                                       value_id=10,
                                       value_required=True,
                                       value_type=IntegerType()))),
    NestedField(field_id=11,
                required=True,
                name="location",
                field_type=ListType(element_id=12,
                                    element_required=True,
                                    element_type=StructType(
                                        NestedField(field_id=13,
                                                    required=False,
                                                    name="latitude",
                                                    field_type=FloatType()),
                                        NestedField(field_id=14,
                                                    required=False,
                                                    name="longitude",
                                                    field_type=FloatType())))),
    NestedField(field_id=15,
                required=False,
                name="person",
                field_type=StructType(
                    NestedField(field_id=16,
                                name="name",
                                required=False,
                                field_type=StringType()),
                    NestedField(field_id=17,
                                name="age",
                                required=True,
                                field_type=IntegerType()),
                )))


def make_manifest_entries(num_entries: int) -> list[ManifestEntry]:
    manifest_entries: list[ManifestEntry] = []
    for i in range(num_entries):
        data_file = DataFile(
            content=DataFileContent.DATA,
            file_path=f"data/path/file-{i}.parquet",
            file_format='PARQUET',
            partition={},
            record_count=i,
            file_size_in_bytes=i,
            column_sizes={},
            value_counts={},
            null_value_counts={},
            nan_value_counts={},
            lower_bounds={},
            upper_bounds={},
            key_metadata=None,
            split_offsets=[],
            equality_ids=[],
            sort_order_id=i,
        )
        manifest_entry = ManifestEntry(status=0,
                                       snapshot_id=i,
                                       data_sequence_number=i,
                                       file_sequence_number=i,
                                       data_file=data_file)
        manifest_entries.append(manifest_entry)
    return manifest_entries


def main(args):
    # TODO: add once we have support serialization of partition specs.
    spec = PartitionSpec(fields=[])

    file_io = FsspecFileIO(properties={})
    output_file = file_io.new_output(args.out_file)
    with write_manifest(schema=nested_schema,
                        snapshot_id=1,
                        spec=spec,
                        output_file=output_file,
                        format_version=2) as writer:
        for entry in make_manifest_entries(args.num_entries):
            writer.add_entry(entry)
    print(f"Successfully generated manifest with {args.num_entries} entries "
          f"at {args.out_file}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Write an Apache Iceberg manifest file")
    parser.add_argument("-o",
                        "--out-file",
                        type=str,
                        required=True,
                        help="Destination file")
    parser.add_argument(
        "-n",
        "--num-entries",
        type=int,
        default=10,
        help="The number of data files to represent in the manifest")
    args = parser.parse_args()
    main(args)
